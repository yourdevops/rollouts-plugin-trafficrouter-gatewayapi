# Concurrency Analysis: ConfigMap and Header Route Operations

## Executive Summary

**CRITICAL ISSUES FOUND**: The plugin has significant race conditions that can lead to lost writes, data corruption, and inconsistent state between the ConfigMap and Gateway API routes. The existing mutex protection is **ineffective** because it's created per-request rather than shared across concurrent plugin invocations.

## Architecture Context

The plugin runs as a long-running RPC server (HashiCorp go-plugin) with:
- A single shared `RpcPlugin` instance serving all requests
- Multiple concurrent calls possible from different Rollouts or simultaneous operations
- `GatewayAPITrafficRouting` struct created fresh for each plugin method call
- `ConfigMapRWMutex` in `GatewayAPITrafficRouting` is **per-call, not shared**

## Critical Race Conditions

### 1. **Ineffective Mutex Protection** ⚠️ CRITICAL

**Location**: `pkg/plugin/types.go:84`, `pkg/plugin/plugin.go:120-149`

**Problem**: The `ConfigMapRWMutex` provides NO protection across concurrent plugin invocations because:
- Each call to `SetHeaderRoute`/`RemoveManagedRoutes` creates a new `GatewayAPITrafficRouting` via `getGatewayAPIConfigWithDiscovery()` (plugin.go:69, 113, 163)
- Each new config gets a fresh `sync.RWMutex` (types.go:84)
- Two concurrent calls have different mutex instances and thus don't synchronize

**Impact**: All concurrency issues below are unprotected.

**Evidence**:
```go
// plugin.go:226-244
func getGatewayAPITrafficRoutingConfig(rollout *v1alpha1.Rollout) (*GatewayAPITrafficRouting, error) {
    gatewayAPIConfig := &GatewayAPITrafficRouting{  // NEW instance per call
        ConfigMap: defaults.ConfigMap,
    }
    // ConfigMapRWMutex is zero-initialized here, independent of other calls
```

### 2. **Lost Writes to Gateway API Routes** ⚠️ CRITICAL

**Location**: `pkg/plugin/httproute.go:106-192`, `pkg/plugin/grpcroute.go:102-193`

**Scenario**: Two concurrent operations on the same HTTPRoute/GRPCRoute:

```
Time  Thread A (SetHeaderRoute "canary-v1")    Thread B (SetHeaderRoute "canary-v2")
----  ------------------------------------    ------------------------------------
T1    Get HTTPRoute (resourceVersion: 100)
T2                                            Get HTTPRoute (resourceVersion: 100)
T3    Add rule #5 for "canary-v1"
T4                                            Add rule #5 for "canary-v2"
T5    Update HTTPRoute → v101 (rule #5 = v1)
T6                                            Update HTTPRoute → v102 (overwrites v1!)
```

**Result**: Thread A's header route is lost. ConfigMap stores index for "canary-v1" but the route rule doesn't exist.

**Root Cause**:
- Both threads read the same resourceVersion
- Both modify in memory independently
- Second update overwrites first (Kubernetes accepts because resourceVersion matches previous read)
- No optimistic locking checks or retry logic

**Files**:
- `httproute.go:106` - Get route
- `httproute.go:179-181` - Append rule to list
- `httproute.go:192` - Update route (can overwrite concurrent changes)

### 3. **Lost Writes to ConfigMap** ⚠️ HIGH

**Location**: `internal/utils/common.go:66-78`

**Scenario**: Similar to route updates:

```
Time  Thread A                        Thread B
----  ------------------------------  ------------------------------
T1    Read ConfigMap (rv: 50)
T2                                    Read ConfigMap (rv: 50)
T3    Add index for "canary-v1"
T4                                    Add index for "canary-v2"
T5    Update ConfigMap → v51
T6                                    Update ConfigMap → v52 or CONFLICT
```

**Two possible outcomes**:
1. **Conflict Error**: Thread B's update fails → transaction rollback kicks in → removes route rule for "canary-v2"
2. **Overwrite**: Thread B's update succeeds → Thread A's ConfigMap entry lost → "canary-v1" becomes orphaned (can't be cleaned up)

**Mitigation Attempt**: Transaction rollback (utils/common.go:80-97) provides cleanup on error but:
- Rollback itself can fail if route was modified by another thread
- No retry logic to handle conflicts
- Doesn't prevent overwrites if both updates succeed

### 4. **SetWeight / SetHeaderRoute Race** ⚠️ HIGH

**Location**: `pkg/plugin/plugin.go:68` (SetWeight), `plugin.go:112` (SetHeaderRoute)

**Problem**: `SetWeight` does NOT acquire any lock, yet modifies the same HTTPRoute/GRPCRoute resources as `SetHeaderRoute`.

**Scenario**:
```
Time  SetHeaderRoute                          SetWeight
----  --------------------------------------  ---------------------------------
T1    Lock mutex (ineffective per issue #1)
T2    Get HTTPRoute (v100, rules=[A, B])
T3                                            Get HTTPRoute (v100, rules=[A, B])
T4    Add header rule C → rules=[A, B, C]
T5                                            Change weights in A, B
T6    Update HTTPRoute → v101
T7                                            Update HTTPRoute → v102 (lost C!)
```

**Result**: Header route added by `SetHeaderRoute` is lost, but ConfigMap still references it.

**Evidence**: `plugin.go:68-110` shows SetWeight iterating through routes without any locking.

### 5. **Duplicate Header Routes** ⚠️ MEDIUM

**Location**: `pkg/plugin/httproute.go:179`, `grpcroute.go:180`

**Problem**: No deduplication check before adding header routes.

**Scenario**: If `SetHeaderRoute` is called twice for the same managed route (e.g., due to retry, crash recovery, or bug):
1. First call: Adds rule at index N, stores "routeName → N" in ConfigMap
2. Second call: Adds ANOTHER rule at index N+1, overwrites ConfigMap entry "routeName → N+1"
3. Result: Rule at index N is orphaned (can never be removed)

**Root Cause**: Code assumes Argo Rollouts always calls `RemoveManagedRoutes` before re-adding, but:
- Crashes between route update and ConfigMap update could break this
- Bugs in Argo Rollouts could break this
- No defensive check in plugin code

**Location**: `httproute.go:179` - Always appends, never checks if already exists

### 6. **Index Corruption from External Modifications** ⚠️ MEDIUM

**Location**: `pkg/plugin/httproute.go:399-431`, `grpcroute.go:400-432`

**Problem**: ConfigMap stores integer indices of managed rules. If routes are modified outside the plugin (manual edits, other controllers), indices become stale.

**Scenario**:
```
Initial: HTTPRoute rules = [0: stable, 1: canary, 2: header-route-A]
ConfigMap: {"route-A": {"my-http-route": 2}}

Human operator adds new rule at position 0
Result: HTTPRoute rules = [0: new-rule, 1: stable, 2: canary, 3: header-route-A]
ConfigMap still says: {"route-A": {"my-http-route": 2}}  ← Now points to canary!

Plugin calls RemoveManagedRoutes for route-A
Action: Deletes rule at index 2 (canary service) instead of header-route-A
```

**Partial Mitigation**: Code detects out-of-bounds indices (httproute.go:409-418):
```go
if managedRouteIndex < 0 || managedRouteIndex >= len(routeRuleList) {
    // Clean up and continue gracefully
}
```

**But**: Doesn't detect in-bounds but wrong indices, which is the common case.

## Data Structure Access Patterns

### ConfigMap Access (per operation):
1. **Read**: `utils.GetOrCreateConfigMap()` → `utils.GetConfigMapData()`
2. **Modify**: In-memory manipulation of `ManagedRouteMap`
3. **Write**: `utils.UpdateConfigMapData()` → Kubernetes Update API

**Gaps**:
- No optimistic concurrency control beyond Kubernetes' default resourceVersion checking
- No retry on conflict
- Read-modify-write is not atomic

### Gateway API Route Access (per operation):
1. **Read**: `httpRouteClient.Get()`
2. **Modify**: In-memory manipulation of route rules
3. **Write**: `httpRouteClient.Update()`

**Gaps**: Same as ConfigMap

## Transaction Mechanism Analysis

**Location**: `internal/utils/common.go:80-97`

**What it provides**: Sequential execution with rollback on failure
- Executes Task.Action() for each task in order
- On error, calls ReverseAction() on all previous tasks
- Returns error if any Action or ReverseAction fails

**What it does NOT provide**:
- **Atomicity**: Gap between route update and ConfigMap update (can crash between them)
- **Isolation**: No protection from concurrent modifications
- **Durability**: Rollback can fail if resource was modified by another process
- **Retry**: No retry on conflicts

## Routes Requiring Protection

### Resources Modified Concurrently:
1. **ConfigMap** (`argo-gatewayapi-configmap` by default):
   - Read/Write in: `setHTTPHeaderRoute`, `removeHTTPManagedRoutes`, `setGRPCHeaderRoute`, `removeGRPCManagedRoutes`
   - Stores: `{"httpManagedRoutes": {...}, "grpcManagedRoutes": {...}}`

2. **HTTPRoute** resources:
   - Read/Write in: `setHTTPRouteWeight`, `setHTTPHeaderRoute`, `removeHTTPManagedRoutes`
   - Modified by: Weight changes, header route additions/removals, in-progress labels

3. **GRPCRoute** resources:
   - Read/Write in: `setGRPCRouteWeight`, `setGRPCHeaderRoute`, `removeGRPCManagedRoutes`
   - Modified by: Weight changes, header route additions/removals, in-progress labels

4. **TCPRoute** resources:
   - Read/Write in: `setTCPRouteWeight`
   - Modified by: Weight changes, in-progress labels

5. **TLSRoute** resources:
   - Read/Write in: `setTLSRouteWeight`
   - Modified by: Weight changes, in-progress labels

## Concurrency Scenarios

### When Multiple Rollouts Exist:
- Each Rollout can trigger plugin calls independently
- Different Rollouts typically manage different routes → Low conflict risk
- **BUT**: If multiple Rollouts use label selectors that overlap, they could target the same routes → HIGH CONFLICT RISK

### When Single Rollout Performs Multiple Operations:
- Argo Rollouts typically calls methods sequentially
- **BUT**: If canary step changes rapidly (e.g., quick progression through steps), operations could overlap
- Experiments (A/B tests) might trigger multiple simultaneous operations

### When Same Route is Managed by Multiple Mechanisms:
- One Rollout uses explicit route name: `httpRoute: "my-route"`
- Another uses label selector: `httpRouteSelector: {app: myapp}`
- Both discover same route → GUARANTEED CONFLICT

## Recommendations

### Must Fix (Critical Priority):

1. **Shared Mutex**: Move `ConfigMapRWMutex` to `RpcPlugin` struct (singleton) instead of per-call `GatewayAPITrafficRouting`

2. **Optimistic Concurrency**: Implement retry loop on Kubernetes conflict errors for both ConfigMap and route updates

3. **Lock SetWeight**: Acquire mutex in `SetWeight()` to protect route resources

### Should Fix (High Priority):

4. **Deduplication**: Check if managed route already exists before adding duplicate header rules

5. **Retry Transaction**: Extend `DoTransaction` to retry on conflict errors with exponential backoff

6. **Fingerprinting**: Store route rule fingerprint (hash) instead of index in ConfigMap to detect external modifications

### Nice to Have (Medium Priority):

7. **Read Lock**: Use RLock for read-only operations if implemented properly

8. **Namespace Routes**: Lock per route name rather than globally to allow concurrent operations on different routes

9. **Metrics**: Add metrics for lock contention, conflicts, retries, and transaction failures

## Conclusion

The current locking mechanism is **fundamentally broken** because the mutex is not shared across concurrent calls. Even after fixing the mutex, the read-modify-write pattern on Kubernetes resources needs optimistic concurrency control (retry on conflict) to handle concurrent modifications safely.

**Priority**: This should be fixed before using the plugin in production with multiple Rollouts or label-based route discovery.
