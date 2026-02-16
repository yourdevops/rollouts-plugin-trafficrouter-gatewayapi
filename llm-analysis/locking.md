# Kubernetes Locking Patterns: Industry Analysis

## Executive Summary

**Nobody uses global/static mutex for Kubernetes API updates** - that's an anti-pattern in distributed systems. The Kubernetes ecosystem has **strongly converged on optimistic concurrency control** as the standard pattern for handling concurrent resource updates.

## The Standard Pattern: Optimistic Concurrency Control

### How Kubernetes Resource Versions Work

Kubernetes employs **optimistic concurrency control** using the `resourceVersion` field:
- `resourceVersion` is a monotonically increasing string identifier maintained by etcd
- When updating a resource, if the resourceVersion changed since you fetched it, the API server returns `409 StatusConflict`
- Clients must fetch the resource, modify it, and pass back the resourceVersion when updating

**Pattern:**
1. Get resource (includes resourceVersion)
2. Modify resource in memory
3. Update with resourceVersion
4. If conflict (409), refetch and retry

**Why this works:**
- Scalable - No upfront locking overhead
- Works in distributed environments
- Native to Kubernetes API design
- Simple retries handle conflicts

---

## Pattern 1: Direct Client Usage with retry.RetryOnConflict

### Standard Pattern from client-go

**Import:**
```go
import "k8s.io/client-go/util/retry"
```

**Complete Example:**
```go
retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
    // 1. MUST fetch inside the retry function to get latest version
    result, getErr := client.Get(context.TODO(), name, metav1.GetOptions{})
    if getErr != nil {
        return fmt.Errorf("Failed to get latest version: %v", getErr)
    }

    // 2. Modify the resource
    result.Spec.Replicas = ptr.To[int32](1)

    // 3. Update and return unmodified error
    _, updateErr := client.Update(context.TODO(), result, metav1.UpdateOptions{})
    return updateErr
})

if retryErr != nil {
    panic(fmt.Errorf("Update failed: %v", retryErr))
}
```

**Critical Points:**
- ✅ **Always fetch inside the retry function** - essential to get latest resourceVersion after conflicts
- ✅ Return unmodified error from update operation
- ✅ Don't wrap or handle the conflict error - let retry.RetryOnConflict handle it

### Backoff Options

**retry.DefaultRetry** - For multiple clients changing the same resource:
- 5 steps, 10ms duration, 1.0 factor, 0.1 jitter
- Use when: Multiple external clients/users updating resources

**retry.DefaultBackoff** - For controller-managed resources:
- 4 steps, 10ms duration, 5.0 factor, 0.1 jitter
- Use when: Controllers managing resources under active control

### Example: Updating ConfigMap and Route Together

```go
err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
    // Fetch ConfigMap
    configMap, err := clientset.CoreV1().ConfigMaps(ns).Get(ctx, cmName, metav1.GetOptions{})
    if err != nil {
        return err
    }

    // Fetch Route
    httpRoute, err := httpRouteClient.Get(ctx, routeName, metav1.GetOptions{})
    if err != nil {
        return err
    }

    // Modify both resources
    managedRouteMap := make(map[string]interface{})
    json.Unmarshal([]byte(configMap.Data["httpManagedRoutes"]), &managedRouteMap)
    managedRouteMap["canary-v1"] = map[string]int{"my-route": 5}
    updatedJSON, _ := json.Marshal(managedRouteMap)
    configMap.Data["httpManagedRoutes"] = string(updatedJSON)

    httpRoute.Spec.Rules = append(httpRoute.Spec.Rules, newHeaderRule)

    // Update both (if either conflicts, entire function retries)
    if _, err := clientset.CoreV1().ConfigMaps(ns).Update(ctx, configMap, metav1.UpdateOptions{}); err != nil {
        return err
    }
    if _, err := httpRouteClient.Update(ctx, httpRoute, metav1.UpdateOptions{}); err != nil {
        return err
    }

    return nil
})
```

**Note:** If either update fails, the entire function retries from the beginning, refetching both resources.

---

## Pattern 2: Controller Pattern (Work Queue)

### Critical Best Practice: DON'T Use RetryOnConflict in Controllers

**From controller-runtime documentation:**

> "Controllers should generally not use RetryOnConflict-semantics. Instead, controllers should abort their current reconciliation run and let the queue handle the conflict error with exponential backoff, because a conflict error indicates that the controller has operated on stale data and might have made wrong decisions earlier on in the reconciliation."

### Why Not RetryOnConflict?

**Reason:** A conflict error indicates you operated on stale cache data and may have made wrong decisions:
- Your reconciliation logic was based on outdated state
- Retrying the update without re-evaluating business logic is incorrect
- Should re-run entire reconcile loop with fresh data

### Controller Pattern

```go
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // Fetch object from cache
    obj := &MyType{}
    if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    // Business logic based on current state
    if obj.Status.Phase != "Running" {
        obj.Status.Phase = "Running"
        obj.Status.Message = "Processing"
    }

    // Just return the error - work queue handles retry with backoff
    if err := r.Status().Update(ctx, obj); err != nil {
        return ctrl.Result{}, err  // Don't wrap, don't retry
    }

    return ctrl.Result{}, nil
}
```

**Key Points:**
- ✅ Return errors immediately
- ✅ Let work queue handle retries with exponential backoff
- ✅ Trust the cache will be updated before next reconcile
- ✅ Re-evaluate business logic on each reconcile attempt

### Work Queue Guarantees

**Critical guarantee:** A reconciler will **never be called concurrently for the same object**.

```go
// Configure concurrency for DIFFERENT objects
&ctrl.Options{
    MaxConcurrentReconciles: 5,  // Process different objects in parallel only
}
```

**Built-in features:**
- Deduplication (multiple adds of same key = single reconcile)
- Exponential backoff on errors
- Rate limiting per-key
- Ordering guarantees per-key

**Complete Work Queue Example:**
```go
// 1. Create rate-limited queue
queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

// 2. Add items via informer
AddFunc: func(obj interface{}) {
    key, err := cache.MetaNamespaceKeyFunc(obj)
    if err == nil {
        queue.Add(key)
    }
}

// 3. Process items
func processNextItem() bool {
    key, quit := queue.Get()
    if quit {
        return false
    }
    defer queue.Done(key)

    err := syncHandler(key.(string))
    handleErr(err, key)
    return true
}

// 4. Handle errors with retry logic
func handleErr(err error, key interface{}) {
    if err == nil {
        queue.Forget(key)  // Success - clear error history
        return
    }

    if queue.NumRequeues(key) < 5 {
        queue.AddRateLimited(key)  // Retry with exponential backoff
        return
    }

    queue.Forget(key)  // Max retries exceeded - give up
}
```

---

## Pattern 3: Patch Operations (Preferred When Possible)

### When to Use Patch

**Use Patch when:**
- ✅ You're the exclusive owner of specific fields
- ✅ Fields are independent of concurrent changes
- ✅ Want better performance and conflict resistance

**Use Update when:**
- ✅ Need to modify the entire object
- ✅ Fields are interdependent
- ✅ Working with full object state

### Patch Types

**MergeFrom - Replaces entire lists:**
```go
patch := client.MergeFrom(obj.DeepCopy())
obj.Status.Conditions = []Condition{{Type: "Ready", Status: "True"}}
err := client.Patch(ctx, obj, patch)
// No retry needed - highly conflict resistant!
```

**StrategicMergeFrom - Merges lists using patchStrategy:**
```go
patch := client.StrategicMergeFrom(obj.DeepCopy())
obj.Spec.Containers[0].Image = "myimage:v2"
err := client.Patch(ctx, obj, patch)
```

### ConfigMap Patch Example

```go
// If you have exclusive ownership of specific ConfigMap keys
configMap := &corev1.ConfigMap{}
err := client.Get(ctx, types.NamespacedName{Name: "my-cm", Namespace: "default"}, configMap)

patch := client.MergeFrom(configMap.DeepCopy())
configMap.Data["httpManagedRoutes"] = newJSONValue
err = client.Patch(ctx, configMap, patch)
// No retry loop needed - patch is conflict-resistant
```

**Benefits:**
- More performant (avoids conflicts and retries)
- Only conflicts if your specific fields were modified
- Simpler code (no retry logic needed)

---

## Real-World Examples from Popular Projects

### Argo Rollouts

**Issue History:**
- Had ReplicaSet conflict issues ([#3218](https://github.com/argoproj/argo-rollouts/issues/3218))
- Status update conflicts ([#1904](https://github.com/argoproj/argo-rollouts/issues/1904))

**Solution:**
- Implemented fallback to patch operations ([#3559](https://github.com/argoproj/argo-rollouts/pull/3559))
- Uses work queue pattern with rate-limited retries
- Direct update approach with error propagation:

```go
updatedRS, err := c.kubeclientset.AppsV1().ReplicaSets(rs.Namespace).
    Update(ctx, rs, metav1.UpdateOptions{})
if err != nil {
    return nil, fmt.Errorf("error updating ReplicaSet %s: %w", rs.Name, err)
}
```

**Retry Strategy:**
- Errors trigger re-queueing via work queue's rate limiter
- Timing-related errors use explicit delays:
```go
c.enqueueRolloutAfter(roCtx.rollout, 20*time.Second)
```

### Istio

**Issue:** Operator doesn't re-reconcile on conflicts ([#38591](https://github.com/istio/istio/issues/38591))
- When encountering temporary conflicts (409), operator didn't return error
- Prevented subsequent reconciliation attempts
- **Lesson:** Always propagate conflict errors to work queue

### Flux

**Challenges at Scale:**
- Status subresource owned by multiple managers causes conflicts
- Kustomize-controller experiences conflicts at scale ([#3689](https://github.com/fluxcd/flux2/issues/3689))
- **Solution:** Uses field managers (e.g., `flux-client-side-apply`) to coordinate
- Image-automation-controller conflicts ([#131](https://github.com/fluxcd/image-automation-controller/issues/131))

---

## When Mutex IS Used (Rarely)

### Valid Use Case: In-Memory Coordination

**Mutex is appropriate for:**
- ✅ Protecting shared in-memory data structures within a single process
- ✅ Coordinating goroutines within your controller
- ✅ Business logic coordination (e.g., Argo Workflows synchronization)

**Example from Argo Workflows:**
- Uses local locks to coordinate workflow execution (business logic)
- Workflows can only acquire a lock if at front of queue
- **Still uses Kubernetes optimistic locking for API updates**
- Mutex protects shared Go data structures, NOT API calls

### Anti-Pattern: Mutex for API Updates

**❌ Don't use mutex to serialize Kubernetes API calls:**
- Won't work across multiple pods/processes
- Defeats purpose of Kubernetes' scalable design
- Creates single point of contention
- Doesn't survive process restarts

**✅ If you need single-writer guarantee:**
- Use **leader election** for multi-replica controllers
- Let only one replica reconcile resources
- Other replicas stand by ready to take over

---

## Leader Election Pattern

### When to Use

Leader election is for **multi-replica controllers** to ensure only one replica actively reconciles at a time. This is **orthogonal to conflict handling** - you still need optimistic locking even with leader election.

### Example from client-go

```go
import (
    "k8s.io/client-go/tools/leaderelection"
    "k8s.io/client-go/tools/leaderelection/resourcelock"
)

lock := &resourcelock.LeaseLock{
    LeaseMeta: metav1.ObjectMeta{
        Name:      "my-lease",
        Namespace: "default",
    },
    Client: clientset.CoordinationV1(),
    LockConfig: resourcelock.ResourceLockConfig{
        Identity: id,  // Unique per replica
    },
}

leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
    Lock:          lock,
    LeaseDuration: 15 * time.Second,
    RenewDeadline: 10 * time.Second,
    RetryPeriod:   2 * time.Second,
    Callbacks: leaderelection.LeaderCallbacks{
        OnStartedLeading: func(ctx context.Context) {
            // Start controller work
        },
        OnStoppedLeading: func() {
            // Stop controller work (lost leadership)
        },
    },
})
```

**Uses Kubernetes Leases** (`coordination.k8s.io/v1`) for distributed coordination.

---

## Recommendations for the Gateway API Plugin

### Option 1: Use retry.RetryOnConflict (Recommended for Plugin)

Since this is an RPC plugin (not a controller), use direct client pattern:

```go
import "k8s.io/client-go/util/retry"

func (r *RpcPlugin) setHTTPHeaderRoute(...) pluginTypes.RpcError {
    err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
        // Fetch ConfigMap
        configMap, err := utils.GetOrCreateConfigMap(...)
        if err != nil {
            return err
        }

        // Fetch HTTPRoute
        httpRoute, err := httpRouteClient.Get(ctx, httpRouteName, metav1.GetOptions{})
        if err != nil {
            return err
        }

        // Modify both resources
        managedRouteMap := make(ManagedRouteMap)
        utils.GetConfigMapData(configMap, HTTPConfigMapKey, &managedRouteMap)

        // Add header route rule
        httpRoute.Spec.Rules = append(httpRoute.Spec.Rules, newHeaderRule)
        managedRouteMap[headerRouting.Name][httpRouteName] = len(httpRoute.Spec.Rules) - 1

        // Update HTTPRoute first
        _, err = httpRouteClient.Update(ctx, httpRoute, metav1.UpdateOptions{})
        if err != nil {
            return err
        }

        // Update ConfigMap second
        err = utils.UpdateConfigMapData(configMap, managedRouteMap, ...)
        return err
    })

    if err != nil {
        return pluginTypes.RpcError{ErrorString: err.Error()}
    }
    return pluginTypes.RpcError{}
}
```

**Benefits:**
- Standard Kubernetes pattern
- Automatic exponential backoff
- Handles conflicts gracefully
- Works across multiple plugin processes

### Option 2: Use Patch for ConfigMap (More Efficient)

If ConfigMap keys are exclusive per route type:

```go
// Patch is more conflict-resistant
patch := client.MergeFrom(configMap.DeepCopy())
configMap.Data["httpManagedRoutes"] = newJSONValue
err := client.Patch(ctx, configMap, patch)

// Still need retry for HTTPRoute update
err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
    httpRoute, err := httpRouteClient.Get(ctx, name, metav1.GetOptions{})
    if err != nil {
        return err
    }
    httpRoute.Spec.Rules = append(httpRoute.Spec.Rules, newRule)
    _, err = httpRouteClient.Update(ctx, httpRoute, metav1.UpdateOptions{})
    return err
})
```

### Remove Mutex Entirely

The `ConfigMapRWMutex` in `GatewayAPITrafficRouting` should be **removed**:
- ❌ Doesn't work (created per-call, not shared across processes)
- ❌ Wrong pattern for distributed Kubernetes systems
- ✅ Optimistic locking + retry is the correct solution

### Update Transaction Mechanism

The current `DoTransaction` rollback mechanism is insufficient. Replace with retry pattern:

**Before (insufficient):**
```go
utils.DoTransaction(r.LogCtx,
    Task{Action: updateRoute, ReverseAction: rollbackRoute},
    Task{Action: updateConfigMap, ReverseAction: rollbackConfigMap},
)
```

**After (correct):**
```go
retry.RetryOnConflict(retry.DefaultBackoff, func() error {
    // Fetch both resources
    // Modify both
    // Update both
    return err  // Let retry handle conflicts
})
```

---

## Summary: Mutex vs Optimistic Locking

### ✅ Use Optimistic Locking (Kubernetes Standard)

**For Kubernetes API updates:**
- All resource updates (ConfigMaps, Routes, Deployments, etc.)
- Direct client usage → `retry.RetryOnConflict`
- Controllers → Return errors, let work queue retry
- Patch operations when possible

**Why it works:**
- Scalable - No upfront locking
- Distributed-system friendly
- Native to Kubernetes design
- Simple retries handle conflicts

### ✅ Use Mutex (In-Process Only)

**For in-memory coordination:**
- Protecting shared Go data structures
- Coordinating goroutines within single process
- Business logic synchronization

**Never for:**
- ❌ Kubernetes API call serialization
- ❌ Cross-process coordination
- ❌ Distributed locking

### ✅ Use Leader Election (Multi-Replica Controllers)

**When:**
- Multiple controller replicas
- Need single active reconciler
- High availability required

**Remember:**
- Still need optimistic locking even with leader election
- Leader election prevents wasted work, not conflicts

---

## Key Takeaways

1. **Industry consensus:** Optimistic concurrency control is the standard Kubernetes pattern
2. **Mutex for API updates:** Anti-pattern in distributed systems
3. **retry.RetryOnConflict:** Standard pattern for direct clients (plugins, CLIs, operators)
4. **Controllers:** Don't retry; return errors and let work queue handle it
5. **Patch > Update:** When you have exclusive field ownership
6. **Work queues:** Provide deduplication, ordering, and backoff automatically
7. **Leader election:** Orthogonal to conflict handling; for multi-replica coordination

---

## Documentation Sources

### Kubernetes Concurrency Documentation
- [Understanding concurrency control in Kubernetes](https://kyungho.me/en/posts/kubernetes-concurrency-control)
- [Kubernetes operators best practices: understanding conflict errors](https://alenkacz.medium.com/kubernetes-operators-best-practices-understanding-conflict-errors-d05353dff421)
- [Kubernetes Resource Version](https://medium.com/@Byunk/kubernetes-resource-version-c7baf409610b)

### Client-Go Retry
- [retry package - k8s.io/client-go/util/retry](https://pkg.go.dev/k8s.io/client-go/util/retry)
- [client-go/util/retry/util.go](https://github.com/kubernetes/client-go/blob/master/util/retry/util.go)
- [client-go examples/create-update-delete-deployment](https://github.com/kubernetes/client-go/blob/master/examples/create-update-delete-deployment/main.go)

### Controller-Runtime Patterns
- [Kubernetes Controllers at Scale: Clients, Caches, Conflicts, Patches Explained](https://medium.com/@timebertt/kubernetes-controllers-at-scale-clients-caches-conflicts-patches-explained-aa0f7a8b4332)
- [Part 5: Concurrent Reconcilers](https://akashjain971.medium.com/part-5-concurrent-reconcilers-c39023435a44)
- [How to elegantly solve the update conflict problem](https://github.com/kubernetes-sigs/controller-runtime/issues/1748)
- [controller-runtime patch.go](https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/client/patch.go)

### Work Queue Pattern
- [client-go/examples/workqueue/main.go](https://github.com/kubernetes/client-go/blob/master/examples/workqueue/main.go)
- [Understanding Kubernetes controllers part I – queues and the core controller loop](https://leftasexercise.com/2019/07/08/understanding-kubernetes-controllers-part-i-queues-and-the-core-controller-loop/)

### Real Project Examples
- [Argo Rollouts ReplicaSet conflict issue #3218](https://github.com/argoproj/argo-rollouts/issues/3218)
- [Argo Rollouts status update conflict #1904](https://github.com/argoproj/argo-rollouts/issues/1904)
- [Argo Rollouts controller.go](https://github.com/argoproj/argo-rollouts/blob/master/rollout/controller.go)
- [Istio operator conflict issue #38591](https://github.com/istio/istio/issues/38591)
- [Flux kustomize-controller reconciling issue #3689](https://github.com/fluxcd/flux2/issues/3689)
- [Flux image-automation-controller conflict #131](https://github.com/fluxcd/image-automation-controller/issues/131)

### Leader Election
- [client-go/examples/leader-election](https://github.com/kubernetes/client-go/tree/master/examples/leader-election)
- [controller-runtime leader_election.go](https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/leaderelection/leader_election.go)
- [Kubernetes Leases](https://kubernetes.io/docs/concepts/architecture/leases/)

### Additional Resources
- [Gardener Kubernetes Clients Guide](https://github.com/gardener/gardener/blob/master/docs/development/kubernetes-clients.md)
- [Kubernetes Leases — Solution to Leader Election/Optimistic Locking](https://medium.com/@sehgal.mohit06/kubernetes-leases-solution-to-leader-election-optimistic-locking-ratelimiting-concurrencycontrol-bb07f53c4462)
- [Argo Workflows Synchronization](https://argo-workflows.readthedocs.io/en/latest/synchronization/)
