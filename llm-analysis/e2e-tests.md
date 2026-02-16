# Options to Force Argo Rollouts Controller Cache Synchronization

## Problem
After patching a Rollout, the Argo Rollouts controller's informer cache may lag behind the API server, causing delays before the controller processes the change.

## Solutions (in order of recommendation)

### Option 1: Force Reconciliation with Annotation (RECOMMENDED)
Add a timestamp annotation to the Rollout, which increments `metadata.generation` and forces immediate reconciliation.

**Pros:**
- Forces immediate controller attention
- No arbitrary sleep needed
- More reliable than time-based delays

**Cons:**
- Adds extra annotation to rollout
- Requires additional API call

**Implementation:**
```go
// After patching the rollout to remove pause step
err = forceRolloutReconciliation(ctx, clusterResources, resourcesMap[ROLLOUT_KEY])
if err != nil {
    logrus.Errorf("failed to force reconciliation: %s", err)
    t.Error()
    return ctx
}

// Wait for controller to observe the change (polls observedGeneration)
err = waitForGenerationObserved(ctx, clusterResources, resourcesMap[ROLLOUT_KEY],
    resourcesMap[ROLLOUT_KEY].GetGeneration(), 30*time.Second)
if err != nil {
    logrus.Errorf("failed waiting for generation to be observed: %s", err)
    t.Error()
    return ctx
}
```

### Option 2: Use kubectl argo rollouts retry
Execute `kubectl argo rollouts retry <rollout-name>` to force controller to reprocess the rollout.

**Pros:**
- Simple command-line tool
- Forces full reconciliation

**Cons:**
- Requires kubectl plugin installed in test environment
- May have side effects (retry semantics)
- Harder to integrate in Go tests

**Implementation:**
```go
cmd := exec.CommandContext(ctx, "kubectl", "argo", "rollouts", "retry",
    resourcesMap[ROLLOUT_KEY].GetName(),
    "-n", resourcesMap[ROLLOUT_KEY].GetNamespace())
output, err := cmd.CombinedOutput()
if err != nil {
    return fmt.Errorf("failed to retry rollout: %w, output: %s", err, output)
}
```

### Option 3: Poll Until ObservedGeneration Matches (CURRENT APPROACH)
Wait for `status.observedGeneration` to match `metadata.generation`, with robust health checks.

**Pros:**
- No controller manipulation needed
- Tests real-world behavior
- Validates cache sync mechanism

**Cons:**
- Requires generous timeout for CI environments
- Still subject to watch latency

**Implementation (already in place):**
```go
// 10-second delay for cache sync
time.Sleep(10 * time.Second)

// Wait up to 180s with robust health checking
err = wait.For(
    waitCondition.ResourceMatch(
        resourcesMap[ROLLOUT_KEY],
        getRolloutHealthyFetcher(t, 2),
    ),
    wait.WithTimeout(VERY_LONG_PERIOD), // 180s
    wait.WithInterval(SHORT_PERIOD),
)
```

### Option 4: Patch Rollout Spec with No-op Change
Add and immediately remove a harmless field to trigger reconciliation.

**Pros:**
- Forces generation increment
- Pure spec change (no annotations)

**Cons:**
- May cause unexpected behavior
- Unclear which fields are safe to modify

**Example (use with caution):**
```go
// Add a label, then remove it
labels := rollout.GetLabels()
labels["temp-reconcile-trigger"] = "true"
rollout.SetLabels(labels)
clusterResources.Update(ctx, rollout)

// Immediately remove it
delete(labels, "temp-reconcile-trigger")
rollout.SetLabels(labels)
clusterResources.Update(ctx, rollout)
```

### Option 5: Restart Controller Pod (NOT RECOMMENDED)
Delete the Argo Rollouts controller pod to force cache clear.

**Pros:**
- Guarantees cache clear

**Cons:**
- Too disruptive
- May affect other tests running in parallel
- Controller downtime
- Not suitable for production-like testing

## Recommendation

For our flaky tests, we have two good options:

1. **Keep current approach** (Option 3) with 10s delay + 180s timeout
   - Simpler, tests real behavior
   - Good for E2E testing

2. **Switch to annotation approach** (Option 1)
   - More deterministic
   - Eliminates arbitrary sleep
   - Better for faster test iteration

**Suggested hybrid approach:**
```go
// After promoting rollout
logrus.Infof("rollout %q was promoted to finish canary deployment", resourcesMap[ROLLOUT_KEY].GetName())

// Force reconciliation with annotation
err = forceRolloutReconciliation(ctx, clusterResources, resourcesMap[ROLLOUT_KEY])
if err != nil {
    logrus.Errorf("failed to force reconciliation: %s", err)
    t.Error()
    return ctx
}

// Wait for controller to observe (much shorter timeout needed now)
err = waitForGenerationObserved(ctx, clusterResources, resourcesMap[ROLLOUT_KEY],
    resourcesMap[ROLLOUT_KEY].GetGeneration(), 30*time.Second)
if err != nil {
    logrus.Errorf("generation not observed: %s", err)
    t.Error()
    return ctx
}

// Now wait for healthy state with standard robust checks
logrus.Infof("waiting for rollout %q to complete and reach healthy status", resourcesMap[ROLLOUT_KEY].GetName())
err = wait.For(
    waitCondition.ResourceMatch(
        resourcesMap[ROLLOUT_KEY],
        getRolloutHealthyFetcher(t, 2),
    ),
    wait.WithTimeout(VERY_LONG_PERIOD),
    wait.WithInterval(SHORT_PERIOD),
)
```

This approach:
- Forces immediate reconciliation (no 10s sleep needed)
- Waits for generation to be observed (ensures controller saw the change)
- Waits for healthy state (validates actual deployment completion)
