# Recovery
## Sub-features
Lease expiry, reassignment, restart fencing, idempotent completion and bounded failure.
## How to get to it (user POV)
Restart the coordinator with its original data directory or replace a dead worker.
## Driving it with the Node HTTP harness
`node tools/verify.mjs` kills an assigned worker, restarts a coordinator with an active lease, posts stale completions and checks persisted accepted attempts.
## Gotchas
One coordinator owns a data directory. Local disk must survive; coordinator disk loss and power-loss guarantees are outside this prototype.
