# Recovery
## Sub-features
Lease expiry, reassignment, restart fencing, idempotent completion bounded failure and exclusive concurrent restart.
## How to get to it (user POV)
Restart the coordinator with its original data directory or replace a dead worker.
## Driving it with the Node HTTP harness
`node tools/verify.mjs` kills an assigned worker, restarts a coordinator with an active lease, posts stale completions checks persisted accepted attempts, and launches four recovery contenders against the same data directory. Exactly one may acquire the OS-owned IPC guard.
## Gotchas
One coordinator owns a data directory. Local disk must survive; coordinator disk loss and power-loss guarantees are outside this prototype.
