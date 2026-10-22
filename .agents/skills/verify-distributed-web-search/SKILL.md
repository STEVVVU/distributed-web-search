---
name: verify-distributed-web-search
description: Drive the real local distributed web search system through an owned HTML fixture and separate worker processes, retaining evidence after cleanup.
---

# Launch
Run `npm run verify` from the project root with Node25+. The maintained driver `tools/verify-search.mjs` creates an isolated coordinator, fixture server and two workers on loopback. It uses ignored `.runs/search-*` storage and stops only its own children through `tools/processes.mjs`. `npm run dev` uses the same lifecycle helper for an interactive owned session and prints its URL. Use `PORT` to override8080; Ctrl+C or `stop` followed by Enter cleans up.

# Doctor
The driver checks authenticated coordinator health against its child PID and fixture readiness. It asserts503 for a missing snapshot and a bounded builder failure with no workers. Readiness waits are5seconds, each HTTP request is capped at5seconds, verification job polling at20seconds, and the overall driver deadline at180seconds (an in-flight request may take up to5additional seconds). On failure inspect the run's coordinator, fixture, builder, no-worker and worker logs. `VERIFY_FAIL_AFTER_START=1` deliberately exercises failure evidence and cleanup.

# Drive
The driver crawls five fixture pages using distributed jobs. It compares exact titles/text/links and the complete term/posting set with a fixture-only oracle. Duplicate/fragments/off-origin/malformed links are filtered, malformed HTML is parsed, and a dangling page remains in the graph. Global and manually seeded topic PageRank are compared with independent dense stochastic-matrix iteration, including nonuniform ranks, meaningful topic bias, and more than two distributed iterations. Query scores/order use independent oracle postings and ranks, including empty, unknown, prototype-word, repeated and bounded inputs. Both workers must execute tasks. Isolated negative jobs reject500/404, redirects,oversized/non-HTML/slow pages and off-origin URLs before fetch; the good snapshot must remain byte-identical. The coordinator is restarted and both search profiles must retain their results.

Browser checks are performed only through CUA. Launch `npm run dev`, inspect desktop1280×720 and narrow views against `.agents/design-reference.md`, submit `search` under both policies, submit `constructor` and an unknown word, and check clear unavailable-index rendering against an owned coordinator with no snapshot. Backend HTTP assertions do not substitute for these browser checks.

`npm run verify` also runs `tools/verify-ui.mjs`, a deterministic Node VM unit harness consuming the actual inline UI script with a minimal public DOM surface and deferred HTTP responses. It reproduces and prevents stale query/policy responses mixing into newer results, including late failures. It checks current HTTP failures versus successful empty results. This is unit evidence, not a browser or end-to-end claim.

`npm run verify` additionally runs `tools/test-search-snapshot.mjs`, a small real-coordinator protocol regression. Status/search share one parsed snapshot and URL map per native file identity. The driver atomically replaces equal-length content with preserved mtime, confirms both policies see new first-match data, removes/recreates the snapshot, verifies corruption fails visibly before and after restart, and confirms repair/valid restart. It retains `.runs/snapshot-contract-*/evidence.json` and owned cleanup. It does not run a corpus build or establish performance.

# Evidence

## Benchmark extension

With verified k6v2.3.0 on PATH (or `K6_BINARY`), run `node tools/benchmark-search.mjs --smoke` for the real lifecycle, exact eight-page posting/rank/query oracle and restart/cleanup. Node retains corpus/build/oracle ownership; `tools/k6-search.js` uses native constant-arrival-rate, fixed256VU cap and native JSON metrics. Follow `docs/benchmark-plan.md` for source/action pins, the32/1,000/5,000-page matrix, five repeats, stability, raw evidence and separate profile. k6 request duration excludes connections and dropped iterations and is not scheduled-to-completion latency. Keep dropped offers, failed/rejected requests and exact-result mismatch counters distinct. Generated data remains in `.runs/`. Synthetic term relevance does not establish human web relevance. Bounds and adapter/oracle self-checks are `tools/test-search-build-config.mjs` and `tools/test-search-load-summary.mjs`.
`.runs/search-<timestamp>/evidence.json` stores `passed`, cleanup status, actions, rejection reasons, job/task ownership and assertions. Coordinator durable state and the persisted `search-snapshot.json` are retained alongside logs. Failed runs keep `passed:false` plus the error stack. The final evidence is read back after cleanup.

Before each benchmark load, Node independently checks every result field and rank, then k6 compares each successful raw body using native SHA256 against that validated immutable body. The small query/policy/hash manifest avoids per-request deep GJS oracle copying. Snapshot identity is checked across the trial. Run `node tools/test-search-k6.mjs` with the pinned k6 binary for actual coordinator valid/wrong-hash/changed-body controls. Started iterations without terminal callbacks at the grace deadline remain distinct `interrupted` outcomes with no invented latency; `requestsFullyFinished` and the zero-loss envelope expose that limitation. Preserve failed run39734298816 and inspect generator resources again before attributing capacity to the service.

# Cleanup
The driver stops and waits for only its tracked child processes in `finally`. Cleanup failures are aggregated and recorded, never swallowed. It preserves run files and confirms evidence remains readable after cleanup. A nonzero exit is failure. Interactive `dev` accepts Ctrl+C or a line containing `stop` to use the same owned cleanup path.
