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

# Evidence
`.runs/search-<timestamp>/evidence.json` stores `passed`, cleanup status, actions, rejection reasons, job/task ownership and assertions. Coordinator durable state and the persisted `search-snapshot.json` are retained alongside logs. Failed runs keep `passed:false` plus the error stack. The final evidence is read back after cleanup.

# Cleanup
The driver stops and waits for only its tracked child processes in `finally`. Cleanup failures are aggregated and recorded, never swallowed. It preserves run files and confirms evidence remains readable after cleanup. A nonzero exit is failure. Interactive `dev` accepts Ctrl+C or a line containing `stop` to use the same owned cleanup path.
