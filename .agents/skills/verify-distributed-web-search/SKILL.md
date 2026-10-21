---
name: verify-distributed-web-search
description: Drive the real local distributed web search system through an owned HTML fixture and separate worker processes, retaining evidence after cleanup.
---

# Launch
Run `node tools/verify-search.mjs` from the project root with Node 25+. The driver creates an isolated coordinator, fixture server and two workers on loopback. It uses ignored `.runs/` storage and stops only its own children.

# Doctor
The driver checks authenticated coordinator health and the fixture readiness message. On failure inspect that run's coordinator, fixture and worker logs.

# Drive
The driver crawls fixture pages using distributed jobs, compares the discovered graph and index with an independent fixture oracle, checks PageRank normalization and deterministic retrieval, and proves task execution by both workers. It restarts the coordinator and rechecks the search snapshot.

# Evidence
`.runs/<timestamp>/evidence.json` stores actions, job/task ownership and assertions. Coordinator durable state and the persisted `search-snapshot.json` are retained alongside logs.

# Cleanup
The driver stops and waits for only its child processes in `finally`. It preserves run files and confirms evidence remains readable after cleanup. A nonzero exit is failure.
