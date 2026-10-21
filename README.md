# Distributed web search

A local educational search engine built on the existing MIT MapReduce runtime. Separate Node worker processes fetch a bounded synthetic HTML graph, build an inverted index, and calculate PageRank through repeated distributed MapReduce jobs. The coordinator keeps accepted job outputs and the completed search snapshot on disk. This is a single-host, multi-process course adaptation; it makes no scale, cloud deployment, or Brown grader compatibility claim.

## Run

Requires Node 25+ on Windows or Linux. Install the pinned HTML5 parser and run both existing runtime verification and the search-specific real-product driver:

```sh
npm ci
npm run check
npm test
npm run verify
```

The search driver owns a loopback fixture, coordinator and two worker processes, verifies crawl/index/rank/retrieval behavior, restarts the coordinator, and leaves logs and evidence under ignored `.runs/`. Read `.agents/skills/verify-distributed-web-search/SKILL.md` for Launch, Doctor, Drive, Evidence and Cleanup steps.

For an interactive fixture session, start the local fixture and coordinator in separate terminals:

```sh
node tools/fixture.mjs
```

Use its printed origin for the worker allowlist. Set a fresh `MAPREDUCE_TOKEN` of at least 16 characters in each terminal. Start coordinator and workers with distinct data directories, then build the index using the same origin and coordinator storage path:

```sh
node src/coordinator.mjs --data data/coordinator --port 8080
node src/worker.mjs --url http://127.0.0.1:8080 --id worker-a --data data/worker-a
```

```sh
FIXTURE_ORIGIN=http://127.0.0.1:<fixture-port> SEARCH_API=http://127.0.0.1:8080 SEARCH_DATA=data/coordinator npm run index
```

Open `http://127.0.0.1:8080/`. Stop only the processes started for this run. `FIXTURE_ORIGIN` must be the owned loopback fixture; redirects are rejected and external links are discarded.

## How it works

The retained runtime partitions JSON records among separately scheduled workers, hashes intermediate keys into reducer partitions, transfers accepted map output over HTTP, and publishes results only after all reducer tasks finish. Existing lease fencing, restart recovery, durable output ordering and exclusive coordinator ownership remain in place.

`build-index.mjs` submits a bounded breadth-first crawl: at most 32 pages at each of five depths. The `crawl-page` job uses parse5, a standards-based HTML5 parser, and rejects non-fixture URLs, redirects, pages over 256 KiB, requests over two seconds and failed HTTP responses. Crawl errors fail the orchestration visibly. Canonical URLs drop fragments and the frontier deduplicates URLs. The page graph and parsed content arrive through normal worker MapReduce job outputs.

`index-document` emits term-frequency postings with titles and page text. `rank-contribution` distributes each source page's rank across outgoing links; the coordinator-side build loop redistributes dangling mass uniformly, applies damping 0.85, and stops at L1 delta below 1e-8 or 20 iterations. Search scores each distinct query term by term frequency times `ln(1 + N/df)`, adds PageRank, then sorts by descending score and URL for stable ties. The snapshot is atomically written to `search-snapshot.json` and survives coordinator restart.

The browser UI reports index readiness, search errors, empty results, snippets, lexical scores and PageRank. It is served by the loopback coordinator.

## API and limits

The runtime's authenticated `/jobs` API remains documented below in its existing route definitions; `GET /api/status` reports snapshot readiness and `GET /api/search?q=...` returns query terms and ranked results. The UI is at `/`. Coordinator and fixture bind only to `127.0.0.1`; the read-only search routes are unauthenticated within this loopback-only prototype.

The source runtime remains MIT licensed. Its base commit is `a4f0f61ab5649b96b71bc51b956ea00809c1ce27` from the local `rillmap` repository; its full history and license are preserved. Search-specific implementation is new.

## Sources and limits

- Brown's [CS1380 milestones](https://cs.brown.edu/courses/csci1380/milestones/) explicitly ask students to implement distributed crawling, indexing and retrieval, with PageRank as an extension. This project uses the existing named-module JSON MapReduce API as its engine and does not claim course completion.
- PageRank follows the random-surfer transition equation and dangling-page handling in the [Stanford Introduction to Information Retrieval text](https://nlp.stanford.edu/IR-book/html/htmledition/the-pagerank-computation-1.html). Damping is 0.85; the fixed teleport distribution is uniform. There is no personalized teleport vector.
- The retained base cites the [MapReduce paper](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/) for partitioning, task re-execution and output publication.
- [parse5](https://github.com/inikulin/parse5) is MIT-licensed and installed at exact version 8.0.1. It parses HTML5 using the WHATWG parsing algorithm. It is a runtime dependency, pinned in `package-lock.json`.

This is a small synthetic graph demonstration. The crawl intentionally cannot access arbitrary Internet URLs. Coordinator data, shuffle and snapshots reside on one disk, memory use grows with the graph and jobs are materialized in memory. No multi-host deployment, replication, throughput or scalability claim is made. Brown staff have not evaluated it.
