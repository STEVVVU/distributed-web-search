# Distributed web search

## Guide

- [Architecture and execution guide](#architecture-and-execution-guide)
- [Run](#run)
- [How it works](#how-it-works)
- [API and limits](#api-and-limits)
- [Sources and limits](#sources-and-limits)

The [owned-graph benchmark plan](docs/benchmark-plan.md) keeps the real Node crawl, rank, index and oracle lifecycle. It uses pinned k6v2.3.0 for HTTP constant-arrival-rate load and native metrics. The completed [October 2026 results](docs/benchmark-results-20261022.md) report five hosted repeats for graphs of 32, 1,000 and 5,000 pages with 1, 2 and 4 workers, plus load at 1, 10 and 100 queries per second. The run passed exact output checks and finished every request without drops or failures. These results describe a synthetic graph on one host. They do not establish arbitrary-web capacity or human relevance. k6 request duration excludes unsent iterations and is not scheduled-to-completion latency. The manual workflow selects `benchmark` or a separate 5,000-page `profile`. The builder retains its 32-page default and documented bounded settings and security limits.

Status and search reuse one parsed snapshot and URL lookup map while its file identity is unchanged. An atomic replacement reloads both together; removal or corruption remains visible. This avoids repeated snapshot parsing and document scans without caching query answers or changing published results. Same-length replacement, removal/recreation and corrupt/valid restart checks run through the real service in `node tools/test-search-snapshot.mjs` and `npm run verify`. A matched performance comparison for this cache change remains pending.

A local educational search engine built on the existing MIT MapReduce runtime. Separate Node worker processes fetch a bounded synthetic HTML graph, build an inverted index, and calculate PageRank through repeated distributed MapReduce jobs. The coordinator keeps accepted job outputs and the completed search snapshot on disk. This is a single-host, multi-process course adaptation; it makes no scale, cloud deployment, or Brown grader compatibility claim.

![Search results with lexical and PageRank scores](docs/images/search-desktop.png)

## Run

Requires Node 25+ on Windows or Linux. Install the pinned HTML5 parser and run both existing runtime verification and the search-specific real-product driver:

```sh
npm ci
npm run check
npm test
npm run verify
```

The search driver owns a loopback fixture, coordinator and two worker processes, verifies crawl/index/rank/retrieval behavior, restarts the coordinator, and leaves logs and evidence under ignored `.runs/`. Read `.agents/skills/verify-distributed-web-search/SKILL.md` for Launch, Doctor, Drive, Evidence and Cleanup steps.

For an interactive session on Windows or Linux:

```sh
npm run dev
```

The launcher creates a fresh token and isolated `.runs/dev-*` storage, starts the owned loopback fixture, coordinator and two workers, configures `FIXTURE_ORIGIN` for both workers and the builder, and builds the index. Wait for its printed ready URL, then open `http://127.0.0.1:8080/`. It fails visibly if that port is occupied. Press Ctrl+C or type `stop` followed by Enter to stop only this session's processes. Logs and the durable index remain under the printed run directory.

To select another port in PowerShell:

```sh
$env:PORT='8087'
npm run dev
```

For an existing owned session, `npm run index` rebuilds using `MAPREDUCE_TOKEN`, `FIXTURE_ORIGIN`, `SEARCH_API`, and `SEARCH_DATA` from its environment. Both workers must also receive that same `FIXTURE_ORIGIN`. The launcher configures all of these automatically. `SEARCH_SEED` optionally selects a same-origin seed path. The research-focus profile requires the crawled `/ranking` page.

## How it works

The retained runtime partitions JSON records among separately scheduled workers, hashes intermediate keys into reducer partitions, transfers accepted map output over HTTP, and publishes results only after all reducer tasks finish. Existing lease fencing, restart recovery, durable output ordering and exclusive coordinator ownership remain in place.

`build-index.mjs` submits a bounded breadth-first crawl with at most 32 pages total and five depths including the seed. URLs are limited to 2048 characters and reject credentials. The `crawl-page` job uses parse5, a standards-based HTML5 parser, and rejects non-fixture URLs, redirects, non-HTML responses, pages over 256 KiB, requests over two seconds and failed HTTP responses. Crawl errors fail the orchestration visibly. Canonical URLs drop fragments and the frontier deduplicates URLs. The page graph and parsed content arrive through normal worker MapReduce job outputs. The fixture includes duplicate/fragment/external/invalid links, malformed HTML repaired by parse5, and a dangling page. Separate negative jobs exercise HTTP500/404, redirects, oversized pages, wrong content type and request timeout without replacing the good snapshot.

`index-document` emits term-frequency postings with titles and page text. `rank-contribution` distributes each source page's rank across outgoing links. The build loop computes two profiles through separate repeated MapReduce jobs. Global PageRank teleports uniformly. Research focus teleports to the manually selected `/ranking` seed. For either profile, dangling mass follows that profile's teleport distribution. With damping0.85 and teleport vector`v`, each update is `r_next = 0.85 * incoming + (0.15 + 0.85 * dangling_mass) * v`. Each profile stops at L1 delta at most1e-10, or fails if 200 iterations do not converge. The maintained verifier independently computes a dense stochastic transition matrix to compare every rank, rather than checking only rank sums.

Search scores each distinct query term by term frequency times `ln(1 + N/df)`, adds the selected PageRank, then sorts by descending score and URL for stable ties. Queries are at most512 characters and16 distinct terms. Own-property term lookup allows terms such as `constructor` without reading inherited object properties. The snapshot is atomically written only after a complete successful build and survives coordinator restart. Failed builds retain the prior snapshot.

Build jobs have a default15-second deadline, configurable with `SEARCH_JOB_TIMEOUT_MS` from100 to60000. The whole build has a180-second deadline. An in-flight coordinator request has its own5-second cap, so a timed-out build can take up to five additional seconds to report failure. A missing worker produces a clear deadline error. The verifier bounds child waits and job polling, records cleanup failures, and preserves `passed:false` evidence on any failed check.

The browser UI reports index readiness, search errors, empty results, snippets, lexical scores and PageRank. Its ranking selector compares global versus research-focus scores for the same query. Non-successful HTTP responses are displayed as service/index failures, including a503 when no snapshot exists. It is served by the loopback coordinator. The selected online design reference and documented departures are in `.agents/design-reference.md`.

## API and limits

The runtime's authenticated `/jobs` API remains defined in `src/coordinator.mjs`; `GET /api/status` reports snapshot readiness and `GET /api/search?q=...&policy=global` returns query terms and ranked results. `policy=topic` selects research focus. Unknown policies and out-of-bounds queries return400; a missing index returns503. The UI is at `/`. Coordinator and fixture bind only to `127.0.0.1`; the read-only search routes are unauthenticated within this loopback-only prototype.

The source runtime remains MIT licensed. Its base commit is `a4f0f61ab5649b96b71bc51b956ea00809c1ce27` from the local `rillmap` repository; its full history and license are preserved. Search-specific implementation is new.

## Sources and limits

- Brown's [CS1380 milestones](https://cs.brown.edu/courses/csci1380/milestones/) explicitly ask students to implement distributed crawling, indexing and retrieval, with PageRank as an extension. This project uses the existing named-module JSON MapReduce API as its engine and does not claim course completion.
- PageRank follows the random-surfer power iteration in the [Stanford Introduction to Information Retrieval text](https://nlp.stanford.edu/IR-book/html/htmledition/the-pagerank-computation-1.html). Its [topic-specific PageRank chapter](https://nlp.stanford.edu/IR-book/html/htmledition/topic-specific-pagerank-1.html) describes nonuniform teleport distributions.
- The research extension applies the precomputed biased-vector idea from Haveliwala's [Topic-Sensitive PageRank, WWW2002](https://www.ra.ethz.ch/CDstore/www2002/refereed/127/), specifically Section2 equations5 and6. This project compares one manually seeded topic against the global baseline through the actual distributed iteration and independent oracle. It is an implementation experiment, not a new PageRank algorithm, an automatic topic classifier, or a reproduction of the paper's16-topic relevance study. The synthetic scores demonstrate a policy effect; they do not establish a relevance or scalability gain.
- The retained base cites the [MapReduce paper](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/) for partitioning, task re-execution and output publication.
- [parse5](https://github.com/inikulin/parse5) is MIT-licensed and installed at exact version 8.0.1. It parses HTML5 using the WHATWG parsing algorithm. It is a runtime dependency, pinned in `package-lock.json`.

This is a small synthetic graph demonstration. The crawl intentionally cannot access arbitrary Internet URLs. Coordinator data, shuffle and snapshots reside on one disk, memory use grows with the graph and jobs are materialized in memory. No multi-host deployment, replication, throughput or scalability claim is made. Brown staff have not evaluated it.

## Architecture and execution guide

This project adds a bounded synthetic-web crawl, inverted index, two PageRank policies, snapshot publication, and a search UI on the existing MIT MapReduce runtime. The runtime owns job scheduling, worker leases, durable task output, and recovery. This is not a general web crawler or multi-host service. The [snapshot](#snapshot) is the committed input to search.

The builder submits named jobs to one coordinator; separate workers run them and persist artifacts. The coordinator serves queries over the completed snapshot.

```mermaid
flowchart LR
  Builder["Index builder"] --> Coordinator["MapReduce coordinator"]
  Coordinator <--> WorkerA["Worker process A"]
  Coordinator <--> WorkerB["Worker process B"]
  WorkerA --> Fixture["Bounded loopback HTML fixture"]
  WorkerB --> Fixture
  WorkerA --> Artifacts["Durable task and shuffle files"]
  WorkerB --> Artifacts
  Builder --> Snapshot["Atomic completed search snapshot"]
  Browser["Search browser UI"] --> Coordinator
  Coordinator --> Snapshot
```

The builder publishes only after crawling, building an [inverted index](#inverted-index), and both [PageRank](#pagerank) calculations finish. Its [teleport vector](#teleport-vector) also redistributes [dangling mass](#dangling-mass). A failed rebuild leaves the previous [snapshot](#snapshot) available; MapReduce retries use [lease fencing](#lease-fencing).

```mermaid
flowchart TD
  subgraph Build["Index build lifecycle"]
    Seed["Validate same-origin seed"] --> Crawl["Crawl bounded frontier"]
    Crawl --> Parse{"Valid bounded HTML?"}
    Parse -->|"Yes"| Pages["Emit page graph and content"]
    Parse -->|"No"| Fail["Fail build and keep old snapshot"]
    Pages --> RankInput["Build adjacency and initial ranks"]
    RankInput --> Global["Iterate global PageRank jobs"]
    Global --> GlobalDone{"Global delta at most 1e-10?"}
    GlobalDone -->|"No, fewer than 200 iterations"| Global
    GlobalDone -->|"Yes, reset ranks for next policy"| Focus["Iterate research-focus jobs"]
    Focus --> FocusDone{"Topic delta at most 1e-10?"}
    FocusDone -->|"No, fewer than 200 iterations"| Focus
    FocusDone -->|"Yes"| Index["Map terms to postings"]
    Index --> SearchIndex["Join postings and both rank profiles"]
    SearchIndex --> Publish["Atomically replace snapshot"]
    GlobalDone -->|"Limit reached"| Fail
    FocusDone -->|"Limit reached"| Fail
  end
  subgraph Query["Search request"]
    Request["Validate query and policy"] --> Ready{"Snapshot ready?"}
    Ready -->|"Yes"| Score["Score terms and PageRank"]
    Score --> Sort["Sort by score then URL"]
    Ready -->|"No"| Unavailable["Return service unavailable"]
  end
```

| Subsystem | Responsibility and owned state | Source |
|---|---|---|
| Builder | Submits crawl/index/rank jobs, checks deadlines, assembles snapshot | [`build-index.mjs`](tools/build-index.mjs) |
| Coordinator | Authenticated job API, task state, search reads and atomic publication | [`coordinator.mjs`](src/coordinator.mjs) |
| Worker and jobs | Fetch fixture pages and perform map/reduce work | [`worker.mjs`](src/worker.mjs), [`crawl-page.mjs`](jobs/crawl-page.mjs), [`index-document.mjs`](jobs/index-document.mjs), [`rank-contribution.mjs`](jobs/rank-contribution.mjs) |
| Search API and UI | Validates queries, selects policy, scores stable results, displays errors | [`coordinator.mjs`](src/coordinator.mjs), [`index.html`](web/index.html) |
| Verification | Drives real processes and compares rank/search with independent expected results | [`features`](features/README.md), [`verify.mjs`](tools/verify.mjs) |

### End-to-end journey

1. Start the loopback fixture, coordinator, and workers with shared token and fixture origin.
2. Run the builder within the configured page and depth budgets. Defaults allow 32 pages over five levels; invalid responses, redirects, oversize pages, timeouts, or non-HTML fail visibly.
3. Run repeated jobs for global PageRank, then reset ranks and compute manually seeded research-focus PageRank. Each stops at the L1 threshold or fails after 200 iterations. Map page text into postings after both policies finish.
4. Atomically replace the snapshot only after the whole build succeeds. A failed build retains its predecessor; coordinator restart reloads the saved snapshot.
5. Search with the global or topic policy. Results combine term frequency and PageRank, then sort ties by URL. Without a snapshot, the API returns 503.

### Design decisions and tradeoffs

Rationales are inferred from the implementation.

| Choice | Practical reason | Alternative and cost to reconsider |
|---|---|---|
| Crawl a same-origin fixture with page and time bounds | Repeatable local inputs and explicit error cases | Crawl arbitrary sites, adding politeness, robots, SSRF, and variable data concerns |
| Run crawl, index, and rank as MapReduce jobs | Exercises the retained task and recovery API | Monolithic process removes distributed task boundaries |
| Keep one selected topic seed beside global rank | Shows a manually chosen teleport policy | Learn vectors or many topics, requiring training data and relevance evaluation |
| Publish a complete snapshot atomically | Readers see old or new complete data | Update files in place and risk partial indexes |
| Bind the search prototype to loopback | No external-user authentication is implemented | Public binding requires access control and security design |

### Glossary

<a id="inverted-index"></a>

<details><summary>What is an inverted index?</summary>

It maps a term to documents that contain it. Here postings carry term frequency and page data for scoring.

</details>

<a id="pagerank"></a>

<details><summary>What is PageRank?</summary>

It assigns scores from links through repeated updates. This system computes global and one manually seeded profile, capped at 200 iterations.

</details>

<a id="teleport-vector"></a>

<details><summary>What is a teleport vector?</summary>

It distributes rank when the random surfer jumps instead of following a link. Here global rank uses a uniform vector and research rank favors `/ranking`.

</details>

<a id="dangling-mass"></a>

<details><summary>What is dangling mass?</summary>

It is rank on pages with no outgoing links. This implementation redistributes it by the selected profile's teleport vector.

</details>

<a id="snapshot"></a>

<details><summary>What is the search snapshot?</summary>

It is the complete persisted index and rank data served to queries. Atomic replacement leaves the old version intact on failure.

</details>

<a id="lease-fencing"></a>

<details><summary>What is lease fencing?</summary>

It prevents a late worker completion from replacing a newer task attempt. The builder relies on this retained coordinator behavior during retries.

</details>
