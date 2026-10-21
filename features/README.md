# Feature map

| Feature | Real path | Evidence |
| --- | --- | --- |
| Bounded distributed crawl | fixture HTTP server → `tools/build-index.mjs` → `crawl-page` MapReduce job → durable coordinator accepted outputs | `node tools/verify-search.mjs`: graph oracle, same-origin and fragment dedup assertions, both worker IDs recorded |
| Inverted index and PageRank | `index-document` and iterative `rank-contribution` MapReduce jobs | driver compares postings and ranks with independent sequential calculations |
| Search and restart | coordinator `/api/search`, `/api/status`, `/`; `web/index.html` | driver checks lexical/rank order, empty/unknown query and snapshot after coordinator restart |
