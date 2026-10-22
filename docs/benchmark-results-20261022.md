# Owned-graph benchmark results

Run 39751503932 completed the frozen benchmark at source commit `64aa42ff6e2b46037df3137d20da849d0d6d30df`. The [GitHub Actions run](https://github.com/STEVVVU/distributed-web-search/actions/runs/39751503932) succeeded and uploaded its allowlisted evidence artifact (14,367,958 bytes). The benchmark verdict was `measurement-complete-zero-loss-at-offered-rates`.

The benchmark ran on Ubuntu 7.0.0-1017-azure with Node 25.9.0 and k6 2.3.0. The hosted runner exposed four CPUs and 16 GiB of memory; its CPU was an AMD EPYC 9V45. The workload used the actual crawl, distributed PageRank and indexing path, an independent query oracle, two PageRank profiles, process restarts and owned cleanup. Each measurement cell built a fresh synthetic graph. Graphs of 32, 1,000 and 5,000 pages each ran five times with one, two and four workers. Worker order alternated between repeats.

Index time runs from builder launch through atomic snapshot publication. It includes fresh-process startup and the complete crawl, rank and index build. The table reports the median and range across five runs. Throughput is pages divided by that wall time.

| Pages | Workers | Index time, median (range) | Pages/s, median-derived |
| ---: | ---: | ---: | ---: |
| 32 | 1 | 49.79 s (48.09–53.50) | 0.64 |
| 32 | 2 | 44.76 s (42.98–47.93) | 0.71 |
| 32 | 4 | 40.64 s (39.38–47.52) | 0.79 |
| 1,000 | 1 | 53.67 s (52.34–56.12) | 18.63 |
| 1,000 | 2 | 45.85 s (42.57–51.12) | 21.81 |
| 1,000 | 4 | 40.55 s (39.04–42.35) | 24.66 |
| 5,000 | 1 | 63.93 s (61.82–66.56) | 78.21 |
| 5,000 | 2 | 53.67 s (51.75–56.08) | 93.16 |
| 5,000 | 4 | 48.00 s (46.62–55.49) | 104.17 |

For every build sample, the independent oracle accepted all query fields and result ordering. All PageRank iterations converged. Restart checks passed, and owned-process cleanup succeeded. The three offered rates ran for ten seconds on every build sample, using k6 constant-arrival-rate with a 256 VU cap. At 1, 10 and 100 offered requests/s, all 490, 4,532 and 45,037 requests completed respectively. Each rate had zero drops, rejections, failures, timeouts, integrity errors and interrupted iterations. The largest observed k6 generator peak RSS was 131.4, 173.1 and 194.8 MiB at those rates. Its largest measured user-plus-system CPU time was 4.22 seconds in a ten-second cell. Coordinator peak RSS was 304.5 MiB across the campaign.

Native `http_req_duration` values are milliseconds: k6 defines Trend time values in milliseconds and describes this HTTP metric as a time Trend ([Trend API](https://grafana.com/docs/k6/latest/javascript-api/k6-metrics/trend/), [built-in metrics](https://grafana.com/docs/k6/latest/using-k6/metrics/reference/)). They describe issued requests only. They include sending, waiting and receiving, but exclude connection setup and dropped iterations. They are not scheduled-to-completion latency. Recomputed from the 45 native summary files at each offered rate, the median of each run's p95 was 2.692 ms, 2.246 ms and 1.491 ms at 1, 10 and 100 requests/s. The corresponding per-run p95 ranges were 0.514–19.417 ms, 0.462–11.254 ms and 0.256–8.128 ms. Median per-run p99 values were 2.706 ms, 2.919 ms and 2.172 ms, with ranges 0.522–23.891 ms, 0.564–17.360 ms and 0.425–16.260 ms. These summarize native per-run quantiles across different graph sizes and worker counts. They are not pooled request percentiles.

A separate 1,000-page, two-worker stability cell offered 10 requests/s for 120 seconds. All 1,200 requests completed with correct outputs and no loss. This cell is excluded from the five-repeat matrix. The separate profile stage was not part of this benchmark run. An earlier 5,000-page profile diagnosis attributed query cost to repeated snapshot parsing and document scans. The cache-after profile is diagnostic evidence only. The 280,000-page, 360 documents/s and 118 ms goals remain unproven. This campaign does not justify extrapolation to those targets or to cloud capacity.

Run the benchmark with `node tools/benchmark-search.mjs` after installing the pinned dependencies and k6 2.3.0. The full workload, timer boundary, correctness oracle, native metrics and cleanup policy are documented in [the benchmark plan](benchmark-plan.md). A compact copy of the reviewed aggregates and evidence hashes is in [`docs/evidence/P03-k6-39751503932.json`](evidence/P03-k6-39751503932.json). The failed upload attempt from run 39740791329 remains preserved as a failed artifact attempt and is not the source of these measurements.
