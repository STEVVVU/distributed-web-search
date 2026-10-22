import fs from 'node:fs';
import path from 'node:path';
import {atomic, rpc, sleep, ensure} from '../src/common.mjs';

const api = process.env.SEARCH_API ?? 'http://127.0.0.1:8080';
const origin = process.env.FIXTURE_ORIGIN;
const dataDir = process.env.SEARCH_DATA ?? 'data/coordinator';
ensure(origin && new URL(origin).hostname === '127.0.0.1' && new URL(origin).origin === origin, 'FIXTURE_ORIGIN must be an owned 127.0.0.1 origin');
ensure(new URL(api).hostname === '127.0.0.1', 'SEARCH_API must be loopback');
const jobTimeout = Number(process.env.SEARCH_JOB_TIMEOUT_MS ?? 15000);
ensure(Number.isInteger(jobTimeout) && jobTimeout >= 100 && jobTimeout <= 60000, 'SEARCH_JOB_TIMEOUT_MS must be 100..60000');
const buildDeadline = Date.now() + 180000;
const call = (route, method, body) => rpc(`${api}${route}`, process.env.MAPREDUCE_TOKEN, method, body);
async function submit(module, keys) {
  const deadline = Math.min(Date.now() + jobTimeout, buildDeadline);
  const {id} = await call('/jobs', 'POST', {module, keys, splits: Math.min(4, Math.max(1, keys.length)), reducers: 2});
  while (Date.now() < deadline) {
    const job = await call(`/jobs/${id}`);
    if (job.status === 'done') return job.output;
    if (job.status === 'failed') throw new Error(`${module} failed: ${JSON.stringify(job.events)}`);
    await sleep(Math.min(50, Math.max(0, deadline - Date.now())));
  }
  throw new Error(`${module} job ${id} exceeded deadline (${jobTimeout} ms per job, 180000 ms per build); check worker availability`);
}
const seed = new URL(process.env.SEARCH_SEED ?? '/', origin); seed.hash = '';
ensure(seed.origin === origin && seed.href.length <= 2048 && !seed.username && !seed.password, 'invalid crawl seed');
const pages = new Map(), seen = new Set();
let frontier = [seed.href];
for (let depth = 0; depth <= 4 && frontier.length; depth++) {
  const batch = [...new Set(frontier)].filter(url => !seen.has(url)).slice(0, 32 - seen.size); frontier = [];
  if (!batch.length) break;
  for (const url of batch) seen.add(url);
  const output = await submit('crawl-page', batch.map(url => [url, null]));
  for (const [url, page] of output) { pages.set(url, page); if (depth < 4) frontier.push(...page.links); }
}
ensure(pages.size, 'crawl found no pages');
const graph = new Map([...pages].map(([url, page]) => [url, {links: page.links.filter(link => pages.has(link)), rank: 1 / pages.size}]));
const topicSeeds = [...pages.keys()].filter(url => new URL(url).pathname === '/ranking');
ensure(topicSeeds.length, 'topic ranking requires a crawled /ranking seed');
const profiles = {}, ranks = new Map([...pages.keys()].map(url => [url, {}]));
for (const policy of ['global', 'topic']) {
  for (const node of graph.values()) node.rank = 1 / graph.size;
  const teleport = url => policy === 'global' ? 1 / graph.size : (topicSeeds.includes(url) ? 1 / topicSeeds.length : 0);
  let iterations = 0, delta = Infinity;
  for (; iterations < 200 && delta > 1e-10; iterations++) {
    const dangling = [...graph.values()].filter(node => !node.links.length).reduce((sum, node) => sum + node.rank, 0);
    const sums = new Map(await submit('rank-contribution', [...graph]));
    delta = 0;
    for (const [url, node] of graph) {
      const rank = (0.15 + 0.85 * dangling) * teleport(url) + 0.85 * (sums.get(url) ?? 0);
      delta += Math.abs(rank - node.rank); node.rank = rank;
    }
  }
  ensure(delta <= 1e-10, `${policy} PageRank did not converge in 200 iterations`);
  for (const [url, node] of graph) ranks.get(url)[policy] = node.rank;
  profiles[policy] = {iterations, delta};
}
const indexed = new Map(await submit('index-document', [...pages]));
const snapshot = {createdAt: new Date().toISOString(), origin, documents: [...pages].map(([url, page]) => ({url, title: page.title, text: page.text, links: graph.get(url).links, rank: ranks.get(url).global, ranks: ranks.get(url)})), terms: Object.fromEntries(indexed), rank: {profiles, damping: 0.85, topicSeeds}};
fs.mkdirSync(path.resolve(dataDir), {recursive: true});
atomic(path.join(path.resolve(dataDir), 'search-snapshot.json'), snapshot);
console.log(JSON.stringify({pages: pages.size, terms: indexed.size, profiles}));
