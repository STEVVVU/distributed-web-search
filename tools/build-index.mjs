import fs from 'node:fs';
import path from 'node:path';
import {atomic, rpc, sleep} from '../src/common.mjs';

const api = process.env.SEARCH_API ?? 'http://127.0.0.1:8080';
const origin = process.env.FIXTURE_ORIGIN;
const dataDir = process.env.SEARCH_DATA ?? 'data/coordinator';
if (!origin || new URL(origin).hostname !== '127.0.0.1') throw new Error('FIXTURE_ORIGIN must be an owned 127.0.0.1 fixture');
const call = (route, method, body) => rpc(`${api}${route}`, process.env.MAPREDUCE_TOKEN, method, body);
async function submit(module, keys) {
  const {id} = await call('/jobs', 'POST', {module, keys, splits: Math.min(4, Math.max(1, keys.length)), reducers: 2});
  for (;;) { const job = await call(`/jobs/${id}`); if (job.status === 'done') return job.output; if (job.status === 'failed') throw new Error(`${module} failed: ${JSON.stringify(job.events)}`); await sleep(50); }
}
const canonical = value => { const url = new URL(value); url.hash = ''; return url.href; };
const seed = canonical(`${origin}/`), pages = new Map(), seen = new Set();
let frontier = [seed];
for (let depth = 0; depth <= 4 && frontier.length; depth++) {
  const batch = [...new Set(frontier)].filter(url => !seen.has(url)).slice(0, 32); frontier = [];
  if (!batch.length) break;
  for (const url of batch) seen.add(url);
  const output = await submit('crawl-page', batch.map(url => [url, null]));
  for (const [url, page] of output) { pages.set(url, page); if (depth < 4) frontier.push(...page.links); }
}
if (!pages.size) throw new Error('crawl found no pages');
const graph = new Map([...pages].map(([url, page]) => [url, {links: page.links.filter(link => pages.has(link)), rank: 1 / pages.size}]));
let iterations = 0, delta = Infinity;
for (; iterations < 20 && delta > 1e-8; iterations++) {
  const dangling = [...graph.values()].filter(node => !node.links.length).reduce((sum, node) => sum + node.rank, 0);
  const sums = new Map(await submit('rank-contribution', [...graph].map(([url, node]) => [url, node])));
  delta = 0;
  for (const [url, node] of graph) {
    const rank = 0.15 / graph.size + 0.85 * (dangling / graph.size + (sums.get(url) ?? 0));
    delta += Math.abs(rank - node.rank); node.rank = rank;
  }
}
const indexed = new Map(await submit('index-document', [...pages]));
const snapshot = {createdAt: new Date().toISOString(), origin, documents: [...pages].map(([url, page]) => ({url, title: page.title, text: page.text, links: graph.get(url).links, rank: graph.get(url).rank})), terms: Object.fromEntries(indexed), rank: {iterations, delta, damping: 0.85}};
fs.mkdirSync(path.resolve(dataDir), {recursive: true});
atomic(path.join(path.resolve(dataDir), 'search-snapshot.json'), snapshot);
console.log(JSON.stringify({pages: pages.size, terms: indexed.size, rankIterations: iterations, delta}));
