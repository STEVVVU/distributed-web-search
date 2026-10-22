import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import {execFileSync} from 'node:child_process';
import {availableParallelism, cpus, totalmem, platform, release, loadavg} from 'node:os';
import {performance, monitorEventLoopDelay} from 'node:perf_hooks';
import {randomUUID} from 'node:crypto';
import {rpc, sleep} from '../src/common.mjs';
import {launch, ready, wait, cleanup, stop} from './processes.mjs';

const smoke = process.argv.includes('--smoke'), profile = process.argv.includes('--profile');
const run = path.resolve(process.env.BENCH_OUTPUT ?? `.runs/benchmark-${Date.now()}`);
fs.mkdirSync(run, {recursive: true});
const evidence = {passed: false, smoke, profile, schema: 1, claim: 'Owned graph index and query scaling, excluding arbitrary web relevance',
  source: execFileSync('git', ['rev-parse', 'HEAD'], {encoding: 'utf8'}).trim(), dirty: execFileSync('git', ['status', '--porcelain'], {encoding: 'utf8'}),
  environment: {node: process.version, platform: platform(), release: release(), cores: availableParallelism(), cpu: cpus()[0]?.model, memory: totalmem(), load: loadavg(), command: process.argv}, samples: []};
const quantiles = values => {
  const s = values.toSorted((a, b) => a - b), q = p => s.length ? s[Math.min(s.length - 1, Math.ceil(p * s.length) - 1)] : null;
  return {n: s.length, p50: q(.5), p95: q(.95), p99: q(.99), min: s[0] ?? null, max: s.at(-1) ?? null};
};
let active = [];
const hardDeadline = setTimeout(() => {
  for (const child of active) child.kill('SIGKILL');
  fs.writeFileSync(path.join(run, 'deadline.txt'), '270 minute campaign deadline'); process.exit(1);
}, 270 * 60_000);

async function cell(config) {
  const directory = path.join(run, `sample-${evidence.samples.length}`); fs.mkdirSync(directory);
  const stats = fs.statfsSync(directory); assert(Number(stats.bavail) * Number(stats.bsize) > 10 * 1024 ** 3, '10 GiB reserve required');
  const token = randomUUID(), children = []; active = children;
  const sample = {...config, passed: false, directory, queries: []}; evidence.samples.push(sample);
  const fixture = http.createServer((request, response) => {
    const id = request.url === '/' ? 0 : request.url === '/ranking' ? 1 : Number(/^\/p\/(\d+)$/.exec(request.url)?.[1]);
    if (!Number.isInteger(id) || id < 0 || id >= config.pages) {response.writeHead(404); response.end(); return;}
    const topic = id % 16;
    const links = id === 0 ? Array.from({length: config.pages - 1}, (_, i) => i + 1) : [0, 1].filter(target => target !== id);
    response.writeHead(200, {'content-type': 'text/html'});
    response.end(`<title>Document ${id}</title><body>topic${topic} common unique${id} ${'benchmark '.repeat(8)}${links.map(target => `<a href="${route(target)}"></a>`).join('')}</body>`);
  });
  const route = id => id === 0 ? '/' : id === 1 ? '/ranking' : `/p/${id}`;
  const start = (script, args, name, env = {}) => {
    const options = {MAPREDUCE_TOKEN: token, BENCH_RESOURCE_FILE: path.join(directory, `${name}-resource.jsonl`), NODE_OPTIONS: `--import=${new URL('./benchmark-resource.mjs', import.meta.url).href}`, ...env};
    const child = profile && name === 'coordinator' ? launch(directory, '--cpu-prof', [`--cpu-prof-dir=${directory}`, script, ...args], name, options) : launch(directory, script, args, name, options);
    children.push(child); return child;
  };
  try {
    await new Promise((done, reject) => {fixture.once('error', reject); fixture.listen(0, '127.0.0.1', done);});
    const origin = `http://127.0.0.1:${fixture.address().port}`;
    const data = path.join(directory, 'coordinator');
    const coordinator = start('src/coordinator.mjs', ['--data', data, '--port', '0'], 'coordinator');
    let api = `http://127.0.0.1:${(await ready(coordinator)).port}`;
    assert.equal((await rpc(`${api}/health`, token)).pid, coordinator.pid);
    for (let i = 0; i < config.workers; i++) start('src/worker.mjs', ['--url', api, '--id', `bench-${i}`, '--data', path.join(directory, `worker-${i}`)], `worker-${i}`, {FIXTURE_ORIGIN: origin});
    const at = performance.now();
    const builder = start('tools/build-index.mjs', [], 'builder', {FIXTURE_ORIGIN: origin, SEARCH_API: api, SEARCH_DATA: data, SEARCH_MAX_PAGES: String(config.pages), SEARCH_MAX_DEPTH: '4', SEARCH_SPLITS: '16', SEARCH_REDUCERS: '16', SEARCH_JOB_TIMEOUT_MS: '60000', SEARCH_BUILD_TIMEOUT_MS: '1800000'});
    assert.equal((await wait(builder, 1810000)).code, 0, `index build failed; inspect ${directory}`);
    sample.indexSeconds = (performance.now() - at) / 1000;
    sample.pagesPerSecond = config.pages / sample.indexSeconds;
    sample.build = JSON.parse(fs.readFileSync(path.join(directory, 'builder.log'), 'utf8').trim().split('\n').at(-1));
    const snapshotFile = path.join(data, 'search-snapshot.json'), snapshot = JSON.parse(fs.readFileSync(snapshotFile));
    sample.snapshotBytes = fs.statSync(snapshotFile).size;
    assert.equal(snapshot.documents.length, config.pages);
    const expected = new Map();
    for (let id = 0; id < config.pages; id++) {
      const url = origin + route(id), title = `Document ${id}`, text = `topic${id % 16} common unique${id} ${'benchmark '.repeat(8)}`.trim();
      expected.set(url, {id, title, text, links: id === 0 ? Array.from({length: config.pages - 1}, (_, i) => origin + route(i + 1)).sort() : [0, 1].filter(target => target !== id).map(target => origin + route(target)).sort()});
    }
    const postings = new Map();
    for (const [url, doc] of expected) {
      const counts = new Map();
      for (const term of `${doc.title} ${doc.text}`.toLowerCase().match(/[a-z0-9]+/g)) counts.set(term, (counts.get(term) ?? 0) + 1);
      for (const [term, tf] of counts) {if (!postings.has(term)) postings.set(term, []); postings.get(term).push({url, tf, title: doc.title, text: doc.text});}
    }
    assert.deepEqual(Object.keys(snapshot.terms).sort(), [...postings.keys()].sort());
    for (const [term, values] of postings) assert.deepEqual(snapshot.terms[term], values.sort((a, b) => a.url.localeCompare(b.url)));
    for (const doc of snapshot.documents) {const oracle = expected.get(doc.url); assert(oracle); assert.equal(doc.title, oracle.title); assert.equal(doc.text, oracle.text); assert.deepEqual(doc.links, oracle.links);}
    const urls = [...expected.keys()], ranks = {};
    for (const policy of ['global', 'topic']) {
      let vector = new Map(urls.map(url => [url, 1 / urls.length])), delta = Infinity;
      for (let iteration = 0; iteration < 10000 && delta > 1e-13; iteration++) {
        const next = new Map(urls.map(url => [url, .15 * (policy === 'global' ? 1 / urls.length : Number(url === origin + '/ranking'))]));
        for (const [from, doc] of expected) for (const to of doc.links) next.set(to, next.get(to) + .85 * vector.get(from) / doc.links.length);
        delta = urls.reduce((sum, url) => sum + Math.abs(next.get(url) - vector.get(url)), 0); vector = next;
      }
      assert(delta <= 1e-13); ranks[policy] = vector;
      for (const doc of snapshot.documents) assert(Math.abs(doc.ranks[policy] - vector.get(doc.url)) < 2e-10, 'independent rank mismatch');
    }
    const queries = ['unique7', 'topic3', 'common', 'benchmark common', 'zzmissing'];
    const responses = new Map();
    for (const policy of ['global', 'topic']) for (const query of queries) {
      const scores = new Map();
      for (const term of new Set(query.split(' '))) for (const posting of postings.get(term) ?? []) scores.set(posting.url, (scores.get(posting.url) ?? 0) + posting.tf * Math.log(1 + config.pages / postings.get(term).length));
      const order = [...scores].map(([url, lexical]) => ({url, score: lexical + ranks[policy].get(url)})).sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
      const response = await rpc(`${api}/api/search?q=${encodeURIComponent(query)}&policy=${policy}`, token);
      assert.deepEqual(response.results.map(doc => doc.url), order.map(doc => doc.url));
      responses.set(`${policy}/${query}`, response.results.map(doc => doc.url));
      sample.queries.push({policy, query, matches: order.length, exactOrder: true, precisionAt10: query === 'zzmissing' ? null : response.results.slice(0, 10).filter(doc => query === 'unique7' ? expected.get(doc.url).id === 7 : query === 'topic3' ? expected.get(doc.url).id % 16 === 3 : true).length / Math.min(10, response.results.length)});
    }
    sample.load = [];
    const loads = smoke ? [{rate: 10, seconds: .4}] : profile ? [{rate: 20, seconds: 5}] : [{rate: 1, seconds: 10}, {rate: 10, seconds: 10}, {rate: 100, seconds: 10}];
    if (config.stability) loads.splice(0, loads.length, {rate: 10, seconds: 120});
    for (const load of loads) {
      const requests = [], pending = new Set(), lag = monitorEventLoopDelay({resolution: 10}); lag.enable();
      const cpu = process.cpuUsage(), base = performance.now(); let bytes = 0, maxPending = 0;
      for (let i = 0; i < Math.ceil(load.rate * load.seconds); i++) {
        const scheduled = base + i * 1000 / load.rate; await sleep(Math.max(0, scheduled - performance.now()));
        const query = queries[i % queries.length], policy = i % 2 ? 'topic' : 'global', record = {query, policy, scheduled, latenessMs: performance.now() - scheduled}; requests.push(record);
        if (pending.size >= 256) {record.rejected = 'generator concurrency ceiling'; continue;}
        const task = (async () => {
          try {
            const response = await fetch(`${api}/api/search?q=${encodeURIComponent(query)}&policy=${policy}`, {signal: AbortSignal.timeout(5000)});
            record.status = response.status;
            const payload = await response.text(); bytes += Buffer.byteLength(payload);
            assert.equal(response.status, 200); const value = JSON.parse(payload);
            assert.deepEqual(value.results.map(doc => doc.url), responses.get(`${policy}/${query}`)); record.correct = true;
          } catch (error) {record.error = error.message; record.timeout = ['TimeoutError', 'AbortError'].includes(error.name);}
          finally {record.ms = performance.now() - scheduled;}
        })();
        pending.add(task); task.finally(() => pending.delete(task)); maxPending = Math.max(maxPending, pending.size);
      }
      await Promise.all(pending); lag.disable();
      sample.load.push({...load, requests, counts: {offered: requests.length, completed: requests.filter(r => r.correct).length, rejected: requests.filter(r => r.rejected).length, failed: requests.filter(r => r.error).length, timedOut: requests.filter(r => r.timeout).length}, latencyMs: quantiles(requests.filter(r => r.correct).map(r => r.ms)), arrivalLatenessMs: quantiles(requests.map(r => r.latenessMs)), responseBytes: bytes, maxPending, clientCpu: process.cpuUsage(cpu), clientRss: process.memoryUsage().rss, clientEventLoopP99Ms: lag.percentile(99) / 1e6});
      assert(requests.every(r => r.correct), 'load had errors/rejections; evidence retained');
    }
    for (const child of children.filter(c => c !== coordinator)) await stop(child);
    await stop(coordinator);
    const restartAt = performance.now();
    const restarted = start('src/coordinator.mjs', ['--data', data, '--port', '0'], 'restart');
    api = `http://127.0.0.1:${(await ready(restarted)).port}`;
    assert.deepEqual((await rpc(`${api}/api/search?q=common`, token)).results.map(doc => doc.url), responses.get('global/common'));
    sample.restartMs = performance.now() - restartAt;
    sample.jobCounts = Object.values(JSON.parse(fs.readFileSync(path.join(data, 'state.json'))).jobs).reduce((counts, job) => {counts[job.status] = (counts[job.status] ?? 0) + 1; return counts;}, {});
    sample.passed = true;
  } finally {
    await new Promise(done => {fixture.close(done); fixture.closeAllConnections();});
    await cleanup(children); sample.cleanup = true; active = [];
    fs.writeFileSync(path.join(directory, 'sample.json'), JSON.stringify(sample, null, 2));
  }
}
try {
  if (smoke) await cell({pages: 8, workers: 2, repeat: 0});
  else if (profile) await cell({pages: 1000, workers: 2, repeat: 0});
  else {
    for (let repeat = 0; repeat < 5; repeat++) for (const pages of [32, 1000, 5000]) for (const workers of repeat % 2 ? [4, 2, 1] : [1, 2, 4]) await cell({pages, workers, repeat});
    await cell({pages: 1000, workers: 2, repeat: 0, stability: true});
  }
  evidence.passed = true;
} catch (error) {evidence.error = error.stack; process.exitCode = 1;}
finally {clearTimeout(hardDeadline); fs.writeFileSync(path.join(run, 'evidence.json'), JSON.stringify(evidence, null, 2)); console.log(JSON.stringify({passed: evidence.passed, run}));}
