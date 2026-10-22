import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import {execFileSync} from 'node:child_process';
import {availableParallelism, cpus, totalmem, platform, release, loadavg} from 'node:os';
import {performance} from 'node:perf_hooks';
import {randomUUID} from 'node:crypto';
import {rpc} from '../src/common.mjs';
import {launch, ready, wait, cleanup, stop} from './processes.mjs';
import {assertRankedResults, k6Counts} from './search-load-summary.mjs';

const smoke = process.argv.includes('--smoke'), profile = process.argv.includes('--profile');
const run = path.resolve(process.env.BENCH_OUTPUT ?? `.runs/benchmark-${Date.now()}`);
fs.mkdirSync(run, {recursive: true});
const k6Binary = process.env.K6_BINARY ?? 'k6';
const k6Version = execFileSync(k6Binary, ['version'], {encoding: 'utf8'}).trim();
assert(/^k6(?:\.exe)? v2\.3\.0\b/.test(k6Version), 'the frozen benchmark requires verified k6 v2.3.0');
const evidence = {passed: false, smoke, profile, schema: 1, claim: 'Owned graph index and query scaling, excluding arbitrary web relevance',
  source: execFileSync('git', ['rev-parse', 'HEAD'], {encoding: 'utf8'}).trim(), dirty: execFileSync('git', ['status', '--porcelain'], {encoding: 'utf8'}),
  environment: {node: process.version, k6: k6Version, platform: platform(), release: release(), cores: availableParallelism(), cpu: cpus()[0]?.model, memory: totalmem(), load: loadavg(), command: process.argv}, samples: []};
let active = [];
const hardDeadline = setTimeout(() => {
  for (const child of active) {
    if (child.exitCode !== null || child.signalCode !== null) continue;
    try {
      if (child.ownedProcessGroup) process.kill(-child.pid, 'SIGKILL');
      else child.kill('SIGKILL');
    } catch (error) {
      if (error.code !== 'ESRCH') fs.appendFileSync(path.join(run, 'deadline-cleanup-error.txt'), error.stack + '\n');
    }
  }
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
      const order = [...scores].map(([url, lexical]) => {
        const doc = expected.get(url), match = doc.text.toLowerCase().indexOf(query.split(' ')[0]);
        const start = Math.max(0, match - 70);
        return {url, title: doc.title, text: doc.text, links: doc.links,
          ranks: {global: ranks.global.get(url), topic: ranks.topic.get(url)}, rank: ranks[policy].get(url),
          lexical, score: lexical + ranks[policy].get(url), snippet: doc.text.slice(start, start + 180)};
      }).sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
      const response = await rpc(`${api}/api/search?q=${encodeURIComponent(query)}&policy=${policy}`, token);
      assertRankedResults(response.results, order);
      responses.set(`${policy}/${query}`, order);
      sample.queries.push({policy, query, matches: order.length, exactOrder: true, precisionAt10: query === 'zzmissing' ? null : response.results.slice(0, 10).filter(doc => query === 'unique7' ? expected.get(doc.url).id === 7 : query === 'topic3' ? expected.get(doc.url).id % 16 === 3 : true).length / Math.min(10, response.results.length)});
    }
    const oracleFile = path.join(directory, 'independent-query-oracle.json');
    fs.writeFileSync(oracleFile, JSON.stringify(Array.from({length: 10}, (_, i) => {
      const query = queries[i % queries.length], policy = i % 2 ? 'topic' : 'global';
      return {query, terms: query.split(' '), policy, results: responses.get(`${policy}/${query}`)};
    })));
    sample.load = [];
    const loads = smoke ? [{rate: 10, seconds: 1}] : profile ? [{rate: 100, seconds: 20}] : [{rate: 1, seconds: 10}, {rate: 10, seconds: 10}, {rate: 100, seconds: 10}];
    if (config.stability) loads.splice(0, loads.length, {rate: 10, seconds: 120});
    for (const load of loads) {
      const tag = `k6-${load.rate}-${load.seconds}`, summaryFile = path.join(directory, `${tag}-summary.json`), usageFile = path.join(directory, `${tag}.usage`);
      const args = ['run', '--quiet', '--no-usage-report', '--out', `json=${path.join(directory, `${tag}-raw.json.gz`)}`, path.resolve('tools/k6-search.js')];
      const nativeTime = process.platform === 'linux';
      const command = nativeTime ? ['-f', '%M %U %S', '-o', usageFile, k6Binary, ...args] : args;
      const child = launch(directory, command[0], command.slice(1), tag,
        {SEARCH_API: api, SEARCH_RATE: String(load.rate), SEARCH_SECONDS: String(load.seconds), SEARCH_ORACLE: oracleFile, SEARCH_SUMMARY: summaryFile},
        {executable: nativeTime ? '/usr/bin/time' : k6Binary, detached: nativeTime});
      children.push(child);
      const outcome = await wait(child, (load.seconds + 30) * 1000);
      assert(fs.existsSync(summaryFile), `k6 did not write a summary; inspect ${tag}.log`);
      const summary = JSON.parse(fs.readFileSync(summaryFile));
      const counts = k6Counts(summary);
      assert(counts.offered > 0 && counts.started > 0, 'k6 must execute real offered requests');
      const result = {...load, driver: 'k6-v2.3.0', maxVUs: 256, counts, summary: path.basename(summaryFile),
        raw: `${tag}-raw.json.gz`, measurementComplete: outcome.code === 0,
        correctOutputs: counts.integrityErrors === 0, zeroLossEnvelope: counts.offered === counts.completed,
        timingBoundary: 'k6 http_req_duration is sending+waiting+receiving for issued requests; it excludes connection/setup and dropped iterations, not scheduled-to-completion latency'};
      if (nativeTime) {
        const fields = fs.readFileSync(usageFile, 'utf8').trim().split('\n').at(-1).split(/\s+/);
        result.generator = {peakRssKiB: Number(fields[0]), userCpuSeconds: Number(fields[1]), systemCpuSeconds: Number(fields[2])};
      }
      sample.load.push(result);
      assert(result.correctOutputs, 'a completed response violated the independent output oracle; k6 evidence retained');
      assert.equal(outcome.code, 0, 'k6 failed; inspect its retained summary/log/raw metrics');
    }
    for (const child of children.filter(c => c !== coordinator)) await stop(child);
    await stop(coordinator);
    const restartAt = performance.now();
    const restarted = start('src/coordinator.mjs', ['--data', data, '--port', '0'], 'restart');
    api = `http://127.0.0.1:${(await ready(restarted)).port}`;
    assertRankedResults((await rpc(`${api}/api/search?q=common`, token)).results, responses.get('global/common'));
    sample.restartMs = performance.now() - restartAt;
    sample.jobCounts = Object.values(JSON.parse(fs.readFileSync(path.join(data, 'state.json'))).jobs).reduce((counts, job) => {counts[job.status] = (counts[job.status] ?? 0) + 1; return counts;}, {});
    sample.passed = true;
    sample.measurementComplete = true;
    sample.correctOutputs = true;
    sample.zeroLossEnvelope = sample.load.every(load => load.zeroLossEnvelope);
  } finally {
    await new Promise(done => {fixture.close(done); fixture.closeAllConnections();});
    await cleanup(children); sample.cleanup = true; active = [];
    fs.writeFileSync(path.join(directory, 'sample.json'), JSON.stringify(sample, null, 2));
  }
}
try {
  if (smoke) await cell({pages: 8, workers: 2, repeat: 0});
  else if (profile) await cell({pages: 5000, workers: 1, repeat: 0});
  else {
    for (let repeat = 0; repeat < 5; repeat++) for (const pages of [32, 1000, 5000]) for (const workers of repeat % 2 ? [4, 2, 1] : [1, 2, 4]) await cell({pages, workers, repeat});
    await cell({pages: 1000, workers: 2, repeat: 0, stability: true});
  }
  evidence.passed = true;
  evidence.measurementComplete = true;
  evidence.correctOutputs = true;
  evidence.zeroLossEnvelope = evidence.samples.every(sample => sample.zeroLossEnvelope);
  evidence.verdict = evidence.zeroLossEnvelope ? 'measurement-complete-zero-loss-at-offered-rates' : 'measurement-complete-with-overload';
} catch (error) {evidence.error = error.stack; process.exitCode = 1;}
finally {clearTimeout(hardDeadline); fs.writeFileSync(path.join(run, 'evidence.json'), JSON.stringify(evidence, null, 2)); console.log(JSON.stringify({passed: evidence.passed, run}));}
