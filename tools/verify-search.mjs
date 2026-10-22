import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {atomic, rpc, sleep} from '../src/common.mjs';
import {launch, ready, wait, stop, cleanup} from './processes.mjs';

const run = path.resolve('.runs', `search-${new Date().toISOString().replaceAll(':', '-')}`), token = randomUUID(), children = [];
fs.mkdirSync(run, {recursive: true});
const evidence = {passed: false, node: process.version, run, actions: [], checks: []};
const deadline = Date.now() + 180000;
const remaining = () => { const ms = deadline - Date.now(); assert(ms > 0, 'verification exceeded 180000 ms deadline'); return ms; };
let coordinator, api, origin, failure;
const start = (script, args, name, env = {}) => { remaining(); const child = launch(run, script, args, name, {MAPREDUCE_TOKEN: token, ...env}); children.push(child); return child; };
const call = (route, method = 'GET', data) => { remaining(); return rpc(`${api}${route}`, token, method, data); };
const dataDir = path.join(run, 'coordinator'), snapshotFile = path.join(dataDir, 'search-snapshot.json');
async function startCoordinator() {
  coordinator = start('src/coordinator.mjs', ['--data', dataDir, '--port', '0'], `coordinator-${children.length}`);
  api = `http://127.0.0.1:${(await ready(coordinator)).port}`;
  assert.equal((await call('/health')).pid, coordinator.pid);
}
async function job(module, keys) {
  const {id} = await call('/jobs', 'POST', {module, keys, splits: 1, reducers: 2});
  const until = Math.min(Date.now() + 20000, deadline);
  while (Date.now() < until) {
    const state = await call(`/jobs/${id}`);
    if (['failed', 'done'].includes(state.status)) return state;
    await sleep(50);
  }
  throw new Error(`${module} verification job ${id} deadline exceeded`);
}
try {
  origin = (await ready(start('tools/fixture.mjs', [], 'fixture'))).origin;
  await startCoordinator();
  if (process.env.VERIFY_FAIL_AFTER_START === '1') throw new Error('injected verifier failure');
  await assert.rejects(call('/api/search?q=search'), error => error.status === 503 && /not ready/.test(error.message));
  const noWorkers = start('tools/build-index.mjs', [], 'no-workers', {FIXTURE_ORIGIN: origin, SEARCH_API: api, SEARCH_DATA: dataDir, SEARCH_JOB_TIMEOUT_MS: '350'});
  assert.equal((await wait(noWorkers, 6000)).code, 1);
  assert.match(fs.readFileSync(path.join(run, 'no-workers.log'), 'utf8'), /exceeded deadline.*worker availability/);
  assert(!fs.existsSync(snapshotFile), 'failed build published a snapshot');
  atomic(snapshotFile, {createdAt: new Date().toISOString(), documents: [], terms: {}});
  for (const term of ['constructor', 'prototype', 'toString', 'hasOwnProperty', '__proto__']) assert.deepEqual((await call(`/api/search?q=${encodeURIComponent(term)}`)).results, []);
  evidence.checks.push({missingIndex: 503, inheritedKeysOnEmptyIndex: 'passed', noWorkerBuild: 'deadline failed without publication'});
  const workers = ['a', 'b'].map(id => start('src/worker.mjs', ['--url', api, '--id', `search-worker-${id}`, '--data', path.join(run, `worker-${id}`)], `worker-${id}`, {FIXTURE_ORIGIN: origin}));
  const builder = start('tools/build-index.mjs', [], 'builder', {FIXTURE_ORIGIN: origin, SEARCH_API: api, SEARCH_DATA: dataDir});
  assert.equal((await wait(builder, remaining())).code, 0, `builder failed; inspect ${run}`);
  const durable = JSON.parse(fs.readFileSync(snapshotFile, 'utf8'));
  const fixture = new Map([
    ['/', ['Distributed Systems', 'Search coordinates many machines. MapReduce Ranking Notes Duplicate Outside Malformed', ['/mapreduce', '/notes', '/ranking']]],
    ['/mapreduce', ['MapReduce Runtime', 'Search distributes map and reduce work across workers. Home Ranking', ['/', '/ranking']]],
    ['/ranking', ['Ranking Research', 'Search uses an inverted index and PageRank. MapReduce Notes Archive', ['/dangling', '/mapreduce', '/notes']]],
    ['/notes', ['Research Notes', 'Search repairs malformed HTML. Ranking Unclosed paragraph', ['/ranking']]],
    ['/dangling', ['Archive', 'Search preserves dangling pages and constructor prototype tostring hasownproperty terms.', []]]
  ].map(([url, [title, text, links]]) => [`${origin}${url}`, {title, text, links: links.map(link => `${origin}${link}`)}]));
  const urls = [...fixture.keys()].sort();
  assert.deepEqual(durable.documents.map(doc => doc.url).sort(), urls);
  for (const doc of durable.documents) {
    assert.deepEqual({title: doc.title, text: doc.text, links: doc.links}, fixture.get(doc.url));
  }
  const postings = new Map();
  for (const [url, doc] of fixture) {
    const counts = new Map();
    for (const term of `${doc.title} ${doc.text}`.toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? []) counts.set(term, (counts.get(term) ?? 0) + 1);
    for (const [term, tf] of counts) { if (!postings.has(term)) postings.set(term, []); postings.get(term).push({url, tf, title: doc.title, text: doc.text}); }
  }
  assert.deepEqual(Object.keys(durable.terms).sort(), [...postings.keys()].sort(), 'missing or extra index terms');
  for (const [term, expected] of postings) assert.deepEqual(durable.terms[term], expected.sort((a, b) => a.url.localeCompare(b.url)), `posting mismatch for ${term}`);
  const oracleRanks = {};
  for (const policy of ['global', 'topic']) {
    const teleport = urls.map(url => policy === 'global' ? 1 / urls.length : Number(url === `${origin}/ranking`));
    const transition = urls.map(from => urls.map((to, j) => {
      const links = fixture.get(from).links;
      return 0.15 * teleport[j] + 0.85 * (links.length ? Number(links.includes(to)) / links.length : teleport[j]);
    }));
    let vector = urls.map(() => 1 / urls.length), delta = Infinity, iterations = 0;
    while (delta > 1e-14 && iterations++ < 10000) {
      const next = urls.map((_, j) => vector.reduce((sum, rank, i) => sum + rank * transition[i][j], 0));
      delta = next.reduce((sum, rank, i) => sum + Math.abs(rank - vector[i]), 0); vector = next;
    }
    assert(delta <= 1e-14, 'independent oracle failed to converge');
    oracleRanks[policy] = new Map(urls.map((url, i) => [url, vector[i]]));
    assert(durable.rank.profiles[policy].iterations > 2, 'fixture did not exercise repeated rank iteration');
    assert(durable.rank.profiles[policy].delta <= 1e-10);
    for (const doc of durable.documents) assert(Math.abs(doc.ranks[policy] - oracleRanks[policy].get(doc.url)) < 2e-10, `${policy} PageRank mismatch for ${doc.url}`);
    assert(Math.abs(vector.reduce((sum, rank) => sum + rank, 0) - 1) < 1e-12);
  }
  assert(Math.max(...oracleRanks.global.values()) - Math.min(...oracleRanks.global.values()) > 0.1, 'global ranks are uniform');
  assert(oracleRanks.topic.get(`${origin}/ranking`) > oracleRanks.global.get(`${origin}/ranking`) + .05, 'topic policy had no meaningful effect');
  const orders = {};
  for (const policy of ['global', 'topic']) for (const query of ['search', 'mapreduce ranking', 'constructor', 'prototype', 'toString', 'hasOwnProperty', '__proto__', 'zzunknown', '', 'search search']) {
    const scores = new Map();
    for (const term of new Set(query.toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? [])) {
      const termPostings = postings.get(term) ?? [];
      for (const {url, tf} of termPostings) scores.set(url, (scores.get(url) ?? 0) + tf * Math.log(1 + urls.length / termPostings.length));
    }
    const expected = [...scores].map(([url, lexical]) => ({url, lexical, score: lexical + oracleRanks[policy].get(url)})).sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
    const actual = (await call(`/api/search?q=${encodeURIComponent(query)}&policy=${policy}`)).results;
    assert.deepEqual(actual.map(item => item.url), expected.map(item => item.url), `query order for ${policy}/${query}`);
    for (let i = 0; i < actual.length; i++) { assert(Math.abs(actual[i].score - expected[i].score) < 2e-10); assert.equal(actual[i].lexical, expected[i].lexical); }
    if (query === 'search') orders[policy] = actual.map(item => item.url);
  }
  for (const query of ['x'.repeat(513), Array.from({length: 17}, (_, i) => `term${i}`).join(' ')]) await assert.rejects(call(`/api/search?q=${encodeURIComponent(query)}`), error => error.status === 400);
  await assert.rejects(call('/api/search?policy=constructor'), error => error.status === 400);
  assert.equal((await call(`/api/search?q=${'x'.repeat(512)}`)).results.length, 0);
  const preserved = fs.readFileSync(snapshotFile, 'utf8'), rejections = [];
  for (const [suffix, message] of [['/failure', 'HTTP 500'], ['/redirect', 'fetch failed'], ['/oversized', 'exceeds 262144 bytes'], ['/plain', 'expected HTML'], ['/slow', 'timeout'], ['/missing', 'HTTP 404']]) {
    const state = await job('crawl-page', [[`${origin}${suffix}`, null]]);
    assert.equal(state.status, 'failed', `${suffix} was accepted`);
    const errors = state.events.filter(event => event.type === 'error').map(event => event.error);
    assert(errors.some(error => error.toLowerCase().includes(message.toLowerCase())), `missing rejection reason for ${suffix}: ${errors}`);
    assert.equal(fs.readFileSync(snapshotFile, 'utf8'), preserved);
    rejections.push({url: `${origin}${suffix}`, status: state.status, errors});
  }
  const external = await job('crawl-page', [['https://example.org/never-requested', null]]);
  assert.equal(external.status, 'failed');
  assert(external.events.some(event => event.error?.includes('outside configured fixture')));
  const state = JSON.parse(fs.readFileSync(path.join(dataDir, 'state.json'), 'utf8'));
  const owners = new Set(Object.values(state.jobs).flatMap(job => job.events.filter(event => event.type === 'assigned').map(event => event.worker)));
  assert(owners.has('search-worker-a') && owners.has('search-worker-b'));
  evidence.actions.push({pages: urls.length, jobs: Object.keys(state.jobs).length, workerOwners: [...owners], rejections});
  evidence.checks.push({graph: durable.documents.map(({url, links}) => ({url, links})), profiles: durable.rank.profiles, exactPostingTerms: postings.size, queryOrders: orders, independentOracle: 'dense transition matrix and fixture-only postings'});
  for (const worker of workers) await stop(worker);
  await stop(coordinator); await startCoordinator();
  assert.equal((await call('/api/status')).pages, 5);
  for (const policy of ['global', 'topic']) assert.deepEqual((await call(`/api/search?q=search&policy=${policy}`)).results.map(item => item.url), orders[policy]);
  evidence.checks.push({restartSnapshot: 'retained', pages: 5});
} catch (error) { failure = error; evidence.error = error.stack; }
finally {
  try { await cleanup(children); evidence.cleanup = true; } catch (error) { evidence.cleanup = false; evidence.cleanupError = error.stack; failure = failure ? new AggregateError([failure, error], 'verification and cleanup failed') : error; }
  evidence.passed = !failure;
  fs.writeFileSync(path.join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
  assert.equal(JSON.parse(fs.readFileSync(path.join(run, 'evidence.json'), 'utf8')).passed, !failure);
  console.log(`Evidence: ${path.join(run, 'evidence.json')}`);
}
if (failure) throw failure;
console.log(JSON.stringify({passed: true, cleanup: evidence.cleanup, run}));
