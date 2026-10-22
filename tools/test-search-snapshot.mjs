import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {atomic, rpc} from '../src/common.mjs';
import {launch, ready, stop, cleanup} from './processes.mjs';

const run = path.resolve(`.runs/snapshot-contract-${Date.now()}`), data = path.join(run, 'coordinator');
const file = path.join(data, 'search-snapshot.json'), token = randomUUID(), children = [];
fs.mkdirSync(data, {recursive: true});
const evidence = {passed: false, checks: []};
let coordinator, api;
async function start() {
  coordinator = launch(run, 'src/coordinator.mjs', ['--data', data, '--port', '0'], `coordinator-${children.length}`, {MAPREDUCE_TOKEN: token});
  children.push(coordinator); api = `http://127.0.0.1:${(await ready(coordinator)).port}`;
}
const call = route => rpc(api + route, token);
const doc = {url: 'http://127.0.0.1/document', title: 'First', text: 'hello', links: [], rank: 1, ranks: {global: 1, topic: .25}};
const snapshot = title => ({createdAt: '2026-10-22T00:00:00.000Z', documents: [{...doc, title}, {...doc, title: 'Other'}],
  terms: {hello: [{url: doc.url, tf: 1, title, text: doc.text}]}});
async function check(title) {
  assert.deepEqual(await call('/api/status'), {ready: true, pages: 2, createdAt: '2026-10-22T00:00:00.000Z'});
  for (const policy of ['global', 'topic']) {
    const lexical = Math.log(3), rank = doc.ranks[policy];
    assert.deepEqual(await call(`/api/search?q=hello&policy=${policy}`), {query: 'hello', terms: ['hello'], policy,
      results: [{...doc, title, rank, lexical, score: lexical + rank, snippet: 'hello'}]});
  }
}
try {
  await start();
  assert.deepEqual(await call('/api/status'), {ready: false, pages: 0, createdAt: null});
  await assert.rejects(call('/api/search?q=hello'), error => error.status === 503);
  atomic(file, snapshot('First')); fs.utimesSync(file, 946684800, 946684800);
  await check('First');
  const before = fs.statSync(file, {bigint: true});
  atomic(file, snapshot('Fresh')); fs.utimesSync(file, 946684800, 946684800);
  const after = fs.statSync(file, {bigint: true});
  assert.equal(before.size, after.size); assert.equal(before.mtimeNs, after.mtimeNs);
  assert.notEqual(before.ino, after.ino);
  await check('Fresh');
  evidence.checks.push('atomic same-length/same-mtime replacement refreshes status and both policies; duplicate URL retains first match');
  fs.unlinkSync(file);
  assert.deepEqual(await call('/api/status'), {ready: false, pages: 0, createdAt: null});
  await assert.rejects(call('/api/search?q=hello'), error => error.status === 503);
  atomic(file, snapshot('Again')); await check('Again');
  evidence.checks.push('removal clears availability; recreation reloads actual data');
  const corrupt = file + '.corrupt'; fs.writeFileSync(corrupt, '{'); fs.renameSync(corrupt, file);
  for (const route of ['/api/status', '/api/search?q=hello']) await assert.rejects(call(route), error => error.status === 500);
  await stop(coordinator); await start();
  for (const route of ['/api/status', '/api/search?q=hello']) await assert.rejects(call(route), error => error.status === 500);
  atomic(file, snapshot('Fixed')); await check('Fixed');
  await stop(coordinator); await start(); await check('Fixed');
  evidence.checks.push('corrupt replacement and corrupt restart fail visibly; repaired snapshot and valid restart recover');
  evidence.passed = true;
} catch (error) { evidence.error = error.stack; process.exitCode = 1; }
finally {
  try { await cleanup(children); evidence.cleanup = true; }
  catch (error) { evidence.passed = false; evidence.cleanup = false; evidence.cleanupError = error.stack; process.exitCode = 1; }
  fs.writeFileSync(path.join(run, 'evidence.json'), JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify({...evidence, run}));
}
