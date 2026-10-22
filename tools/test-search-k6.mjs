import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash, randomUUID} from 'node:crypto';
import {launch, ready, wait, cleanup} from './processes.mjs';
import {assertRankedResults, k6Counts} from './search-load-summary.mjs';

const run = path.resolve(`.runs/k6-contract-${Date.now()}`), children = [];
const data = path.join(run, 'coordinator'), snapshotFile = path.join(data, 'search-snapshot.json');
fs.mkdirSync(data, {recursive: true});
const doc = {url: 'http://127.0.0.1/document', title: 'Doc', text: 'hello', links: [], rank: 1, ranks: {global: 1, topic: 1}};
const snapshot = {documents: [doc], terms: {hello: [{url: doc.url, tf: 1, title: doc.title, text: doc.text}]}};
fs.writeFileSync(snapshotFile, JSON.stringify(snapshot));
const result = {passed: false, controls: []};
try {
  const coordinator = launch(run, 'src/coordinator.mjs', ['--data', data, '--port', '0'], 'coordinator', {MAPREDUCE_TOKEN: randomUUID()});
  children.push(coordinator);
  const api = `http://127.0.0.1:${(await ready(coordinator)).port}`;
  const response = await fetch(`${api}/api/search?q=hello&policy=global`, {signal: AbortSignal.timeout(5000)});
  assert.equal(response.status, 200);
  const body = await response.text(), lexical = Math.log(2);
  assertRankedResults(JSON.parse(body), {query: 'hello', terms: ['hello'], policy: 'global',
    results: [{...doc, lexical, score: lexical + 1, snippet: 'hello'}]});
  const sha256 = createHash('sha256').update(body).digest('hex');
  for (const control of ['valid', 'wrong-hash', 'changed-body']) {
    if (control === 'changed-body') fs.writeFileSync(snapshotFile, JSON.stringify({...snapshot, documents: [{...doc, title: 'Changed'}]}));
    const oracle = path.join(run, `${control}-oracle.json`), summary = path.join(run, `${control}-summary.json`);
    fs.writeFileSync(oracle, JSON.stringify([{query: 'hello', policy: 'global', sha256: control === 'wrong-hash' ? '0'.repeat(64) : sha256}]));
    const child = launch(run, 'run', ['--quiet', '--no-usage-report', '--out', `json=${path.join(run, `${control}-raw.json.gz`)}`, path.resolve('tools/k6-search.js')], control,
      {SEARCH_API: api, SEARCH_RATE: '1', SEARCH_SECONDS: '1', SEARCH_ORACLE: oracle, SEARCH_SUMMARY: summary},
      {executable: process.env.K6_BINARY ?? 'k6'});
    children.push(child);
    const outcome = await wait(child, 20000), counts = k6Counts(JSON.parse(fs.readFileSync(summary)));
    assert(counts.started > 0);
    assert.equal(counts.interrupted, 0);
    assert.equal(counts.integrityErrors, control === 'valid' ? 0 : counts.started);
    assert.equal(counts.completed, control === 'valid' ? counts.started : 0);
    assert.equal(outcome.code, control === 'valid' ? 0 : 99, 'native zero-mismatch threshold must fail wrong responses');
    result.controls.push({control, counts, exitCode: outcome.code});
  }
  result.passed = true;
} catch (error) { result.error = error.stack; process.exitCode = 1; }
finally {
  await cleanup(children); result.cleanup = true;
  fs.writeFileSync(path.join(run, 'evidence.json'), JSON.stringify(result, null, 2));
  console.log(JSON.stringify({...result, run}));
}
