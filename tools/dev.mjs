import fs from 'node:fs';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {launch, ready, wait, cleanup} from './processes.mjs';

const run = path.resolve('.runs', `dev-${new Date().toISOString().replaceAll(':', '-')}`), children = [], token = randomUUID();
fs.mkdirSync(run, {recursive: true});
const start = (script, args, name, env = {}) => { const child = launch(run, script, args, name, {MAPREDUCE_TOKEN: token, ...env}); children.push(child); return child; };
let finish;
const interrupted = new Promise(resolve => { finish = resolve; });
process.once('SIGINT', finish); process.once('SIGTERM', finish);
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => { if (chunk.trim() === 'stop') finish(); });
try {
  const origin = (await ready(start('tools/fixture.mjs', [], 'fixture'))).origin;
  const data = path.join(run, 'coordinator');
  const coordinator = await ready(start('src/coordinator.mjs', ['--data', data, '--port', process.env.PORT ?? '8080'], 'coordinator'));
  const api = `http://127.0.0.1:${coordinator.port}`;
  for (const id of ['a', 'b']) start('src/worker.mjs', ['--url', api, '--id', `dev-worker-${id}`, '--data', path.join(run, `worker-${id}`)], `worker-${id}`, {FIXTURE_ORIGIN: origin});
  console.log(`Building owned fixture index. Logs: ${run}`);
  const builder = start('tools/build-index.mjs', [], 'builder', {FIXTURE_ORIGIN: origin, SEARCH_API: api, SEARCH_DATA: data});
  const result = await Promise.race([wait(builder, 190000), interrupted.then(() => null)]);
  if (result) {
    if (result.code !== 0) throw new Error(`index builder failed (${result.code}); see ${run}`);
    console.log(JSON.stringify({ready: true, url: `${api}/`, run, launcherPid: process.pid, coordinatorPid: coordinator.pid, childPids: children.map(child => child.pid)}));
    console.log('Press Ctrl+C or type stop then Enter to stop this session.');
    await Promise.race([interrupted, ...children.filter(child => child !== builder).map(child => child.finished.then(outcome => { throw new Error(`owned service ${child.pid} exited (${outcome.code})`); }))]);
  }
} finally { await cleanup(children); process.stdin.pause(); }
