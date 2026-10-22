import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import net from 'node:net';
import {createHash, randomUUID} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {atomic, body, config, ensure, pairs, read, bucket} from './common.mjs';

const options = config(), root = path.resolve(options.data ?? 'data/coordinator');
const port = Number(options.port ?? 8080), leaseMs = Number(options.lease ?? 10000);
ensure(Number.isInteger(port) && port >= 0 && port <= 65535, 'invalid port');
ensure(Number.isInteger(leaseMs) && leaseMs >= 100 && leaseMs <= 600000, 'lease must be 100..600000 ms');
fs.mkdirSync(root, {recursive: true});
ensure(['win32', 'linux'].includes(process.platform), 'coordinator ownership requires Windows or Linux');
const canonicalRoot = fs.realpathSync.native(root);
const identity = process.platform === 'win32' ? canonicalRoot.toLowerCase() : canonicalRoot;
const ownerName = `mapreduce-${createHash('sha256').update(identity).digest('hex')}`;
const ownerEndpoint = process.platform === 'win32' ? `\\\\?\\pipe\\${ownerName}` : `\0${ownerName}`;
const ownerGuard = net.createServer(socket => socket.destroy());
await new Promise((resolve, reject) => {
  ownerGuard.once('error', reject);
  ownerGuard.listen(ownerEndpoint, () => { ownerGuard.off('error', reject); resolve(); });
});
ownerGuard.on('error', error => { console.error('Coordinator ownership failed', error); process.exit(1); });
const lock = path.join(root, 'owner.json');
if (fs.existsSync(lock)) {
  const owner = read(lock);
  let alive = true;
  try { process.kill(owner.pid, 0); } catch (error) { if (error.code === 'ESRCH') alive = false; else throw error; }
  ensure(!alive, 'data directory already has a live coordinator');
  fs.unlinkSync(lock);
}
fs.writeFileSync(lock, JSON.stringify({pid: process.pid}), {flag: 'wx'});
const stateFile = path.join(root, 'state.json');
const snapshotFile = path.join(root, 'search-snapshot.json');
const state = fs.existsSync(stateFile) ? read(stateFile) : {jobs: {}};
function persist(file, value) {
  try { atomic(file, value); }
  catch (error) { console.error('Persistence failed; refusing further requests', error); process.exit(1); }
}
const save = () => persist(stateFile, state);
for (const job of Object.values(state.jobs)) for (const task of job.tasks) if (task.status === 'running') task.status = 'pending';
save();
const artifact = task => path.join(root, 'attempts', `${task.accepted}.json`);
const taskList = count => Array.from({length: count}, (_, index) => ({index, status: 'pending', attempts: 0}));
function advance(job) {
  if (job.status === 'map' && job.tasks.every(t => t.status === 'done')) {
    job.maps = job.tasks; job.tasks = taskList(job.reducers); job.status = 'reduce';
  }
  if (job.status === 'reduce' && job.tasks.every(t => t.status === 'done')) {
    const output = job.tasks.flatMap(t => read(artifact(t))).sort((a, b) => a[0].localeCompare(b[0]));
    persist(path.join(root, 'results', `${job.id}.json`), output);
    job.status = 'done';
  }
}
function claim(worker) {
  for (const job of Object.values(state.jobs)) {
    if (!['map', 'reduce'].includes(job.status)) continue;
    for (const task of job.tasks) {
      if (task.status === 'running' && task.until <= Date.now()) { task.status = 'pending'; job.events.push({type: 'expired', token: task.token}); }
      if (task.status !== 'pending') continue;
      if (task.attempts >= 4) { job.status = 'failed'; job.error = 'task exceeded four attempts'; save(); break; }
      Object.assign(task, {status: 'running', worker, token: randomUUID(), until: Date.now() + leaseMs, attempts: task.attempts + 1});
      job.events.push({type: 'assigned', phase: job.status, index: task.index, worker, token: task.token});
      save();
      return {job: job.id, module: job.module, phase: job.status, index: task.index, token: task.token, reducers: job.reducers, maps: job.maps?.length ?? 0, leaseMs};
    }
  }
  return null;
}
const server = http.createServer(async (req, res) => {
  try {
    const pathname = new URL(req.url, 'http://localhost').pathname;
    if (req.method === 'GET' && pathname === '/') {
      const html = fs.readFileSync(fileURLToPath(new URL('../web/index.html', import.meta.url)));
      res.writeHead(200, {'content-type': 'text/html; charset=utf-8'}); res.end(html); return;
    }
    if (req.method === 'GET' && pathname === '/api/status') {
      const snapshot = fs.existsSync(snapshotFile) ? read(snapshotFile) : null;
      res.writeHead(200, {'content-type': 'application/json'}); res.end(JSON.stringify({ready: Boolean(snapshot), pages: snapshot?.documents.length ?? 0, createdAt: snapshot?.createdAt ?? null})); return;
    }
    if (req.method === 'GET' && pathname === '/api/search') {
      const snapshot = fs.existsSync(snapshotFile) ? read(snapshotFile) : null;
      ensure(snapshot, 'search index is not ready', 503);
      const params = new URL(req.url, 'http://localhost').searchParams;
      const query = params.get('q') ?? '', policy = params.get('policy') ?? 'global';
      ensure(query.length <= 512, 'query exceeds 512 characters');
      ensure(['global', 'topic'].includes(policy), 'unknown ranking policy');
      const terms = query.toLocaleLowerCase().match(/[\p{L}\p{N}]+/gu) ?? [];
      ensure(new Set(terms).size <= 16, 'query exceeds 16 distinct terms');
      const scores = new Map();
      for (const term of new Set(terms)) for (const posting of Object.hasOwn(snapshot.terms, term) ? snapshot.terms[term] : []) {
        const score = Math.log(1 + snapshot.documents.length / snapshot.terms[term].length) * posting.tf;
        scores.set(posting.url, (scores.get(posting.url) ?? 0) + score);
      }
      const results = [...scores].map(([url, lexical]) => {
        const doc = snapshot.documents.find(item => item.url === url);
        const match = doc.text.toLocaleLowerCase().indexOf(terms[0] ?? '');
        const rank = policy === 'global' ? doc.rank : doc.ranks?.topic;
        ensure(Number.isFinite(rank), 'ranking policy unavailable; rebuild index', 503);
        return {...doc, rank, lexical, score: lexical + rank, snippet: doc.text.slice(Math.max(0, match - 70), Math.max(0, match - 70) + 180)};
      }).sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
      res.writeHead(200, {'content-type': 'application/json'}); res.end(JSON.stringify({query, terms, policy, results})); return;
    }
    ensure(req.headers.authorization === `Bearer ${process.env.MAPREDUCE_TOKEN}`, 'unauthorized', 401);
    const parts = pathname.split('/').filter(Boolean);
    let result;
    if (req.method === 'GET' && parts.join('/') === 'health') result = {pid: process.pid, version: 1};
    else if (req.method === 'POST' && parts.join('/') === 'jobs') {
      const input = await body(req);
      ensure(input && typeof input === 'object' && !Array.isArray(input), 'expected job object');
      ensure(/^[a-z][a-z0-9-]{0,63}$/.test(input.module), 'invalid module name');
      ensure(fs.existsSync(fileURLToPath(new URL(`../jobs/${input.module}.mjs`, import.meta.url))), 'unknown trusted module');
      pairs(input.keys);
      const splits = input.splits ?? 3, reducers = input.reducers ?? 3;
      ensure(Number.isInteger(splits) && splits >= 1 && splits <= 64 && Number.isInteger(reducers) && reducers >= 1 && reducers <= 64, 'partition counts must be 1..64');
      const id = randomUUID(), chunks = Array.from({length: splits}, () => []);
      input.keys.forEach((pair, i) => chunks[i % splits].push(pair));
      chunks.forEach((chunk, i) => persist(path.join(root, 'inputs', id, `${i}.json`), chunk));
      state.jobs[id] = {id, module: input.module, reducers, status: 'map', tasks: taskList(splits), events: [{type: 'submitted'}]};
      save(); result = {id};
    } else if (req.method === 'POST' && parts.join('/') === 'claim') {
      const request = await body(req);
      ensure(request && typeof request === 'object', 'expected claim object');
      const {worker} = request;
      ensure(typeof worker === 'string' && /^[a-zA-Z0-9-]{1,64}$/.test(worker), 'invalid worker');
      result = claim(worker);
    } else if (parts[0] === 'jobs' && parts.length >= 2) {
      ensure(Object.hasOwn(state.jobs, parts[1]), 'unknown job', 404);
      const job = state.jobs[parts[1]];
      if (req.method === 'GET' && parts.length === 2) result = {...job, output: job.status === 'done' ? read(path.join(root, 'results', `${job.id}.json`)) : undefined};
      else if (req.method === 'GET' && parts[2] === 'input' && parts.length === 4) {
        ensure(/^\d+$/.test(parts[3]) && Number(parts[3]) < (job.maps ?? job.tasks).length, 'invalid input');
        result = read(path.join(root, 'inputs', job.id, `${Number(parts[3])}.json`));
      } else if (req.method === 'GET' && parts[2] === 'shuffle' && parts.length === 5) {
        ensure(job.maps && /^\d+$/.test(parts[3]) && /^\d+$/.test(parts[4]), 'map barrier not reached');
        const map = job.maps[Number(parts[3])], partition = Number(parts[4]);
        ensure(map && partition < job.reducers, 'invalid shuffle partition');
        result = read(artifact(map))[partition];
      } else if (req.method === 'POST' && parts[2] === 'complete' && parts.length === 3) {
        const completion = await body(req);
        ensure(completion && typeof completion === 'object', 'expected completion object');
        const tasks = completion.phase === 'map' ? (job.maps ?? (job.status === 'map' ? job.tasks : [])) : job.status !== 'map' ? job.tasks : [];
        const task = tasks.find(t => t.index === completion.index);
        ensure(task && ['map', 'reduce'].includes(completion.phase), 'unknown task', 409);
        if (task.status === 'done' && task.accepted === completion.token) result = {accepted: true, duplicate: true};
        else {
          ensure(job.status === completion.phase && task.status === 'running' && task.token === completion.token && task.until > Date.now(), 'stale attempt', 409);
          if (completion.error) {
            ensure(typeof completion.error === 'string' && completion.error.length <= 2000, 'invalid error');
            task.status = 'pending'; job.events.push({type: 'error', token: task.token, error: completion.error}); save(); result = {retry: true};
          } else {
            if (completion.phase === 'map') {
              ensure(Array.isArray(completion.output) && completion.output.length === job.reducers, 'invalid partition count');
              completion.output.forEach((part, i) => { pairs(part); for (const [key] of part) ensure(bucket(key, job.reducers) === i, 'mispartitioned key'); });
            } else {
              pairs(completion.output);
              const keys = new Set();
              for (const [key] of completion.output) { ensure(bucket(key, job.reducers) === task.index && !keys.has(key), 'invalid or duplicate reduced key'); keys.add(key); }
            }
            persist(path.join(root, 'attempts', `${task.token}.json`), completion.output);
            task.status = 'done'; task.accepted = task.token;
            job.events.push({type: 'published', token: task.token}); advance(job); save(); result = {accepted: true};
          }
        }
      } else ensure(false, 'not found', 404);
    } else ensure(false, 'not found', 404);
    res.writeHead(200, {'content-type': 'application/json'}); res.end(JSON.stringify(result));
  } catch (error) {
    res.writeHead(error.status ?? 500, {'content-type': 'application/json'}); res.end(JSON.stringify({error: error.message}));
  }
});
server.requestTimeout = 10000; server.headersTimeout = 10000;
server.listen(port, '127.0.0.1', () => console.log(JSON.stringify({ready: true, port: server.address().port, pid: process.pid})));
function stop() { server.close(() => { fs.unlinkSync(lock); process.exit(0); }); server.closeAllConnections(); }
process.on('SIGTERM', stop); process.on('SIGINT', stop);
