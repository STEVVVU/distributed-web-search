import path from 'node:path';
import {atomic, bucket, config, ensure, pairs, rpc, sleep} from './common.mjs';

const options = config(), url = options.url ?? 'http://127.0.0.1:8080';
ensure(new URL(url).hostname === '127.0.0.1', 'worker requires a loopback coordinator');
const worker = options.id ?? `worker-${process.pid}`, root = path.resolve(options.data ?? `data/${worker}`);
const call = (route, method, data) => rpc(`${url}${route}`, process.env.MAPREDUCE_TOKEN, method, data);
while (true) {
  let task;
  try {
    task = await call('/claim', 'POST', {worker});
    if (!task) { await sleep(100); continue; }
    console.log(JSON.stringify({assigned: task, worker, pid: process.pid}));
    const job = await import(new URL(`../jobs/${task.module}.mjs`, import.meta.url));
    let output;
    if (task.phase === 'map') {
      const input = await call(`/jobs/${task.job}/input/${task.index}`);
      atomic(path.join(root, task.token, 'input.json'), input);
      output = Array.from({length: task.reducers}, () => []);
      for (const [key, value] of input) for (const pair of pairs(await job.map(key, value))) output[bucket(pair[0], task.reducers)].push(pair);
    } else {
      const groups = new Map();
      for (let i = 0; i < task.maps; i++) {
        const partition = await call(`/jobs/${task.job}/shuffle/${i}/${task.index}`);
        atomic(path.join(root, task.token, `shuffle-${i}.json`), partition);
        for (const [key, value] of partition) { if (!groups.has(key)) groups.set(key, []); groups.get(key).push(value); }
      }
      output = [];
      for (const [key, values] of [...groups].sort((a, b) => a[0].localeCompare(b[0]))) output.push([key, await job.reduce(key, values)]);
    }
    atomic(path.join(root, task.token, 'output.json'), output);
    await call(`/jobs/${task.job}/complete`, 'POST', {...task, output});
  } catch (error) {
    console.error(JSON.stringify({error: error.message, token: task?.token}));
    if (task && !error.status) {
      try { await call(`/jobs/${task.job}/complete`, 'POST', {...task, error: error.message.slice(0, 2000)}); }
      catch (reportError) { console.error(JSON.stringify({reportError: reportError.message})); }
    }
    await sleep(100);
  }
}
