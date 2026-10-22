import fs from 'node:fs';
import path from 'node:path';
import {spawn} from 'node:child_process';

export function launch(run, script, args, name, env = {}, {executable = process.execPath, detached = false} = {}) {
  const log = fs.createWriteStream(path.join(run, `${name}.log`), {flags: 'a'});
  const child = spawn(executable, [script, ...args], {env: {...process.env, ...env}, stdio: ['ignore', 'pipe', 'pipe'], detached, windowsHide: true});
  child.ownedProcessGroup = detached;
  child.stdout.pipe(log, {end: false}); child.stderr.pipe(log, {end: false});
  child.output = ''; child.stdout.on('data', chunk => { child.output += chunk; });
  child.finished = new Promise(resolve => {
    child.once('error', error => resolve({error}));
    child.once('close', (code, signal) => { log.end(); resolve({code, signal}); });
  });
  return child;
}
export async function wait(child, timeoutMs = 120000) {
  let timer;
  try {
    const outcome = await Promise.race([child.finished, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`child ${child.pid} exceeded ${timeoutMs} ms deadline`)), timeoutMs); })]);
    if (outcome.error) throw outcome.error;
    return outcome;
  } finally { clearTimeout(timer); }
}
export async function ready(child) {
  let timer, onData;
  try {
    return await Promise.race([
      new Promise((resolve, reject) => {
        const inspect = () => {
          for (const line of child.output.split('\n')) {
            let value;
            try { value = JSON.parse(line); } catch (error) { if (error instanceof SyntaxError) continue; reject(error); return; }
            if (value.ready) { resolve(value); return; }
          }
        };
        onData = inspect; child.stdout.on('data', onData); inspect();
        timer = setTimeout(() => reject(new Error(`child ${child.pid} readiness deadline exceeded`)), 5000);
      }),
      child.finished.then(outcome => { throw outcome.error ?? new Error(`child ${child.pid} exited before ready (${outcome.code})`); })
    ]);
  } finally { clearTimeout(timer); if (onData) child.stdout.off('data', onData); }
}
export async function stop(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  if (child.ownedProcessGroup) process.kill(-child.pid, 'SIGTERM');
  else if (!child.kill('SIGTERM')) throw new Error(`could not terminate owned child ${child.pid}`);
  await wait(child, 5000);
}
export async function cleanup(children) {
  const errors = [];
  for (const child of [...children].reverse()) {
    try { await stop(child); } catch (error) { errors.push(error); }
  }
  if (errors.length) throw new AggregateError(errors, 'owned process cleanup failed');
}
