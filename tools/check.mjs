import {readdirSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
for (const dir of ['src', 'jobs', 'tools']) for (const file of readdirSync(dir).filter(f => f.endsWith('.mjs'))) {
  const result = spawnSync(process.execPath, ['--check', `${dir}/${file}`], {stdio: 'inherit'});
  if (result.status !== 0) process.exit(result.status ?? 1);
}
console.log('All source syntax checks passed');
