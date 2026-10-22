import {appendFileSync} from 'node:fs';
const file = process.env.BENCH_RESOURCE_FILE;
if (file) {
  const record = () => appendFileSync(file, JSON.stringify({at: Date.now(), pid: process.pid, cpu: process.cpuUsage(), ...process.memoryUsage(), ...process.resourceUsage()}) + '\n');
  record();
  setInterval(record, 1000).unref();
  process.once('exit', record);
}
