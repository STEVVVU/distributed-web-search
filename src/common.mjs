import fs from 'node:fs';
import path from 'node:path';
import {createHash, randomUUID} from 'node:crypto';

export const LIMIT = 4 * 1024 * 1024;
export function fail(message, status = 400) { throw Object.assign(new Error(message), {status}); }
export function ensure(condition, message, status) { if (!condition) fail(message, status); }
export function pairs(value) {
  ensure(Array.isArray(value) && value.length <= 100000, 'expected at most 100000 pairs');
  for (const item of value) ensure(Array.isArray(item) && item.length === 2 && typeof item[0] === 'string' && item[0].length <= 4096, 'invalid key/value pair');
  return value;
}
export function bucket(key, count) { return createHash('sha256').update(key).digest().readUInt32BE() % count; }
export function atomic(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temp = `${file}.${randomUUID()}.tmp`;
  const fd = fs.openSync(temp, 'wx');
  try { fs.writeFileSync(fd, JSON.stringify(value)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  fs.renameSync(temp, file);
}
export function read(file) { return JSON.parse(fs.readFileSync(file, 'utf8')); }
export async function body(req) {
  let size = 0; const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    ensure(size <= LIMIT, 'request exceeds 4 MiB', 413);
    chunks.push(chunk);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString()); } catch { fail('invalid JSON'); }
}
export async function rpc(url, token, method = 'GET', value) {
  const res = await fetch(url, {method, headers: {authorization: `Bearer ${token}`, 'content-type': 'application/json'}, body: value === undefined ? undefined : JSON.stringify(value), signal: AbortSignal.timeout(5000)});
  const data = await res.json();
  if (!res.ok) throw Object.assign(new Error(data.error), {status: res.status});
  return data;
}
export function config() {
  const args = process.argv.slice(2), result = {};
  ensure(args.length % 2 === 0, 'arguments must be --name value');
  for (let i = 0; i < args.length; i += 2) { ensure(args[i].startsWith('--'), 'expected option'); result[args[i].slice(2)] = args[i + 1]; }
  ensure(process.env.MAPREDUCE_TOKEN?.length >= 16, 'MAPREDUCE_TOKEN must contain at least 16 characters');
  return result;
}
export const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
