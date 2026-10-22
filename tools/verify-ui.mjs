import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const script = fs.readFileSync(new URL('../web/index.html', import.meta.url), 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
class Element {
  constructor() { this.children = []; this.fields = new Map(); this.value = ''; }
  replaceChildren() { this.children = []; this.message = ''; }
  set textContent(value) { this.message = value; this.children = []; }
  get textContent() { return this.message; }
  append(child) { this.children.push(child); }
  querySelector(selector) { if (!this.fields.has(selector)) this.fields.set(selector, new Element()); return this.fields.get(selector); }
  addEventListener() {}
}
const elements = new Map(['#status', '#results', '#search', '#q', '#policy'].map(selector => [selector, new Element()]));
elements.get('#q').value = 'search'; elements.get('#policy').value = 'global';
const pending = [];
const context = vm.createContext({URL, AbortSignal, document: {querySelector: selector => elements.get(selector), createElement: () => new Element()}, fetch: url => new Promise(resolve => pending.push({url, resolve}))});
const response = (request, data, ok = true) => request.resolve({ok, status: ok ? 200 : 503, json: async () => data});
const initial = vm.runInContext(script, context);
response(pending.shift(), {ready: true, pages: 1}); response(pending.shift(), {results: []}); await initial;
const item = title => ({url: `http://127.0.0.1/${title}`, title, snippet: title, score: 1, lexical: 1, rank: 0});
async function overlap(oldFails) {
  elements.get('#q').value = 'old'; elements.get('#policy').value = 'global';
  const oldSearch = context.search(), oldRequest = pending.shift();
  elements.get('#q').value = 'new'; elements.get('#policy').value = 'topic';
  const newSearch = context.search(), newRequest = pending.shift();
  assert.match(oldRequest.url, /q=old&policy=global/); assert.match(newRequest.url, /q=new&policy=topic/);
  response(newRequest, {results: [item('new')]}); await newSearch;
  response(oldRequest, oldFails ? {error: 'old unavailable'} : {results: [item('old')]}, !oldFails); await oldSearch;
  assert.deepEqual(elements.get('#results').children.map(article => article.querySelector('a').textContent), ['new'], 'an older response replaced or mixed with the newest query/policy');
}
await overlap(false); await overlap(true);
const failed = context.search(); response(pending.shift(), {error: 'search index is not ready'}, false); await failed;
assert.equal(elements.get('#results').textContent, 'Search unavailable: search index is not ready');
const empty = context.search(); response(pending.shift(), {results: []}); await empty;
assert.equal(elements.get('#results').textContent, 'No matching pages.');
console.log('Actual inline UI script unit checks passed: latest response wins, HTTP error, successful empty response.');
