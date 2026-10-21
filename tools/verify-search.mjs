import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {rpc, sleep} from '../src/common.mjs';

const run=path.resolve('.runs',new Date().toISOString().replaceAll(':','-')), token=randomUUID(), children=[];
fs.mkdirSync(run,{recursive:true}); const evidence={node:process.version,run,actions:[],checks:[]}; let fixture, coordinator, api, origin;
function launch(script,args,name,env={}){const log=fs.createWriteStream(path.join(run,`${name}.log`),{flags:'a'});const child=spawn(process.execPath,[script,...args],{env:{...process.env,...env},stdio:['ignore','pipe','pipe']});child.stdout.pipe(log,{end:false});child.stderr.pipe(log,{end:false});child.once('close',()=>log.end());children.push(child);return child}
async function ready(child){return new Promise((resolve,reject)=>{let buffer='';const timeout=setTimeout(()=>reject(new Error('readiness timeout')),5000);child.stdout.on('data',chunk=>{buffer+=chunk;for(const line of buffer.split('\n'))try{const data=JSON.parse(line);if(data.ready){clearTimeout(timeout);resolve(data);return}}catch{}});child.once('exit',code=>reject(new Error(`process exited ${code}`)))})}
async function stop(child){if(!child||child.exitCode!==null||child.signalCode!==null)return;const closed=new Promise(resolve=>child.once('close',resolve));child.kill('SIGTERM');await closed}
const call=(route,method='GET',data)=>rpc(`${api}${route}`,token,method,data);
async function startCoordinator(){coordinator=launch('src/coordinator.mjs',['--data',path.join(run,'coordinator'),'--port','0'],`coordinator-${children.length}`,{MAPREDUCE_TOKEN:token});const state=await ready(coordinator);api=`http://127.0.0.1:${state.port}`;assert.equal((await call('/health')).pid,coordinator.pid)}
try{
  fixture=launch('tools/fixture.mjs',[],'fixture');origin=(await ready(fixture)).origin;
  await startCoordinator();
  const w1=launch('src/worker.mjs',['--url',api,'--id','search-worker-a','--data',path.join(run,'worker-a')],'worker-a',{MAPREDUCE_TOKEN:token,FIXTURE_ORIGIN:origin});
  const w2=launch('src/worker.mjs',['--url',api,'--id','search-worker-b','--data',path.join(run,'worker-b')],'worker-b',{MAPREDUCE_TOKEN:token,FIXTURE_ORIGIN:origin});
  const builder=launch('tools/build-index.mjs',[],'builder',{MAPREDUCE_TOKEN:token,FIXTURE_ORIGIN:origin,SEARCH_API:api,SEARCH_DATA:path.join(run,'coordinator')});
  const built=await new Promise((resolve,reject)=>{let output='';builder.stdout.on('data',chunk=>output+=chunk);builder.once('exit',code=>code===0?resolve(JSON.parse(output.trim().split('\n').at(-1))):reject(new Error(`index builder exited ${code}`)))});
  const durable=JSON.parse(fs.readFileSync(path.join(run,'coordinator','search-snapshot.json'),'utf8'));
  const expectedUrls=[`${origin}/`,`${origin}/mapreduce`,`${origin}/ranking`].sort();
  assert.deepEqual(durable.documents.map(d=>d.url).sort(),expectedUrls);
  const expectedLinks=new Map([[`${origin}/`,[`${origin}/mapreduce`,`${origin}/ranking`]], [`${origin}/mapreduce`,[`${origin}/`,`${origin}/ranking`]], [`${origin}/ranking`,[`${origin}/mapreduce`,`${origin}/ranking`]]]);
  for(const doc of durable.documents)assert.deepEqual(doc.links,expectedLinks.get(doc.url));
  const oracle=new Map(expectedUrls.map(url=>[url,1/expectedUrls.length]));
  for(let i=0;i<20;i++){const dangling=[...oracle].filter(([url])=>!expectedLinks.get(url).length).reduce((s,[,rank])=>s+rank,0),next=new Map();for(const url of expectedUrls){const incoming=expectedUrls.reduce((sum,from)=>sum+(expectedLinks.get(from).length?expectedLinks.get(from).includes(url)?oracle.get(from)/expectedLinks.get(from).length:0:0),0);next.set(url,.15/expectedUrls.length+.85*(dangling/expectedUrls.length+incoming))}for(const [url,rank]of next)oracle.set(url,rank)}
  for(const doc of durable.documents)assert(Math.abs(doc.rank-oracle.get(doc.url))<1e-8,`PageRank mismatch for ${doc.url}`);
  const postingByTerm=new Map();
  const oracleDocs=new Map([[`${origin}/`,['Distributed Systems','Distributed systems coordinate many machines. MapReduce Ranking']], [`${origin}/mapreduce`,['MapReduce Runtime','MapReduce distributes map and reduce work across workers. Home Ranking']], [`${origin}/ranking`,['Search Ranking','Search uses an inverted index and PageRank ranking. MapReduce Duplicate fragment']]]);
  for(const [url,[title,text]]of oracleDocs){const counts=new Map();for(const term of (title+' '+text).toLowerCase().match(/[\p{L}\p{N}]+/gu)??[])counts.set(term,(counts.get(term)??0)+1);for(const [term,tf]of counts){if(!postingByTerm.has(term))postingByTerm.set(term,[]);postingByTerm.get(term).push([url,tf])}}
  for(const [term,postings]of postingByTerm){const actual=durable.terms[term].map(p=>[p.url,p.tf]).sort((a,b)=>a[0].localeCompare(b[0]));assert.deepEqual(actual,postings.sort((a,b)=>a[0].localeCompare(b[0])),`inverted index mismatch for ${term}`)}
  assert.equal(Math.abs(durable.documents.reduce((sum,d)=>sum+d.rank,0)-1)<1e-10,true);
  const state=JSON.parse(fs.readFileSync(path.join(run,'coordinator','state.json'),'utf8'));
  const owners=new Set(Object.values(state.jobs).flatMap(job=>job.events.filter(e=>e.type==='assigned').map(e=>e.worker)));
  assert(owners.has('search-worker-a')&&owners.has('search-worker-b'),`tasks were not distributed across both workers: ${[...owners]}`);
  const results=(await call('/api/search?q=mapreduce')).results;assert(results.length>=2);assert(results[0].title==='MapReduce Runtime');
  assert.deepEqual((await call('/api/search?q=zzunknown')).results,[]);
  assert.deepEqual((await call('/api/search?q=')).results,[]);
  const expectedResults=expectedUrls.map(url=>{const postings=durable.terms.mapreduce??[];const p=postings.find(item=>item.url===url);return p?{url,score:p.tf*Math.log(1+expectedUrls.length/postings.length)+oracle.get(url)}:null}).filter(Boolean).sort((a,b)=>b.score-a.score||a.url.localeCompare(b.url));
  assert.deepEqual(results.map(item=>item.url),expectedResults.map(item=>item.url));
  evidence.actions.push({pages:built.pages,jobs:Object.keys(state.jobs).length,workerOwners:[...owners]});
  evidence.checks.push({graph:durable.documents.map(d=>({url:d.url,links:d.links})),rankSum:durable.documents.reduce((s,d)=>s+d.rank,0),firstSearchResult:results[0].url,unknownTerms:0});
  await stop(w1);await stop(w2);await stop(coordinator);await startCoordinator();
  assert.equal((await call('/api/status')).pages,3);assert.equal((await call('/api/search?q=ranking')).results.length>0,true);
  evidence.checks.push({restartSnapshot:'retained',pages:(await call('/api/status')).pages});
}catch(error){evidence.error=error.stack;throw error}finally{
  for(const child of [...children].reverse())await stop(child).catch(()=>{});
  fs.writeFileSync(path.join(run,'evidence.json'),JSON.stringify(evidence,null,2));
  assert(fs.existsSync(path.join(run,'evidence.json')));
  console.log(`Evidence: ${path.join(run,'evidence.json')}`);
}
