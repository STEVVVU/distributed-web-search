import http from 'node:http';
const pages = {
  '/': '<title>Distributed Systems</title><main>Distributed systems coordinate many machines.<a href="/mapreduce#overview">MapReduce</a><a href="/ranking">Ranking</a><a href="https://example.org/out">Outside</a></main>',
  '/mapreduce': '<title>MapReduce Runtime</title><main>MapReduce distributes map and reduce work across workers.<a href="/">Home</a><a href="/ranking">Ranking</a></main>',
  '/ranking': '<title>Search Ranking</title><main>Search uses an inverted index and PageRank ranking.<a href="/mapreduce">MapReduce</a><a href="/ranking#top">Duplicate fragment</a></main>'
};
const server=http.createServer((req,res)=>{const html=pages[new URL(req.url,'http://local').pathname];if(!html){res.writeHead(404);res.end('missing');return}res.writeHead(200,{'content-type':'text/html; charset=utf-8'});res.end(`<!doctype html><html><head>${html.split('<main>')[0]}</head><body>${html.split('<main>')[1]}</body></html>`)});
server.listen(0,'127.0.0.1',()=>console.log(JSON.stringify({ready:true,origin:`http://127.0.0.1:${server.address().port}`})));
