import http from 'node:http';
const pages = {
  '/': '<title>Distributed Systems</title><main>Search coordinates many machines.<a href="/mapreduce#overview">MapReduce</a><a href="/ranking">Ranking</a><a href="/notes">Notes</a><a href="/ranking#duplicate">Duplicate</a><a href="https://example.org/out">Outside</a><a href="http://[">Malformed</a></main>',
  '/mapreduce': '<title>MapReduce Runtime</title><main>Search distributes map and reduce work across workers.<a href="/">Home</a><a href="/ranking">Ranking</a></main>',
  '/ranking': '<title>Ranking Research</title><main>Search uses an inverted index and PageRank.<a href="/mapreduce">MapReduce</a><a href="/notes">Notes</a><a href="/dangling">Archive</a></main>',
  '/notes': '<title>Research Notes</title><main>Search repairs malformed HTML.<a href="/ranking#top">Ranking</a><p>Unclosed paragraph',
  '/dangling': '<title>Archive</title><main>Search preserves dangling pages and constructor prototype tostring hasownproperty terms.</main>'
};
const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, 'http://local').pathname;
  if (pathname === '/failure') { res.writeHead(500); res.end('fixture failure'); return; }
  if (pathname === '/redirect') { res.writeHead(302, {location: '/'}); res.end(); return; }
  if (pathname === '/oversized') { res.writeHead(200, {'content-type': 'text/html'}); res.end('x'.repeat(256 * 1024 + 1)); return; }
  if (pathname === '/plain') { res.writeHead(200, {'content-type': 'text/plain'}); res.end('not HTML'); return; }
  if (pathname === '/slow') { setTimeout(() => { if (!res.destroyed) { res.writeHead(200, {'content-type': 'text/html'}); res.end('<p>late</p>'); } }, 2500).unref(); return; }
  const html = pages[pathname];
  if (!html) { res.writeHead(404); res.end('missing'); return; }
  res.writeHead(200, {'content-type': 'text/html; charset=utf-8'});
  const [head, body] = html.split('<main>');
  res.end(`<!doctype html><html><head>${head}</head><body><main>${body}</body></html>`);
});
server.listen(0, '127.0.0.1', () => console.log(JSON.stringify({ready: true, origin: `http://127.0.0.1:${server.address().port}`})));
