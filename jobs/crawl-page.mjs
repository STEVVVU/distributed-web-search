import {parse} from 'parse5';

const MAX_BYTES = 256 * 1024;
function text(node) {
  if (node.nodeName === '#text') return node.value;
  return (node.childNodes ?? []).map(text).join(' ');
}
function nodes(root, name, out = []) {
  if (root.tagName === name) out.push(root);
  for (const child of root.childNodes ?? []) nodes(child, name, out);
  return out;
}
export async function map(url) {
  const origin = process.env.FIXTURE_ORIGIN;
  const target = new URL(url);
  if (!origin || target.origin !== origin || target.username || target.password || url.length > 2048) throw new Error('URL outside configured fixture origin or URL bounds');
  const response = await fetch(url, {redirect: 'error', signal: AbortSignal.timeout(2000)});
  if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);
  if (!response.headers.get('content-type')?.toLowerCase().startsWith('text/html')) throw new Error(`expected HTML for ${url}`);
  const reader = response.body.getReader(), chunks = []; let size = 0;
  try {
    for (;;) {
      const {done, value} = await reader.read(); if (done) break;
      size += value.length; if (size > MAX_BYTES) throw new Error(`page exceeds ${MAX_BYTES} bytes: ${url}`);
      chunks.push(value);
    }
  } catch (error) { await reader.cancel(error); throw error; }
  finally { reader.releaseLock(); }
  const document = parse(Buffer.concat(chunks).toString('utf8'));
  const title = nodes(document, 'title')[0];
  const content = nodes(document, 'body')[0] ?? document;
  const links = nodes(content, 'a').flatMap(a => {
    const href = a.attrs?.find(attr => attr.name === 'href')?.value;
    if (!href) return [];
    try {
      const target = new URL(href, url); target.hash = '';
      if (target.origin !== origin || !['http:', 'https:'].includes(target.protocol) || target.username || target.password || target.href.length > 2048) return [];
      return [target.href];
    } catch { return []; }
  });
  return [[url, {url, title: text(title ?? {childNodes: []}).trim(), text: text(content).replace(/\s+/g, ' ').trim(), links: [...new Set(links)].sort()}]];
}
export function reduce(_url, pages) { return pages[0]; }
