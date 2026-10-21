export function map(url, document) {
  const terms = (document.title + ' ' + document.text).toLocaleLowerCase().match(/[\p{L}\p{N}]+/gu) ?? [];
  const counts = new Map(); for (const term of terms) counts.set(term, (counts.get(term) ?? 0) + 1);
  return [...counts].map(([term, tf]) => [term, {url, tf, title: document.title, text: document.text}]);
}
export function reduce(_term, postings) {
  const unique = new Map(postings.map(p => [p.url, p]));
  return [...unique.values()].sort((a, b) => a.url.localeCompare(b.url));
}
