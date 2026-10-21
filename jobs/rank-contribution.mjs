export function map(source, graph) {
  if (!graph.links.length) return [];
  return graph.links.map(target => [target, graph.rank / graph.links.length]);
}
export function reduce(_url, contributions) { return contributions.reduce((sum, value) => sum + value, 0); }
