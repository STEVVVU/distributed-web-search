export const map = (document, text) => [...new Set(String(text).toLowerCase().match(/[a-z0-9]+/g) ?? [])].map(word => [word, document]);
export const reduce = (_key, documents) => [...new Set(documents)].sort();
