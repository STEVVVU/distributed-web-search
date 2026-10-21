export const map = (_key, text) => (String(text).toLowerCase().match(/[a-z0-9]+/g) ?? []).map(word => [word, 1]);
export const reduce = (_key, counts) => counts.reduce((a, b) => a + b, 0);
