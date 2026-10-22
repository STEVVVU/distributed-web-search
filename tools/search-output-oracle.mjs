export function sameSearchOutput(actual, expected) {
  if (typeof expected === 'number') return Number.isFinite(actual) && Math.abs(actual - expected) < 2e-10;
  if (actual === expected) return true;
  if (!actual || !expected || typeof actual !== 'object' || typeof expected !== 'object' || Array.isArray(actual) !== Array.isArray(expected)) return false;
  const keys = Object.keys(expected);
  return Object.keys(actual).length === keys.length && keys.every(key => Object.hasOwn(actual, key) && sameSearchOutput(actual[key], expected[key]));
}
