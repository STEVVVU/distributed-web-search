import assert from 'node:assert/strict';

export function quantiles(values) {
  const sorted = values.toSorted((a, b) => a - b), q = p => sorted.length ? sorted[Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)] : null;
  return {n: sorted.length, p50: q(.5), p95: q(.95), p99: sorted.length >= 100 ? q(.99) : null,
    min: sorted[0] ?? null, max: sorted.at(-1) ?? null, method: 'empirical nearest rank; p99 omitted below100 observations'};
}

export function assertRankedResults(actual, expected) {
  assert(Array.isArray(actual) && actual.length === expected.length, 'result count differs from independent oracle');
  for (let i = 0; i < expected.length; i++) {
    for (const key of ['url', 'title', 'text']) assert.equal(actual[i][key], expected[i][key], `${key} differs from independent oracle`);
    for (const key of ['score', 'lexical', 'rank']) assert(Number.isFinite(actual[i][key]) && Math.abs(actual[i][key] - expected[i][key]) < 2e-10, `${key} differs from independent oracle`);
  }
}

export function loadSummary(requests) {
  assert(requests.every(r => Boolean(r.correct) + Boolean(r.rejected) + Boolean(r.error && !r.rejected) === 1), 'each offer must have one terminal outcome');
  const counts = {offered: requests.length, completed: requests.filter(r => r.correct).length,
    rejected: requests.filter(r => r.rejected).length, failed: requests.filter(r => r.error && !r.rejected).length,
    timedOut: requests.filter(r => r.timeout).length, integrityErrors: requests.filter(r => r.integrityError).length};
  return {counts, measurementComplete: true, correctOutputs: counts.integrityErrors === 0,
    zeroLossEnvelope: counts.completed === counts.offered};
}
