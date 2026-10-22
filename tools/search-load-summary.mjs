import assert from 'node:assert/strict';
import {sameSearchOutput} from './search-output-oracle.mjs';

export function assertRankedResults(actual, expected) {
  assert(sameSearchOutput(actual, expected), 'search result differs from independent oracle');
}

export function k6Counts(summary) {
  const count = name => summary.metrics[name]?.values.count ?? 0;
  const counts = {offered: count('search_started') + count('dropped_iterations'), started: count('search_started'),
    completed: count('search_completed'), rejected: count('search_rejected'), failed: count('search_failed'),
    dropped: count('dropped_iterations'), timedOut: count('search_timeouts'), integrityErrors: count('search_oracle_mismatches')};
  counts.interrupted = counts.started - counts.completed - counts.rejected - counts.failed;
  assert(counts.interrupted >= 0, 'terminal outcomes cannot exceed started k6 iterations');
  return counts;
}
