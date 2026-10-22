import http from 'k6/http';
import exec from 'k6/execution';
import {SharedArray} from 'k6/data';
import {Counter, Trend} from 'k6/metrics';
import {sameSearchOutput} from './search-output-oracle.mjs';

const cases = new SharedArray('independent-search-oracle', () => JSON.parse(open(__ENV.SEARCH_ORACLE)));
const started = new Counter('search_started');
const completed = new Counter('search_completed');
const rejected = new Counter('search_rejected');
const failed = new Counter('search_failed');
const timeouts = new Counter('search_timeouts');
const mismatches = new Counter('search_oracle_mismatches');
const successDuration = new Trend('search_validated_duration', true);
const rejectedDuration = new Trend('search_rejected_duration', true);
const failedDuration = new Trend('search_failed_duration', true);
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(__ENV.SEARCH_API)) throw new Error('owned loopback API required');

export const options = {
  scenarios: {search: {executor: 'constant-arrival-rate', rate: Number(__ENV.SEARCH_RATE), timeUnit: '1s',
    duration: `${__ENV.SEARCH_SECONDS}s`, preAllocatedVUs: 256, maxVUs: 256, gracefulStop: '10s'}},
  thresholds: {search_oracle_mismatches: ['count==0']},
  maxRedirects: 0,
  summaryTrendStats: ['min', 'med', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  for (const counter of [started, completed, rejected, failed, timeouts, mismatches]) counter.add(0);
}

export default function () {
  const expected = cases[exec.scenario.iterationInTest % cases.length];
  started.add(1);
  const response = http.get(`${__ENV.SEARCH_API}/api/search?q=${encodeURIComponent(expected.query)}&policy=${expected.policy}`,
    {timeout: '5s', tags: {name: 'search', query: expected.query, policy: expected.policy}});
  if ([1050, 1211].includes(response.error_code)) timeouts.add(1);
  if ([429, 503].includes(response.status)) {
    rejected.add(1); rejectedDuration.add(response.timings.duration); return;
  }
  if (response.status !== 200 || response.error) {
    failed.add(1); failedDuration.add(response.timings.duration); return;
  }
  let correct = false;
  try { correct = sameSearchOutput(response.json(), expected); } catch (_) { correct = false; }
  if (!correct) {
    mismatches.add(1); failed.add(1); failedDuration.add(response.timings.duration);
    console.error(`Independent search oracle mismatch for ${expected.policy}/${expected.query}`);
    return;
  }
  completed.add(1); successDuration.add(response.timings.duration);
}

export function handleSummary(data) {
  return {[__ENV.SEARCH_SUMMARY]: JSON.stringify(data, null, 2)};
}
