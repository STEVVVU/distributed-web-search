import assert from 'node:assert/strict';
import {buildConfig} from './search-build-config.mjs';
assert.deepEqual(buildConfig({}), {pages: 32, depth: 4, splits: 4, reducers: 2, jobTimeout: 15000, buildTimeout: 180000});
for (const [key, min, max] of [['SEARCH_MAX_PAGES', 1, 5000], ['SEARCH_MAX_DEPTH', 0, 16], ['SEARCH_SPLITS', 1, 64], ['SEARCH_REDUCERS', 1, 64], ['SEARCH_JOB_TIMEOUT_MS', 100, 60000], ['SEARCH_BUILD_TIMEOUT_MS', 1000, 1800000]]) {
  for (const value of [min - 1, max + 1, 'NaN', '', 1.5]) {
    if (value === '' && min === 0) continue;
    assert.throws(() => buildConfig({[key]: String(value)}), new RegExp(key));
  }
  assert.doesNotThrow(() => buildConfig({[key]: String(min)}));
  assert.doesNotThrow(() => buildConfig({[key]: String(max)}));
}
console.log('Search build bounds passed');
