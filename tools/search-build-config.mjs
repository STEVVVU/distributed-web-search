import {ensure} from '../src/common.mjs';

export function buildConfig(env = process.env) {
  const integer = (name, fallback, min, max) => {
    const value = Number(env[name] ?? fallback);
    ensure(Number.isInteger(value) && value >= min && value <= max, `${name} must be ${min}..${max}`);
    return value;
  };
  return {
    pages: integer('SEARCH_MAX_PAGES', 32, 1, 5000),
    depth: integer('SEARCH_MAX_DEPTH', 4, 0, 16),
    splits: integer('SEARCH_SPLITS', 4, 1, 64),
    reducers: integer('SEARCH_REDUCERS', 2, 1, 64),
    jobTimeout: integer('SEARCH_JOB_TIMEOUT_MS', 15000, 100, 60000),
    buildTimeout: integer('SEARCH_BUILD_TIMEOUT_MS', 180000, 1000, 1800000),
  };
}
