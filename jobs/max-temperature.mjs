export function map(_key, reading) {
  if (typeof reading?.station !== 'string' || !Number.isFinite(reading.temperature)) throw new Error('invalid temperature reading');
  return [[reading.station, reading.temperature]];
}
export const reduce = (_key, readings) => Math.max(...readings);
