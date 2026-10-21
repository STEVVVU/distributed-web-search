import {map as count, reduce} from './wordcount.mjs';
export {reduce};
export async function map(key, text) {
  await new Promise(resolve => setTimeout(resolve, 300));
  return count(key, text);
}
