// SPDX-License-Identifier: Apache-2.0

/** Array nesting SQLite's JSON functions cannot parse; JavaScript can. */
export const PAST_SQLITE_JSON_DEPTH = 1_100;

/** `levels` nested arrays around a string leaf: JSON nesting depth `levels`. */
export function nestedArray(levels: number): unknown {
  let value: unknown = 'leaf';
  for (let level = 0; level < levels; level++) value = [value];
  return value;
}

/** `levels` nested objects around a string leaf: JSON nesting depth `levels`. */
export function nestedObject(levels: number): unknown {
  let value: unknown = 'leaf';
  for (let level = 0; level < levels; level++) value = { next: value };
  return value;
}

/**
 * `snapshot` carrying a value nested past SQLite's JSON depth: under a key of
 * its request context when that is an object, where a tenant's value reaches a
 * stored run, otherwise at `result`.
 */
export function withDeepValue<T extends object>(snapshot: T): T {
  const deep = nestedArray(PAST_SQLITE_JSON_DEPTH);
  const context = (snapshot as { requestContext?: unknown }).requestContext;
  if (
    context !== null &&
    typeof context === 'object' &&
    !Array.isArray(context)
  )
    return { ...snapshot, requestContext: { ...context, tenantValue: deep } };
  return { ...snapshot, result: deep };
}
