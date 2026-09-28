// SPDX-License-Identifier: Apache-2.0
// Readers for host-supplied configuration. A leaf that imports nothing from
// the package, so a validating module in any layer can use it without an
// import cycle.
//
// @internal

/**
 * @internal Describe a refused host value without coercing it: a string is
 * quoted, anything else is named by its type, because a template literal
 * would call an object's own `toString`.
 */
export function describeEntry(value: unknown): string {
  if (typeof value === 'string') return JSON.stringify(value);
  return value === null ? 'null' : typeof value;
}

/**
 * @internal The keys of an options type, each mapped to `true`. A table is
 * written `{ … } satisfies Record<keyof T, true>`, so a member the type gains
 * or drops fails to compile until the table follows.
 */
export type KnownFields = Readonly<Record<string, true>>;

/**
 * @internal The first own enumerable string key of `value` that `fields`
 * lacks, or `undefined` when there is none. Symbol keys are not read, because
 * a brand symbol is not an option.
 */
export function unknownFieldOf(
  value: object,
  fields: KnownFields,
): string | undefined {
  for (const key of Object.keys(value)) {
    if (!Object.hasOwn(fields, key)) return key;
  }
  return undefined;
}

/**
 * @internal Refuse `value` unless it is a non-array object whose own
 * enumerable string keys all appear in `fields`: a misspelled option is
 * otherwise ignored, and the requirement it was meant to set is dropped. The
 * refusal names `subject` and the key, and lists the keys of `fields`.
 */
export function assertKnownFields(
  subject: string,
  value: unknown,
  fields: KnownFields,
): asserts value is object {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError(
      `${subject} must be an object (got ${Array.isArray(value) ? 'an array' : describeEntry(value)})`,
    );
  }
  const unknown = unknownFieldOf(value, fields);
  if (unknown !== undefined) {
    throw new TypeError(
      `${subject} has unknown field ${JSON.stringify(unknown)} (valid fields: ${Object.keys(fields).join(', ')})`,
    );
  }
}

/**
 * @internal Return `value` when it is a number that `accepts` admits, and
 * otherwise throw a `TypeError` saying `subject` must be `range`. A numeric
 * string is refused, not coerced.
 */
export function readNumberInRange(
  subject: string,
  value: unknown,
  accepts: (value: number) => boolean,
  range: string,
): number {
  if (typeof value !== 'number' || !accepts(value)) {
    throw new TypeError(
      `${subject} must be ${range} (got ${typeof value === 'number' ? String(value) : describeEntry(value)})`,
    );
  }
  return value;
}

/**
 * @internal Read a host-supplied list into a frozen copy after
 * `Array.isArray`, reading `length` once and each index once, and passing
 * each entry through `readEntry`. `subject` names the caller's field in
 * refusals. A `length` that is not a non-negative safe integer is refused,
 * not coerced, so the non-empty check and the loop bound decide on the same
 * number.
 */
export function readFrozenList<T>(
  subject: string,
  list: unknown,
  readEntry: (entry: unknown, index: number) => T,
  requireEntries = false,
): readonly T[] {
  if (!Array.isArray(list)) {
    throw new TypeError(`${subject} must be an array`);
  }
  const length: unknown = list.length;
  if (
    typeof length !== 'number' ||
    !Number.isSafeInteger(length) ||
    length < 0
  ) {
    throw new TypeError(`${subject} must be an array with a valid length`);
  }
  if (requireEntries && length === 0) {
    throw new TypeError(`${subject} must not be empty`);
  }
  const copy: T[] = [];
  for (let index = 0; index < length; index += 1) {
    copy.push(readEntry(list[index], index));
  }
  return Object.freeze(copy);
}
