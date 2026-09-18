// SPDX-License-Identifier: Apache-2.0

export interface BoundedPlainDataOptions {
  readonly maxDepth: number;
  readonly maxNodes: number;
  readonly maxScalarBytes: number;
  readonly maxSerializedBytes: number;
  readonly error: () => Error;
}

type PlainData =
  | null
  | boolean
  | number
  | string
  | PlainDataArray
  | PlainDataMap;

interface PlainDataArray extends ReadonlyArray<PlainData> {}

interface PlainDataMap {
  readonly [key: string]: PlainData;
}

interface PlainDataBudget {
  nodes: number;
  scalarUtf8Bytes: number;
}

type PlainDataResult =
  | Readonly<{ valid: true; value: PlainData }>
  | Readonly<{ valid: false }>;

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function chargeScalar(
  budget: PlainDataBudget,
  value: null | boolean | number | string,
  maximum: number,
): boolean {
  if (
    typeof value === 'string' &&
    value.length > maximum - budget.scalarUtf8Bytes
  ) {
    return false;
  }
  const serialized = JSON.stringify(value);
  budget.scalarUtf8Bytes += utf8Length(serialized);
  return budget.scalarUtf8Bytes <= maximum;
}

function clonePlainData(
  value: unknown,
  options: BoundedPlainDataOptions,
  ancestors: Set<object>,
  depth: number,
  budget: PlainDataBudget,
): PlainDataResult {
  budget.nodes += 1;
  if (depth > options.maxDepth || budget.nodes > options.maxNodes) {
    return { valid: false };
  }
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value))
  ) {
    return chargeScalar(budget, value, options.maxScalarBytes)
      ? ({ valid: true, value } as PlainDataResult)
      : { valid: false };
  }
  if (typeof value !== 'object' || ancestors.has(value)) {
    return { valid: false };
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (Object.getPrototypeOf(value) !== Array.prototype) {
        return { valid: false };
      }
      const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
      const length =
        lengthDescriptor && 'value' in lengthDescriptor
          ? lengthDescriptor.value
          : undefined;
      if (
        !Number.isSafeInteger(length) ||
        Number(length) < 0 ||
        lengthDescriptor?.enumerable !== false ||
        Number(length) > options.maxNodes - budget.nodes
      ) {
        return { valid: false };
      }
      const keys = Reflect.ownKeys(value);
      if (
        keys.length !== Number(length) + 1 ||
        !keys.includes('length') ||
        keys.some((key) => typeof key !== 'string')
      ) {
        return { valid: false };
      }
      const cloned: PlainData[] = [];
      for (let index = 0; index < Number(length); index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(
          value,
          String(index),
        );
        if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
          return { valid: false };
        }
        const result = clonePlainData(
          descriptor.value,
          options,
          ancestors,
          depth + 1,
          budget,
        );
        if (!result.valid) return result;
        cloned.push(result.value);
      }
      return { valid: true, value: cloned };
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      return { valid: false };
    }
    const keys = Reflect.ownKeys(value);
    if (keys.length > options.maxNodes - budget.nodes) {
      return { valid: false };
    }
    const cloned = Object.create(null) as Record<string, PlainData>;
    for (const key of keys) {
      if (
        typeof key !== 'string' ||
        !chargeScalar(budget, key, options.maxScalarBytes)
      ) {
        return { valid: false };
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
        return { valid: false };
      }
      const result = clonePlainData(
        descriptor.value,
        options,
        ancestors,
        depth + 1,
        budget,
      );
      if (!result.valid) return result;
      cloned[key] = result.value;
    }
    return { valid: true, value: cloned };
  } finally {
    ancestors.delete(value);
  }
}

export function cloneBoundedPlainData(
  value: unknown,
  options: BoundedPlainDataOptions,
): unknown {
  let result: PlainDataResult = { valid: false };
  let serializedWithinBound = false;
  try {
    const invalidOptions =
      !Number.isSafeInteger(options.maxDepth) ||
      options.maxDepth < 0 ||
      !Number.isSafeInteger(options.maxNodes) ||
      options.maxNodes < 1 ||
      !Number.isSafeInteger(options.maxScalarBytes) ||
      options.maxScalarBytes < 0 ||
      !Number.isSafeInteger(options.maxSerializedBytes) ||
      options.maxSerializedBytes < 0;
    if (!invalidOptions) {
      result = clonePlainData(value, options, new Set<object>(), 0, {
        nodes: 0,
        scalarUtf8Bytes: 0,
      });
      serializedWithinBound =
        result.valid &&
        utf8Length(JSON.stringify(result.value)) <= options.maxSerializedBytes;
    }
  } catch {
    throw options.error();
  }
  if (!result.valid || !serializedWithinBound) throw options.error();
  return result.value;
}
