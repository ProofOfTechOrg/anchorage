// SPDX-License-Identifier: Apache-2.0

function invalid(name: string, domain: string): never {
  throw new RangeError(`${name} must be ${domain}`);
}

export function nonnegativeSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    return invalid(name, 'a nonnegative safe integer');
  }
  return value;
}

export function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    return invalid(name, 'a positive safe integer');
  }
  return value;
}

export function finiteNonnegativeNumber(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0) {
    return invalid(name, 'a finite nonnegative number');
  }
  return value;
}
