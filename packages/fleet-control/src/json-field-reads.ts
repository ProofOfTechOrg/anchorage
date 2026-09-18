// SPDX-License-Identifier: Apache-2.0

export function readField(value: unknown, name: string): unknown {
  return value && typeof value === 'object'
    ? Reflect.get(value, name)
    : undefined;
}

export function readStringField(
  value: unknown,
  name: string,
): string | undefined {
  const candidate = readField(value, name);
  return typeof candidate === 'string' ? candidate : undefined;
}

export function readArrayField(
  value: unknown,
  name: string,
): readonly unknown[] {
  const candidate = readField(value, name);
  return Array.isArray(candidate) ? candidate : [];
}
