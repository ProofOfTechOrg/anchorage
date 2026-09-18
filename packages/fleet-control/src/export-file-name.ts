// SPDX-License-Identifier: Apache-2.0

const FILE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;
const WINDOWS_DEVICE_NAME = /^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)/i;

export function isPortablePathSegment(value: string): boolean {
  return (
    FILE_NAME_PATTERN.test(value) &&
    !value.endsWith('.') &&
    !WINDOWS_DEVICE_NAME.test(value)
  );
}

export function assertFileName(fileName: string): void {
  if (!isPortablePathSegment(fileName)) {
    throw new Error('export fileName must be one portable path segment');
  }
}
