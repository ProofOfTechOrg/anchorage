// SPDX-License-Identifier: Apache-2.0

import { existsSync } from 'node:fs';
import { expect } from 'vitest';

export function expectBuiltDist(
  specifier: string,
  importMetaUrl: string,
  message: string,
): void {
  expect(existsSync(new URL(specifier, importMetaUrl)), message).toBe(true);
}
