// SPDX-License-Identifier: Apache-2.0
// License-header guard: every source file in this package carries the SPDX
// identifier on line 1 — line 2 only when line 1 is a tooling pragma
// (`// @vitest-environment`), which must stay first for vitest to read it.
// tsc keeps leading comments, so the notice ships in dist too.

import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const SPDX = '// SPDX-License-Identifier: Apache-2.0';
const SRC_DIR = dirname(fileURLToPath(import.meta.url));

function sourceFiles(dir: string): string[] {
  const files: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...sourceFiles(path));
    else if (/\.tsx?$/.test(entry.name)) files.push(path);
  }
  return files;
}

describe('SPDX headers', () => {
  it('every src .ts/.tsx file starts with the Apache-2.0 identifier', () => {
    const offenders = sourceFiles(SRC_DIR).filter((file) => {
      const lines = readFileSync(file, 'utf8').split('\n');
      const first = lines[0] ?? '';
      return first.startsWith('// @vitest-environment')
        ? lines[1] !== SPDX
        : first !== SPDX;
    });
    expect(offenders).toEqual([]);
  });
});
