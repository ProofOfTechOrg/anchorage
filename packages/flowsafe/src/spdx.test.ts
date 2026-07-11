// SPDX-License-Identifier: Apache-2.0
// License-header guard: every source file in this package carries the SPDX
// identifier on line 1 — line 2 only when line 1 is a tooling pragma
// (`// @vitest-environment`), which must stay first for vitest to read it.
// tsc keeps leading comments, so the notice ships in dist too.
//
// Node builtins load via process.getBuiltinModule (the test-support/sqlite.ts
// pattern): this file compiles in the workers-typed package test pass, which
// has no @types/node to resolve `node:` import specifiers against.

import { describe, expect, it } from 'vitest';

const SPDX = '// SPDX-License-Identifier: Apache-2.0';

interface FsModule {
  readdirSync(
    path: string,
    options: { withFileTypes: true },
  ): Array<{ name: string; isDirectory(): boolean }>;
  readFileSync(path: string, encoding: 'utf8'): string;
}
interface PathModule {
  dirname(path: string): string;
  join(...parts: string[]): string;
}
interface UrlModule {
  fileURLToPath(url: string): string;
}

function builtin<T>(id: string): T {
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error(`${id} unavailable — tests require node >= 22`);
  }
  return getBuiltin(id) as T;
}

const fs = builtin<FsModule>('node:fs');
const path = builtin<PathModule>('node:path');
const { fileURLToPath } = builtin<UrlModule>('node:url');

// The workers-typed ImportMeta carries no `url`; at runtime (vitest on node)
// it is always present.
const SRC_DIR = path.dirname(
  fileURLToPath((import.meta as unknown as { url: string }).url),
);

function sourceFiles(dir: string): string[] {
  const files: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...sourceFiles(entryPath));
    else if (/\.tsx?$/.test(entry.name)) files.push(entryPath);
  }
  return files;
}

describe('SPDX headers', () => {
  it('every src .ts/.tsx file starts with the Apache-2.0 identifier', () => {
    const offenders = sourceFiles(SRC_DIR).filter((file) => {
      const lines = fs.readFileSync(file, 'utf8').split('\n');
      const first = lines[0] ?? '';
      return first.startsWith('// @vitest-environment')
        ? lines[1] !== SPDX
        : first !== SPDX;
    });
    expect(offenders).toEqual([]);
  });
});
