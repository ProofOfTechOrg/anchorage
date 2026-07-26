// SPDX-License-Identifier: Apache-2.0
// The package root promises a complete mirror of four lightweight subpath
// barrels. Runtime comparison alone misses erased interfaces and type aliases,
// so this guard checks both emitted namespace keys and TypeScript module symbols.

import ts from 'typescript';
import { describe, expect, it } from 'vitest';

import * as approvalApi from './approval-api/index.js';
import * as artifacts from './artifacts/index.js';
import * as auditExport from './audit-export/index.js';
import * as doRunner from './do-runner/index.js';
import * as root from './index.js';

interface PathModule {
  dirname(path: string): string;
  resolve(...parts: string[]): string;
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

const { dirname, resolve } = builtin<PathModule>('node:path');
const { fileURLToPath } = builtin<UrlModule>('node:url');
const HERE = dirname(
  fileURLToPath((import.meta as unknown as { url: string }).url),
);

const mirrors = [
  {
    name: 'approval-api',
    file: resolve(HERE, 'approval-api', 'index.ts'),
    runtime: approvalApi,
  },
  {
    name: 'artifacts',
    file: resolve(HERE, 'artifacts', 'index.ts'),
    runtime: artifacts,
  },
  {
    name: 'audit-export',
    file: resolve(HERE, 'audit-export', 'index.ts'),
    runtime: auditExport,
  },
  {
    name: 'do-runner',
    file: resolve(HERE, 'do-runner', 'index.ts'),
    runtime: doRunner,
  },
] as const;

const rootFile = resolve(HERE, 'index.ts');
const program = ts.createProgram({
  rootNames: [rootFile, ...mirrors.map((mirror) => mirror.file)],
  options: {
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    skipLibCheck: true,
    target: ts.ScriptTarget.ES2022,
  },
});
const checker = program.getTypeChecker();

function moduleExports(file: string): Set<string> {
  const source = program.getSourceFile(file);
  if (!source) throw new Error(`TypeScript did not load ${file}`);
  const symbol = checker.getSymbolAtLocation(source);
  if (!symbol) throw new Error(`TypeScript found no module symbol for ${file}`);
  return new Set(
    checker.getExportsOfModule(symbol).map((exported) => exported.getName()),
  );
}

const rootExports = moduleExports(rootFile);

describe.each(mirrors)('root barrel parity: $name', (mirror) => {
  it('mirrors every runtime export', () => {
    const missing = Object.keys(mirror.runtime).filter(
      (name) => !(name in root),
    );
    expect(missing).toEqual([]);
  });

  it('mirrors every type and value export', () => {
    const subpathExports = moduleExports(mirror.file);
    expect(subpathExports.size).toBeGreaterThan(0);
    const missing = [...subpathExports].filter(
      (name) => !rootExports.has(name),
    );
    expect(missing).toEqual([]);
  });
});

describe('root barrel parity controls', () => {
  it('the symbol guard sees both erased types and runtime values', () => {
    const doRunnerExports = moduleExports(
      resolve(HERE, 'do-runner', 'index.ts'),
    );
    expect(doRunnerExports).toContain('D1StorageOptions');
    expect(doRunnerExports).toContain('RunnerRuntime');
  });
});
