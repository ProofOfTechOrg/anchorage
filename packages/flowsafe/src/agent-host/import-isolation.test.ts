// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

interface Fs {
  existsSync(path: string): boolean;
  readFileSync(path: string, encoding: 'utf8'): string;
}
interface Path {
  dirname(path: string): string;
  resolve(...parts: string[]): string;
}
interface Url {
  fileURLToPath(url: string): string;
}

function builtin<T>(id: string): T {
  const get = (
    globalThis as {
      process?: { getBuiltinModule?: (name: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!get) throw new Error('node >=22 is required');
  return get(id) as T;
}

const fs = builtin<Fs>('node:fs');
const path = builtin<Path>('node:path');
const url = builtin<Url>('node:url');
const here = path.dirname(
  url.fileURLToPath((import.meta as unknown as { url: string }).url),
);

function specifiers(source: string): string[] {
  const found: string[] = [];
  for (const pattern of [
    /(?:^|\n)\s*(?:import|export)\b[\s\S]*?from\s*['"]([^'"]+)['"]/g,
    /(?:^|\n)\s*import\s*['"]([^'"]+)['"]/g,
    /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
  ]) {
    for (const match of source.matchAll(pattern)) {
      if (match[1]) found.push(match[1]);
    }
  }
  return found;
}

function graph(entry: string) {
  const visited = new Set<string>();
  const bare = new Set<string>();
  const unresolved: string[] = [];
  const pending = [entry];
  while (pending.length > 0) {
    const file = pending.pop();
    if (!file || visited.has(file)) continue;
    visited.add(file);
    for (const specifier of specifiers(fs.readFileSync(file, 'utf8'))) {
      if (!specifier.startsWith('.')) {
        bare.add(specifier);
        continue;
      }
      const base = path.resolve(path.dirname(file), specifier);
      const candidates = specifier.endsWith('.js')
        ? [base.replace(/\.js$/, '.ts'), base.replace(/\.js$/, '.tsx')]
        : [`${base}.ts`, `${base}.tsx`, path.resolve(base, 'index.ts')];
      const target = candidates.find(fs.existsSync);
      if (target) pending.push(target);
      else unresolved.push(`${file} -> ${specifier}`);
    }
  }
  return { bare, visited, unresolved };
}

describe('agent-host subpath isolation', () => {
  const entries = {
    root: path.resolve(here, '..', 'index.ts'),
    hostKit: path.resolve(here, '..', 'host-kit', 'index.ts'),
    agentRunner: path.resolve(here, '..', 'agent-runner', 'index.ts'),
    signalsClient: path.resolve(here, '..', 'signals', 'client.ts'),
  };

  it.each(
    Object.entries(entries),
  )('keeps $0 free of agent-host and Breakwater', (_name, entry) => {
    const result = graph(entry);
    expect(result.unresolved).toEqual([]);
    expect(
      [...result.visited].some((file) => file.includes('/agent-host/')),
    ).toBe(false);
    expect(
      [...result.bare].filter((specifier) =>
        specifier.startsWith('@proofoftech/breakwater'),
      ),
    ).toEqual([]);
  });

  it('positive control: agent-host alone imports the guarded Breakwater subpath', () => {
    const result = graph(path.resolve(here, 'index.ts'));
    expect(result.unresolved).toEqual([]);
    expect(result.bare).toContain('@proofoftech/breakwater/agent');
  });
});

describe('do-runner -> approval-api edge', () => {
  // contract.ts documents approval-api -> do-runner as the intended direction.
  // thread-do.ts now reaches BACK for the execution-principal validator, because
  // reconstructing a principal at the DO trust boundary must use the same
  // validator every other consumer does. That one edge is accepted; pin it so a
  // future import cannot widen the direction silently.
  const ALLOWED = new Set([
    'approval-api/principal.ts', // the validator thread-do.ts reaches for
    'approval-api/contract.ts', // principal.ts's role vocabulary
    // Reached today only by contract.ts's `import type { ApprovalRecord }`,
    // which erases. NOTE: this pin is file-level — the walker does not
    // distinguish `import type` from a runtime import, so it catches a NEW file
    // being reached, not this one being reached a new way.
    'approval-api/types.ts',
  ]);

  it('reaches approval-api only through the principal and contract leaves', () => {
    const result = graph(path.resolve(here, '..', 'do-runner', 'index.ts'));
    expect(result.unresolved).toEqual([]);
    const reached = [...result.visited]
      .filter((file) => file.includes('/approval-api/'))
      .map((file) => file.slice(file.indexOf('approval-api/')))
      .sort();
    expect(reached.filter((entry) => !ALLOWED.has(entry))).toEqual([]);
  });
});
