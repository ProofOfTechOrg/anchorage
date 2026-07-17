// SPDX-License-Identifier: Apache-2.0
// Barrel-isolation guard: the `@proofoftech/flowsafe/host-kit` barrel must NOT
// drag the durable `Agent` OR breakwater into its transitive import graph.
//
// host-kit is a subpath a route-mounting / browser-facing consumer imports
// WITHOUT authoring workflows, and two isolation properties the architecture
// rests on are otherwise enforced only STRUCTURALLY (a subpath split + typecheck)
// — which typecheck alone does NOT protect against regressing:
//
//   - `@mastra/core/agent/durable` drags @mastra's Node built-ins. The approval
//     bridge derives agent-gate connectors from the PURE LEAF
//     `../agent-runner/approval-shapes.js`, never the `./agent-runner` barrel
//     (which re-exports durable-agent-runner.ts -> @mastra/core/agent/durable).
//     Because `agentGateConnectors` is exported from BOTH the leaf and the
//     barrel, a "tidy" that rewrites the deep import up to the barrel typechecks
//     clean — only an import-graph assertion catches it.
//   - `@proofoftech/breakwater` (a devDependency) is reachable only from
//     `host-kit/module.ts`, which ships under the SEPARATE `./host-kit/module`
//     subpath and is never re-exported from this barrel — so a consumer of
//     `createRunRouter` resolves no breakwater type.
//
// Mechanism: statically walk the barrel's import graph, following ONLY relative
// (./ ../) specifiers within src, collecting every bare module specifier. The
// agent-runner barrel and host-kit/module.ts serve as POSITIVE CONTROLS, proving
// the walk actually detects each taint when it IS present (so an absence is a
// real property, not a walk that missed).

import { describe, expect, it } from 'vitest';

// Node builtins load via process.getBuiltinModule (the test-support/sqlite.ts +
// spdx.test.ts pattern): this file compiles in the workers-typed package test
// pass, which has no @types/node to resolve `node:` import specifiers against.
interface FsModule {
  existsSync(path: string): boolean;
  readFileSync(path: string, encoding: 'utf8'): string;
}
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

const { existsSync, readFileSync } = builtin<FsModule>('node:fs');
const { dirname, resolve } = builtin<PathModule>('node:path');
const { fileURLToPath } = builtin<UrlModule>('node:url');

// The workers-typed ImportMeta carries no `url`; at runtime (vitest on node) it
// is always present.
const HERE = dirname(
  fileURLToPath((import.meta as unknown as { url: string }).url),
);
const DURABLE_AGENT = '@mastra/core/agent/durable';
const BREAKWATER = '@proofoftech/breakwater';

// Import/export module specifiers of a TS source, anchored to an import/export
// keyword so a `from '...'` inside a comment or string is never matched. Covers
// `import ... from '...'`, `export ... from '...'` (incl. `type`/`export *`,
// multiline), side-effect `import '...'`, and dynamic `import('...')`.
function importSpecifiers(source: string): string[] {
  const patterns = [
    /(?:^|\n)\s*(?:import|export)\b[\s\S]*?from\s*['"]([^'"]+)['"]/g,
    /(?:^|\n)\s*import\s*['"]([^'"]+)['"]/g,
    /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
  ];
  const specs: string[] = [];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      const spec = match[1];
      if (spec !== undefined) specs.push(spec);
    }
  }
  return specs;
}

// Resolve a relative specifier to its on-disk source. Every relative import in
// this package is ESM-style `.js`-suffixed (pinned by the sibling grep in this
// change), so `.js` maps to the sibling `.ts`/`.tsx`; the extensionless branch
// is defensive.
function resolveRelative(fromFile: string, spec: string): string | undefined {
  const base = resolve(dirname(fromFile), spec);
  const candidates = spec.endsWith('.js')
    ? [base.replace(/\.js$/, '.ts'), base.replace(/\.js$/, '.tsx')]
    : [`${base}.ts`, `${base}.tsx`, resolve(base, 'index.ts')];
  return candidates.find((candidate) => existsSync(candidate));
}

// Every BARE (non-relative) specifier reachable from `entry` by following only
// relative specifiers, the set of visited files, and any relative specifier that
// failed to resolve (asserted empty, so a resolver gap fails LOUDLY rather than
// silently dropping a subtree and hiding a taint).
function importGraph(entry: string): {
  bare: Set<string>;
  visited: Set<string>;
  unresolved: string[];
} {
  const bare = new Set<string>();
  const visited = new Set<string>();
  const unresolved: string[] = [];
  const stack = [entry];
  while (stack.length > 0) {
    const file = stack.pop();
    if (file === undefined || visited.has(file)) continue;
    visited.add(file);
    for (const spec of importSpecifiers(readFileSync(file, 'utf8'))) {
      if (spec.startsWith('.')) {
        const resolved = resolveRelative(file, spec);
        if (resolved === undefined) unresolved.push(`${file} -> ${spec}`);
        else stack.push(resolved);
      } else {
        bare.add(spec);
      }
    }
  }
  return { bare, visited, unresolved };
}

const hostKit = importGraph(resolve(HERE, 'index.ts'));

describe('host-kit barrel import isolation', () => {
  it('resolves every relative specifier in the graph (no vacuous pass)', () => {
    // #then — a resolver gap would silently drop a subtree and hide a taint
    expect(hostKit.unresolved).toEqual([]);
    // and the walk actually reached the bridge and the pure agent leaf
    const basenames = [...hostKit.visited].map((f) => f.split('/').at(-1));
    expect(basenames).toContain('approval-bridge.ts');
    expect(basenames).toContain('approval-shapes.ts');
  });

  it('does NOT transitively import @mastra/core/agent/durable', () => {
    // #then — the bridge reaches the pure leaf, never the agent-runner barrel
    const durable = [...hostKit.bare].filter((s) =>
      s.startsWith(DURABLE_AGENT),
    );
    expect(durable).toEqual([]);
  });

  it('does NOT transitively import @proofoftech/breakwater (barrel stays breakwater-free)', () => {
    // #then — breakwater is reachable only from host-kit/module.ts (./host-kit/module)
    const bw = [...hostKit.bare].filter((s) => s.startsWith(BREAKWATER));
    expect(bw).toEqual([]);
  });

  it('POSITIVE CONTROL: the walk DOES detect the durable Agent via the agent-runner barrel', () => {
    // #given the agent-runner barrel — the tainted graph the bridge must avoid
    const agentRunner = importGraph(
      resolve(HERE, '..', 'agent-runner', 'index.ts'),
    );
    // #then the walk finds @mastra/core/agent/durable there, so its ABSENCE from
    // the host-kit graph above is a real property, not a walk that missed
    expect(agentRunner.unresolved).toEqual([]);
    expect([...agentRunner.bare]).toContain(DURABLE_AGENT);
  });

  it('POSITIVE CONTROL: the walk DOES detect breakwater via host-kit/module.ts', () => {
    // #given the module-authoring contract — the ./host-kit/module subpath
    const moduleGraph = importGraph(resolve(HERE, 'module.ts'));
    // #then it carries the breakwater AuditLogger type, proving the barrel's
    // freeness above is a real property
    expect([...moduleGraph.bare]).toContain(BREAKWATER);
  });
});
