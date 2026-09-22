// SPDX-License-Identifier: Apache-2.0
// Shared line-oriented edge patterns for Flowsafe's packed declaration gates
// and source leaf-edge tests. Fleet Control's static graph uses a TypeScript AST
// walk because it resolves a different surface.
// This file imports nothing relative: packed .mjs gates load its .ts path under
// Node type stripping, where a .js specifier has no emitted file to resolve.

/**
 * Each pattern is one line form a source names another module in: a `from`
 * clause on an import or a re-export, a side-effect import, a dynamic import,
 * a `require`, a module augmentation, a triple-slash reference. Group 1 is the
 * specifier. A form missing from this list is an edge a scan cannot see.
 */
export const MODULE_EDGE_PATTERNS: readonly RegExp[] = [
  /\bfrom\s*['"]([^'"]+)['"]\s*;?\s*$/,
  /^\s*import\s*['"]([^'"]+)['"]\s*;?\s*$/,
  /\bimport\(\s*['"]([^'"]+)['"]\s*\)/,
  /\brequire\(\s*['"]([^'"]+)['"]\s*\)/,
  /^\s*declare\s+module\s+['"]([^'"]+)['"]/,
  /^\s*\/\/\/\s*<reference\s+(?:path|types|lib)\s*=\s*['"]([^'"]+)['"]/,
];

/** The module specifiers a source names, in source order. */
export function specifiersIn(source: string): string[] {
  return source
    .split('\n')
    .flatMap((line) =>
      MODULE_EDGE_PATTERNS.map((pattern) => pattern.exec(line)?.[1]).filter(
        (specifier): specifier is string => specifier !== undefined,
      ),
    );
}

/** Whether a source names a module other than itself. */
export function namesAnotherModule(source: string): boolean {
  return specifiersIn(source).length > 0;
}
