// SPDX-License-Identifier: Apache-2.0
// One home for the line forms a module states an edge in. The packed-surface
// gates in ../scripts scan emitted declarations with these patterns, and the
// leaf-edge cases in ../src/do-runner scan a source sibling with them.
//
// Nothing relative is imported here, which is what lets both kinds of consumer
// reach the same file: the `.mjs` gates import it by its `.ts` path under
// Node's type stripping, which has nothing further to resolve from here, and
// the flowsafe test program compiles it as an ordinary member.

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
