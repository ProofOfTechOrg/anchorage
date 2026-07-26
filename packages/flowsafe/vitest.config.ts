import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: [
      // Cross-package contract/e2e tests import breakwater's SOURCE (exact
      // root-barrel match only) so `pnpm -r test` never depends on a built
      // breakwater dist. tsconfig.test.json mirrors this with `paths`.
      {
        find: /^@proofoftech\/breakwater$/,
        replacement: new URL('../breakwater/src/index.ts', import.meta.url)
          .pathname,
      },
      {
        find: /^@proofoftech\/breakwater\/agent$/,
        replacement: new URL(
          '../breakwater/src/agent/index.ts',
          import.meta.url,
        ).pathname,
      },
      {
        find: /^@proofoftech\/breakwater\/audit$/,
        replacement: new URL(
          '../breakwater/src/audit/index.ts',
          import.meta.url,
        ).pathname,
      },
      // deploy/worker.e2e.test.ts imports the copy-ready template, whose
      // package-specifier imports must resolve to THIS package's source (the
      // exports map points at dist/, which tests must not depend on).
      // tsconfig.test.json mirrors these with `paths`.
      ...[
        'agent-host',
        'approval-api',
        'audit-export',
        'do-runner',
        'host-kit',
      ].map((subpath) => ({
        find: new RegExp(`^@proofoftech/flowsafe/${subpath}$`),
        replacement: new URL(`./src/${subpath}/index.ts`, import.meta.url)
          .pathname,
      })),
    ],
  },
  test: {
    passWithNoTests: true,
  },
});
