import { fileURLToPath } from 'node:url';
import { configDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: [
      // Cross-package contract/e2e tests import breakwater's SOURCE so
      // `pnpm -r test` never depends on a built breakwater dist.
      // tsconfig.test.json mirrors this with `paths`.
      {
        find: /^@proofoftech\/breakwater$/,
        replacement: fileURLToPath(
          new URL('../breakwater/src/index.ts', import.meta.url),
        ),
      },
      {
        find: /^@proofoftech\/breakwater\/agent$/,
        replacement: fileURLToPath(
          new URL('../breakwater/src/agent/index.ts', import.meta.url),
        ),
      },
      {
        find: /^@proofoftech\/breakwater\/audit$/,
        replacement: fileURLToPath(
          new URL('../breakwater/src/audit/index.ts', import.meta.url),
        ),
      },
      {
        find: /^@proofoftech\/breakwater\/connector-sdk$/,
        replacement: fileURLToPath(
          new URL('../breakwater/src/connector-sdk/index.ts', import.meta.url),
        ),
      },
      {
        find: /^@proofoftech\/breakwater\/rbac$/,
        replacement: fileURLToPath(
          new URL('../breakwater/src/rbac/index.ts', import.meta.url),
        ),
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
        replacement: fileURLToPath(
          new URL(`./src/${subpath}/index.ts`, import.meta.url),
        ),
      })),
      // The deadline subpaths are single modules rather than directory
      // barrels, so they carry their own aliases instead of joining the list
      // above, whose replacement appends `/index.ts`. tsconfig.test.json
      // mirrors them with `paths`.
      ...['do-runner/constants', 'do-runner/testing'].map((subpath) => ({
        find: new RegExp(`^@proofoftech/flowsafe/${subpath}$`),
        replacement: fileURLToPath(
          new URL(`./src/${subpath}.ts`, import.meta.url),
        ),
      })),
    ],
  },
  test: {
    exclude: [...configDefaults.exclude, '**/*.workerd.test.ts'],
  },
});
