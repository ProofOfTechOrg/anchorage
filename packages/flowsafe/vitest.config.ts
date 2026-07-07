import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: [
      // Cross-package contract/e2e tests import breakwater's SOURCE (exact
      // root-barrel match only) so `pnpm -r test` never depends on a built
      // breakwater dist. tsconfig.test.json mirrors this with `paths`.
      {
        find: /^@proofoftech\/breakwater$/,
        replacement: new URL(
          '../breakwater/src/index.ts',
          import.meta.url,
        ).pathname,
      },
    ],
  },
  test: {
    passWithNoTests: true,
  },
});
