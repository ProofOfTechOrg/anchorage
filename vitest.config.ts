import { defineConfig } from 'vitest/config';

// One `vitest` process for the whole workspace: `pnpm test` / `pnpm test:watch`
// at the root run every package's suite (each with its own config/aliases),
// with unified reporting and cross-package watch.
export default defineConfig({
  test: {
    projects: ['packages/*/vitest.config.ts'],
  },
});
