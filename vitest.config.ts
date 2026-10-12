import { defineConfig } from 'vitest/config';

// One `vitest` process provides unified reporting and cross-package watch.
export default defineConfig({
  test: {
    projects: [
      // Placement rule: a package's default suite arrives through this glob.
      'packages/*/vitest.config.ts',
      // Root rule: a `vitest.*.config.*` at the repository root is registered
      // here — by glob where a family shares a name shape, by explicit name
      // otherwise — and `scripts/architecture-positive-controls.test.mjs` turns
      // an unregistered one red.
      'vitest.*-workers.config.*',
      'vitest.flowsafe-harness.config.ts',
      'vitest.workerd-lifecycle.config.ts',
    ],
  },
});
