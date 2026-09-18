import { defineConfig } from 'vitest/config';

// One `vitest` process for the whole workspace: `pnpm test` / `pnpm test:watch`
// at the root run every package's suite (each with its own config/aliases),
// with unified reporting and cross-package watch. Root `package.json` splits
// that run by project name for CI: `test:without-direct-scenario` and
// `test:direct-scenario` pass `--project`, and the `verify-core` and
// `direct-scenario` jobs in `.github/workflows/ci.yml` run one each.
export default defineConfig({
  test: {
    projects: [
      // Placement rule: a package's default suite arrives through this glob.
      'packages/*/vitest.config.ts',
      // A package-local config naming a second, non-default file set does not
      // match the glob and is registered by its explicit path, as this entry
      // is. It sits beside its package so the package's own `test` script can
      // run it with a package-relative `--config`, which names a config file
      // rather than a project. Standalone: this config imports nothing from
      // `packages/fleet-control/vitest.config.ts`, so an option added there
      // does not reach the direct-scenario suites.
      'packages/fleet-control/vitest.direct-scenario.config.ts',
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
