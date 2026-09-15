import { defineConfig } from 'vitest/config';

// The direct credentialed scenario and the offline fence suite drive the
// reference and tenant Workers through full scenario runs on local workerd;
// the pair takes about 35 minutes and needs the breakwater, flowsafe and
// fleet-control dists (the suites import @proofoftech/flowsafe subpaths that
// resolve into flowsafe's dist, and the observations module imports the
// fleet-control package). They are a root project of their own so CI can run
// them in the `direct-scenario` job beside `verify-core`, while `pnpm test`
// at the root and the package `test` script still run them with everything
// else. The package project (vitest.config.ts) excludes the same two files.
// The project name below is the one the root scripts
// `test:without-direct-scenario` and `test:direct-scenario` and the CI job
// select; only the positive selection fails loudly when the names drift.
export default defineConfig({
  test: {
    name: 'fleet-control-direct-scenario',
    include: [
      'test/direct-credentialed-scenario.test.ts',
      'test/direct-reference-fence.harness.test.ts',
    ],
    // Same hang bound as the package project; both suites set their own
    // per-title caps above it.
    testTimeout: 20_000,
  },
});
