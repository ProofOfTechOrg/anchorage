import { defineConfig } from 'vitest/config';

// The direct credentialed scenario and the offline fence suite drive the
// reference and tenant Workers through full scenario runs on local workerd;
// the pair needs the breakwater, flowsafe and fleet-control dists (the suites
// import @proofoftech/flowsafe subpaths that resolve into flowsafe's dist, and
// the observations module imports the fleet-control package). They are a root
// project of their own so `pnpm test` can leave them out and
// `pnpm test:direct-scenario` can select them; the package project
// (vitest.config.ts) excludes their files. This configuration sits in the
// package because its include paths resolve against this directory.
// The project name below is the literal the root scripts select and negate.
export default defineConfig({
  test: {
    name: 'fleet-control-direct-scenario',
    include: [
      'test/direct-credentialed-scenario.test.ts',
      'test/direct-reference-fence.harness.test.ts',
    ],
    // Same hang bound as the package project, written out here because this
    // project inherits nothing from vitest.config.ts.
    testTimeout: 20_000,
  },
});
