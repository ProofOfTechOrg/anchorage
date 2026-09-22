import { defineConfig } from 'vitest/config';

// The process-loss titles are the long ones, so a checkpoint reruns them only when a file they load changes.
export default defineConfig({
  test: {
    name: 'fleet-control-direct-scenario-seams',
    include: ['test/direct-credentialed-scenario.seams.test.ts'],
    testTimeout: 20_000,
  },
});
