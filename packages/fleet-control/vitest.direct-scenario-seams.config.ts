import { defineConfig } from 'vitest/config';

// The process-loss titles run apart from the rest so their fifty-five minutes
// can run in a separate job and the project can be selected alone.
export default defineConfig({
  test: {
    name: 'fleet-control-direct-scenario-seams',
    include: ['test/direct-credentialed-scenario.seams.test.ts'],
    testTimeout: 20_000,
  },
});
