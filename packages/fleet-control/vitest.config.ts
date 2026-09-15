import { configDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // The two direct scenario suites belong to the root project
    // `fleet-control-direct-scenario` (vitest.direct-scenario.config.ts),
    // which the package `test` script runs after this config and CI runs in
    // its own job beside `verify-core`. Listing them here as well would run
    // them twice under `pnpm test`.
    exclude: [
      ...configDefaults.exclude,
      'test/direct-credentialed-scenario.test.ts',
      'test/direct-reference-fence.harness.test.ts',
    ],
    // Timeouts here bound hangs, not durations: no title asserts its own
    // duration, and the in-body watchdogs, races, and vi.waitFor bounds a few
    // titles carry are hang detectors, not budgets. Two titles have run past
    // vitest's 5 s default inside the full package suite (5.3 s observed): one
    // in cloudflare-client-plain-worker.test.ts sleeps through the Cloudflare
    // SDK's retry backoff and costs about 5 s regardless of load (the 15 s cap
    // this default replaced had shielded it from the 5 s default); the other,
    // in cross-backend-continuation.test.ts, runs about 2.8 s alone with real
    // scratch-directory work and timed out at 5 s only when the forks pool
    // shared the machine. Suites that drive real workerd set their own caps
    // above this; the deliberately tight per-title caps that sit below it say
    // so where they stand. hookTimeout stays at vitest's 10 s: the hooks that
    // boot or close workerd set their own, and the rest are per-test teardowns
    // that never touch workerd.
    testTimeout: 20_000,
  },
});
