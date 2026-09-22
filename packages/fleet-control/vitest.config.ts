import { configDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // The direct scenario suites belong to the root direct-scenario projects,
    // which the package `test` script runs after this config and CI runs in
    // jobs beside `verify-core`. Listing them here as well would run them twice
    // under `pnpm test`. Those projects are standalone: an option added here
    // does not reach them, so a shared option is written in each config.
    exclude: [
      ...configDefaults.exclude,
      'test/direct-credentialed-scenario.test.ts',
      'test/direct-credentialed-scenario.seams.test.ts',
      'test/direct-reference-fence.harness.test.ts',
    ],
    // Timeouts here bound hangs, not durations: no title asserts its own
    // duration, and the in-body watchdogs, races, and vi.waitFor bounds a few
    // titles carry are hang detectors, not budgets. Titles that sleep through
    // the Cloudflare SDK's retry backoff, and titles that do real
    // scratch-directory work while the forks pool shares the machine, run past
    // vitest's 5 s default inside the full package suite (5.3 s at the slowest
    // observed), so this default clears them. Suites that drive real workerd
    // set their own caps above this; the deliberately tight per-title caps
    // that sit below it say so where they stand. hookTimeout stays at vitest's
    // 10 s: the hooks that boot or close workerd set their own, and the rest
    // are per-test teardowns that never touch workerd.
    testTimeout: 20_000,
  },
});
