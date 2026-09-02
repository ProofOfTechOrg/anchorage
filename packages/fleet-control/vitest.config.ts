import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // Timeouts here bound hangs, not durations: no title asserts its own
    // duration (the in-body 1 s watchdogs in export-store.test.ts are hang
    // detectors, not budgets). Two titles have crossed vitest's 5 s default
    // inside the full suite (5.3 s observed): one sleeps through the
    // Cloudflare SDK's retry backoff and costs about 5 s on its own; the
    // other is 2.2-2.8 s alone with real scratch-directory work and crosses
    // only when the default forks pool shares the machine. Suites that drive
    // real workerd set their own caps above this; the two 2 s caps in
    // r2-export-store.test.ts are deliberate hang detectors. hookTimeout
    // stays at vitest's 10 s: every heavy hook sets its own.
    testTimeout: 20_000,
  },
});
