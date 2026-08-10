import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    name: 'flowsafe-harness',
    include: ['scripts/flowsafe-harness.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
