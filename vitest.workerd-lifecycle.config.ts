import { defineConfig } from 'vitest/config';

// The shared workerd dev-server lifecycle lives at the repository root because
// harnesses in two different packages use it, so its suite needs a root project
// the way scripts/flowsafe-harness.test.ts does. The other root `scripts/`
// tests are node:test files run through their own package scripts.
export default defineConfig({
  test: {
    name: 'workerd-lifecycle',
    include: ['scripts/workerd-server-lifecycle.test.mjs'],
  },
});
