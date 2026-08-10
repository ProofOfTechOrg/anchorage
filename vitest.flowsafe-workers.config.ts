import { defineConfig } from 'vitest/config';

export default defineConfig(async () => {
  const { cloudflareTest } = await import('@cloudflare/vitest-pool-workers');
  return {
    plugins: [
      cloudflareTest({
        // Load the real bindings and DO migrations without booting the full spike.
        main: './packages/flowsafe/test-support/cloudflare-test-worker.ts',
        miniflare: {
          // The pool otherwise gives its runner today's unsupported date.
          compatibilityDate: '2025-06-01',
        },
        wrangler: {
          configPath: './packages/flowsafe/spike/wrangler.jsonc',
        },
      }),
    ],
    test: {
      name: 'flowsafe-workers',
      include: ['packages/flowsafe/**/*.workerd.test.ts'],
    },
  };
});
