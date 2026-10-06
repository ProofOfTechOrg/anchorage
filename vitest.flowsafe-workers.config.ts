import { defineConfig } from 'vitest/config';

// `vitest.breakwater-workers.config.mts` carries the miniflare/workerd override
// record and the pool-upgrade rule that govern this config too.
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
          // @mastra/core imports Node modules this date provides only by flag;
          // without the flag the test module fails to load.
          compatibilityFlags: [
            'nodejs_compat',
            'enable_nodejs_os_module',
            'enable_nodejs_fs_module',
            'enable_nodejs_http_modules',
            'enable_nodejs_child_process_module',
          ],
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
