import { fileURLToPath } from 'node:url';

import { cloudflareTest } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: {
        configPath: fileURLToPath(
          new URL(
            './packages/breakwater/worker-tests/wrangler.jsonc',
            import.meta.url,
          ),
        ),
      },
    }),
  ],
  test: {
    name: 'breakwater-workers',
    include: ['packages/breakwater/worker-tests/**/*.workers.test.ts'],
  },
});
