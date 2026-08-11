import { fileURLToPath } from 'node:url';

import { cloudflareTest } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';

const packageRoot = fileURLToPath(
  new URL('./packages/breakwater/', import.meta.url),
);

export default defineConfig({
  root: packageRoot,
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
    include: ['worker-tests/**/*.workers.test.ts'],
  },
});
