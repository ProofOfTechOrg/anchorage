import { fileURLToPath } from 'node:url';

import { cloudflareTest } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';

// The Workers pool for the breakwater worker-tests suite, on a split toolchain:
// the miniflare @cloudflare/vitest-pool-workers resolves depends on workerd
// 1.20260730.1, and the root pnpm override
// `miniflare@5.20260730.0-alpha>workerd` (package.json) re-points that workerd
// to the version recorded there. An upgrade of the pool re-points the override
// to the workerd the new miniflare depends on, or deletes the override once the
// pool resolves a miniflare whose own workerd is the wanted one.
//
// `wrangler.configPath` and the test `include` resolve against different bases.
// `configPath` is file-relative, built from `import.meta.url`. The `include` is
// repository-relative because this config sits at the repository root:
// `vitest.config.ts` registers this file by path, and vitest roots a project at
// its config file's own directory.
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
