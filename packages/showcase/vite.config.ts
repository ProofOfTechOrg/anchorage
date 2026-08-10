import { cloudflare } from '@cloudflare/vite-plugin';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const src = (path: string) => new URL(path, import.meta.url).pathname;

// Full Vite React app and Cloudflare Worker for the Anchorage showcase. The
// Cloudflare plugin drives both development and production from wrangler.jsonc,
// so API requests, bindings, Durable Objects, and WebSockets use the deployed
// topology while Vite bundles the SPA and approval-ui components.
export default defineConfig({
  plugins: [react(), cloudflare()],
  resolve: {
    // Mirrors tsconfig.json paths — @/ (SPA) and @flowsafe/ (deep DOM-free
    // flowsafe source; the SPA bundles the library from source, exactly as it
    // did when both lived in one package). #worker/* needs no entry — Vite
    // resolves the package.json "imports" field natively.
    alias: [
      { find: /^@\/(.*)$/, replacement: src('./src/$1') },
      { find: /^@flowsafe\/(.*)$/, replacement: src('../flowsafe/src/$1') },
    ],
  },
  server: { port: 4321 },
  preview: { port: 8787 },
});
