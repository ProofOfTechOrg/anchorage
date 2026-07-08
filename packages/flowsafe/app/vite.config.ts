import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

import { runApiDevPlugin } from './run-api-dev-plugin.js';

// Full Vite React app for the Anchorage showcase. Vite bundles the Astryx CSS
// (imported in src/index.css) and the approval-ui components. In `serve` mode
// runApiDevPlugin mounts the in-process showcase host — /api/approvals (the
// dashboard) + /runs + /workflows (the launcher/status panel) — so the app is a
// real working backend. A production `build` is a pure client bundle that
// targets the deployed worker on the same origin (or VITE_*_API_URL overrides).
export default defineConfig(({ command }) => ({
  plugins: [react(), ...(command === 'serve' ? [runApiDevPlugin()] : [])],
  server: { port: 4321 },
}));
