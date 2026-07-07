import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

import { approvalApiDevPlugin } from './approval-api-dev-plugin.js';

// Full Vite React app for the approval dashboard. Vite bundles the Astryx CSS
// (imported in src/index.css) and the approval-ui components. In `serve` mode
// approvalApiDevPlugin mounts a live seeded approval-api at /api/approvals; a
// production `build` is a pure client bundle that targets VITE_APPROVAL_API_URL.
export default defineConfig(({ command }) => ({
  plugins: [react(), ...(command === 'serve' ? [approvalApiDevPlugin()] : [])],
  server: { port: 4321 },
}));
