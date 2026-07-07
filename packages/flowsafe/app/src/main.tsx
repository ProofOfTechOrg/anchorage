import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from '../../src/approval-ui/App.js';
import { ApprovalApiClient } from '../../src/approval-ui/client.js';
import { ApprovalUIProvider } from '../../src/approval-ui/components.js';
import { astryxComponents } from './astryx-components.js';
import './index.css';

const container = document.getElementById('root');
if (!container) throw new Error('missing #root element');

// fetch is omitted: ApprovalApiClient defaults to globalThis.fetch, which the
// browser entry always has.
const client = new ApprovalApiClient({
  baseUrl: import.meta.env.VITE_APPROVAL_API_URL ?? '/api/approvals',
});

// The library is style-agnostic; this app injects the Astryx adapter. Swap
// astryxComponents for another adapter (or omit the provider for unstyled HTML).
createRoot(container).render(
  <StrictMode>
    <ApprovalUIProvider components={astryxComponents}>
      <App client={client} pollIntervalMs={5000} />
    </ApprovalUIProvider>
  </StrictMode>,
);
