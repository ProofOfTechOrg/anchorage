import { type ReactElement, StrictMode, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from '../../src/approval-ui/App.js';
import { ApprovalApiClient } from '../../src/approval-ui/client.js';
import { ApprovalUIProvider } from '../../src/approval-ui/components.js';
import { astryxComponents } from './astryx-components.js';
import { RunClient } from './run-client.js';
import {
  ActorSwitcher,
  actorForToken,
  DEFAULT_ACTOR,
  LauncherPanel,
  type RunEntry,
  RunStatusPanel,
} from './showcase-panels.js';
import './index.css';

const container = document.getElementById('root');
if (!container) throw new Error('missing #root element');

// Same-origin by default: the single-deploy worker serves the approval API and
// the run surface on one origin. Overridable for a split deployment.
const APPROVAL_BASE = import.meta.env.VITE_APPROVAL_API_URL ?? '/api/approvals';
const RUN_BASE = import.meta.env.VITE_RUN_API_URL ?? '';

// The showcase shell: hold the acting identity + the launched runs at the root,
// derive both API clients from the identity (a new client instance re-triggers
// the dashboard fetch and re-scopes the run polling), and render the controls
// beside the approval dashboard under the shared Astryx provider.
function Root(): ReactElement {
  const [actorToken, setActorToken] = useState(DEFAULT_ACTOR.token);
  const [runs, setRuns] = useState<readonly RunEntry[]>([]);

  const actor = actorForToken(actorToken);
  const authHeaders = useMemo(
    () => ({ authorization: `Bearer ${actorToken}` }),
    [actorToken],
  );
  const approvalClient = useMemo(
    () =>
      new ApprovalApiClient({ baseUrl: APPROVAL_BASE, headers: authHeaders }),
    [authHeaders],
  );
  const runClient = useMemo(
    () => new RunClient({ baseUrl: RUN_BASE, headers: authHeaders }),
    [authHeaders],
  );

  function addRun(entry: RunEntry): void {
    setRuns((current) => [entry, ...current]);
  }

  return (
    <ApprovalUIProvider components={astryxComponents}>
      <ActorSwitcher actorToken={actorToken} onSelect={setActorToken} />
      <LauncherPanel
        runClient={runClient}
        actorRole={actor.role}
        onStarted={addRun}
      />
      <RunStatusPanel runClient={runClient} runs={runs} />
      <App client={approvalClient} pollIntervalMs={5000} />
    </ApprovalUIProvider>
  );
}

createRoot(container).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);
