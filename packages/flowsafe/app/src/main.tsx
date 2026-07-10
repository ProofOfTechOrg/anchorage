import {
  lazy,
  type ReactElement,
  StrictMode,
  Suspense,
  useMemo,
  useState,
} from 'react';
import { createRoot } from 'react-dom/client';

import { App } from '../../src/approval-ui/App.js';
import { ApprovalApiClient } from '../../src/approval-ui/client.js';
import { ApprovalUIProvider } from '../../src/approval-ui/components.js';
import { astryxComponents } from './astryx-components.js';
import {
  DemoActorSwitcher,
  type DemoTokenSet,
  readDemoTokensFromHash,
  useDemoSignIn,
} from './demo-session.js';
import { RunClient } from './run-client.js';
import {
  LauncherPanel,
  type RunEntry,
  RunStatusPanel,
  TokenGate,
} from './showcase-panels.js';
import './index.css';

const container = document.getElementById('root');
if (!container) throw new Error('missing #root element');

// Same-origin by default: the single-deploy worker serves the approval API and
// the run surface on one origin. Overridable for a split deployment.
const APPROVAL_BASE = import.meta.env.VITE_APPROVAL_API_URL ?? '/api/approvals';
const RUN_BASE = import.meta.env.VITE_RUN_API_URL ?? '';

// Demo tokens exist ONLY in dev: the switcher module (the sole importer of
// showcase/demo-actors.js) is reachable exclusively through this dead-branch
// dynamic import, which the production build eliminates —
// scripts/assert-clean-app-bundle.mjs proves the bundle is token-free. (The
// PUBLIC demo's tokens are runtime values from the OAuth callback fragment,
// never literals.)
const DevActorSwitcher = import.meta.env.DEV
  ? lazy(async () => ({
      default: (await import('./dev-actor-switcher.js')).DevActorSwitcher,
    }))
  : null;

// The showcase shell: hold the acting token + the launched runs at the root,
// derive both API clients from the token (a new client instance re-triggers
// the dashboard fetch and re-scopes the run polling), and render the controls
// beside the approval dashboard under the shared Astryx provider. Identity
// (id/role/tenant) is never derived client-side — the server echoes it per
// request.
function Root(): ReactElement {
  const [actorToken, setActorToken] = useState<string | null>(null);
  // The OAuth callback delivers a per-visitor sandbox token set in the URL
  // fragment; read exactly once (the initializer also scrubs the hash).
  const [demoSession, setDemoSession] = useState<DemoTokenSet | null>(
    readDemoTokensFromHash,
  );
  const [runs, setRuns] = useState<readonly RunEntry[]>([]);
  const demoSignInProvider = useDemoSignIn();

  const authHeaders = useMemo(() => {
    const headers: Record<string, string> = {};
    if (actorToken !== null) headers.authorization = `Bearer ${actorToken}`;
    return headers;
  }, [actorToken]);
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

  function endDemoSession(): void {
    setDemoSession(null);
    setActorToken(null);
  }

  return (
    <ApprovalUIProvider components={astryxComponents}>
      {DevActorSwitcher ? (
        <Suspense fallback={null}>
          <DevActorSwitcher actorToken={actorToken} onSelect={setActorToken} />
        </Suspense>
      ) : demoSession ? (
        <DemoActorSwitcher
          session={demoSession}
          actorToken={actorToken}
          onSelect={setActorToken}
          onSession={setDemoSession}
          onExpired={endDemoSession}
        />
      ) : (
        <TokenGate
          signedIn={actorToken !== null}
          onSubmit={setActorToken}
          onSignOut={() => setActorToken(null)}
          demoSignInProvider={demoSignInProvider}
        />
      )}
      {actorToken !== null ? (
        <>
          <LauncherPanel runClient={runClient} onStarted={addRun} />
          <RunStatusPanel runClient={runClient} runs={runs} />
          <App client={approvalClient} pollIntervalMs={5000} />
        </>
      ) : null}
    </ApprovalUIProvider>
  );
}

createRoot(container).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);
