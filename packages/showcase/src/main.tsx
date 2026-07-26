import { ToastViewport } from '@astryxdesign/core/Toast';
import { Theme } from '@astryxdesign/core/theme';
import { y2kTheme } from '@astryxdesign/theme-y2k/built';
import type { ApprovalStreamOption } from '@flowsafe/approval-ui/use-approval-dashboard';
import {
  lazy,
  type ReactElement,
  StrictMode,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from 'react';
import { createRoot } from 'react-dom/client';
import { AstryxApprovalUIProvider } from '@/astryx-components';
import {
  DemoActorSwitcher,
  type DemoTokenSet,
  readDemoTokensFromHash,
  useDemoSignIn,
} from '@/demo-session';
import { AppErrorBoundary } from '@/error-boundary';
import { createHubStreamClient } from '@/hub-stream-client';
import { NarratingApprovalClient } from '@/narrating-approval-client';
import { sessionReadyEvent } from '@/narration';
import { RunClient } from '@/run-client';
import { ShowcaseApp } from '@/showcase-app';
import { OperatorIdentityChip, TokenGate } from '@/token-gate';
import { useActivityFeed } from '@/use-activity-feed';
import type { RunEntry, RunStreamOption } from '@/use-run-polling';
import { createShowcaseStreamTransport } from '@/web-socket-transport';
import '@/index.css';

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
      default: (await import('@/dev-actor-switcher')).DevActorSwitcher,
    }))
  : null;

// The showcase shell: hold the acting token + the launched runs at the root,
// derive both API clients from the token (a new client instance re-triggers
// the dashboard fetch and re-scopes the run polling), and hand the activity
// feed's stable `record` down as the narration sink. Identity (id/role/tenant)
// is never derived client-side — the server echoes it per request.
function Root(): ReactElement {
  const [actorToken, setActorToken] = useState<string | null>(null);
  // The OAuth callback delivers a per-visitor sandbox token set in the URL
  // fragment; read exactly once (the initializer also scrubs the hash).
  const [demoSession, setDemoSession] = useState<DemoTokenSet | null>(
    readDemoTokensFromHash,
  );
  const [runs, setRuns] = useState<readonly RunEntry[]>([]);
  const demoSignIn = useDemoSignIn();
  const demoSignInProvider =
    demoSignIn.status === 'oauth' ? demoSignIn.provider : undefined;
  const feed = useActivityFeed();
  const narrate = feed.record;

  const authHeaders = useMemo(() => {
    const headers: Record<string, string> = {};
    if (actorToken !== null) headers.authorization = `Bearer ${actorToken}`;
    return headers;
  }, [actorToken]);
  // narrate is the feed hook's stable useCallback — pinned here so identity
  // churn can never rebuild the client and re-trigger the dashboard fetches.
  const approvalClient = useMemo(
    () =>
      new NarratingApprovalClient({
        baseUrl: APPROVAL_BASE,
        headers: authHeaders,
        narrate,
      }),
    [authHeaders, narrate],
  );
  const runClient = useMemo(
    () => new RunClient({ baseUrl: RUN_BASE, headers: authHeaders }),
    [authHeaders],
  );

  // Live streaming (Part B): ONE browser-WebSocket transport (stateless, stable
  // identity so the hooks never re-subscribe on it), and per-client ticket
  // thunks bound to the authenticated approval client. A new client (actor
  // switch) rebuilds the thunks, which correctly re-subscribes with new auth.
  // Absent HUB/secret on the server => the ticket mint fails and the client
  // stays on polling (graceful degradation, DL-019) — no SPA branch needed.
  const streamTransport = useMemo(() => createShowcaseStreamTransport(), []);
  const streamTickets = useMemo(
    () => createHubStreamClient(approvalClient),
    [approvalClient],
  );
  const approvalStream = useMemo<ApprovalStreamOption>(
    () => ({ transport: streamTransport, ticket: streamTickets.hubTicket }),
    [streamTransport, streamTickets],
  );
  const runStream = useMemo<RunStreamOption>(
    () => ({ transport: streamTransport, ticket: streamTickets.runTicket }),
    [streamTransport, streamTickets],
  );

  const addRun = useCallback((entry: RunEntry) => {
    setRuns((current) => [entry, ...current]);
  }, []);

  // Clearing runs leaves useRunPolling's internal results map with stale
  // entries; harmless — RunCards renders from `runs`, and post-reset runs get
  // fresh runIds (never reused) and poll fresh.
  const clearRuns = useCallback(() => {
    setRuns([]);
  }, []);

  // The reset affordance only renders where the server can honor it: the
  // OAuth demo sandbox and local dev (tenant 'demo' is the dev plugin's demo
  // tenant). A static-operator-token session would always 403.
  const canReset = DevActorSwitcher !== null || demoSession !== null;

  const endDemoSession = useCallback(() => {
    setDemoSession(null);
    setActorToken(null);
  }, []);

  // Announce the sandbox once per tenant — the key dedups re-renders and the
  // refreshed token sets that keep the same tenantId.
  useEffect(() => {
    if (!demoSession) return;
    narrate([
      sessionReadyEvent({
        provider: demoSignInProvider ?? 'OAuth',
        tenantId: demoSession.tenantId,
        expiresAtMs: Date.parse(demoSession.tenantExpiresAt),
      }),
    ]);
  }, [demoSession, demoSignInProvider, narrate]);

  const identityControls = DevActorSwitcher ? (
    <Suspense fallback={null}>
      <DevActorSwitcher
        actorToken={actorToken}
        onSelect={setActorToken}
        narrate={narrate}
      />
    </Suspense>
  ) : demoSession ? (
    <DemoActorSwitcher
      session={demoSession}
      actorToken={actorToken}
      onSelect={setActorToken}
      onSession={setDemoSession}
      onExpired={endDemoSession}
      narrate={narrate}
    />
  ) : actorToken !== null ? (
    <OperatorIdentityChip onSignOut={() => setActorToken(null)} />
  ) : null;

  return (
    <AstryxApprovalUIProvider>
      {actorToken === null ? (
        <>
          {/* The switchers bootstrap the first token, so they must mount
              even before sign-in; the token gate covers the rest. */}
          {identityControls}
          {!DevActorSwitcher && !demoSession ? (
            <TokenGate onSubmit={setActorToken} demoSignIn={demoSignIn} />
          ) : null}
        </>
      ) : (
        <ShowcaseApp
          approvalClient={approvalClient}
          runClient={runClient}
          runs={runs}
          onStarted={addRun}
          onRunsCleared={clearRuns}
          canReset={canReset}
          feed={feed}
          identityControls={identityControls}
          approvalStream={approvalStream}
          runStream={runStream}
        />
      )}
    </AstryxApprovalUIProvider>
  );
}

// <Theme> stamps data-astryx-theme="y2k" on <html> — the theme.css rules are
// @scope'd to that attribute and are inert without it. y2kTheme embeds its
// icon registry, so mounting the provider also registers the icon set.
createRoot(container).render(
  <StrictMode>
    {/* Outside Theme/ToastViewport: the plain-HTML fallback must render even
        when the theme provider itself is what threw. */}
    <AppErrorBoundary>
      <Theme theme={y2kTheme}>
        <ToastViewport position="bottomEnd" maxVisible={3}>
          <Root />
        </ToastViewport>
      </Theme>
    </AppErrorBoundary>
  </StrictMode>,
);
