// The showcase controls layered beside the approval dashboard: a token gate
// (production sign-in), a launcher (start any of the five workflows), and a run
// status panel (poll started runs to success). All render through the injected
// ApprovalUIComponents slots, so they inherit the Astryx look with zero adapter
// changes — and the run-status panel deliberately does NOT use the Table slot
// (that slot is hard-typed to ApprovalRecord).
//
// Identity is SERVER-DERIVED: GET /workflows echoes the authenticated actor,
// and every role gate here renders from that echo. This module holds no token
// table and must not import showcase/demo-actors.js — the dev-only actor
// switcher (dev-actor-switcher.tsx) is the single module that may, and main.tsx
// loads it only behind import.meta.env.DEV, keeping demo tokens out of the
// production bundle.

import { type ReactElement, useEffect, useState } from 'react';

import {
  type Tone,
  useApprovalUIComponents,
} from '../../src/approval-ui/components.js';
import {
  type CatalogActor,
  RunApiError,
  type RunClient,
  type RunSummary,
  type WorkflowMeta,
} from './run-client.js';

/**
 * Roles allowed to START any workflow — the host's coarse start-role gate,
 * applied to POST /runs before any per-workflow allowedRoles check. Mirrors
 * RUN_START_ROLES in ../../src/approval-api/contract.ts BY VALUE: the app
 * consumes the approval-ui (browser) subpackage and does not reach into the
 * approval-api (server) subpackage's internal modules. reviewer/viewer are
 * review-only. Drift is fail-safe — the backend re-checks authoritatively, so
 * a stale mirror only re-enables a button the server still 403s.
 */
const RUN_START_ROLES: readonly string[] = ['admin', 'operator', 'builder'];

/** A run the launcher started, tracked so the status panel can poll it. */
export interface RunEntry {
  workflowId: string;
  runId: string;
  title: string;
  /** The approval auto-queued by a start that suspended (run → queue link). */
  approvalId?: string;
  /** Client clock at launch — display/ordering only, never sent anywhere. */
  startedAt: number;
}

/** Rendered when the status endpoint, not the run, is what failed. */
const UNAVAILABLE = 'unavailable';

function statusTone(status: string): Tone {
  switch (status) {
    case 'success':
      return 'success';
    case 'failed':
    case 'tripwire':
    case UNAVAILABLE:
      return 'danger';
    case 'suspended':
      return 'warning';
    case 'running':
      return 'info';
    default:
      return 'neutral';
  }
}

const TERMINAL_STATUSES = new Set([
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
]);

const POLL_INTERVAL_MS = 3000;

/**
 * Consecutive transient failures tolerated before a run stops being polled.
 * At POLL_INTERVAL_MS that is ~15s of grace — ample for a just-started run's
 * snapshot to materialize.
 */
const MAX_TRANSIENT_FAILURES = 5;

/** What the last poll of a run produced. `summary` survives a later error. */
interface RunResult {
  summary?: RunSummary;
  error?: string;
}

/**
 * A 404 is transient: the run was accepted but its snapshot may not be readable
 * yet. So is a network blip (no RunApiError at all). Everything else — 401 after
 * an actor switch, 403, 500 — is a hard failure that will not fix itself, so
 * polling it forever is pure noise.
 */
function isTransient(error: unknown): boolean {
  return !(error instanceof RunApiError) || error.status === 404;
}

/** Wordmark-correct display names; anything unlisted gets a plain capitalize. */
const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  google: 'Google',
  github: 'GitHub',
};

function providerDisplayName(provider: string): string {
  return (
    PROVIDER_DISPLAY_NAMES[provider] ??
    provider.charAt(0).toUpperCase() + provider.slice(1)
  );
}

/**
 * Production sign-in: paste the deployment's bearer token. The token is held in
 * memory only; identity and role come back from the server's catalog echo. In
 * dev, main.tsx renders the demo actor switcher instead.
 */
export function TokenGate({
  signedIn,
  onSubmit,
  onSignOut,
  demoSignInProvider,
}: {
  signedIn: boolean;
  onSubmit: (token: string) => void;
  onSignOut: () => void;
  /**
   * When the worker has the public demo configured: the OAuth provider name
   * from the server's /auth/config echo. Drives the entry href and label.
   */
  demoSignInProvider?: string;
}): ReactElement {
  const C = useApprovalUIComponents();
  const [draft, setDraft] = useState('');
  const demoSignInHref = demoSignInProvider
    ? `/auth/${demoSignInProvider}`
    : undefined;

  function submit(): void {
    const token = draft.trim();
    if (token.length === 0) return;
    setDraft('');
    onSubmit(token);
  }

  return (
    <C.Section aria-label="Sign in">
      <C.Stack gap="sm">
        <C.Heading level={2}>Sign in</C.Heading>
        {signedIn ? (
          <C.Stack direction="horizontal" gap="sm">
            <C.Text>Token set — identity is verified by the server.</C.Text>
            <C.Button
              label="Sign out"
              variant="secondary"
              onClick={onSignOut}
            />
          </C.Stack>
        ) : (
          <C.Stack gap="sm">
            {demoSignInHref && demoSignInProvider ? (
              <C.Stack gap="sm">
                <C.Text>
                  Try the demo: sign in to get your own isolated sandbox — four
                  switchable roles, your data invisible to every other visitor.
                </C.Text>
                <C.Button
                  label={`Sign in with ${providerDisplayName(demoSignInProvider)}`}
                  variant="primary"
                  onClick={() => {
                    window.location.href = demoSignInHref;
                  }}
                />
              </C.Stack>
            ) : null}
            <C.Text>
              {demoSignInHref
                ? 'Or paste an operator API token.'
                : "Paste this deployment's API token. Your identity and role are resolved server-side; the app ships with no credentials."}
            </C.Text>
            <C.TextField
              label="API token"
              value={draft}
              onChange={setDraft}
              onSubmit={submit}
            />
            <C.Button
              label="Sign in"
              variant={demoSignInHref ? 'secondary' : 'primary'}
              onClick={submit}
            />
          </C.Stack>
        )}
      </C.Stack>
    </C.Section>
  );
}

export function LauncherPanel({
  runClient,
  onStarted,
}: {
  runClient: RunClient;
  onStarted: (entry: RunEntry) => void;
}): ReactElement {
  const C = useApprovalUIComponents();
  const [workflows, setWorkflows] = useState<WorkflowMeta[]>([]);
  const [actor, setActor] = useState<CatalogActor | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  // null selection => the first workflow; null input => derive from the sample.
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [editedJson, setEditedJson] = useState<string | null>(null);
  const [launchError, setLaunchError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // Fetch the catalog (workflows + the server-derived identity) when the
  // client identity changes. External-data sync — a legitimate effect.
  useEffect(() => {
    let alive = true;
    runClient
      .catalog()
      .then((catalog) => {
        if (!alive) return;
        setWorkflows(catalog.workflows);
        setActor(catalog.actor);
        setLoadError(null);
      })
      .catch((error: unknown) => {
        if (!alive) return;
        // Fail closed: with no server echo there is no identity, so every
        // role gate below renders as "cannot start".
        setActor(null);
        setLoadError(error instanceof Error ? error.message : String(error));
      });
    return () => {
      alive = false;
    };
  }, [runClient]);

  // Selection + editor value are DERIVED (no reset effect): the edited JSON
  // overrides the selected workflow's sample until the user picks a different
  // workflow, which clears the edit.
  const effectiveId = selectedId ?? workflows[0]?.id ?? null;
  const selected = workflows.find((w) => w.id === effectiveId) ?? null;
  const inputJson =
    editedJson ??
    (selected ? JSON.stringify(selected.sampleInput, null, 2) : '');
  // Coarse start-role gate (mirrors every backend's POST /runs check) runs
  // FIRST, then the per-workflow allowedRoles gate. Both read the SERVER'S
  // actor echo — no identity yet (or a failed catalog read) means no launch.
  const actorRole = actor?.role;
  const canStartAny =
    actorRole !== undefined && RUN_START_ROLES.includes(actorRole);
  const roleAllowed =
    canStartAny &&
    (!selected?.allowedRoles ||
      (actorRole !== undefined && selected.allowedRoles.includes(actorRole)));

  function selectWorkflow(id: string): void {
    setSelectedId(id);
    setEditedJson(null);
    setLaunchError(null);
  }

  async function launch(): Promise<void> {
    if (!selected) return;
    let input: unknown;
    try {
      input = JSON.parse(inputJson);
    } catch (error) {
      setLaunchError(
        `invalid JSON: ${error instanceof Error ? error.message : String(error)}`,
      );
      return;
    }
    setBusy(true);
    setLaunchError(null);
    try {
      const summary = await runClient.start(selected.id, input);
      onStarted({
        workflowId: selected.id,
        runId: summary.runId,
        title: selected.title,
        approvalId: summary.approval?.id,
        startedAt: Date.now(),
      });
    } catch (error) {
      setLaunchError(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  }

  return (
    <C.Section aria-label="Launch a workflow">
      <C.Stack gap="md">
        <C.Heading level={2}>Launch a workflow</C.Heading>
        {actor ? (
          <C.Text>
            {`Acting as ${actor.id} (${actor.role}) — identity verified by the server.`}
          </C.Text>
        ) : null}
        {loadError ? (
          <C.Banner
            tone="danger"
            title={`Could not load workflows: ${loadError}`}
          />
        ) : null}
        <C.Stack direction="horizontal" gap="sm">
          {workflows.map((w) => (
            <C.Button
              key={w.id}
              label={w.title}
              variant={w.id === effectiveId ? 'primary' : 'secondary'}
              pressed={w.id === effectiveId}
              onClick={() => selectWorkflow(w.id)}
            />
          ))}
        </C.Stack>
        {selected ? (
          <C.Stack gap="sm">
            <C.Text>{selected.description}</C.Text>
            {selected.allowedRoles ? (
              <C.Badge
                tone={roleAllowed ? 'success' : 'warning'}
                label={`start roles: ${selected.allowedRoles.join(', ')}`}
              />
            ) : null}
            <C.TextField
              label="Input (JSON)"
              value={inputJson}
              onChange={setEditedJson}
              rows={6}
            />
            {!roleAllowed ? (
              <C.Banner
                tone="warning"
                title={
                  actorRole === undefined
                    ? 'No verified identity — sign in with a valid token to launch workflows.'
                    : selected.allowedRoles
                      ? `Your role '${actorRole}' cannot start this workflow — switch to ${selected.allowedRoles.join(' or ')}.`
                      : `Your role '${actorRole}' cannot start any workflow — switch to ${RUN_START_ROLES.join(', ')}.`
                }
              />
            ) : null}
            {launchError ? (
              <C.Banner tone="danger" title={launchError} />
            ) : null}
            <C.Button
              label={busy ? 'Launching…' : 'Launch'}
              variant="primary"
              onClick={launch}
              disabled={busy || !roleAllowed}
            />
          </C.Stack>
        ) : null}
      </C.Stack>
    </C.Section>
  );
}

export function RunStatusPanel({
  runClient,
  runs,
}: {
  runClient: RunClient;
  runs: readonly RunEntry[];
}): ReactElement {
  const C = useApprovalUIComponents();
  const [results, setResults] = useState<Record<string, RunResult>>({});

  // Poll each tracked run's status. External sync — a legitimate effect; re-runs
  // when the run set or the acting client changes.
  useEffect(() => {
    if (runs.length === 0) return;
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // Per-effect-run bookkeeping, deliberately NOT a ref: re-arming (a new run,
    // an actor switch) should forgive earlier failures, and StrictMode's double
    // invoke gets its own counters instead of sharing one set.
    const failures = new Map<string, number>();
    const abandoned = new Set<string>();
    const lastError = new Map<string, string>();

    async function probe(run: RunEntry): Promise<[string, RunResult]> {
      if (abandoned.has(run.runId)) {
        return [run.runId, { error: lastError.get(run.runId) ?? UNAVAILABLE }];
      }
      try {
        const summary = await runClient.status(run.workflowId, run.runId);
        failures.delete(run.runId);
        return [run.runId, { summary }];
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const count = (failures.get(run.runId) ?? 0) + 1;
        failures.set(run.runId, count);
        lastError.set(run.runId, message);
        if (!isTransient(error) || count >= MAX_TRANSIENT_FAILURES) {
          abandoned.add(run.runId);
        }
        return [run.runId, { error: message }];
      }
    }

    // Returns true once every tracked run is finished or abandoned — the only
    // two ways polling can stop. A swallowed error used to render as a plausible
    // 'pending' that never became terminal, so a broken run polled forever.
    async function poll(): Promise<boolean> {
      const entries = await Promise.all(runs.map(probe));
      if (!alive) return true;
      setResults((previous) => {
        const next: Record<string, RunResult> = {};
        for (const [runId, result] of entries) {
          // Keep the last good summary visible beneath the error banner; an
          // untracked run drops out of the map entirely.
          next[runId] = result.error
            ? { summary: previous[runId]?.summary, error: result.error }
            : result;
        }
        return next;
      });
      return entries.every(([runId, result]) =>
        result.summary && !result.error
          ? TERMINAL_STATUSES.has(result.summary.status)
          : abandoned.has(runId),
      );
    }

    // Self-scheduling rather than setInterval: a poll slower than the interval
    // cannot stack behind itself, and the chain simply stops when done.
    async function tick(): Promise<void> {
      const done = await poll();
      if (!alive || done) return;
      timer = setTimeout(() => void tick(), POLL_INTERVAL_MS);
    }
    void tick();

    return () => {
      alive = false;
      if (timer !== undefined) clearTimeout(timer);
    };
  }, [runClient, runs]);

  return (
    <C.Section aria-label="Runs">
      <C.Stack gap="md">
        <C.Heading level={2}>Runs</C.Heading>
        {runs.length === 0 ? (
          <C.EmptyState title="No runs yet — launch one above." />
        ) : (
          runs.map((run) => {
            const { summary, error: pollError } = results[run.runId] ?? {};
            // A failed STATUS READ is not a run status: say so, rather than
            // rendering it as a plausible-looking 'pending'.
            const status = pollError
              ? UNAVAILABLE
              : (summary?.status ?? 'pending');
            return (
              <C.Section key={run.runId} aria-label={`Run ${run.runId}`}>
                <C.Stack gap="sm">
                  <C.Stack direction="horizontal" gap="sm">
                    <C.Heading level={2}>{run.title}</C.Heading>
                    <C.Badge tone={statusTone(status)} label={status} />
                  </C.Stack>
                  {pollError ? (
                    <C.Banner
                      tone="danger"
                      title={`Could not read run status: ${pollError}`}
                    />
                  ) : null}
                  <C.MetadataList>
                    <C.MetadataItem label="workflow">
                      {run.workflowId}
                    </C.MetadataItem>
                    <C.MetadataItem label="run">{run.runId}</C.MetadataItem>
                    {summary?.updatedAt ? (
                      <C.MetadataItem label="updated">
                        {summary.updatedAt}
                      </C.MetadataItem>
                    ) : null}
                  </C.MetadataList>
                  {status === 'suspended' ? (
                    <C.Text>
                      Awaiting approval — decide it in the queue below, then
                      watch it resume.
                    </C.Text>
                  ) : null}
                  {summary?.result !== undefined ? (
                    <C.Code
                      language="json"
                      code={JSON.stringify(summary.result, null, 2)}
                    />
                  ) : null}
                  {summary?.error ? (
                    <C.Banner tone="danger" title={summary.error} />
                  ) : null}
                </C.Stack>
              </C.Section>
            );
          })
        )}
      </C.Stack>
    </C.Section>
  );
}
