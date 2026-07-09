// The showcase controls layered beside the approval dashboard: an actor switcher
// (live RBAC / SoD), a launcher (start any of the five workflows), and a run
// status panel (poll started runs to success). All render through the injected
// ApprovalUIComponents slots, so they inherit the Astryx look with zero adapter
// changes — and the run-status panel deliberately does NOT use the Table slot
// (that slot is hard-typed to ApprovalRecord).

import { type ReactElement, useEffect, useState } from 'react';

import {
  type Tone,
  useApprovalUIComponents,
} from '../../src/approval-ui/components.js';
import type { RunClient, RunSummary, WorkflowMeta } from './run-client.js';

export type Role = 'admin' | 'builder' | 'operator' | 'reviewer' | 'viewer';

export interface DemoActor {
  token: string;
  label: string;
  role: Role;
}

/**
 * The demo identities the switcher offers. These bearer tokens must match the
 * dev backend's map (run-api-dev-plugin.ts) and the deployed worker's
 * APPROVAL_ACTOR_TOKENS var (showcase/wrangler.jsonc).
 */
export const DEMO_ACTORS: readonly DemoActor[] = [
  { token: 'demo-admin', label: 'admin', role: 'admin' },
  { token: 'demo-builder', label: 'builder', role: 'builder' },
  { token: 'demo-operator', label: 'operator', role: 'operator' },
  { token: 'demo-reviewer', label: 'reviewer', role: 'reviewer' },
  { token: 'demo-viewer', label: 'viewer', role: 'viewer' },
];

/**
 * Roles allowed to START any workflow — the host's coarse start-role gate,
 * applied to POST /runs before any per-workflow allowedRoles check. Mirrors
 * RUN_START_ROLES in ../../src/approval-api/contract.ts BY VALUE, the same way
 * the Role type above mirrors ApprovalRole: the app consumes the approval-ui
 * (browser) subpackage and does not reach into the approval-api (server)
 * subpackage's internal modules. reviewer/viewer are review-only. Drift is
 * fail-safe — the backend re-checks authoritatively, so a stale mirror only
 * re-enables a button the server still 403s.
 */
const RUN_START_ROLES: readonly Role[] = ['admin', 'operator', 'builder'];

/** The identity the app starts as (admin — can start any workflow). */
export const DEFAULT_ACTOR: DemoActor = DEMO_ACTORS[0] ?? {
  token: 'demo-admin',
  label: 'admin',
  role: 'admin',
};

export function actorForToken(token: string): DemoActor {
  return DEMO_ACTORS.find((actor) => actor.token === token) ?? DEFAULT_ACTOR;
}

/** A run the launcher started, tracked so the status panel can poll it. */
export interface RunEntry {
  workflowId: string;
  runId: string;
  title: string;
}

function statusTone(status: string): Tone {
  switch (status) {
    case 'success':
      return 'success';
    case 'failed':
    case 'tripwire':
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

export function ActorSwitcher({
  actorToken,
  onSelect,
}: {
  actorToken: string;
  onSelect: (token: string) => void;
}): ReactElement {
  const C = useApprovalUIComponents();
  return (
    <C.Section aria-label="Acting identity">
      <C.Stack gap="sm">
        <C.Heading level={2}>Acting as</C.Heading>
        <C.Text>
          Switch identity to see RBAC and separation-of-duties live.
          Grant-minting always stays server-side, whichever actor you pick.
        </C.Text>
        <C.Stack direction="horizontal" gap="sm">
          {DEMO_ACTORS.map((actor) => (
            <C.Button
              key={actor.token}
              label={actor.label}
              variant={actor.token === actorToken ? 'primary' : 'secondary'}
              pressed={actor.token === actorToken}
              onClick={() => onSelect(actor.token)}
            />
          ))}
        </C.Stack>
      </C.Stack>
    </C.Section>
  );
}

export function LauncherPanel({
  runClient,
  actorRole,
  onStarted,
}: {
  runClient: RunClient;
  actorRole: Role;
  onStarted: (entry: RunEntry) => void;
}): ReactElement {
  const C = useApprovalUIComponents();
  const [workflows, setWorkflows] = useState<WorkflowMeta[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  // null selection => the first workflow; null input => derive from the sample.
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [editedJson, setEditedJson] = useState<string | null>(null);
  const [launchError, setLaunchError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // Fetch the workflow catalog when the client identity changes (any actor may
  // read it). External-data sync — a legitimate effect.
  useEffect(() => {
    let alive = true;
    runClient
      .workflows()
      .then((list) => {
        if (!alive) return;
        setWorkflows(list);
        setLoadError(null);
      })
      .catch((error: unknown) => {
        if (alive) {
          setLoadError(error instanceof Error ? error.message : String(error));
        }
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
  // FIRST, then the per-workflow allowedRoles gate. Without the coarse check,
  // reviewer/viewer saw an enabled Launch button the backend then 403s.
  const canStartAny = RUN_START_ROLES.includes(actorRole);
  const roleAllowed =
    canStartAny &&
    (!selected?.allowedRoles || selected.allowedRoles.includes(actorRole));

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
                  selected.allowedRoles
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
  const [summaries, setSummaries] = useState<
    Record<string, RunSummary | undefined>
  >({});

  // Poll each tracked run's status. External sync — a legitimate effect; re-runs
  // when the run set or the acting client changes.
  useEffect(() => {
    if (runs.length === 0) return;
    let alive = true;
    // Returns true once every tracked run has reached a terminal status.
    async function poll(): Promise<boolean> {
      const entries = await Promise.all(
        runs.map(async (run) => {
          try {
            const summary = await runClient.status(run.workflowId, run.runId);
            return [run.runId, summary] as const;
          } catch {
            return [run.runId, undefined] as const;
          }
        }),
      );
      if (!alive) return true;
      const next = Object.fromEntries(entries);
      setSummaries(next);
      return runs.every((run) =>
        TERMINAL_STATUSES.has(next[run.runId]?.status ?? ''),
      );
    }
    void poll();
    // Stop polling once all runs finish — no point hitting finished runs forever
    // (a newly launched run re-arms the effect via the `runs` dependency).
    const timer = setInterval(() => {
      void poll().then((done) => {
        if (done) clearInterval(timer);
      });
    }, 3000);
    return () => {
      alive = false;
      clearInterval(timer);
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
            const summary = summaries[run.runId];
            const status = summary?.status ?? 'pending';
            return (
              <C.Section key={run.runId} aria-label={`Run ${run.runId}`}>
                <C.Stack gap="sm">
                  <C.Stack direction="horizontal" gap="sm">
                    <C.Heading level={2}>{run.title}</C.Heading>
                    <C.Badge tone={statusTone(status)} label={status} />
                  </C.Stack>
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
