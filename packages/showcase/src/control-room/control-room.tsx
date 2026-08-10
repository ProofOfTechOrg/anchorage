// The guardrails control room: the flagship section a visitor lands on after
// sign-in. Pick an attack/abuse scenario; the guarded agent streams on the
// left while the control plane on the right fills with the REAL breakwater
// decisions the scenario triggered (audit records, the blocking layer). Seven
// scenarios run the published library in this tab (deterministic, no run-cap
// cost); the wire-transfer scenario starts a real durable run and hands off to
// the approval queue further down the same page — real approval
// infrastructure, not a simulation of it.

import { Badge, type BadgeVariant } from '@astryxdesign/core/Badge';
import { Banner } from '@astryxdesign/core/Banner';
import { Button } from '@astryxdesign/core/Button';
import { Card, type CardVariant } from '@astryxdesign/core/Card';
import { Divider } from '@astryxdesign/core/Divider';
import { Grid } from '@astryxdesign/core/Grid';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { SelectableCard } from '@astryxdesign/core/SelectableCard';
import { Spinner } from '@astryxdesign/core/Spinner';
import { StatusDot } from '@astryxdesign/core/StatusDot';
import { Text } from '@astryxdesign/core/Text';
import { Token } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { VStack } from '@astryxdesign/core/VStack';
import {
  type CSSProperties,
  type ReactElement,
  type ReactNode,
  useEffect,
  useRef,
  useState,
} from 'react';
import { cardInkMap } from '@/card-ink';
import type { EngineEvent, GuardrailLayer } from '@/control-room/engine';
import { type GuardrailScenario, SCENARIOS } from '@/control-room/scenarios';
import { GLOSSARY } from '@/glossary';
import type { NarrationEvent } from '@/narration';
import { startErrorEvent, startEvent } from '@/narration';
import type { CatalogActor, RunClient } from '@/run-client';
import { RUN_START_ROLES } from '@/run-roles';
import type { RunEntry } from '@/use-run-polling';

const WIRE_SAMPLE = {
  amount: 25000,
  currency: 'USD',
  beneficiary: 'Northwind Metals Ltd',
  reference: 'INV-2311',
};

/** Human labels + tones for the guardrail layers a scenario badges. */
const LAYER_META: Record<
  GuardrailLayer,
  { label: string; badge: BadgeVariant }
> = {
  policy: { label: 'policy engine', badge: 'warning' },
  rbac: { label: 'RBAC', badge: 'info' },
  egress: { label: 'egress', badge: 'warning' },
  isolation: { label: 'isolation', badge: 'warning' },
  approval: { label: 'approval', badge: 'success' },
  audit: { label: 'audit', badge: 'neutral' },
};

/** Raised by the injected sleep when a run is abandoned (scenario switch). */
class AbandonedError extends Error {}

/** True when the visitor asked the OS to reduce motion; collapses pacing to instant. */
function prefersReducedMotion(): boolean {
  return (
    typeof window !== 'undefined' &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  );
}

type RunPhase = 'idle' | 'running' | 'done';

interface ControlPlaneEntry {
  id: number;
  event: EngineEvent;
}

function LayerBadges({
  layers,
}: {
  layers: readonly GuardrailLayer[];
}): ReactElement {
  return (
    <HStack gap={1} wrap="wrap">
      {layers.map((layer) => (
        <Badge
          key={layer}
          variant={LAYER_META[layer].badge}
          label={LAYER_META[layer].label}
        />
      ))}
    </HStack>
  );
}

/** One control-plane row: an audit record, a block marker, or a scripted note. */
function ControlPlaneRow({ event }: { event: EngineEvent }): ReactElement {
  if (event.kind === 'audit') {
    const { audit } = event;
    const tone =
      audit.decision === 'denied' || audit.decision === 'error'
        ? 'error'
        : 'success';
    return (
      <VStack gap={0.5}>
        <HStack gap={2} align="center" wrap="wrap">
          <StatusDot variant={tone} label={audit.decision} />
          <Token label={audit.action} size="sm" color="gray" />
          <Text size="sm" weight="medium">
            {audit.decision}
          </Text>
          <Text size="sm" color="secondary">
            {audit.resource}
          </Text>
        </HStack>
        {audit.reason ? (
          <Text size="sm" color="secondary">
            {audit.reason}
          </Text>
        ) : null}
      </VStack>
    );
  }
  if (event.kind === 'blocked') {
    return (
      <HStack gap={2} align="center" wrap="wrap">
        <StatusDot variant="error" label="blocked" />
        <Badge
          variant={LAYER_META[event.layer].badge}
          label={LAYER_META[event.layer].label}
        />
        <Text size="sm" weight="medium">
          {event.reason}
        </Text>
      </HStack>
    );
  }
  return (
    <HStack gap={2} align="center" wrap="wrap">
      <StatusDot variant="neutral" label="context" />
      <Text size="sm" color="secondary">
        {event.text}
      </Text>
    </HStack>
  );
}

/** The scrolling pane shared by the transcript and the control plane. */
function ScrollPane({
  ariaLabel,
  children,
}: {
  ariaLabel: string;
  children: ReactNode;
}): ReactElement {
  return (
    <VStack
      gap={2}
      aria-label={ariaLabel}
      style={{
        // Bounded so the two panes stay side by side within a viewport and
        // only their own content scrolls; overscroll contained so a wheel at
        // the pane edge never rubber-bands the page.
        maxHeight: 'min(52vh, 560px)',
        minHeight: 220,
        overflowY: 'auto',
        overscrollBehavior: 'contain',
        paddingRight: 4,
      }}
    >
      {children}
    </VStack>
  );
}

function ClientScenarioPanel({
  scenario,
  actor,
  phase,
  agentText,
  events,
  outcome,
  onRun,
}: {
  scenario: GuardrailScenario;
  actor: CatalogActor | null;
  phase: RunPhase;
  agentText: string;
  events: readonly ControlPlaneEntry[];
  outcome: { status: 'blocked' | 'clean'; headline: string } | null;
  onRun: () => void;
}): ReactElement {
  const running = phase === 'running';
  return (
    <VStack gap={3}>
      <VStack gap={2}>
        <HStack gap={2} align="center" justify="between" wrap="wrap">
          <Heading level={3}>{scenario.title}</Heading>
          <LayerBadges layers={scenario.layers} />
        </HStack>
        <Text color="secondary">{scenario.blurb}</Text>
        {scenario.roleSensitive ? (
          <Banner
            status="info"
            title="This outcome depends on who you are. Switch roles in the header and run it again to compare."
          />
        ) : null}
        <HStack gap={2} align="center" wrap="wrap">
          <Button
            label={phase === 'idle' ? 'Run scenario' : 'Run again'}
            variant="primary"
            onClick={onRun}
            isLoading={running}
            isDisabled={running || actor === null}
          />
          {actor === null ? (
            <Text size="sm" color="secondary">
              Sign in to run scenarios.
            </Text>
          ) : (
            <Text size="sm" color="secondary">
              Runs the real @proofoftech/breakwater library in this tab. No run
              budget is spent.
            </Text>
          )}
        </HStack>
      </VStack>

      <Grid columns={{ minWidth: 280 }} gap={4} className="anchorage-pane-grid">
        <VStack gap={2}>
          <HStack gap={2} align="center">
            <Heading level={4}>Guarded agent</Heading>
            {running ? <Spinner label="streaming" size="sm" /> : null}
          </HStack>
          <ScrollPane ariaLabel="Agent transcript">
            <Card variant="muted" padding={3}>
              <VStack gap={2}>
                <Text size="sm" color="secondary">
                  {'> '}
                  {scenario.prompt}
                </Text>
                <Divider />
                {agentText ? (
                  <Text
                    size="sm"
                    style={{ whiteSpace: 'pre-wrap' } as CSSProperties}
                  >
                    {agentText}
                  </Text>
                ) : (
                  <Text size="sm" color="secondary">
                    {running
                      ? 'Streaming…'
                      : 'Run the scenario to stream the guarded agent.'}
                  </Text>
                )}
                {outcome?.status === 'blocked' ? (
                  <HStack gap={2} align="center">
                    <StatusDot variant="error" label="terminated" />
                    <Text size="sm" weight="semibold">
                      Output stream terminated. Nothing past this point was
                      emitted.
                    </Text>
                  </HStack>
                ) : null}
              </VStack>
            </Card>
          </ScrollPane>
        </VStack>

        <VStack gap={2}>
          <Tooltip content="Decision rows are real breakwater results; audit rows are the same records a production SIEM sink would receive. Context rows narrate the scripted setup.">
            <Heading level={4}>Control plane</Heading>
          </Tooltip>
          <ScrollPane ariaLabel="Control plane">
            {events.length === 0 ? (
              <Text size="sm" color="secondary">
                Guardrail decisions stream here as the agent runs.
              </Text>
            ) : (
              events.map((entry) => (
                <ControlPlaneRow key={entry.id} event={entry.event} />
              ))
            )}
          </ScrollPane>
        </VStack>
      </Grid>

      {outcome ? (
        <Banner
          status={outcome.status === 'blocked' ? 'success' : 'info'}
          title={
            outcome.status === 'blocked'
              ? 'Guardrail held.'
              : 'Scenario completed.'
          }
          description={outcome.headline}
        />
      ) : null}
    </VStack>
  );
}

/** Wire run state, hoisted to ControlRoom so it survives scenario-card switches. */
type WireState = 'idle' | 'starting' | 'suspended' | 'error';

function WireScenarioPanel({
  actor,
  state,
  approvalId,
  error,
  onStart,
  onReviewApproval,
}: {
  actor: CatalogActor | null;
  state: WireState;
  /** The queued approval's id, so the approve button can open it directly. */
  approvalId: string | null;
  error: string | null;
  onStart: () => void;
  onReviewApproval: (approvalId: string) => void;
}): ReactElement {
  const actorRole = actor?.role;
  const canStart =
    actorRole !== undefined && RUN_START_ROLES.includes(actorRole);

  return (
    <VStack gap={3}>
      <VStack gap={2}>
        <HStack gap={2} align="center" justify="between" wrap="wrap">
          <Heading level={3}>Wire transfer</Heading>
          <LayerBadges layers={['approval', 'audit']} />
        </HStack>
        <Text color="secondary">
          The one server-backed scenario. An agent prepares a $25,000 payment
          that pauses at a human approval gate on a real Durable Object. The
          release connector runs only under a grant derived from the APPROVED
          record, so approving it is what mints the capability, and a forged
          resume is denied.
        </Text>
        <Banner
          status="info"
          title="This starts a real durable run and consumes one of your session's 20 run mutations."
        />
      </VStack>

      <Card variant="muted" padding={3}>
        <VStack gap={2}>
          <Text size="sm" color="secondary">
            {'> Release USD 25,000 to Northwind Metals Ltd (INV-2311).'}
          </Text>
          <Divider />
          {state === 'idle' ? (
            <Text size="sm" color="secondary">
              Start the run to prepare the payment and suspend at the gate.
            </Text>
          ) : null}
          {state === 'starting' ? (
            <HStack gap={2} align="center">
              <Spinner label="starting" size="sm" />
              <Text size="sm">Preparing the transfer…</Text>
            </HStack>
          ) : null}
          {state === 'suspended' ? (
            <HStack gap={2} align="center" wrap="wrap">
              <StatusDot variant="warning" label="suspended" isPulsing />
              <Text size="sm" weight="medium">
                Suspended at the approval gate. The wire is held until a
                reviewer decides.
              </Text>
            </HStack>
          ) : null}
          {state === 'error' && error ? (
            <Banner status="error" title={error} />
          ) : null}
        </VStack>
      </Card>

      <HStack gap={2} align="center" wrap="wrap">
        {state === 'suspended' && approvalId ? (
          <Button
            label="Approve in the queue below ↓"
            variant="primary"
            onClick={() => onReviewApproval(approvalId)}
          />
        ) : (
          <Button
            label={state === 'error' ? 'Try again' : 'Start the wire run'}
            variant="primary"
            onClick={onStart}
            isLoading={state === 'starting'}
            // 'suspended' also disables: a suspended response that carried no
            // approval id would otherwise fall through to an ENABLED start
            // button, and a click would fire a second real run at the cap.
            isDisabled={
              state === 'starting' || state === 'suspended' || !canStart
            }
          />
        )}
        {!canStart && actor !== null ? (
          <Text size="sm" color="secondary">
            Your role '{actorRole}' cannot start runs. Switch to operator or
            admin.
          </Text>
        ) : null}
        {actor === null ? (
          <Text size="sm" color="secondary">
            Sign in to start the run.
          </Text>
        ) : null}
      </HStack>

      {state === 'suspended' ? (
        <Tooltip content={GLOSSARY.grantDerivation}>
          <Text size="sm" color="secondary">
            Approving mints the connector grant from the stored decision. The
            grant never travels in a request body, and separation of duties
            stops the requester from clearing their own wire.
          </Text>
        </Tooltip>
      ) : null}
    </VStack>
  );
}

/** One y2k categorical tone per scenario card (unknown ids stay neutral). */
const SCENARIO_VARIANT: Record<string, CardVariant> = {
  'pii-leak': 'pink',
  'secret-exfil': 'red',
  'prompt-injection': 'purple',
  'role-gate': 'blue',
  'egress-violation': 'orange',
  'cross-workflow': 'teal',
  'tenant-isolation': 'yellow',
  'wire-transfer': 'cyan',
};

// Matched dark ink per tinted card — without it the theme's near-white
// dark-mode text lands on the pastel tints and the titles wash out.
const SCENARIO_INK = cardInkMap(SCENARIO_VARIANT);

export function ControlRoom({
  actor,
  runClient,
  onStarted,
  narrate,
  onReviewApproval,
}: {
  actor: CatalogActor | null;
  runClient: RunClient;
  onStarted: (entry: RunEntry) => void;
  narrate: (events: readonly NarrationEvent[]) => void;
  /** Select the approval + jump to the queue (the wire card's approve action). */
  onReviewApproval: (approvalId: string) => void;
}): ReactElement {
  // The full card list: the pure scenarios plus the server-backed wire card,
  // which selects into its own panel.
  const cards = [
    ...SCENARIOS.map((s) => ({
      id: s.id,
      title: s.title,
      layers: s.layers,
    })),
    {
      id: 'wire-transfer',
      title: 'Wire transfer',
      layers: ['approval', 'audit'] as const,
    },
  ];
  const [selectedId, setSelectedId] = useState<string>(cards[0]?.id ?? '');
  const [phase, setPhase] = useState<RunPhase>('idle');
  const [agentText, setAgentText] = useState('');
  const [events, setEvents] = useState<ControlPlaneEntry[]>([]);
  const [outcome, setOutcome] = useState<{
    status: 'blocked' | 'clean';
    headline: string;
  } | null>(null);
  // Wire run state lives HERE, not in WireScenarioPanel: the panel unmounts on
  // every scenario-card switch, so panel-local state would reset to 'idle' while
  // a real run stays suspended server-side — and a re-click would fire a SECOND
  // real run against the 20-mutation session cap. Hoisting keeps the suspended state
  // (and its "approve", not "start", button) alive across card switches.
  const [wireState, setWireState] = useState<WireState>('idle');
  const [wireApprovalId, setWireApprovalId] = useState<string | null>(null);
  const [wireError, setWireError] = useState<string | null>(null);
  // Bumped on every run start and every scenario switch — the emit callbacks
  // and the sleep guard drop writes from an abandoned run, so an in-flight
  // stream cannot bleed into the newly selected scenario.
  const genRef = useRef(0);
  const nextControlPlaneEventIdRef = useRef(0);
  // Abandon any in-flight scenario run when the control room unmounts (sign-
  // out tears down the app), so an orphaned run stops making real
  // PolicyEngine/audit calls into a detached instance. A mounted flag (not a
  // genRef bump) so the cleanup writes a constant instead of reading the
  // ever-changing genRef; live() below folds it in. Re-set true on mount for
  // StrictMode's mount→cleanup→mount double-invoke.
  const mountedRef = useRef(true);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const scenario = SCENARIOS.find((s) => s.id === selectedId) ?? null;
  const isWire = selectedId === 'wire-transfer';

  function appendControlPlaneEvent(event: EngineEvent): void {
    const id = nextControlPlaneEventIdRef.current;
    nextControlPlaneEventIdRef.current += 1;
    setEvents((previous) => [...previous, { id, event }]);
  }

  async function startWire(): Promise<void> {
    if (actor === null) return;
    setWireState('starting');
    setWireError(null);
    try {
      const response = await runClient.start('wire-transfer', WIRE_SAMPLE);
      const entry: RunEntry = {
        workflowId: 'wire-transfer',
        runId: response.runId,
        title: 'Wire Transfer',
        approvalId: response.approval?.id,
        startedAt: Date.now(),
      };
      onStarted(entry);
      narrate(
        startEvent(entry, response, {
          actor: actor ?? undefined,
          steps: ['prepareTransfer'],
        }),
      );
      setWireApprovalId(response.approval?.id ?? null);
      setWireState(response.status === 'suspended' ? 'suspended' : 'error');
      if (response.status !== 'suspended') {
        setWireError(`Unexpected run status: ${response.status}`);
      }
    } catch (caught) {
      setWireError(caught instanceof Error ? caught.message : String(caught));
      setWireState('error');
      narrate([startErrorEvent('wire-transfer', caught, actor.role)]);
    }
  }

  function selectCard(id: string): void {
    if (id === selectedId) return;
    genRef.current += 1; // abandon any in-flight run
    setSelectedId(id);
    setPhase('idle');
    setAgentText('');
    setEvents([]);
    setOutcome(null);
  }

  function runScenario(): void {
    if (!scenario || actor === null) return;
    genRef.current += 1;
    const gen = genRef.current;
    const live = () => gen === genRef.current && mountedRef.current;
    setPhase('running');
    setAgentText('');
    setEvents([]);
    setOutcome(null);
    const reduce = prefersReducedMotion();
    scenario
      .run({
        actor: { id: actor.id, role: actor.role },
        isolationScope: 'showcase',
        emitText: (text) => {
          if (live()) setAgentText((prev) => prev + text);
        },
        emitEvent: (event) => {
          if (live()) appendControlPlaneEvent(event);
        },
        sleep: (ms) =>
          new Promise<void>((resolve, reject) => {
            setTimeout(
              () => (live() ? resolve() : reject(new AbandonedError())),
              reduce ? 0 : ms,
            );
          }),
      })
      .then((result) => {
        if (!live()) return;
        setOutcome(result);
        setPhase('done');
      })
      .catch((caught: unknown) => {
        if (caught instanceof AbandonedError || !live()) return;
        appendControlPlaneEvent({
          kind: 'note',
          text: `Scenario error: ${caught instanceof Error ? caught.message : String(caught)}`,
        });
        setPhase('done');
      });
  }

  return (
    <VStack gap={4}>
      <VStack gap={1}>
        <Heading level={2}>Guardrails control room</Heading>
        <Text color="secondary">
          Pick a scenario. A scripted agent runs it straight into breakwater's
          real enforcement path: watch it get caught, and switch roles to see
          the decisions change.
        </Text>
      </VStack>

      <Grid
        columns={{ minWidth: 150 }}
        gap={2}
        className="anchorage-scenario-grid"
      >
        {cards.map((card) => (
          <SelectableCard
            key={card.id}
            label={card.title}
            variant={SCENARIO_VARIANT[card.id] ?? 'default'}
            style={SCENARIO_INK[card.id]}
            padding={3}
            isSelected={card.id === selectedId}
            onChange={(isSelected) => {
              if (isSelected) selectCard(card.id);
            }}
          >
            <VStack gap={1}>
              <Text weight="semibold">{card.title}</Text>
              <LayerBadges layers={card.layers} />
            </VStack>
          </SelectableCard>
        ))}
      </Grid>

      <Card variant="default" padding={4}>
        {isWire ? (
          <WireScenarioPanel
            actor={actor}
            state={wireState}
            approvalId={wireApprovalId}
            error={wireError}
            onStart={() => void startWire()}
            onReviewApproval={onReviewApproval}
          />
        ) : scenario ? (
          <ClientScenarioPanel
            // Remount on scenario switch so no pane state carries across.
            key={scenario.id}
            scenario={scenario}
            actor={actor}
            phase={phase}
            agentText={agentText}
            events={events}
            outcome={outcome}
            onRun={runScenario}
          />
        ) : null}
      </Card>
    </VStack>
  );
}
