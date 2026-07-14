// The signed-in composition: header (wordmark + identity + explainers), the
// intro tour, walkthrough banner, a two-column body (launch + runs | activity
// feed + reality legend), and the approval dashboard composed from the
// library's headless hook + views (dropping the library App shell so run
// cards and toasts can deep-link into the queue via select()). The catalog
// fetch lives here because the header, the SoD notice, and the launcher all
// need the server's actor echo.

import { AlertDialog } from '@astryxdesign/core/AlertDialog';
import { Banner } from '@astryxdesign/core/Banner';
import { Button } from '@astryxdesign/core/Button';
import { Card } from '@astryxdesign/core/Card';
import { Grid } from '@astryxdesign/core/Grid';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { Spinner } from '@astryxdesign/core/Spinner';
import { Tab, TabList } from '@astryxdesign/core/TabList';
import { Text } from '@astryxdesign/core/Text';
import { TextInput } from '@astryxdesign/core/TextInput';
import { Token } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { VStack } from '@astryxdesign/core/VStack';

import type { ApprovalApiClient } from '@flowsafe/approval-ui/client';
import { useApprovalUIComponents } from '@flowsafe/approval-ui/components';
import { DetailView } from '@flowsafe/approval-ui/DetailView';
import { FilterBar } from '@flowsafe/approval-ui/FilterBar';
import { MetricsView } from '@flowsafe/approval-ui/MetricsView';
import { QueueView } from '@flowsafe/approval-ui/QueueView';
import {
  type ApprovalStreamOption,
  useApprovalDashboard,
} from '@flowsafe/approval-ui/use-approval-dashboard';
import {
  type ReactElement,
  type ReactNode,
  useCallback,
  useEffect,
  useState,
} from 'react';

import { ActivityFeedPanel } from '@/activity-feed';
import { WhatsRealHere, WhereThingsRunDialog } from '@/architecture-legend';
import { GLOSSARY, ROLE_NOTES, TAGLINE } from '@/glossary';
import { IntroTourDialog, useIntroTour } from '@/intro-tour';
import { resetErrorEvent, resetEvent, shortId } from '@/narration';
import { RunCards } from '@/run-cards';
import {
  type CatalogActor,
  RunApiError,
  type RunClient,
  type WorkflowMeta,
} from '@/run-client';
import type { ActivityFeed } from '@/use-activity-feed';
import { useNarrationToasts } from '@/use-narration-toasts';
import {
  type RunEntry,
  type RunStreamOption,
  useRunPolling,
} from '@/use-run-polling';
import { useSnapshotNarration } from '@/use-snapshot-narration';
import { WorkflowLauncher } from '@/workflow-launcher';

function scrollTo(elementId: string): void {
  document
    .getElementById(elementId)
    ?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

export interface ShowcaseAppProps {
  approvalClient: ApprovalApiClient;
  runClient: RunClient;
  runs: readonly RunEntry[];
  onStarted: (entry: RunEntry) => void;
  /** Empties the root's run list after a successful sandbox reset. */
  onRunsCleared: () => void;
  /** Render the reset affordance (demo sandbox / dev only — see main.tsx). */
  canReset: boolean;
  feed: ActivityFeed;
  /** The acting-identity controls (actor switcher / operator chip). */
  identityControls?: ReactNode;
  /** Live queue stream for the dashboard (Part B). Absent => poll-only. */
  approvalStream?: ApprovalStreamOption;
  /** Live per-run stream for the run poll (Part B). Absent => poll-only. */
  runStream?: RunStreamOption;
}

export function ShowcaseApp({
  approvalClient,
  runClient,
  runs,
  onStarted,
  onRunsCleared,
  canReset,
  feed,
  identityControls,
  approvalStream,
  runStream,
}: ShowcaseAppProps): ReactElement {
  const narrate = feed.record;

  // The workflow catalog + the SERVER-derived identity of the presented token,
  // hoisted here because the approval dashboard reads actor?.id as its
  // optimistic-decide attribution (actorId). The catalog EFFECT that fills these
  // lives below (it also drives the launcher role gates + the SoD notice).
  const [workflows, setWorkflows] = useState<WorkflowMeta[]>([]);
  const [actor, setActor] = useState<CatalogActor | null>(null);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  // FIRST-load only (never resets on an actor switch, where the previous
  // catalog stays rendered while the refetch is in flight): before it settles
  // the launcher shows a spinner instead of an empty panel.
  const [catalogSettled, setCatalogSettled] = useState(false);

  const C = useApprovalUIComponents();
  const dashboard = useApprovalDashboard(approvalClient, {
    pollIntervalMs: 5000,
    // Live stream (Part B): live-merge queue events + presence on top of the
    // poll, which stays the reconciler (DL-021). actorId attributes an
    // optimistic decide so a live 'decided' by a DIFFERENT reviewer surfaces as
    // a conflict.
    stream: approvalStream,
    actorId: actor?.id,
  });
  // Bumped by a run card's "Retry live updates" after polling abandoned a run.
  const [retryNonce, setRetryNonce] = useState(0);
  const runResults = useRunPolling(runClient, runs, retryNonce, runStream);
  const [tab, setTab] = useState('queue');
  const [batchComment, setBatchComment] = useState('');
  const [legendOpen, setLegendOpen] = useState(false);
  const [resetOpen, setResetOpen] = useState(false);
  const [resetBusy, setResetBusy] = useState(false);
  // Remount key for the launcher: a successful reset bumps it, wiping the
  // selection/edited-JSON/error state in one move instead of threading reset
  // props through the component.
  const [resetEpoch, setResetEpoch] = useState(0);
  const tour = useIntroTour();

  // Fills the catalog state hoisted above. Refetches when the acting client
  // changes (actor switch). External-data sync — a legitimate effect.
  useEffect(() => {
    let alive = true;
    runClient
      .catalog()
      .then((catalog) => {
        if (!alive) return;
        setWorkflows(catalog.workflows);
        setActor(catalog.actor);
        setCatalogError(null);
        setCatalogSettled(true);
      })
      .catch((error: unknown) => {
        if (!alive) return;
        // Fail closed: with no server echo there is no identity, so every
        // role gate below renders as "cannot start".
        setActor(null);
        setCatalogError(error instanceof Error ? error.message : String(error));
        setCatalogSettled(true);
      });
    return () => {
      alive = false;
    };
  }, [runClient]);

  // Narration: diff every polled snapshot, toast the new events. Not ready
  // until the dashboard's first refresh settles — the pre-fetch empty state
  // must not become the diff baseline (reload noise).
  const dashboardSettled =
    dashboard.metrics !== null || dashboard.error !== null;
  useSnapshotNarration(
    runs,
    runResults,
    dashboard.records,
    narrate,
    dashboardSettled,
  );
  const select = dashboard.select;
  const reviewApproval = useCallback(
    (approvalId: string) => {
      select(approvalId);
      scrollTo('approvals-panel');
    },
    [select],
  );
  const viewRun = useCallback((runId: string) => {
    scrollTo(`run-${runId}`);
  }, []);
  useNarrationToasts(feed.events, {
    onReview: reviewApproval,
    onViewRun: viewRun,
  });

  const selected = dashboard.selected;
  const selfRequested =
    selected !== null &&
    actor !== null &&
    selected.requestedBy === actor.id &&
    (selected.status === 'pending' ||
      selected.status === 'claimed' ||
      selected.status === 'escalated');
  // Does the server exempt THIS identity from SoD (e.g. admin in this demo)?
  // Drives which self-request notice shows: the "will refuse" warning, or the
  // "you may decide it here" relaxation note.
  const canSelfDecide = actor?.canSelfDecide ?? false;

  // The reset confirm's onAction. The button stays enabled for every role —
  // a non-admin click earns the server's 403, narrated as the RBAC lesson —
  // but the dialog warns about the refusal BEFORE the click.
  async function performReset(): Promise<void> {
    setResetBusy(true);
    try {
      const outcome = await runClient.reset();
      onRunsCleared();
      feed.clear();
      narrate([resetEvent(outcome.purged)]);
      void dashboard.refresh();
      setTab('queue');
      setResetEpoch((epoch) => epoch + 1);
      setResetOpen(false);
    } catch (error) {
      narrate([resetErrorEvent(error, actor?.role)]);
      // A 5xx can mean a PARTIAL purge (the server's deletes are not one
      // transaction), so the local view is no longer authoritative — resync
      // the queue from the server rather than assume nothing changed. A
      // 401/403 changed nothing, but refreshing it costs one poll.
      void dashboard.refresh();
      if (
        error instanceof RunApiError &&
        (error.status === 401 || error.status === 403)
      ) {
        // The refusal is the story; keep the dialog only for retryable 5xx.
        setResetOpen(false);
      }
    } finally {
      setResetBusy(false);
    }
  }

  // The dialog owns a DESTRUCTIVE request that cannot be recalled once sent:
  // Escape and Cancel must not dismiss it mid-flight, or the wipe lands
  // silently on a user who believes they backed out (the action button is
  // already disabled while busy, but the dismissal paths are not).
  function changeResetOpen(open: boolean): void {
    if (open || !resetBusy) setResetOpen(open);
  }

  const resetDescription = `Deletes ALL of this sandbox's runs and approval records server-side and clears the local activity feed. You stay signed in, and the run budget is NOT refilled.${
    actor && actor.role !== 'admin'
      ? ` Requires the admin role; you are '${actor.role}', so the server will refuse (403).`
      : ''
  }`;

  return (
    <VStack gap={5} padding={5} maxWidth={1280} style={{ margin: '0 auto' }}>
      <HStack justify="between" align="center" wrap="wrap" gap={3}>
        <VStack gap={1}>
          <Heading level={1} type="display-3">
            Anchorage
          </Heading>
          <Text size="sm" color="secondary">
            {TAGLINE}
          </Text>
        </VStack>
        <VStack gap={1} align="end">
          <HStack gap={2} align="center" wrap="wrap">
            <Button
              label="Where things run"
              variant="ghost"
              onClick={() => setLegendOpen(true)}
            />
            <Button
              label="Tour"
              tooltip="Replay the 60-second tour"
              variant="ghost"
              onClick={tour.open}
            />
            {canReset ? (
              <Button
                label="Reset sandbox"
                tooltip={GLOSSARY.reset}
                variant="ghost"
                onClick={() => setResetOpen(true)}
              />
            ) : null}
            {identityControls}
          </HStack>
          {actor ? (
            <Tooltip content={GLOSSARY.actorEcho}>
              <Token
                label={`server-verified: ${actor.id} · ${actor.role}`}
                size="sm"
                color="cyan"
              />
            </Tooltip>
          ) : null}
        </VStack>
      </HStack>

      <WhereThingsRunDialog isOpen={legendOpen} onOpenChange={setLegendOpen} />
      <IntroTourDialog tour={tour} onStartTour={() => scrollTo('launcher')} />
      <AlertDialog
        isOpen={resetOpen}
        onOpenChange={changeResetOpen}
        title="Reset this sandbox?"
        description={resetDescription}
        actionLabel="Reset everything"
        actionVariant="destructive"
        isActionLoading={resetBusy}
        onAction={() => void performReset()}
      />

      <Banner
        status="info"
        title="Suggested path: start a run as operator → switch to reviewer → approve → watch the run resume."
        description={
          actor
            ? `Currently acting as ${actor.role}.${ROLE_NOTES[actor.role] ? ` ${ROLE_NOTES[actor.role]}` : ''}`
            : undefined
        }
      />

      <Grid columns={{ minWidth: 460 }} gap={5}>
        <VStack gap={5} id="launcher">
          <WorkflowLauncher
            key={resetEpoch}
            workflows={workflows}
            actor={actor}
            isLoading={!catalogSettled}
            loadError={catalogError}
            runClient={runClient}
            onStarted={onStarted}
            narrate={narrate}
          />
          <RunCards
            runs={runs}
            results={runResults}
            records={dashboard.records}
            onReview={reviewApproval}
            onRetryPolling={() => setRetryNonce((nonce) => nonce + 1)}
          />
        </VStack>
        <VStack gap={3} className="anchorage-activity-column">
          <ActivityFeedPanel
            feed={feed}
            onReview={reviewApproval}
            onViewRun={viewRun}
          />
          <WhatsRealHere />
        </VStack>
      </Grid>

      <div id="approvals-panel">
        <Card variant="default" padding={4} aria-label="Approvals">
          <VStack gap={3}>
            <Heading level={2}>Approvals</Heading>
            <TabList value={tab} onChange={setTab}>
              <Tab value="queue" label="Queue" />
              <Tab value="metrics" label="Metrics" />
            </TabList>
            {tab === 'queue' ? (
              <VStack gap={3}>
                {dashboard.error ? (
                  <Banner status="error" title={dashboard.error} />
                ) : null}
                {dashboard.conflict ? (
                  // A live 'decided' event named a different reviewer than this
                  // tab's optimistic decide: the queue already reconciled to the
                  // authoritative record; the toast just explains why the row
                  // flipped. Rendered through the injected Astryx Toast slot.
                  <C.Toast
                    tone="warning"
                    title={`Another reviewer decided this request first (${dashboard.conflict.actualDecider}). The queue has been reconciled.`}
                    onDismiss={dashboard.dismissConflict}
                  />
                ) : null}
                <FilterBar
                  filter={dashboard.filter}
                  onApply={dashboard.setFilter}
                  disabled={dashboard.busy}
                />
                {dashboard.lastBatch && dashboard.lastBatch.failed > 0 ? (
                  <Banner
                    status="warning"
                    title={`Batch decide: ${dashboard.lastBatch.decided} decided · ${dashboard.lastBatch.failed} failed`}
                    description={dashboard.lastBatch.results
                      .flatMap((item) =>
                        item.ok
                          ? []
                          : [
                              `${shortId(item.id)}: ${item.error ?? item.code ?? 'failed'}`,
                            ],
                      )
                      .join(' · ')}
                  />
                ) : null}
                {dashboard.selectedIds.length > 0 ? (
                  <HStack gap={2} align="end" wrap="wrap">
                    <Text size="sm">
                      {dashboard.selectedIds.length} selected
                    </Text>
                    <TextInput
                      label="Batch comment"
                      value={batchComment}
                      onChange={setBatchComment}
                      isDisabled={dashboard.busy}
                    />
                    <Button
                      label="Approve selected"
                      variant="primary"
                      isDisabled={dashboard.busy}
                      onClick={() => {
                        dashboard.decideSelected('approve', batchComment);
                        setBatchComment('');
                      }}
                    />
                    <Button
                      label="Reject selected"
                      variant="destructive"
                      isDisabled={dashboard.busy}
                      onClick={() => {
                        dashboard.decideSelected('reject', batchComment);
                        setBatchComment('');
                      }}
                    />
                    <Button
                      label="Clear selection"
                      variant="ghost"
                      isDisabled={dashboard.busy}
                      onClick={dashboard.clearSelection}
                    />
                  </HStack>
                ) : null}
                {!dashboardSettled ? (
                  // Before the first poll settles the queue is UNKNOWN, not
                  // empty — mirror MetricsView's loading spinner instead of
                  // claiming "no approval requests".
                  <Spinner label="Loading the approval queue…" />
                ) : (
                  <QueueView
                    records={dashboard.records}
                    nowMs={dashboard.nowMs}
                    selectedId={dashboard.selectedId}
                    onSelect={dashboard.select}
                    selectedIds={dashboard.selectedIds}
                    onToggleSelect={dashboard.toggleSelect}
                    presence={dashboard.presence}
                  />
                )}
                {selfRequested ? (
                  <Banner
                    status="info"
                    title={
                      canSelfDecide
                        ? 'You advanced this run into its gate. This deployment lets your role decide its own requests, so you can approve it here.'
                        : 'Separation of duties: you advanced this run into its gate, so the server will refuse your decision (403). Switch to a different reviewer or to admin to decide it.'
                    }
                    description={GLOSSARY.sod}
                  />
                ) : null}
                {selected ? (
                  <DetailView
                    key={selected.id}
                    record={selected}
                    nowMs={dashboard.nowMs}
                    busy={dashboard.busy}
                    onClaim={dashboard.claim}
                    onDecide={dashboard.decide}
                    onDelegate={dashboard.delegate}
                    presence={dashboard.presence}
                  />
                ) : null}
              </VStack>
            ) : (
              <MetricsView metrics={dashboard.metrics} />
            )}
          </VStack>
        </Card>
      </div>

      <Text size="sm" color="secondary">
        Anchorage demo: flowsafe + breakwater running on Cloudflare Workers,
        Durable Objects, and D1. Connectors are offline; your sandbox and
        everything in it self-destructs at expiry. · Theme: Astryx y2k.
      </Text>
    </VStack>
  );
}
