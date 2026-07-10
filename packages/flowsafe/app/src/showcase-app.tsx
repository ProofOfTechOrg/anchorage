// The signed-in composition: header (wordmark + identity), walkthrough banner,
// launcher + run cards, and the approval dashboard composed from the library's
// headless hook + views (dropping the library App shell so run cards can
// deep-link into the queue via select()). The catalog fetch lives here because
// the header, the SoD notice, and the launcher all need the server's actor
// echo.

import { Banner } from '@astryxdesign/core/Banner';
import { Card } from '@astryxdesign/core/Card';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { Tab, TabList } from '@astryxdesign/core/TabList';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { type ReactElement, type ReactNode, useEffect, useState } from 'react';

import type { ApprovalApiClient } from '../../src/approval-ui/client.js';
import { DetailView } from '../../src/approval-ui/DetailView.js';
import { MetricsView } from '../../src/approval-ui/MetricsView.js';
import { QueueView } from '../../src/approval-ui/QueueView.js';
import { useApprovalDashboard } from '../../src/approval-ui/use-approval-dashboard.js';
import { GLOSSARY, TAGLINE } from './glossary.js';
import type { NarrationEvent } from './narration.js';
import type { CatalogActor, RunClient, WorkflowMeta } from './run-client.js';
import { RunCards } from './run-cards.js';
import { type RunEntry, useRunPolling } from './use-run-polling.js';
import { WorkflowLauncher } from './workflow-launcher.js';

export interface ShowcaseAppProps {
  approvalClient: ApprovalApiClient;
  runClient: RunClient;
  runs: readonly RunEntry[];
  onStarted: (entry: RunEntry) => void;
  narrate: (events: readonly NarrationEvent[]) => void;
  /** The acting-identity controls (actor switcher / operator chip). */
  identityControls?: ReactNode;
}

export function ShowcaseApp({
  approvalClient,
  runClient,
  runs,
  onStarted,
  narrate,
  identityControls,
}: ShowcaseAppProps): ReactElement {
  const dashboard = useApprovalDashboard(approvalClient, {
    pollIntervalMs: 5000,
  });
  const runResults = useRunPolling(runClient, runs);
  const [tab, setTab] = useState('queue');

  // The workflow catalog + the SERVER-derived identity of the presented token.
  // Refetches when the acting client changes (actor switch). External-data
  // sync — a legitimate effect.
  const [workflows, setWorkflows] = useState<WorkflowMeta[]>([]);
  const [actor, setActor] = useState<CatalogActor | null>(null);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    runClient
      .catalog()
      .then((catalog) => {
        if (!alive) return;
        setWorkflows(catalog.workflows);
        setActor(catalog.actor);
        setCatalogError(null);
      })
      .catch((error: unknown) => {
        if (!alive) return;
        // Fail closed: with no server echo there is no identity, so every
        // role gate below renders as "cannot start".
        setActor(null);
        setCatalogError(error instanceof Error ? error.message : String(error));
      });
    return () => {
      alive = false;
    };
  }, [runClient]);

  const selected = dashboard.selected;
  const selfRequested =
    selected !== null &&
    actor !== null &&
    selected.requestedBy === actor.id &&
    (selected.status === 'pending' ||
      selected.status === 'claimed' ||
      selected.status === 'escalated');

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
          {identityControls}
          {actor ? (
            <Text size="sm" color="secondary">
              server-verified: {actor.id} ({actor.role})
            </Text>
          ) : null}
        </VStack>
      </HStack>

      <Banner
        status="info"
        title="Suggested path: start a run as operator → switch to reviewer → approve → watch the run resume."
        description={actor ? `Currently acting as ${actor.role}.` : undefined}
      />

      <WorkflowLauncher
        workflows={workflows}
        actor={actor}
        loadError={catalogError}
        runClient={runClient}
        onStarted={onStarted}
        narrate={narrate}
      />

      <RunCards
        runs={runs}
        results={runResults}
        records={dashboard.records}
        onReview={dashboard.select}
      />

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
              <QueueView
                records={dashboard.records}
                nowMs={dashboard.nowMs}
                selectedId={dashboard.selectedId}
                onSelect={dashboard.select}
              />
              {selfRequested ? (
                <Banner
                  status="info"
                  title="Separation of duties: you advanced this run into its gate, so the server will refuse your decision (403)."
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
                />
              ) : null}
            </VStack>
          ) : (
            <MetricsView metrics={dashboard.metrics} />
          )}
        </VStack>
      </Card>

      <Text size="sm" color="secondary">
        Anchorage demo — flowsafe + breakwater running on Cloudflare Workers,
        Durable Objects, and D1. Connectors are offline; your sandbox and
        everything in it self-destructs at expiry. · Theme: Astryx y2k.
      </Text>
    </VStack>
  );
}
