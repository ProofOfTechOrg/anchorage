// One Card per tracked run: live status, the step chip strip (definition order
// from the workflow guide, gate steps and the currently-suspended step marked),
// the suspension story (reason, connectors-to-grant, fingerprint, a "Review
// approval" jump into the queue), and the terminal story (interpreted outcome
// badge + the raw result JSON as proof). The badge interprets; the collapsed
// CodeBlock beneath proves.

import { Badge, type BadgeVariant } from '@astryxdesign/core/Badge';
import { Banner } from '@astryxdesign/core/Banner';
import { Button } from '@astryxdesign/core/Button';
import { Card } from '@astryxdesign/core/Card';
import { CodeBlock } from '@astryxdesign/core/CodeBlock';
import { Collapsible } from '@astryxdesign/core/Collapsible';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { StatusDot, type StatusDotVariant } from '@astryxdesign/core/StatusDot';
import { Text } from '@astryxdesign/core/Text';
import { Timestamp } from '@astryxdesign/core/Timestamp';
import { Token } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { VStack } from '@astryxdesign/core/VStack';
import type { ReactElement, ReactNode } from 'react';

import {
  type ApprovalRecord,
  OPEN_STATUSES,
} from '../../src/approval-api/types.js';
import { APPROVAL_TIPS } from '../../src/approval-ui/tips.js';
import { DRY_RUN_TRIO_FOOTER, GLOSSARY, WORKFLOW_GUIDES } from './glossary.js';
import {
  interpretRunResult,
  type ResultInterpretation,
  shortId,
} from './narration.js';
import type { RunSummary } from './run-client.js';
import {
  type RunEntry,
  type RunResult,
  UNAVAILABLE,
} from './use-run-polling.js';

const STATUS_DOT: Record<string, StatusDotVariant> = {
  success: 'success',
  failed: 'error',
  tripwire: 'error',
  [UNAVAILABLE]: 'error',
  suspended: 'warning',
  running: 'accent',
  pending: 'neutral',
};

const STATUS_BADGE: Record<string, BadgeVariant> = {
  success: 'success',
  failed: 'error',
  tripwire: 'error',
  [UNAVAILABLE]: 'error',
  suspended: 'warning',
  running: 'info',
  pending: 'neutral',
};

interface FlavorBadge {
  label: string;
  variant: BadgeVariant;
}

/** The Simulated-visibility badges — interpretation only, JSON is the proof. */
function flavorBadge(interp: ResultInterpretation): FlavorBadge | undefined {
  switch (interp.flavor) {
    case 'simulated':
      return { label: 'SIMULATED', variant: 'warning' };
    case 'real-write':
      return { label: 'REAL WRITE · SANDBOX BUCKET', variant: 'success' };
    case 'declined':
      return { label: 'DECLINED', variant: 'error' };
    case 'preview':
      return { label: 'DRY-RUN PREVIEW', variant: 'neutral' };
    default:
      return undefined;
  }
}

function suspensionOf(summary: RunSummary | undefined):
  | {
      step: string;
      reason?: string;
      connectors?: readonly string[];
      suspendedAtMs?: number;
      ordinal: number;
    }
  | undefined {
  const path = summary?.suspended?.[0];
  if (!summary || !path || path.length === 0) return undefined;
  const step = path.join('.');
  const payload = summary.suspendPayload?.[step];
  return {
    step,
    reason: typeof payload?.reason === 'string' ? payload.reason : undefined,
    connectors: Array.isArray(payload?.connectors)
      ? payload.connectors
      : undefined,
    suspendedAtMs: summary.suspendedAt?.[step],
    ordinal: summary.resumeCount?.[step] ?? 0,
  };
}

function StepChips({
  workflowId,
  suspendedStep,
}: {
  workflowId: string;
  suspendedStep?: string;
}): ReactElement | null {
  const guide = WORKFLOW_GUIDES[workflowId];
  if (!guide) return null;
  const chips: ReactNode[] = [];
  guide.steps.forEach((step, index) => {
    if (index > 0) {
      chips.push(
        <Text key={`arrow-${step}`} size="sm" color="secondary">
          →
        </Text>,
      );
    }
    const isGate = guide.gateSteps.includes(step);
    const isSuspendedHere = step === suspendedStep;
    const color = isSuspendedHere ? 'yellow' : isGate ? 'cyan' : 'default';
    chips.push(
      isGate ? (
        <Tooltip key={step} content={GLOSSARY.approvalGate}>
          <Token label={step} size="sm" color={color} />
        </Tooltip>
      ) : (
        <Token key={step} label={step} size="sm" color={color} />
      ),
    );
  });
  return (
    <HStack gap={1} align="center" wrap="wrap">
      {chips}
    </HStack>
  );
}

function RunCard({
  run,
  result,
  records,
  onReview,
}: {
  run: RunEntry;
  result: RunResult | undefined;
  records: readonly ApprovalRecord[];
  onReview: (approvalId: string) => void;
}): ReactElement {
  const summary = result?.summary;
  const pollError = result?.error;
  // A failed STATUS READ is not a run status: say so, rather than rendering it
  // as a plausible-looking 'pending'.
  const status = pollError ? UNAVAILABLE : (summary?.status ?? 'pending');
  const susp = status === 'suspended' ? suspensionOf(summary) : undefined;
  const guide = WORKFLOW_GUIDES[run.workflowId];
  const rawInterp =
    summary?.result !== undefined
      ? interpretRunResult(summary.result)
      : undefined;
  const totalResumes = Object.values(summary?.resumeCount ?? {}).reduce(
    (sum, count) => sum + count,
    0,
  );
  // An approval anywhere in this run's history proves a gate suspended it —
  // the terminal snapshot alone can't (the resume ledger drops at terminal).
  const neverSuspended =
    status === 'success' &&
    totalResumes === 0 &&
    Object.keys(summary?.suspendedAt ?? {}).length === 0 &&
    run.approvalId === undefined &&
    !records.some((record) => record.runId === run.runId);
  // A gate-less run labelled 'declined' by its workflow (lead-generation's
  // all-cold path) was never rejected by anyone — drop the DECLINED badge and
  // let the short-circuit note tell the real story.
  const interp =
    rawInterp && neverSuspended && rawInterp.flavor === 'declined'
      ? { ...rawInterp, flavor: 'plain' as const, line: undefined }
      : rawInterp;
  const badge = interp ? flavorBadge(interp) : undefined;
  // Prefer the OPEN record for this run (gate 2's approval is a different
  // record than the one the start response carried).
  const openApprovalId =
    records.find(
      (record) =>
        record.runId === run.runId && OPEN_STATUSES.includes(record.status),
    )?.id ?? run.approvalId;

  return (
    <Card
      variant="default"
      padding={4}
      aria-label={`Run ${run.runId}`}
      id={`run-${run.runId}`}
    >
      <VStack gap={3}>
        <HStack gap={3} align="center" justify="between">
          <Heading level={3}>{run.title}</Heading>
          <HStack gap={2} align="center">
            {/* StatusDot's label is a11y-only; the Badge carries the visible text. */}
            <StatusDot
              variant={STATUS_DOT[status] ?? 'neutral'}
              label={status}
              isPulsing={status === 'running' || status === 'suspended'}
              tooltip={
                status === 'suspended' ? GLOSSARY.suspended : GLOSSARY.polling
              }
            />
            <Badge variant={STATUS_BADGE[status] ?? 'neutral'} label={status} />
          </HStack>
        </HStack>
        <HStack gap={2} align="center" wrap="wrap">
          <Text size="sm" color="secondary">
            {run.workflowId}
          </Text>
          <Tooltip content={GLOSSARY.runId}>
            <Token label={shortId(run.runId)} size="sm" color="gray" />
          </Tooltip>
          {summary?.updatedAt ? (
            <Timestamp
              value={summary.updatedAt}
              format="relative"
              isLive
              size="sm"
              color="secondary"
            />
          ) : null}
          {totalResumes > 0 ? (
            <Tooltip content={GLOSSARY.resumeCount}>
              <Token
                label={`resumed ×${totalResumes}`}
                size="sm"
                color="green"
              />
            </Tooltip>
          ) : null}
        </HStack>
        <StepChips workflowId={run.workflowId} suspendedStep={susp?.step} />
        {pollError ? (
          <Banner
            status="error"
            title={`Could not read run status: ${pollError}`}
          />
        ) : null}
        {susp ? (
          <VStack gap={2}>
            <Banner
              status="warning"
              title={`Awaiting approval at ${susp.step}`}
              description={susp.reason ? `'${susp.reason}'` : undefined}
            />
            <HStack gap={2} align="center" wrap="wrap">
              {susp.connectors?.map((connector) => (
                <Tooltip
                  key={connector}
                  content={APPROVAL_TIPS.grantsOnApprove}
                >
                  <Token label={connector} size="sm" color="orange" />
                </Tooltip>
              ))}
              {susp.suspendedAtMs !== undefined ? (
                <HStack gap={1} align="center">
                  <Text size="sm" color="secondary">
                    suspended
                  </Text>
                  <Timestamp
                    value={new Date(susp.suspendedAtMs).toISOString()}
                    format="relative"
                    isLive
                    size="sm"
                    color="secondary"
                  />
                </HStack>
              ) : null}
            </HStack>
            <Tooltip content={GLOSSARY.fingerprint}>
              <Text size="sm" color="secondary">
                fingerprint: suspension {susp.suspendedAtMs ?? '?'} · resume #
                {susp.ordinal}
              </Text>
            </Tooltip>
            {openApprovalId ? (
              <Button
                label="Review approval"
                variant="primary"
                onClick={() => onReview(openApprovalId)}
              />
            ) : (
              <Text size="sm" color="secondary">
                Its approval request is in the queue below.
              </Text>
            )}
          </VStack>
        ) : null}
        {summary?.result !== undefined || summary?.error ? (
          <VStack gap={2}>
            <HStack gap={2} align="center" wrap="wrap">
              {badge ? (
                <Badge variant={badge.variant} label={badge.label} />
              ) : null}
              {interp?.replayed ? (
                <Tooltip content={GLOSSARY.idempotency}>
                  <Badge variant="neutral" label="REPLAYED" />
                </Tooltip>
              ) : null}
            </HStack>
            {interp?.line ? <Text size="sm">{interp.line}</Text> : null}
            {neverSuspended && guide?.shortCircuitNote ? (
              <Text size="sm" color="secondary">
                {guide.shortCircuitNote}
              </Text>
            ) : null}
            {summary.error ? (
              <Banner status="error" title={summary.error} />
            ) : null}
            {summary.result !== undefined ? (
              <Collapsible
                trigger={
                  <Text size="sm" weight="medium">
                    Result JSON (the proof)
                  </Text>
                }
                defaultIsOpen={false}
              >
                <CodeBlock
                  language="json"
                  code={JSON.stringify(summary.result, null, 2)}
                  isWrapped
                  hasCopyButton
                />
              </Collapsible>
            ) : null}
            {guide ? (
              <Text size="sm" color="secondary">
                {guide.note}
              </Text>
            ) : null}
            {run.workflowId === 'product-launch' ? (
              <Text size="sm" color="secondary">
                {DRY_RUN_TRIO_FOOTER}
              </Text>
            ) : null}
          </VStack>
        ) : null}
      </VStack>
    </Card>
  );
}

export function RunCards({
  runs,
  results,
  records,
  onReview,
}: {
  runs: readonly RunEntry[];
  results: Record<string, RunResult>;
  /** The approval queue — used to jump from a suspended run to its request. */
  records: readonly ApprovalRecord[];
  onReview: (approvalId: string) => void;
}): ReactElement {
  return (
    <VStack gap={3} aria-label="Runs">
      <Heading level={2}>Runs</Heading>
      {runs.length === 0 ? (
        <EmptyState
          title="Nothing running yet"
          description="Pick a workflow on the left — it will run its real steps and suspend at an approval gate within a couple of seconds."
        />
      ) : (
        runs.map((run) => (
          <RunCard
            key={run.runId}
            run={run}
            result={results[run.runId]}
            records={records}
            onReview={onReview}
          />
        ))
      )}
    </VStack>
  );
}
