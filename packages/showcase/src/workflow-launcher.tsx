// The workflow picker + start form. Role gates render from the SERVER'S
// catalog actor echo (passed down from the app shell) — the RUN_START_ROLES
// mirror is a pre-flight courtesy only; the backend re-checks authoritatively,
// so a stale mirror only re-enables a button the server still 403s. Start
// failures that the user must act on (role gate, invalid JSON) stay INLINE;
// the narration layer additionally toasts server-side refusals (429/403/503).

import { Banner } from '@astryxdesign/core/Banner';
import { Button } from '@astryxdesign/core/Button';
import type { CardVariant } from '@astryxdesign/core/Card';
import { Collapsible } from '@astryxdesign/core/Collapsible';
import { Grid } from '@astryxdesign/core/Grid';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { SelectableCard } from '@astryxdesign/core/SelectableCard';
import { Spinner } from '@astryxdesign/core/Spinner';
import { Text } from '@astryxdesign/core/Text';
import { TextArea } from '@astryxdesign/core/TextArea';
import { Token } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { VStack } from '@astryxdesign/core/VStack';
import { type CSSProperties, type ReactElement, useState } from 'react';

import { claimableSteps, GLOSSARY, WORKFLOW_GUIDES } from '@/glossary';
import { type NarrationEvent, startErrorEvent, startEvent } from '@/narration';
import type { CatalogActor, RunClient, WorkflowMeta } from '@/run-client';
import type { RunEntry } from '@/use-run-polling';

/**
 * Roles allowed to START any workflow — the host's coarse start-role gate,
 * applied to POST /runs before any per-workflow allowedRoles check. Mirrors
 * RUN_START_ROLES in ../../src/approval-api/contract.ts BY VALUE: the app
 * consumes the approval-ui (browser) subpackage and does not reach into the
 * approval-api (server) subpackage's internal modules.
 */
const RUN_START_ROLES: readonly string[] = ['admin', 'operator', 'builder'];

/** One y2k categorical card color per workflow; unknown ids stay neutral. */
const CARD_VARIANTS: Record<string, CardVariant> = {
  'gtm-outbound': 'blue',
  'content-pipeline': 'purple',
  'lead-generation': 'green',
  'product-launch': 'orange',
  'access-request': 'pink',
};

/** The Card variants that tint their background and ship a matched text var. */
const UNTONED_VARIANTS: readonly CardVariant[] = [
  'default',
  'transparent',
  'muted',
];

/**
 * Toned ink for a tinted variant. The y2k card tints keep the SAME light hex
 * in dark mode, but --color-text-primary flips to near-white there — so an
 * unstyled card title renders near-white on pastel. Re-point the text vars at
 * the variant's matched --color-text-<variant> (dark in BOTH modes — the
 * theme's own Banner/Token pairing), and set `color` so the hover overlay's
 * currentColor tint stays dark-on-pastel too. Custom properties, not a bare
 * color: the theme styles Text via `color: var(--color-text-primary)`.
 * Mode-aware variants keep the theme's own ink.
 */
function cardInk(variant: CardVariant): CSSProperties | undefined {
  if (UNTONED_VARIANTS.includes(variant)) return undefined;
  const ink = `var(--color-text-${variant})`;
  return {
    color: ink,
    '--color-text-primary': ink,
    '--color-text-secondary': ink,
  } as CSSProperties;
}

// DERIVED from CARD_VARIANTS, never hand-repeated: a parallel id→tone map
// would silently drift on a recolor and reinstate the wash-out this fixes.
const CARD_INK: Record<string, CSSProperties | undefined> = Object.fromEntries(
  Object.entries(CARD_VARIANTS).map(([id, variant]) => [id, cardInk(variant)]),
);

export function WorkflowLauncher({
  workflows,
  actor,
  isLoading = false,
  loadError,
  runClient,
  onStarted,
  narrate,
}: {
  workflows: readonly WorkflowMeta[];
  actor: CatalogActor | null;
  /** True until the FIRST catalog fetch settles — renders a spinner. */
  isLoading?: boolean;
  loadError: string | null;
  runClient: RunClient;
  onStarted: (entry: RunEntry) => void;
  narrate: (events: readonly NarrationEvent[]) => void;
}): ReactElement {
  // null selection => the first workflow; null input => derive from the sample.
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [editedJson, setEditedJson] = useState<string | null>(null);
  const [launchError, setLaunchError] = useState<string | null>(null);
  // Busy is scoped to the workflow whose launch is in flight — switching to
  // another workflow must not leave ITS form spuriously disabled.
  const [busyId, setBusyId] = useState<string | null>(null);

  // Selection + editor value are DERIVED (no reset effect): the edited JSON
  // overrides the selected workflow's sample until the user picks a different
  // workflow, which clears the edit.
  const effectiveId = selectedId ?? workflows[0]?.id ?? null;
  const selected = workflows.find((w) => w.id === effectiveId) ?? null;
  const guide = selected ? WORKFLOW_GUIDES[selected.id] : undefined;
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
      // Inline only, never a toast: the fix is in the field right below.
      setLaunchError(
        `invalid JSON: ${error instanceof Error ? error.message : String(error)}`,
      );
      return;
    }
    const launchedId = selected.id;
    setBusyId(launchedId);
    setLaunchError(null);
    try {
      const response = await runClient.start(selected.id, input);
      const entry: RunEntry = {
        workflowId: selected.id,
        runId: response.runId,
        title: selected.title,
        approvalId: response.approval?.id,
        startedAt: Date.now(),
      };
      onStarted(entry);
      // The ○ "steps execute in the DO" line may only claim steps that ran
      // unconditionally — branch-conditional steps are excluded (the input
      // decides which branch ran; claiming both would be false).
      narrate(
        startEvent(entry, response, {
          actor: actor ?? undefined,
          steps: claimableSteps(guide, response.suspended?.[0]?.join('.')),
        }),
      );
    } catch (error) {
      setLaunchError(error instanceof Error ? error.message : String(error));
      narrate([startErrorEvent(selected.id, error, actorRole)]);
    } finally {
      // Another workflow's launch may have started meanwhile — clear only
      // our own claim.
      setBusyId((current) => (current === launchedId ? null : current));
    }
  }

  return (
    <VStack gap={4} aria-label="Launch a workflow">
      <Heading level={2}>Launch a workflow</Heading>
      {loadError ? (
        <Banner
          status="error"
          title={`Could not load workflows: ${loadError}`}
        />
      ) : null}
      {isLoading ? <Spinner label="Loading workflows…" /> : null}
      <Grid columns={{ minWidth: 150 }} gap={2}>
        {workflows.map((w) => (
          <SelectableCard
            key={w.id}
            label={w.title}
            variant={CARD_VARIANTS[w.id] ?? 'default'}
            style={CARD_INK[w.id]}
            padding={3}
            isSelected={w.id === effectiveId}
            onChange={(isSelected) => {
              if (isSelected) selectWorkflow(w.id);
            }}
          >
            {/* label is the a11y name only — visible content is children. */}
            <Text weight="semibold">{w.title}</Text>
          </SelectableCard>
        ))}
      </Grid>
      {selected ? (
        <VStack gap={3}>
          <Text>{selected.description}</Text>
          <HStack gap={2} wrap="wrap">
            {guide?.capabilities.map((capability) => (
              <Tooltip key={capability.label} content={capability.tip}>
                <Token label={capability.label} size="sm" />
              </Tooltip>
            ))}
            {selected.allowedRoles ? (
              <Tooltip content="Only these roles can start this workflow — enforced server-side on POST /runs.">
                <Token
                  label={`start roles: ${selected.allowedRoles.join(', ')}`}
                  size="sm"
                  color={roleAllowed ? 'green' : 'yellow'}
                />
              </Tooltip>
            ) : null}
          </HStack>
          <Collapsible
            trigger={<Text weight="medium">Input (JSON)</Text>}
            defaultIsOpen={false}
          >
            <VStack gap={2}>
              <TextArea
                label="Input (JSON)"
                value={inputJson}
                onChange={(next) => setEditedJson(next)}
                rows={6}
                isDisabled={busyId === selected.id}
              />
              {editedJson !== null ? (
                <Button
                  label="Reset input"
                  variant="ghost"
                  onClick={() => setEditedJson(null)}
                />
              ) : null}
            </VStack>
          </Collapsible>
          {!roleAllowed ? (
            <Banner
              status="warning"
              title={
                actorRole === undefined
                  ? 'No verified identity — sign in with a valid token to launch workflows.'
                  : selected.allowedRoles
                    ? `Your role '${actorRole}' cannot start this workflow — switch to ${selected.allowedRoles.join(' or ')}.`
                    : `Your role '${actorRole}' cannot start any workflow — switch to ${RUN_START_ROLES.join(', ')}.`
              }
            />
          ) : null}
          {launchError ? <Banner status="error" title={launchError} /> : null}
          <HStack gap={2} align="center">
            <Button
              label="Start run"
              variant="primary"
              onClick={() => void launch()}
              isLoading={busyId === selected.id}
              isDisabled={busyId === selected.id || !roleAllowed}
            />
            <Tooltip content={GLOSSARY.runCaps}>
              <Text size="sm" color="secondary">
                Demo budget: 20 runs per sandbox, 500/day across all visitors.
              </Text>
            </Tooltip>
          </HStack>
        </VStack>
      ) : null}
    </VStack>
  );
}
