// The architecture explainers: the "Where things run" dialog (the five zones +
// the enforcement story) and the "What's real here?" collapsible (exactly
// which effects are real, simulated, or in between). One module so the copy
// that teaches the architecture lives in one place.

import { Card } from '@astryxdesign/core/Card';
import { Collapsible } from '@astryxdesign/core/Collapsible';
import { Dialog } from '@astryxdesign/core/Dialog';
import { Grid } from '@astryxdesign/core/Grid';
import { Heading } from '@astryxdesign/core/Heading';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import type { ReactElement } from 'react';

import { GLOSSARY, ZONES } from '@/glossary';
import { MarkerRow } from '@/marker-row';
import type { NarrationZone } from '@/narration';
import { ZoneBadge } from '@/zone-badge';

const ZONE_ORDER: readonly NarrationZone[] = [
  'browser',
  'worker',
  'do',
  'd1',
  'cron',
];

export function WhereThingsRunDialog({
  isOpen,
  onOpenChange,
}: {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
}): ReactElement {
  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} width={720} padding={5}>
      <VStack gap={4}>
        <Heading level={2}>Where things run</Heading>
        <Grid columns={{ minWidth: 280 }} gap={3}>
          {ZONE_ORDER.map((zone) => (
            <Card key={zone} variant="muted" padding={3}>
              <VStack gap={2}>
                <ZoneBadge zone={zone} />
                <Text size="sm">{ZONES[zone].blurb}</Text>
              </VStack>
            </Card>
          ))}
        </Grid>
        <VStack gap={2}>
          <Text size="sm">
            <strong>Four gates on every connector call:</strong>{' '}
            {GLOSSARY.fourGates}
          </Text>
          <Text size="sm">
            <strong>Grants:</strong> {GLOSSARY.grantDerivation}
          </Text>
          <Text size="sm">
            <strong>Isolation scope:</strong> {GLOSSARY.isolationScope}
          </Text>
          <Text size="sm" color="secondary">
            {GLOSSARY.polling}
          </Text>
        </VStack>
      </VStack>
    </Dialog>
  );
}

function RealityRow({
  marker,
  title,
  items,
}: {
  marker: string;
  title: string;
  items: string;
}): ReactElement {
  return (
    <MarkerRow marker={`${marker} ${title}`} color="secondary">
      {items}
    </MarkerRow>
  );
}

export function WhatsRealHere(): ReactElement {
  return (
    <Collapsible
      trigger={<Text weight="medium">What's real here?</Text>}
      defaultIsOpen={false}
    >
      <VStack gap={2} paddingBlock={2}>
        <RealityRow
          marker="✔"
          title="REAL"
          items="the approval queue (D1 records), grant derivation + fail-closed enforcement, RBAC + separation of duties, deployment-scoped isolation keys, durable suspend/resume across restarts, audit logging, idempotency keys, rate limits."
        />
        <RealityRow
          marker="◌"
          title="SIMULATED"
          items="the blast radius: email delivery, the CRM POST, deploy/promote calls. There are no live bindings; the full code path runs and the envelope is logged."
        />
        <RealityRow
          marker="◐"
          title="IN BETWEEN"
          items="content-pipeline publishes real writes to an in-memory bucket standing in for R2."
        />
      </VStack>
    </Collapsible>
  );
}
