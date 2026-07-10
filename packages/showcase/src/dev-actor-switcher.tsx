// DEV-ONLY. The single app module allowed to import the public demo tokens:
// main.tsx loads it lazily behind `import.meta.env.DEV`, so the production
// bundle contains no token literal (scripts/assert-clean-app-bundle.mjs pins
// that). Identity itself still comes from the server's catalog echo — these
// buttons only choose which PUBLIC dev token to present.

import { HStack } from '@astryxdesign/core/HStack';
import {
  SegmentedControl,
  SegmentedControlItem,
} from '@astryxdesign/core/SegmentedControl';
import { Token } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { type ReactElement, useEffect } from 'react';

import { DEMO_ACTORS } from '#worker/demo-actors';
import { actorSwitchedEvent, type NarrationEvent } from '@/narration';

export function DevActorSwitcher({
  actorToken,
  onSelect,
  narrate,
}: {
  actorToken: string | null;
  onSelect: (token: string) => void;
  narrate: (events: readonly NarrationEvent[]) => void;
}): ReactElement {
  // Dev bootstrap: start signed in as the first demo actor, preserving the
  // zero-click dev-server experience. Runs once; the selection lives in Root.
  const bootstrapToken = actorToken === null ? DEMO_ACTORS[0]?.token : null;
  useEffect(() => {
    if (bootstrapToken) onSelect(bootstrapToken);
  }, [bootstrapToken, onSelect]);

  const selectedId =
    DEMO_ACTORS.find((actor) => actor.token === actorToken)?.id ?? '';

  function switchActor(id: string): void {
    const actor = DEMO_ACTORS.find((entry) => entry.id === id);
    if (!actor || actor.token === actorToken) return;
    onSelect(actor.token);
    narrate([actorSwitchedEvent(actor.id, actor.role)]);
  }

  return (
    <HStack gap={2} align="center" aria-label="Acting identity">
      <Tooltip content="Local dev only: these are the public local-dev bearer tokens; the production bundle ships none.">
        <Token label="dev tokens" size="sm" color="gray" />
      </Tooltip>
      <SegmentedControl
        label="Acting identity"
        value={selectedId}
        onChange={switchActor}
        size="sm"
      >
        {DEMO_ACTORS.map((actor) => (
          <SegmentedControlItem
            key={actor.id}
            value={actor.id}
            label={actor.id}
          />
        ))}
      </SegmentedControl>
    </HStack>
  );
}
