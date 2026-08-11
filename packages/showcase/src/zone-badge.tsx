// Where an event happened: a fixed-color Token per architecture zone, with the
// zone's explanation on hover. One color per zone everywhere — the feed and
// the legend must agree.

import { Token, type TokenColor } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import type { ReactElement } from 'react';

import { ZONES } from '@/glossary';
import type { NarrationZone } from '@/narration';

const ZONE_COLOR: Record<NarrationZone, TokenColor> = {
  browser: 'gray',
  worker: 'blue',
  do: 'purple',
  d1: 'teal',
  alarm: 'yellow',
};

export function ZoneBadge({ zone }: { zone: NarrationZone }): ReactElement {
  return (
    <Tooltip content={ZONES[zone].blurb}>
      <Token label={ZONES[zone].label} size="sm" color={ZONE_COLOR[zone]} />
    </Tooltip>
  );
}
