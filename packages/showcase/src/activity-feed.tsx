// The activity feed: every observed API response — and the by-design server
// steps between them — as a reverse-chronological story. ● (solid, toned dot)
// = the browser saw this in a response; ○ (neutral dot + "by design" token)
// = what the deployed architecture does between observations. The header says
// so out loud: this is reconstructed client-side from polling, and the
// authoritative audit trail is in Workers Logs.

import { Button } from '@astryxdesign/core/Button';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { StatusDot, type StatusDotVariant } from '@astryxdesign/core/StatusDot';
import { Text } from '@astryxdesign/core/Text';
import { Timestamp } from '@astryxdesign/core/Timestamp';
import { Token } from '@astryxdesign/core/Token';
import { Tooltip } from '@astryxdesign/core/Tooltip';
import { VStack } from '@astryxdesign/core/VStack';
import { type ReactElement, useEffect, useRef } from 'react';

import { GLOSSARY } from '@/glossary';
import type { NarrationEvent, NarrationTone } from '@/narration';
import type { ActivityFeed } from '@/use-activity-feed';
import { ZoneBadge } from '@/zone-badge';

const TONE_DOT: Record<NarrationTone, StatusDotVariant> = {
  neutral: 'neutral',
  info: 'accent',
  success: 'success',
  warning: 'warning',
  danger: 'error',
};

function FeedRow({
  event,
  onReview,
  onViewRun,
}: {
  event: NarrationEvent;
  onReview: (approvalId: string) => void;
  onViewRun: (runId: string) => void;
}): ReactElement {
  return (
    <VStack gap={0.5}>
      <HStack gap={2} align="center" wrap="wrap">
        <StatusDot
          variant={event.observed ? TONE_DOT[event.tone] : 'neutral'}
          label={event.observed ? 'observed' : 'by design'}
          tooltip={
            event.observed
              ? 'Observed: restates an API response this tab received.'
              : 'By design: what the deployed architecture does here; the browser cannot observe it directly.'
          }
        />
        {!event.observed ? <Token label="by design" size="sm" /> : null}
        <ZoneBadge zone={event.zone} />
        <Timestamp
          value={new Date(event.at).toISOString()}
          format="time"
          size="sm"
          color="secondary"
        />
        {event.approvalId !== undefined ? (
          <Button
            label="Review"
            variant="ghost"
            size="sm"
            onClick={() => onReview(event.approvalId ?? '')}
          />
        ) : event.runId !== undefined ? (
          <Button
            label="View run"
            variant="ghost"
            size="sm"
            onClick={() => onViewRun(event.runId ?? '')}
          />
        ) : null}
      </HStack>
      <Text size="sm" weight="medium">
        {event.title}
      </Text>
      {event.detail ? (
        <Text size="sm" color="secondary">
          {event.detail}
        </Text>
      ) : null}
    </VStack>
  );
}

export function ActivityFeedPanel({
  feed,
  onReview,
  onViewRun,
}: {
  feed: ActivityFeed;
  onReview: (approvalId: string) => void;
  onViewRun: (runId: string) => void;
}): ReactElement {
  // The list scrolls inside its own bounded container; new activity lands at
  // the top (the feed is newest-first), so snap the container back to the top
  // whenever the head event changes. DOM-scroll sync — a legitimate effect.
  // clear() empties the feed (headKey undefined) and must not scroll.
  const listRef = useRef<HTMLElement | null>(null);
  const headKey = feed.events[0]?.key;
  useEffect(() => {
    const el = listRef.current;
    if (!el || headKey === undefined) return;
    const reduce = window.matchMedia(
      '(prefers-reduced-motion: reduce)',
    ).matches;
    el.scrollTo({ top: 0, behavior: reduce ? 'auto' : 'smooth' });
  }, [headKey]);

  return (
    <VStack gap={3} aria-label="Activity">
      <HStack gap={2} align="center" justify="between">
        <Heading level={2}>Activity</Heading>
        {feed.events.length > 0 ? (
          <Button label="Clear" variant="ghost" onClick={feed.clear} />
        ) : null}
      </HStack>
      <Tooltip content={GLOSSARY.polling}>
        <Text size="sm" color="secondary">
          Derived in your browser from polling; the authoritative audit trail is
          in Workers Logs. ● observed, ○ by design.
        </Text>
      </Tooltip>
      {feed.events.length === 0 ? (
        <EmptyState
          title="Quiet in here"
          description="As you act, every observed API response and the by-design server steps between them get narrated here. ● observed, ○ by design."
        />
      ) : (
        <VStack
          gap={3}
          ref={listRef}
          style={{
            // 60vh keeps the column within one viewport on laptops; the px
            // ceiling stops it ballooning on tall monitors (200 capped rows
            // would otherwise run thousands of px). Only the rows scroll —
            // the header/honesty note above stay put.
            maxHeight: 'min(60vh, 720px)',
            overflowY: 'auto',
            overscrollBehavior: 'contain',
            paddingRight: 4,
          }}
        >
          {feed.events.map((event) => (
            <FeedRow
              key={event.key}
              event={event}
              onReview={onReview}
              onViewRun={onViewRun}
            />
          ))}
        </VStack>
      )}
    </VStack>
  );
}
