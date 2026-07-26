// Turns new feed events with toast: true into Astryx toasts. A cursor Set of
// seen keys lives in a ref; the FIRST pass after mount swallows pre-existing
// events without toasting (a remount must not replay the session), and every
// later pass toasts only keys it has never seen. Astryx's own uniqueID +
// collisionBehavior is the second line of dedup defense.

import { Button } from '@astryxdesign/core/Button';
import { Text } from '@astryxdesign/core/Text';
import { useToast } from '@astryxdesign/core/Toast';
import { VStack } from '@astryxdesign/core/VStack';
import { useEffect, useRef } from 'react';

import type { NarrationEvent } from '@/narration';

const AUTO_HIDE_MS = 6_000;
const AUTO_HIDE_LONG_MS = 10_000;

export interface NarrationToastActions {
  /** Jump to an approval in the queue (dashboard select). */
  onReview: (approvalId: string) => void;
  /** Scroll a run card into view. */
  onViewRun: (runId: string) => void;
}

export function useNarrationToasts(
  events: readonly NarrationEvent[],
  actions: NarrationToastActions,
): void {
  const showToast = useToast();
  const seenRef = useRef<Set<string> | null>(null);
  // Read the latest actions from the effect without re-running it when the
  // callbacks' identities churn.
  const actionsRef = useRef(actions);
  useEffect(() => {
    actionsRef.current = actions;
  }, [actions]);

  useEffect(() => {
    if (seenRef.current === null) {
      seenRef.current = new Set(events.map((event) => event.key));
      return;
    }
    const seen = seenRef.current;
    // The feed stores newest-first; toast oldest-first so stacking order
    // matches the story's order.
    for (let index = events.length - 1; index >= 0; index -= 1) {
      const event = events[index];
      if (event === undefined || seen.has(event.key)) continue;
      seen.add(event.key);
      if (!event.toast) continue;

      // Holder object: the jump button's closure needs the dismiss function
      // that showToast only returns after the button element is built.
      const handle: { dismiss?: () => void } = {};
      const jump =
        event.approvalId !== undefined
          ? {
              label: 'Review',
              go: () => actionsRef.current.onReview(event.approvalId ?? ''),
            }
          : event.runId !== undefined
            ? {
                label: 'View run',
                go: () => actionsRef.current.onViewRun(event.runId ?? ''),
              }
            : undefined;
      handle.dismiss = showToast({
        type: event.tone === 'danger' ? 'error' : 'info',
        body: (
          <VStack gap={0.5}>
            <Text size="sm" weight="semibold">
              {event.title}
            </Text>
            {event.detail ? <Text size="sm">{event.detail}</Text> : null}
          </VStack>
        ),
        endContent: jump ? (
          <Button
            label={jump.label}
            variant="ghost"
            size="sm"
            onClick={() => {
              jump.go();
              handle.dismiss?.();
            }}
          />
        ) : undefined,
        uniqueID: event.toastReplaceId ?? event.key,
        collisionBehavior: event.toastReplaceId ? 'overwrite' : 'ignore',
        isAutoHide: event.toastSticky !== true,
        autoHideDuration: event.toastLong ? AUTO_HIDE_LONG_MS : AUTO_HIDE_MS,
      });
    }
  }, [events, showToast]);
}
