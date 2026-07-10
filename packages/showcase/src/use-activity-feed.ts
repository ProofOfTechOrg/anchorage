// The activity feed store: an append-only, key-deduped event list. Dedup
// happens INSIDE the setState updater so double invocation (StrictMode) and
// racing poll streams cannot double-append — the second record of a batch
// finds its keys present and returns the current array identity unchanged.

import { useCallback, useState } from 'react';

import type { NarrationEvent } from '@/narration';

/** Newest-first cap — old entries fall off; the feed is a session aid, not an audit log. */
const FEED_CAP = 200;

export interface ActivityFeed {
  /** Newest first. */
  events: readonly NarrationEvent[];
  /** Append a derived batch; events whose key was already recorded are dropped. */
  record: (batch: readonly NarrationEvent[]) => void;
  clear: () => void;
}

export function useActivityFeed(): ActivityFeed {
  const [events, setEvents] = useState<readonly NarrationEvent[]>([]);

  const record = useCallback((batch: readonly NarrationEvent[]) => {
    if (batch.length === 0) return;
    setEvents((current) => {
      const seen = new Set(current.map((event) => event.key));
      const fresh: NarrationEvent[] = [];
      for (const event of batch) {
        if (seen.has(event.key)) continue;
        seen.add(event.key);
        fresh.push(event);
      }
      if (fresh.length === 0) return current;
      // Batches arrive oldest→newest; the feed stores newest-first.
      fresh.reverse();
      return [...fresh, ...current].slice(0, FEED_CAP);
    });
  }, []);

  const clear = useCallback(() => setEvents([]), []);

  return { events, record, clear };
}
