// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  type AuditMessageBatch,
  type AuditQueueMessage,
  createAuditQueueConsumer,
  queueAuditSink,
} from './index.js';

interface TestEvent {
  action: string;
  decision: string;
}

function makeBatch(events: TestEvent[]): {
  batch: AuditMessageBatch<TestEvent>;
  ackAll: ReturnType<typeof vi.fn>;
  retryAll: ReturnType<typeof vi.fn>;
} {
  const ackAll = vi.fn();
  const retryAll = vi.fn();
  const messages: AuditQueueMessage<TestEvent>[] = events.map((body) => ({
    body,
    ack: vi.fn(),
    retry: vi.fn(),
  }));
  return { batch: { messages, ackAll, retryAll }, ackAll, retryAll };
}

describe('queueAuditSink', () => {
  it('sends each event to the queue binding', async () => {
    // #given
    const send = vi.fn().mockResolvedValue(undefined);
    const sink = queueAuditSink<TestEvent>({ send });

    // #when
    await sink({ action: 'approval.decide', decision: 'allowed' });

    // #then
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith({
      action: 'approval.decide',
      decision: 'allowed',
    });
  });
});

describe('createAuditQueueConsumer', () => {
  it('POSTs the batch as NDJSON with merged headers and acks on 2xx', async () => {
    // #given
    const doFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    const { batch, ackAll, retryAll } = makeBatch([
      { action: 'a1', decision: 'allowed' },
      { action: 'a2', decision: 'denied' },
    ]);
    const consume = createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      headers: { authorization: 'Splunk tok' },
      fetch: doFetch,
    });

    // #when
    await consume(batch);

    // #then
    expect(doFetch).toHaveBeenCalledTimes(1);
    expect(doFetch).toHaveBeenCalledWith('https://siem.example/collect', {
      method: 'POST',
      headers: {
        'content-type': 'application/x-ndjson',
        authorization: 'Splunk tok',
      },
      body: '{"action":"a1","decision":"allowed"}\n{"action":"a2","decision":"denied"}',
    });
    expect(ackAll).toHaveBeenCalledTimes(1);
    expect(retryAll).not.toHaveBeenCalled();
  });

  it('lets callers override the content-type header', async () => {
    // #given
    const doFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    const { batch } = makeBatch([{ action: 'a', decision: 'allowed' }]);

    // #when
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      headers: { 'content-type': 'application/json' },
      transform: (event) => [event],
      fetch: doFetch,
    })(batch);

    // #then
    expect(doFetch.mock.calls[0]?.[1].headers['content-type']).toBe(
      'application/json',
    );
  });

  it('applies the transform envelope per event', async () => {
    // #given
    const doFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    const { batch } = makeBatch([{ action: 'a', decision: 'allowed' }]);

    // #when — Splunk HEC-style envelope
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      transform: (event) => ({ event, sourcetype: 'anchorage:audit' }),
      fetch: doFetch,
    })(batch);

    // #then
    expect(doFetch.mock.calls[0]?.[1].body).toBe(
      '{"event":{"action":"a","decision":"allowed"},"sourcetype":"anchorage:audit"}',
    );
  });

  it('retries the whole batch on a non-2xx response', async () => {
    // #given
    const doFetch = vi.fn().mockResolvedValue({ ok: false, status: 503 });
    const { batch, ackAll, retryAll } = makeBatch([
      { action: 'a', decision: 'allowed' },
    ]);

    // #when
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      fetch: doFetch,
    })(batch);

    // #then
    expect(retryAll).toHaveBeenCalledTimes(1);
    expect(ackAll).not.toHaveBeenCalled();
  });

  it('retries the whole batch when fetch throws', async () => {
    // #given
    const doFetch = vi.fn().mockRejectedValue(new Error('network down'));
    const { batch, ackAll, retryAll } = makeBatch([
      { action: 'a', decision: 'allowed' },
    ]);

    // #when
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      fetch: doFetch,
    })(batch);

    // #then
    expect(retryAll).toHaveBeenCalledTimes(1);
    expect(ackAll).not.toHaveBeenCalled();
  });

  it('does not POST an empty batch', async () => {
    // #given
    const doFetch = vi.fn();
    const { batch, ackAll, retryAll } = makeBatch([]);

    // #when
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      fetch: doFetch,
    })(batch);

    // #then
    expect(doFetch).not.toHaveBeenCalled();
    expect(ackAll).not.toHaveBeenCalled();
    expect(retryAll).not.toHaveBeenCalled();
  });

  it('drops events a transform swallows but still exports the rest', async () => {
    // #given
    const doFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    const { batch, ackAll } = makeBatch([
      { action: 'keep', decision: 'allowed' },
      { action: 'swallow', decision: 'allowed' },
    ]);

    // #when
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      transform: (event) => (event.action === 'swallow' ? undefined : event),
      fetch: doFetch,
    })(batch);

    // #then
    expect(doFetch.mock.calls[0]?.[1].body).toBe(
      '{"action":"keep","decision":"allowed"}',
    );
    expect(ackAll).toHaveBeenCalledTimes(1);
  });

  it('drops an event whose transform throws but still exports the rest', async () => {
    // #given — one event serializes fine, one throws in transform
    const doFetch = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    const { batch, ackAll, retryAll } = makeBatch([
      { action: 'keep', decision: 'allowed' },
      { action: 'boom', decision: 'allowed' },
    ]);

    // #when — a throwing transform must not poison the whole batch
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      transform: (event) => {
        if (event.action === 'boom') throw new Error('kaboom');
        return event;
      },
      fetch: doFetch,
    })(batch);

    // #then — the good event exports and the batch acks (no DLQ storm)
    expect(doFetch.mock.calls[0]?.[1].body).toBe(
      '{"action":"keep","decision":"allowed"}',
    );
    expect(ackAll).toHaveBeenCalledTimes(1);
    expect(retryAll).not.toHaveBeenCalled();
  });

  it('acks without POSTing when a transform swallows every event', async () => {
    // #given — nothing exportable: retrying is useless, dropping must be
    // explicit (acked) rather than a poison batch.
    const doFetch = vi.fn();
    const { batch, ackAll, retryAll } = makeBatch([
      { action: 'swallow', decision: 'allowed' },
    ]);

    // #when
    await createAuditQueueConsumer<TestEvent>({
      endpoint: 'https://siem.example/collect',
      transform: () => undefined,
      fetch: doFetch,
    })(batch);

    // #then
    expect(doFetch).not.toHaveBeenCalled();
    expect(ackAll).toHaveBeenCalledTimes(1);
    expect(retryAll).not.toHaveBeenCalled();
  });
});
