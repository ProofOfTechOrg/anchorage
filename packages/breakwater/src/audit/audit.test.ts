import { describe, expect, it } from 'vitest';

import { AuditLogger } from './index.js';

describe('AuditLogger', () => {
  it('caps the buffer at maxBuffered, dropping oldest first', () => {
    // #given
    const audit = new AuditLogger({ maxBuffered: 2 });

    // #when
    for (const n of [1, 2, 3]) {
      audit.record({
        actor: null,
        action: `a${n}`,
        resource: 'r',
        decision: 'allowed',
      });
    }

    // #then
    expect(audit.events().map((e) => e.action)).toEqual(['a2', 'a3']);
  });

  it('keeps the event and reports via onSinkError when a sync sink throws', () => {
    // #given
    const sinkErrors: unknown[] = [];
    const audit = new AuditLogger({
      sink: () => {
        throw new Error('sink down');
      },
      onSinkError: (error) => sinkErrors.push(error),
    });

    // #when
    audit.record({
      actor: null,
      action: 'a',
      resource: 'r',
      decision: 'allowed',
    });

    // #then
    expect(audit.events()).toHaveLength(1);
    expect(sinkErrors).toHaveLength(1);
  });

  it('reports async sink rejections via onSinkError', async () => {
    // #given
    const sinkErrors: unknown[] = [];
    const audit = new AuditLogger({
      sink: () => Promise.reject(new Error('async sink down')),
      onSinkError: (error) => sinkErrors.push(error),
    });

    // #when
    audit.record({
      actor: null,
      action: 'a',
      resource: 'r',
      decision: 'allowed',
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    // #then
    expect(sinkErrors).toHaveLength(1);
    expect(audit.events()).toHaveLength(1);
  });
});
