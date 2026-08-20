// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import type { RunSummary } from './runtime.js';
import {
  dueSuspensionDeadline,
  isReadableRunSummary,
  isSuspensionTimeoutResumeData,
  MASTRA_WORKFLOW_META_KEY,
  MAX_SUSPENSION_DEADLINE_ATTEMPTS,
  MAX_SUSPENSION_DEADLINE_MS,
  MAX_SUSPENSION_DEADLINES_PER_RUN,
  MIN_SUSPENSION_DEADLINE_MS,
  mergeSuspensionDeadlines,
  nextSuspensionDeadlineAt,
  parseSuspensionDeadlineRecord,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
  SUSPENSION_TIMEOUT_RESUME_KEY,
  type SuspensionDeadlineEntry,
  type SuspensionDeadlineRecord,
  suspensionDeadlinesOf,
  suspensionTimeoutResumeData,
  tombstoned,
} from './suspension-deadline.js';

const SUSPENDED_AT = 1_751_882_400_000;

function suspendedSummary(steps: {
  [step: string]: {
    payload?: unknown;
    suspendedAt?: number;
    resumeCount?: number;
  };
}): RunSummary {
  const keys = Object.keys(steps);
  const summary: RunSummary = {
    runId: 'run-1',
    status: 'suspended',
    suspended: keys.map((key) => [key]),
    suspendPayload: Object.fromEntries(
      keys.map((key) => [key, steps[key]?.payload]),
    ),
  };
  const suspendedAt = keys.filter(
    (key) => steps[key]?.suspendedAt !== undefined,
  );
  if (suspendedAt.length > 0) {
    summary.suspendedAt = Object.fromEntries(
      suspendedAt.map((key) => [key, steps[key]?.suspendedAt as number]),
    );
  }
  const resumeCount = keys.filter(
    (key) => steps[key]?.resumeCount !== undefined,
  );
  if (resumeCount.length > 0) {
    summary.resumeCount = Object.fromEntries(
      resumeCount.map((key) => [key, steps[key]?.resumeCount as number]),
    );
  }
  return summary;
}

function armedSummary(deadlineMs: unknown): RunSummary {
  return suspendedSummary({
    gate: {
      payload: {
        reason: 'awaiting signal',
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: deadlineMs,
      },
      suspendedAt: SUSPENDED_AT,
    },
  });
}

function storedRecord(
  entries: SuspensionDeadlineEntry[],
): SuspensionDeadlineRecord {
  return { version: 1, workflowId: 'gated', runId: 'run-1', entries };
}

describe('suspensionDeadlinesOf', () => {
  it('arms nothing for a run that is not suspended', () => {
    expect(
      suspensionDeadlinesOf({ runId: 'run-1', status: 'success' }),
    ).toEqual({ entries: [], rejected: [] });
  });

  it('arms nothing when the suspension carries no reserved key', () => {
    const summary = suspendedSummary({
      gate: {
        payload: { reason: 'awaiting signal' },
        suspendedAt: SUSPENDED_AT,
      },
    });

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [],
    });
  });

  it('arms nothing when the suspension carries no payload object at all', () => {
    for (const payload of [undefined, null, 'awaiting', 7, [900_000]]) {
      expect(
        suspensionDeadlinesOf(
          suspendedSummary({ gate: { payload, suspendedAt: SUSPENDED_AT } }),
        ),
      ).toEqual({ entries: [], rejected: [] });
    }
  });

  it('derives the deadline from the suspension time, never from now', () => {
    const { entries, rejected } = suspensionDeadlinesOf(armedSummary(900_000));

    expect(rejected).toEqual([]);
    expect(entries).toEqual([
      {
        step: 'gate',
        deadlineAt: SUSPENDED_AT + 900_000,
        suspendedAt: SUSPENDED_AT,
        resumeCount: 0,
      },
    ]);
  });

  it('derives byte-identical entries from the same summary twice', () => {
    const summary = armedSummary(900_000);

    expect(suspensionDeadlinesOf(summary).entries).toEqual(
      suspensionDeadlinesOf(summary).entries,
    );
  });

  it.each([
    ['below the minimum', MIN_SUSPENSION_DEADLINE_MS - 1],
    ['above the maximum', MAX_SUSPENSION_DEADLINE_MS + 1],
    ['zero', 0],
    ['negative', -1_000],
    ['fractional', 1_500.5],
    ['NaN', Number.NaN],
    ['a numeric string', '900000'],
    ['a boolean', true],
    ['null', null],
    ['an object', { ms: 900_000 }],
  ])('rejects a %s deadline without throwing', (_label, deadlineMs) => {
    const { entries, rejected } = suspensionDeadlinesOf(
      armedSummary(deadlineMs),
    );

    expect(entries).toEqual([]);
    expect(rejected).toHaveLength(1);
    expect(rejected[0]?.step).toBe('gate');
    expect(rejected[0]?.reason).toContain(SUSPENSION_DEADLINE_PAYLOAD_KEY);
  });

  it.each([
    MIN_SUSPENSION_DEADLINE_MS,
    MAX_SUSPENSION_DEADLINE_MS,
  ])('accepts the inclusive bound %i', (deadlineMs) => {
    const { entries, rejected } = suspensionDeadlinesOf(
      armedSummary(deadlineMs),
    );

    expect(rejected).toEqual([]);
    expect(entries[0]?.deadlineAt).toBe(SUSPENDED_AT + deadlineMs);
  });

  it('refuses to arm a step with no suspendedAt fence', () => {
    const summary = suspendedSummary({
      gate: { payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 } },
    });

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [{ step: 'gate', reason: 'no suspendedAt fence' }],
    });
  });

  it('refuses to arm a step whose resumeCount fence is malformed', () => {
    const summary = suspendedSummary({
      gate: {
        payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
        suspendedAt: SUSPENDED_AT,
        resumeCount: -1,
      },
    });

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [{ step: 'gate', reason: 'resumeCount fence is malformed' }],
    });
  });

  it('normalizes an absent resumeCount to 0 and carries a present one', () => {
    const first = suspensionDeadlinesOf(armedSummary(900_000));
    const resumed = suspensionDeadlinesOf(
      suspendedSummary({
        gate: {
          payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
          suspendedAt: SUSPENDED_AT,
          resumeCount: 2,
        },
      }),
    );

    expect(first.entries[0]?.resumeCount).toBe(0);
    expect(resumed.entries[0]?.resumeCount).toBe(2);
  });

  it('arms one entry per suspended step, earliest deadline first', () => {
    const summary = suspendedSummary({
      slow: {
        payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
        suspendedAt: SUSPENDED_AT,
      },
      quick: {
        payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 60_000 },
        suspendedAt: SUSPENDED_AT,
      },
    });

    expect(suspensionDeadlinesOf(summary).entries.map((e) => e.step)).toEqual([
      'quick',
      'slow',
    ]);
  });

  it('derives one entry for a dotted step id from either projection', () => {
    // The same suspension as both projections report it: the live result keeps
    // a top-level step id whole, the rehydrated one splits the stored key on
    // every dot. Both index the payload and the fence by the joined key, so
    // both must derive the entry the wake will look for.
    const armed = {
      [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000,
      reason: 'awaiting signal',
    };
    const live: RunSummary = {
      runId: 'run-1',
      status: 'suspended',
      suspended: [['a.b']],
      suspendPayload: { 'a.b': armed },
      suspendedAt: { 'a.b': SUSPENDED_AT },
    };
    const rehydrated: RunSummary = { ...live, suspended: [['a', 'b']] };

    expect(suspensionDeadlinesOf(live)).toEqual({
      entries: [
        {
          step: 'a.b',
          deadlineAt: SUSPENDED_AT + 900_000,
          suspendedAt: SUSPENDED_AT,
          resumeCount: 0,
        },
      ],
      rejected: [],
    });
    expect(suspensionDeadlinesOf(rehydrated)).toEqual(
      suspensionDeadlinesOf(live),
    );
  });

  it('refuses both suspensions when two suspended paths join to one key', () => {
    // The live projection of the collision: a top-level step id 'a.b' and a
    // nested workflow 'a' whose inner step 'b' suspended too. Mastra keys its
    // snapshot namespace by the joined path, so both answer to 'a.b' and no
    // fence can say which suspension an entry describes.
    const summary: RunSummary = {
      runId: 'run-1',
      status: 'suspended',
      suspended: [['a.b'], ['a', 'b']],
      suspendPayload: {
        'a.b': { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
        a: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
      },
      suspendedAt: { 'a.b': SUSPENDED_AT, a: SUSPENDED_AT },
    };

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [
        { step: 'a.b', reason: 'ambiguous suspended step path' },
        { step: 'a.b', reason: 'ambiguous suspended step path' },
      ],
    });
  });

  it('refuses a key implied by the nesting marker of another suspension', () => {
    // The SAME collision on the rehydrated projection, where the two paths no
    // longer coincide: the nested one collapses to 'a', and its marker names
    // the inner step, so 'a.b' is a key two suspensions could have written.
    const summary: RunSummary = {
      runId: 'run-1',
      status: 'suspended',
      suspended: [['a', 'b'], ['a']],
      suspendPayload: {
        'a.b': { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
        a: {
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000,
          [MASTRA_WORKFLOW_META_KEY]: { runId: 'run-1', path: ['b'] },
        },
      },
      suspendedAt: { 'a.b': SUSPENDED_AT, a: SUSPENDED_AT },
    };

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [
        { step: 'a.b', reason: 'ambiguous suspended step path' },
        { step: 'a', reason: 'nested suspension paths are not supported' },
      ],
    });
  });

  it('arms both when a plain step id and a dotted one suspend together', () => {
    // The negative control for the rule above: 'a' and 'a.b' are two ordinary
    // top-level steps, neither carries a nesting marker, and refusing them
    // would strand deadlines the author is entitled to.
    const summary = suspendedSummary({
      a: {
        payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
        suspendedAt: SUSPENDED_AT,
      },
      'a.b': {
        payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
        suspendedAt: SUSPENDED_AT,
      },
    });

    const { entries, rejected } = suspensionDeadlinesOf(summary);

    expect(rejected).toEqual([]);
    expect(entries.map((entry) => entry.step)).toEqual(['a', 'a.b']);
  });

  it('rejects a nested suspension instead of silently arming nothing', () => {
    // The shape Mastra's live result actually produces for a nested
    // suspension: the nested path in `suspended`, but the payload and the
    // fence keyed by the TOP-LEVEL step. There is no fence for the step that
    // suspended, so the deadline is unarmable — and saying so is the point,
    // because the author asked for one.
    const summary: RunSummary = {
      runId: 'run-1',
      status: 'suspended',
      suspended: [['nested', 'approval']],
      suspendPayload: {
        nested: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
      },
      suspendedAt: { nested: SUSPENDED_AT },
    };

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [
        {
          step: 'nested.approval',
          reason: 'nested suspension paths are not supported',
        },
      ],
    });
  });

  it('rejects a nested suspension on the rehydrated projection too', () => {
    // The SAME suspension read back from the snapshot: it collapses to the
    // enclosing step, so it is single-segment and does have a `suspendedAt` —
    // it looks armable and is not, because that fence belongs to the enclosing
    // step and not to the inner one that suspended. Mastra's own nesting
    // marker in the persisted payload is the evidence that says so.
    const summary: RunSummary = {
      runId: 'run-1',
      status: 'suspended',
      suspended: [['nested']],
      suspendPayload: {
        nested: {
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000,
          [MASTRA_WORKFLOW_META_KEY]: { runId: 'run-1', path: ['approval'] },
        },
      },
      suspendedAt: { nested: SUSPENDED_AT },
    };

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [
        {
          step: 'nested',
          reason: 'nested suspension paths are not supported',
        },
      ],
    });
  });

  it.each([
    ['an absent marker', undefined],
    ['a renamed or reshaped marker', { runId: 'run-1' }],
    ['a marker whose path is not an array', { path: 'approval' }],
    ['a marker with an empty path', { path: [] }],
  ])('arms a top-level step despite %s', (_label, meta) => {
    // The marker is framework data inside an author payload, so a Mastra change
    // must not start refusing ordinary top-level deadlines. The tripwire test
    // in runtime.test.ts is what makes such a change loud.
    const summary = suspendedSummary({
      gate: {
        payload: {
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000,
          [MASTRA_WORKFLOW_META_KEY]: meta,
        },
        suspendedAt: SUSPENDED_AT,
      },
    });

    expect(suspensionDeadlinesOf(summary).entries).toHaveLength(1);
  });

  it('stays silent for a nested suspension that arms no deadline', () => {
    const summary: RunSummary = {
      runId: 'run-1',
      status: 'suspended',
      suspended: [['nested', 'approval']],
      suspendPayload: { nested: { reason: 'awaiting signal' } },
      suspendedAt: { nested: SUSPENDED_AT },
    };

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [],
    });
  });

  it('rejects a deadline whose fence plus offset leaves the safe range', () => {
    const summary = suspendedSummary({
      gate: {
        payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 },
        suspendedAt: Number.MAX_SAFE_INTEGER,
      },
    });

    expect(suspensionDeadlinesOf(summary)).toEqual({
      entries: [],
      rejected: [
        {
          step: 'gate',
          reason: `${SUSPENSION_DEADLINE_PAYLOAD_KEY} exceeds the supported range`,
        },
      ],
    });
  });

  it('caps the armed entries per run and reports the overflow', () => {
    const steps: Parameters<typeof suspendedSummary>[0] = {};
    for (let index = 0; index <= MAX_SUSPENSION_DEADLINES_PER_RUN; index += 1) {
      steps[`gate-${index}`] = {
        payload: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 60_000 + index },
        suspendedAt: SUSPENDED_AT,
      };
    }

    const { entries, rejected } = suspensionDeadlinesOf(
      suspendedSummary(steps),
    );

    expect(entries).toHaveLength(MAX_SUSPENSION_DEADLINES_PER_RUN);
    expect(rejected).toEqual([
      {
        step: `gate-${MAX_SUSPENSION_DEADLINES_PER_RUN}`,
        reason: `run already arms ${MAX_SUSPENSION_DEADLINES_PER_RUN} suspension deadlines`,
      },
    ]);
  });
});

describe('parseSuspensionDeadlineRecord', () => {
  it('round-trips a stored record, retry ledger included', () => {
    const record = storedRecord([
      {
        step: 'gate',
        deadlineAt: SUSPENDED_AT + 900_000,
        suspendedAt: SUSPENDED_AT,
        resumeCount: 1,
        attempts: 2,
        nextAttemptAt: SUSPENDED_AT + 960_000,
      },
    ]);

    expect(
      parseSuspensionDeadlineRecord(JSON.parse(JSON.stringify(record))),
    ).toEqual(record);
  });

  it('passes an absent record through', () => {
    expect(parseSuspensionDeadlineRecord(undefined)).toBeUndefined();
  });

  it.each([
    ['a non-object', 'record'],
    ['null', null],
    ['an array', []],
    ['an unknown version', { ...storedRecord([]), version: 2 }],
    [
      'a path-unsafe workflowId',
      { ...storedRecord([]), workflowId: 'gated/forged' },
    ],
    ['a path-unsafe runId', { ...storedRecord([]), runId: 'run 1' }],
    ['missing entries', { version: 1, workflowId: 'gated', runId: 'run-1' }],
    ['non-array entries', { ...storedRecord([]), entries: {} }],
    [
      'more entries than the per-run cap',
      {
        ...storedRecord([]),
        entries: Array.from(
          { length: MAX_SUSPENSION_DEADLINES_PER_RUN + 1 },
          () => ({
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
          }),
        ),
      },
    ],
    ['a non-object entry', { ...storedRecord([]), entries: ['gate'] }],
    [
      'an empty step',
      {
        ...storedRecord([]),
        entries: [
          {
            step: '',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
          },
        ],
      },
    ],
    [
      'an unbounded step',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'a'.repeat(501),
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
          },
        ],
      },
    ],
    [
      'a fractional deadlineAt',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT + 0.5,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
          },
        ],
      },
    ],
    [
      'a missing suspendedAt fence',
      {
        ...storedRecord([]),
        entries: [{ step: 'gate', deadlineAt: SUSPENDED_AT, resumeCount: 0 }],
      },
    ],
    [
      'a missing resumeCount fence',
      {
        ...storedRecord([]),
        entries: [
          { step: 'gate', deadlineAt: SUSPENDED_AT, suspendedAt: SUSPENDED_AT },
        ],
      },
    ],
    [
      'a negative resumeCount',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: -1,
          },
        ],
      },
    ],
    [
      'attempts without a retry floor',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
            attempts: 1,
          },
        ],
      },
    ],
    [
      'a retry floor without attempts',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
            nextAttemptAt: SUSPENDED_AT,
          },
        ],
      },
    ],
    [
      'a zero attempt count',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
            attempts: 0,
            nextAttemptAt: SUSPENDED_AT,
          },
        ],
      },
    ],
    // A spent budget reads back as a tombstone, and a tombstone is never
    // retried, so a retry floor on one is a shape this module never writes.
    [
      'a tombstone carrying a retry floor',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
            attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
            nextAttemptAt: SUSPENDED_AT,
          },
        ],
      },
    ],
    [
      'a fractional unreadable-state clock',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
            unreadableSince: SUSPENDED_AT + 0.5,
          },
        ],
      },
    ],
    // More than the budget cannot have been written here, and accepting it
    // would let a foreign writer make this module's own drop log overcount.
    [
      'more attempts than the retry budget',
      {
        ...storedRecord([]),
        entries: [
          {
            step: 'gate',
            deadlineAt: SUSPENDED_AT,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
            attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS + 1,
            nextAttemptAt: SUSPENDED_AT,
          },
        ],
      },
    ],
  ])('rejects %s', (_label, stored) => {
    expect(() => parseSuspensionDeadlineRecord(stored)).toThrow(
      'stored suspension deadline is malformed',
    );
  });

  it('round-trips the unreadable-state clock, unpaired with the retry ledger', () => {
    // Unlike the retry floor, this one is not half of a pair: it counts how
    // long reads have been failing, which is independent of how many resumes
    // have — an entry can carry it with no ledger at all.
    const record = storedRecord([
      {
        step: 'gate',
        deadlineAt: SUSPENDED_AT + 900_000,
        suspendedAt: SUSPENDED_AT,
        resumeCount: 0,
        unreadableSince: SUSPENDED_AT + 60_000,
      },
    ]);

    expect(
      parseSuspensionDeadlineRecord(JSON.parse(JSON.stringify(record))),
    ).toEqual(record);
  });

  it('round-trips a tombstone, whose spent budget carries no retry floor', () => {
    const record = storedRecord([
      {
        step: 'gate',
        deadlineAt: SUSPENDED_AT,
        suspendedAt: SUSPENDED_AT,
        resumeCount: 0,
        attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
      },
    ]);

    expect(
      parseSuspensionDeadlineRecord(JSON.parse(JSON.stringify(record))),
    ).toEqual(record);
  });
});

describe('nextSuspensionDeadlineAt', () => {
  it('has no wake for an absent or empty record', () => {
    expect(nextSuspensionDeadlineAt(undefined)).toBeUndefined();
    expect(nextSuspensionDeadlineAt(storedRecord([]))).toBeUndefined();
  });

  it('takes the earliest entry, honouring a retry floor over the deadline', () => {
    const record = storedRecord([
      {
        step: 'retrying',
        deadlineAt: SUSPENDED_AT,
        suspendedAt: SUSPENDED_AT,
        resumeCount: 0,
        attempts: 1,
        nextAttemptAt: SUSPENDED_AT + 600_000,
      },
      {
        step: 'waiting',
        deadlineAt: SUSPENDED_AT + 300_000,
        suspendedAt: SUSPENDED_AT,
        resumeCount: 0,
      },
    ]);

    expect(nextSuspensionDeadlineAt(record)).toBe(SUSPENDED_AT + 300_000);
  });

  it('arms nothing for a tombstone, whose past deadline would be the floor', () => {
    const tombstone: SuspensionDeadlineEntry = {
      step: 'abandoned',
      deadlineAt: SUSPENDED_AT,
      suspendedAt: SUSPENDED_AT,
      resumeCount: 0,
      attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
    };

    expect(nextSuspensionDeadlineAt(storedRecord([tombstone]))).toBeUndefined();
    expect(
      nextSuspensionDeadlineAt(
        storedRecord([
          tombstone,
          {
            step: 'waiting',
            deadlineAt: SUSPENDED_AT + 300_000,
            suspendedAt: SUSPENDED_AT,
            resumeCount: 0,
          },
        ]),
      ),
    ).toBe(SUSPENDED_AT + 300_000);
  });
});

describe('dueSuspensionDeadline', () => {
  const record = storedRecord([
    {
      step: 'later',
      deadlineAt: SUSPENDED_AT + 600_000,
      suspendedAt: SUSPENDED_AT,
      resumeCount: 0,
    },
    {
      step: 'earlier',
      deadlineAt: SUSPENDED_AT + 300_000,
      suspendedAt: SUSPENDED_AT,
      resumeCount: 0,
    },
    {
      step: 'backing-off',
      deadlineAt: SUSPENDED_AT,
      suspendedAt: SUSPENDED_AT,
      resumeCount: 0,
      attempts: 1,
      nextAttemptAt: SUSPENDED_AT + 900_000,
    },
  ]);

  it('selects nothing before the earliest deadline elapses', () => {
    expect(
      dueSuspensionDeadline(record, SUSPENDED_AT + 299_999),
    ).toBeUndefined();
    expect(dueSuspensionDeadline(undefined, SUSPENDED_AT)).toBeUndefined();
  });

  it('selects the earliest due entry and skips one still backing off', () => {
    expect(dueSuspensionDeadline(record, SUSPENDED_AT + 700_000)?.step).toBe(
      'earlier',
    );
  });

  it('selects a backing-off entry once its retry floor elapses', () => {
    expect(dueSuspensionDeadline(record, SUSPENDED_AT + 900_000)?.step).toBe(
      'backing-off',
    );
  });

  it('never selects a tombstone, however long past due', () => {
    const tombstoneRecord = storedRecord([
      {
        step: 'abandoned',
        deadlineAt: SUSPENDED_AT,
        suspendedAt: SUSPENDED_AT,
        resumeCount: 0,
        attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
      },
    ]);

    expect(
      dueSuspensionDeadline(tombstoneRecord, SUSPENDED_AT + 31_536_000_000),
    ).toBeUndefined();
  });
});

describe('tombstoned', () => {
  const spent: SuspensionDeadlineEntry = {
    step: 'gate',
    deadlineAt: SUSPENDED_AT + 900_000,
    suspendedAt: SUSPENDED_AT,
    resumeCount: 0,
    attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS - 1,
    nextAttemptAt: SUSPENDED_AT + 960_000,
    unreadableSince: SUSPENDED_AT + 60_000,
  };

  it('keeps the suspension it was armed against and drops all that only a retry needs', () => {
    const stone = tombstoned(spent);

    expect(stone).toEqual({
      step: 'gate',
      deadlineAt: SUSPENDED_AT + 900_000,
      suspendedAt: SUSPENDED_AT,
      resumeCount: 0,
      attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
    });
    // Absent, not present-and-undefined: a stored property carrying no value
    // is one the strict parser would have to special-case on read-back.
    expect(Object.keys(stone)).not.toContain('nextAttemptAt');
    expect(Object.keys(stone)).not.toContain('unreadableSince');
  });

  it('is read back as spent by the parser and refused by both selectors', () => {
    const record = storedRecord([tombstoned(spent)]);

    // #then — the shape survives the storage round-trip it is written for ...
    expect(
      parseSuspensionDeadlineRecord(JSON.parse(JSON.stringify(record))),
    ).toEqual(record);
    // ... and neither selector will retry it or wake the object for it,
    // however long its deadline has been past.
    expect(
      dueSuspensionDeadline(record, SUSPENDED_AT + 31_536_000_000),
    ).toBeUndefined();
    expect(nextSuspensionDeadlineAt(record)).toBeUndefined();
  });
});

describe('isReadableRunSummary', () => {
  it('rejects the degraded in-memory fallback: suspended with no paths', () => {
    expect(isReadableRunSummary({ runId: 'run-1', status: 'suspended' })).toBe(
      false,
    );
    expect(
      isReadableRunSummary({
        runId: 'run-1',
        status: 'suspended',
        suspended: [],
      }),
    ).toBe(false);
  });

  it('accepts a genuinely suspended summary and every other status', () => {
    expect(
      isReadableRunSummary(suspendedSummary({ gate: { payload: {} } })),
    ).toBe(true);
    expect(isReadableRunSummary({ runId: 'run-1', status: 'success' })).toBe(
      true,
    );
    expect(isReadableRunSummary({ runId: 'run-1', status: 'running' })).toBe(
      true,
    );
    // Including the shape this predicate cannot catch: Mastra's in-memory
    // fallback reports the Run object's own status, and that is 'pending' for
    // a run which has never been resumed — the dominant degraded read. Only
    // the isFromInMemory marker inside RunnerRuntime.authoritativeStatus tells
    // it from a run that genuinely has not started executing yet.
    expect(isReadableRunSummary({ runId: 'run-1', status: 'pending' })).toBe(
      true,
    );
  });
});

describe('suspension timeout resume data', () => {
  it('wraps the expired entry under the reserved key', () => {
    const entry: SuspensionDeadlineEntry = {
      step: 'gate',
      deadlineAt: SUSPENDED_AT + 900_000,
      suspendedAt: SUSPENDED_AT,
      resumeCount: 0,
    };

    const resumeData = suspensionTimeoutResumeData(
      entry,
      SUSPENDED_AT + 900_123,
    );

    expect(resumeData).toEqual({
      [SUSPENSION_TIMEOUT_RESUME_KEY]: {
        step: 'gate',
        deadlineAt: SUSPENDED_AT + 900_000,
        expiredAt: SUSPENDED_AT + 900_123,
      },
    });
    expect(isSuspensionTimeoutResumeData(resumeData)).toBe(true);
  });

  it.each([
    ['a real signal payload', { approvedBy: 'bob' }],
    ['no payload', undefined],
    ['null', null],
    ['a string', SUSPENSION_TIMEOUT_RESUME_KEY],
    ['an array', [{ [SUSPENSION_TIMEOUT_RESUME_KEY]: {} }]],
    ['an empty envelope', { [SUSPENSION_TIMEOUT_RESUME_KEY]: {} }],
    [
      'an envelope with no step',
      {
        [SUSPENSION_TIMEOUT_RESUME_KEY]: {
          deadlineAt: SUSPENDED_AT,
          expiredAt: SUSPENDED_AT,
        },
      },
    ],
    [
      'an envelope with a fractional time',
      {
        [SUSPENSION_TIMEOUT_RESUME_KEY]: {
          step: 'gate',
          deadlineAt: SUSPENDED_AT + 0.5,
          expiredAt: SUSPENDED_AT,
        },
      },
    ],
  ])('does not mistake %s for a timeout', (_label, value) => {
    expect(isSuspensionTimeoutResumeData(value)).toBe(false);
  });
});

describe('mergeSuspensionDeadlines', () => {
  const entry: SuspensionDeadlineEntry = {
    step: 'gate',
    deadlineAt: SUSPENDED_AT + 900_000,
    suspendedAt: SUSPENDED_AT,
    resumeCount: 0,
  };
  const previous = storedRecord([
    { ...entry, attempts: 2, nextAttemptAt: SUSPENDED_AT + 960_000 },
  ]);

  it('carries the retry ledger across an unchanged suspension', () => {
    expect(mergeSuspensionDeadlines(previous, [entry])).toEqual([
      { ...entry, attempts: 2, nextAttemptAt: SUSPENDED_AT + 960_000 },
    ]);
  });

  it.each([
    ['step', { ...entry, step: 'other' }],
    ['suspendedAt', { ...entry, suspendedAt: SUSPENDED_AT + 1 }],
    ['resumeCount', { ...entry, resumeCount: 1 }],
    ['deadlineAt', { ...entry, deadlineAt: SUSPENDED_AT + 1 }],
  ])('starts a clean ledger when %s moved', (_label, derived) => {
    expect(mergeSuspensionDeadlines(previous, [derived])).toEqual([derived]);
  });

  it('drops a stored entry the summary no longer derives', () => {
    expect(mergeSuspensionDeadlines(previous, [])).toEqual([]);
    expect(mergeSuspensionDeadlines(undefined, [entry])).toEqual([entry]);
  });

  it('carries a tombstone across the unchanged suspension without a floor', () => {
    const tombstoneRecord = storedRecord([
      { ...entry, attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS },
    ]);

    const merged = mergeSuspensionDeadlines(tombstoneRecord, [entry]);

    expect(merged).toEqual([
      { ...entry, attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS },
    ]);
    // Not merely an equal shape: no `nextAttemptAt: undefined` property that
    // the strict parser would have to special-case on read-back.
    expect(Object.keys(merged[0] as object)).not.toContain('nextAttemptAt');
  });

  it('clears the unreadable-state clock rather than carrying it', () => {
    // A merge only ever follows an authoritative read that SUCCEEDED, and a
    // successful read is exactly what ends an unreadable stretch — so not
    // carrying the stamp IS the clear.
    const unread = storedRecord([
      { ...entry, unreadableSince: SUSPENDED_AT + 60_000 },
    ]);

    const merged = mergeSuspensionDeadlines(unread, [entry]);

    expect(merged).toEqual([entry]);
    expect(Object.keys(merged[0] as object)).not.toContain('unreadableSince');
  });

  it('drops the tombstone when the fence moves, starting a fresh budget', () => {
    const tombstoneRecord = storedRecord([
      { ...entry, attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS },
    ]);
    const resuspended = {
      ...entry,
      suspendedAt: SUSPENDED_AT + 60_000,
      deadlineAt: SUSPENDED_AT + 960_000,
      resumeCount: 1,
    };

    expect(mergeSuspensionDeadlines(tombstoneRecord, [resuspended])).toEqual([
      resuspended,
    ]);
  });

  it('lets a tombstone hold a capped slot against a live deadline', () => {
    // The cap is applied to derived entries in deadline order, and a tombstone
    // belongs to an OLD suspension, so its entry sorts first and keeps its
    // slot for as long as that suspension lasts. Past the cap that is an
    // abandoned deadline displacing a live one: the abandonment contract is
    // cap-conditional, which the design doc now says out loud.
    const steps: Record<string, { payload: unknown; suspendedAt: number }> = {};
    for (let index = 0; index <= MAX_SUSPENSION_DEADLINES_PER_RUN; index += 1) {
      steps[`step-${String(index).padStart(2, '0')}`] = {
        payload: {
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]: MIN_SUSPENSION_DEADLINE_MS,
        },
        suspendedAt: SUSPENDED_AT + index,
      };
    }
    const { entries, rejected } = suspensionDeadlinesOf(
      suspendedSummary(steps),
    );

    expect(entries).toHaveLength(MAX_SUSPENSION_DEADLINES_PER_RUN);
    expect(rejected).toEqual([
      {
        step: `step-${MAX_SUSPENSION_DEADLINES_PER_RUN}`,
        reason: `run already arms ${MAX_SUSPENSION_DEADLINES_PER_RUN} suspension deadlines`,
      },
    ]);

    const abandoned: SuspensionDeadlineEntry = {
      ...(entries[0] as SuspensionDeadlineEntry),
      attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
    };
    const merged = mergeSuspensionDeadlines(storedRecord([abandoned]), entries);

    // #then — the tombstone survived in the slot the newest live suspension
    // could not have, and the alarm arms for the earliest entry that is not it
    expect(merged[0]).toEqual(abandoned);
    expect(
      merged.some(
        (entry) => entry.step === `step-${MAX_SUSPENSION_DEADLINES_PER_RUN}`,
      ),
    ).toBe(false);
    expect(nextSuspensionDeadlineAt(storedRecord(merged))).toBe(
      merged[1]?.deadlineAt,
    );
  });
});
