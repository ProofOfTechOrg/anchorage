// SPDX-License-Identifier: Apache-2.0
// `SignalDatabase`, `SnapshotDatabase` and `InitialAdmissionDatabase` are
// structural subsets of D1 held in method syntax, so a host passes its
// `env.DB` binding straight through with no adapter. These erased assertions
// hold that open against a real D1Database, on the public type each consumer
// names. `ScheduleDatabase` is an alias of `SignalDatabase` (schedules-d1.ts),
// so the signal pin covers it. Same technique as the R2 seam beside this file
// and the runtime pins in src/do-runner/cf-types.ts.
import type { D1Database } from '@cloudflare/workers-types';

import type {
  InitialAdmissionDatabase,
  SnapshotDatabase,
  SnapshotStatement,
} from '../src/do-runner/index.js';
import type { SignalDatabase, SignalStatement } from '../src/signals/index.js';

type AssertTrue<T extends true> = T;
type _D1SatisfiesSignalDatabase = AssertTrue<
  D1Database extends SignalDatabase ? true : false
>;
type _D1SatisfiesSnapshotDatabase = AssertTrue<
  D1Database extends SnapshotDatabase ? true : false
>;
type _D1SatisfiesInitialAdmissionDatabase = AssertTrue<
  D1Database extends InitialAdmissionDatabase ? true : false
>;

// The assertions above hold one direction of the seam: a real D1Database
// satisfies it. These hold the other direction. A batch element carries
// `results` — the requirement the `batch` docstrings in signals/d1-shared.ts
// and do-runner/workflow-snapshot-row.ts place on a hand-written adapter — and
// an element widened back to `unknown` admits an adapter that resolves write
// metadata alone, while the assertions above still pass.
type AssertFalse<T extends false> = T;
/** D1 write metadata alone, without the rows a `D1Result` carries. */
type MetaOnlyBatchResult = { meta?: { changes?: number } };
interface MetaOnlySignalAdapter {
  prepare(query: string): SignalStatement;
  batch(statements: SignalStatement[]): Promise<MetaOnlyBatchResult[]>;
}
interface MetaOnlySnapshotAdapter {
  prepare(query: string): SnapshotStatement;
  batch(statements: SnapshotStatement[]): Promise<MetaOnlyBatchResult[]>;
}
type _MetaOnlyAdapterFailsSignalDatabase = AssertFalse<
  MetaOnlySignalAdapter extends SignalDatabase ? true : false
>;
type _MetaOnlyAdapterFailsSnapshotDatabase = AssertFalse<
  MetaOnlySnapshotAdapter extends SnapshotDatabase ? true : false
>;
type _MetaOnlyAdapterFailsInitialAdmissionDatabase = AssertFalse<
  MetaOnlySnapshotAdapter extends InitialAdmissionDatabase ? true : false
>;
