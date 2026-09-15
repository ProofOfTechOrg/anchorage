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
} from '../src/do-runner/index.js';
import type { SignalDatabase } from '../src/signals/index.js';

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
