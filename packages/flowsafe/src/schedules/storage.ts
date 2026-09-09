// SPDX-License-Identifier: Apache-2.0

import type { MastraStorageDomains } from '@mastra/core/storage';

import type { D1DatabaseBinding } from '../do-runner/index.js';
import { validateTablePrefix } from '../do-runner/table-prefix.js';
import { D1SchedulesStorage, type ScheduleDatabase } from './schedules-d1.js';

export function createScheduleStorageDomains(
  binding: D1DatabaseBinding,
  tablePrefix = '',
): MastraStorageDomains {
  const prefix = validateTablePrefix(tablePrefix) ?? '';
  const db = binding as unknown as ScheduleDatabase;
  return { schedules: new D1SchedulesStorage(db, prefix) };
}
