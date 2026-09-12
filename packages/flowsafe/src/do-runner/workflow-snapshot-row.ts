// SPDX-License-Identifier: Apache-2.0

import { missingTableReadsEmpty } from './cause-chain.js';
import { ExecutionFenceUnreadableError } from './execution-admission.js';
import { isPathSafeId } from './path-safe-id.js';
import { validateTablePrefix } from './table-prefix.js';

/** Structural D1 surface; a transactional batch is required for admission. */
export interface SnapshotDatabase {
  prepare(query: string): SnapshotStatement;
  /**
   * Initial admission (`captureBatchResults`, fenced-workflows-d1.ts) reads
   * `results` off every element; retention (`retentionBatchResult`,
   * d1-storage.ts) reads `results` off the first element and requires
   * `meta.changes` as a safe non-negative integer off the rest. So a
   * hand-written adapter returning only `meta`, or omitting `meta` after the
   * first element, fails at runtime.
   */
  batch?(
    statements: SnapshotStatement[],
  ): Promise<Array<{ results: unknown[]; meta?: { changes?: number } }>>;
}

export interface SnapshotStatement {
  bind(...values: unknown[]): SnapshotStatement;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

export interface D1RunAddress {
  readonly tablePrefix: string;
  readonly workflowId: string;
  readonly runId: string;
}

export interface RawWorkflowSnapshot extends D1RunAddress {
  readonly resourceId: string | null;
  readonly snapshot: string;
  readonly createdAt: string;
  readonly updatedAt: string;
}

/** @internal Strict row envelopes, independent of SELECT/DML metadata. */
export function snapshotResultRows(result: unknown): Record<string, unknown>[] {
  if (
    result === null ||
    typeof result !== 'object' ||
    ('success' in result && result.success !== true) ||
    !('results' in result)
  )
    throw new Error('workflow snapshot result is malformed');
  const rows: unknown = result.results;
  if (!Array.isArray(rows))
    throw new Error('workflow snapshot result is malformed');
  const length = rows.length;
  if (!Number.isSafeInteger(length) || length < 0)
    throw new Error('workflow snapshot result is malformed');
  return Array.from({ length }, (_, index) => {
    if (!Object.hasOwn(rows, index))
      throw new Error('workflow snapshot result row is malformed');
    const row = rows[index];
    if (row === null || typeof row !== 'object' || Array.isArray(row))
      throw new Error('workflow snapshot result row is malformed');
    return row;
  });
}

/** @internal Prepare the same exact read used by consistent readback batches. */
export function prepareRawWorkflowSnapshotRead(
  db: Pick<SnapshotDatabase, 'prepare'>,
  input: D1RunAddress,
): { statement: SnapshotStatement; address: D1RunAddress } {
  const { tablePrefix, workflowId, runId } = input;
  if (
    typeof tablePrefix !== 'string' ||
    !isPathSafeId(workflowId) ||
    !isPathSafeId(runId)
  )
    throw new Error('workflow snapshot address is malformed');
  const prefix = validateTablePrefix(tablePrefix)?.toLowerCase() ?? '';
  const address = Object.freeze({ tablePrefix: prefix, workflowId, runId });
  const statement = db
    .prepare(`SELECT workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt
    FROM "${prefix}mastra_workflow_snapshot"
    WHERE workflow_name = ? AND run_id = ? LIMIT 2`)
    .bind(workflowId, runId);
  return { statement, address };
}

/** @internal Decode exact stored bytes without JSON or timestamp coercion. */
export function decodeRawWorkflowSnapshotResult(
  result: unknown,
  address: D1RunAddress,
): RawWorkflowSnapshot | undefined {
  const rows = snapshotResultRows(result);
  if (rows.length > 1) throw new Error('workflow snapshot is not a singleton');
  const row = rows[0];
  if (row === undefined) return undefined;
  for (const key of [
    'workflow_name',
    'run_id',
    'resourceId',
    'snapshot',
    'createdAt',
    'updatedAt',
  ]) {
    if (!Object.hasOwn(row, key))
      throw new Error('workflow snapshot field is missing');
  }
  const { workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt } =
    row;
  if (
    workflow_name !== address.workflowId ||
    run_id !== address.runId ||
    (resourceId !== null && typeof resourceId !== 'string') ||
    typeof snapshot !== 'string' ||
    typeof createdAt !== 'string' ||
    typeof updatedAt !== 'string'
  )
    throw new Error('workflow snapshot fields are malformed');
  return Object.freeze({
    ...address,
    resourceId,
    snapshot,
    createdAt,
    updatedAt,
  });
}

export async function readRawWorkflowSnapshot(
  db: Pick<SnapshotDatabase, 'prepare'>,
  input: D1RunAddress,
  options: { readonly missingTable: 'empty' | 'error' },
): Promise<RawWorkflowSnapshot | undefined> {
  let table: string | undefined;
  const mode = options.missingTable;
  try {
    if (mode !== 'empty' && mode !== 'error')
      throw new Error('missing-table mode is required');
    const { statement, address } = prepareRawWorkflowSnapshotRead(db, input);
    table = `${address.tablePrefix}mastra_workflow_snapshot`;
    return decodeRawWorkflowSnapshotResult(await statement.all(), address);
  } catch (error) {
    if (
      mode === 'empty' &&
      table !== undefined &&
      missingTableReadsEmpty(error, table)
    )
      return undefined;
    throw new ExecutionFenceUnreadableError(
      'workflow snapshot is not readable',
      { cause: error },
    );
  }
}
