// SPDX-License-Identifier: Apache-2.0
// Track C (M-004) — the D1 seam the flowsafe-owned signal domains
// (D1NotificationsStorage, D1ThreadStateStorage) share. @mastra/cloudflare-d1
// 1.1.1 ships NO notifications/thread-state domain (unlike backgroundTasks), so
// these two are hand-written to mirror core's abstract domain + InMemory
// reference — the project's "custom state store" caveat does not bite here
// because Mastra's own D1 adapter simply does not own these tables.
//
// The structural D1 subset (method-syntax, so a real D1Database and the
// node:sqlite `d1DatabaseLike` test adapter both satisfy it) keeps this module
// free of @cloudflare/workers-types — the same convention the do-runner's
// SnapshotDatabase and the approval store's ApprovalDatabase take.

/** The prepared-statement subset both signal domains use. */
export interface SignalStatement {
  bind(...values: unknown[]): SignalStatement;
  first<T = unknown>(): Promise<T | null>;
  all<T = unknown>(): Promise<{ results: T[] }>;
  run(): Promise<{ meta?: { changes?: number } }>;
}

/** The D1 database subset both signal domains use. */
export interface SignalDatabase {
  prepare(query: string): SignalStatement;
}

/** Rows affected by a D1 write, read from its `{ meta: { changes } }` envelope. */
export function d1Changes(result: { meta?: { changes?: number } }): number {
  const changes = result?.meta?.changes;
  return typeof changes === 'number' ? changes : 0;
}

/**
 * A Date → ISO-8601 TEXT column value, or null. ISO text so a lexicographic `<`
 * against a cutoff is a correct timestamp comparison — the same encoding the
 * snapshot/memory tables use and the retention purges (and the schema guard)
 * ride on.
 */
export function isoOrNull(value: Date | undefined): string | null {
  return value === undefined ? null : value.toISOString();
}

/** An ISO-8601 TEXT column value back to a Date, or undefined. */
export function dateOrUndefined(value: unknown): Date | undefined {
  return typeof value === 'string' ? new Date(value) : undefined;
}

/** A JSON column value, or null for undefined. */
export function jsonOrNull(value: unknown): string | null {
  return value === undefined ? null : JSON.stringify(value);
}

/** A JSON column value back to its parsed shape, or undefined. */
export function parseJsonOrUndefined<T = unknown>(
  value: unknown,
): T | undefined {
  if (typeof value !== 'string') return undefined;
  try {
    return JSON.parse(value) as T;
  } catch {
    return undefined;
  }
}
