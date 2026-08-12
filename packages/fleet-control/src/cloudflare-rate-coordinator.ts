// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from 'node:crypto';
import PQueue from 'p-queue';
import type { FleetStateDatabase } from './state-store.js';

const RATE_RESERVATION_TABLE = 'anchorage_cloudflare_api_rate_reservations';
const RATE_RESERVATION_INDEX =
  'idx_anchorage_cloudflare_api_rate_reservations_scope_time';
const CLOUDFLARE_INTERVAL_MS = 5 * 60_000;
const CLOUDFLARE_INTERVAL_CAP = 1_100;
const DB_NOW_MS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

export interface CloudflareApiRateCoordinator {
  acquire(signal?: AbortSignal): Promise<void>;
}

export interface D1CloudflareApiRateCoordinatorOptions {
  /**
   * Opaque, nonsecret identity for the Cloudflare user or account-token quota.
   * Every replica and account sharing that provider quota must use the same
   * value. Never use or derive this value from the API token.
   */
  readonly quotaScope: string;
}

function validateQuotaScope(value: string): string {
  const hasControlCharacter = [...value].some((character) => {
    const codePoint = character.codePointAt(0);
    return codePoint !== undefined && (codePoint <= 0x1f || codePoint === 0x7f);
  });
  if (
    value.length === 0 ||
    value !== value.trim() ||
    new TextEncoder().encode(value).byteLength > 256 ||
    hasControlCharacter
  ) {
    throw new Error(
      'quotaScope must be a non-empty, trimmed, nonsecret identifier of at most 256 UTF-8 bytes',
    );
  }
  return value;
}

function positiveInteger(
  row: Readonly<Record<string, unknown>> | undefined,
  key: string,
): number {
  const value = Number(row?.[key]);
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`Cloudflare API quota coordinator returned invalid ${key}`);
  }
  return value;
}

function wait(delayMs: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(finish, delayMs);
    function finish(): void {
      signal?.removeEventListener('abort', abort);
      resolve();
    }
    function abort(): void {
      clearTimeout(timeout);
      reject(signal?.reason ?? new DOMException('Aborted', 'AbortError'));
    }
    signal?.addEventListener('abort', abort, { once: true });
  });
}

/**
 * Coordinates Anchorage-originated Cloudflare API traffic across every replica
 * sharing a fleet-state D1 database and quota scope.
 */
export class D1CloudflareApiRateCoordinator
  implements CloudflareApiRateCoordinator
{
  readonly #db: FleetStateDatabase;
  readonly #quotaScope: string;
  #schemaReady: Promise<void> | undefined;

  constructor(
    db: FleetStateDatabase,
    options: D1CloudflareApiRateCoordinatorOptions,
  ) {
    this.#db = db;
    this.#quotaScope = validateQuotaScope(options.quotaScope);
  }

  async acquire(signal?: AbortSignal): Promise<void> {
    await this.#ensureSchema();
    for (;;) {
      signal?.throwIfAborted();
      const reservationId = randomUUID();
      const results = await this.#db.batch([
        {
          sql: `DELETE FROM ${RATE_RESERVATION_TABLE}
            WHERE quota_scope = ?
              AND reserved_at <= ${DB_NOW_MS} - ?`,
          bindings: [this.#quotaScope, CLOUDFLARE_INTERVAL_MS],
        },
        {
          sql: `INSERT INTO ${RATE_RESERVATION_TABLE} (
              quota_scope, reservation_id, reserved_at
            )
            SELECT ?, ?, ${DB_NOW_MS}
            WHERE (SELECT COUNT(*) FROM ${RATE_RESERVATION_TABLE}
              WHERE quota_scope = ?) < ?
            RETURNING reservation_id, reserved_at`,
          bindings: [
            this.#quotaScope,
            reservationId,
            this.#quotaScope,
            CLOUDFLARE_INTERVAL_CAP,
          ],
        },
        {
          sql: `SELECT MAX(1, MIN(reserved_at) + ? - ${DB_NOW_MS})
              AS retry_after_ms
            FROM ${RATE_RESERVATION_TABLE}
            WHERE quota_scope = ?`,
          bindings: [CLOUDFLARE_INTERVAL_MS, this.#quotaScope],
        },
      ]);
      if (results[1]?.[0]?.reservation_id === reservationId) return;
      await wait(positiveInteger(results[2]?.[0], 'retry_after_ms'), signal);
    }
  }

  async #ensureSchema(): Promise<void> {
    const pending = this.#schemaReady ?? this.#createSchema();
    this.#schemaReady = pending;
    try {
      await pending;
    } catch (error) {
      if (this.#schemaReady === pending) this.#schemaReady = undefined;
      throw error;
    }
  }

  async #createSchema(): Promise<void> {
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${RATE_RESERVATION_TABLE} (
      quota_scope TEXT NOT NULL,
      reservation_id TEXT NOT NULL,
      reserved_at INTEGER NOT NULL,
      PRIMARY KEY (quota_scope, reservation_id)
    )`);
    await this.#db.execute(
      `CREATE INDEX IF NOT EXISTS ${RATE_RESERVATION_INDEX}
        ON ${RATE_RESERVATION_TABLE} (quota_scope, reserved_at)`,
    );
  }
}

/**
 * One-process helper for tests and credentialed conformance only. It does not
 * coordinate multiple processes or replicas.
 */
export class ProcessLocalCloudflareApiRateCoordinator
  implements CloudflareApiRateCoordinator
{
  readonly #queue: PQueue;

  constructor(intervalCap = CLOUDFLARE_INTERVAL_CAP) {
    if (
      !Number.isSafeInteger(intervalCap) ||
      intervalCap < 1 ||
      intervalCap > CLOUDFLARE_INTERVAL_CAP
    ) {
      throw new Error('intervalCap must be an integer from 1 through 1100');
    }
    this.#queue = new PQueue({
      concurrency: intervalCap,
      interval: CLOUDFLARE_INTERVAL_MS,
      intervalCap,
      strict: true,
    });
  }

  async acquire(signal?: AbortSignal): Promise<void> {
    await this.#queue.add(async () => {}, { signal });
  }
}
