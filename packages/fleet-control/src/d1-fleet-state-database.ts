// SPDX-License-Identifier: Apache-2.0

import type {
  D1Database,
  D1PreparedStatement,
} from '@cloudflare/workers-types';
import type { FleetStateDatabase } from './state-store.js';

type Row = Readonly<Record<string, unknown>>;

function isRecord(value: unknown): value is Row {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isUnknownArray(value: unknown): value is readonly unknown[] {
  return Array.isArray(value);
}

function validateAck(
  value: unknown,
  message: string,
): asserts value is Row & Readonly<{ success: true; meta: Row }> {
  // Extra envelope and meta fields stay accepted as D1 adds fields; a defined
  // `error` beside `success: true` is contradictory and refused.
  if (
    !isRecord(value) ||
    value.success !== true ||
    value.error !== undefined ||
    !isRecord(value.meta)
  ) {
    throw new Error(message);
  }
}

function validateEnvelope(
  value: unknown,
  message: string,
): asserts value is Readonly<{
  success: true;
  meta: Row;
  results: readonly Row[];
}> {
  validateAck(value, message);
  if (!isUnknownArray(value.results) || !value.results.every(isRecord)) {
    throw new Error(message);
  }
}

/**
 * Adapts a Workers `D1Database` binding to the state store's database port.
 * It validates acknowledgement and envelope shapes and leaves binding errors
 * unchanged, preserving state-store duplicate-column cause traversal and
 * migration-ledger causes.
 * The same instance also satisfies `MigrationDatabase`: that port's batch
 * result is `unknown`, and its `readonly string[]` bindings fit these
 * `readonly unknown[]` parameters.
 */
export class D1FleetStateDatabase implements FleetStateDatabase {
  readonly #binding: D1Database;

  constructor(binding: D1Database) {
    if (
      typeof binding?.prepare !== 'function' ||
      typeof binding.batch !== 'function'
    ) {
      throw new Error(
        'D1FleetStateDatabase requires the Workers D1Database prepare/batch interface',
      );
    }
    this.#binding = binding;
  }

  async query(
    sql: string,
    bindings: readonly unknown[] = [],
  ): Promise<readonly Row[]> {
    const envelope: unknown = await this.#statement(sql, bindings).all();
    validateEnvelope(envelope, 'D1 query returned a malformed result');
    return envelope.results;
  }

  /**
   * Validates the acknowledgement because the shim does not backfill
   * `results` for `run()`, and this method does not read rows.
   */
  async execute(sql: string, bindings: readonly unknown[] = []): Promise<void> {
    const envelope: unknown = await this.#statement(sql, bindings).run();
    validateAck(envelope, 'D1 execute returned a malformed result');
  }

  async batch(
    statements: readonly Readonly<{
      sql: string;
      bindings?: readonly unknown[];
    }>[],
  ): Promise<readonly (readonly Row[])[]> {
    // The port's result is one entry per statement, so an empty list has no
    // statement to send.
    if (statements.length === 0) return [];
    const envelopes: unknown = await this.#binding.batch(
      statements.map(({ sql, bindings = [] }) =>
        this.#statement(sql, bindings),
      ),
    );
    if (!isUnknownArray(envelopes)) {
      throw new Error('D1 returned a malformed batch response');
    }
    if (envelopes.length !== statements.length) {
      throw new Error(
        `D1 returned ${envelopes.length} batch results for ${statements.length} statements`,
      );
    }
    const results: (readonly Row[])[] = [];
    for (const [index, envelope] of envelopes.entries()) {
      validateEnvelope(
        envelope,
        `D1 batch statement ${index} returned a malformed result`,
      );
      results.push(envelope.results);
    }
    return results;
  }

  #statement(sql: string, bindings: readonly unknown[]): D1PreparedStatement {
    const prepared = this.#binding.prepare(sql);
    return bindings.length > 0 ? prepared.bind(...bindings) : prepared;
  }
}
