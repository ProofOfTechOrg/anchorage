// SPDX-License-Identifier: Apache-2.0

/**
 * Nesting allowed in tenant-supplied JSON a run stores. It stays far below the
 * depth SQLite's JSON functions parse, because the snapshot nests each value
 * inside its own structure, nested workflows' results included.
 */
export const MAX_RUN_INPUT_DEPTH = 256;

/** The refusal message for a field that nests past MAX_RUN_INPUT_DEPTH. */
export function runInputDepthMessage(field: string): string {
  return `${field} is nested more than ${MAX_RUN_INPUT_DEPTH} levels deep`;
}

/**
 * Does `value` nest arrays and objects more than MAX_RUN_INPUT_DEPTH levels? A
 * cycle does. A typed array counts as one level and is not entered, because
 * JSON renders it as one flat object.
 */
export function exceedsRunInputDepth(value: unknown): boolean {
  return exceedsLevels(value, MAX_RUN_INPUT_DEPTH);
}

function exceedsLevels(value: unknown, levels: number): boolean {
  if (value === null || typeof value !== 'object') return false;
  if (levels === 0) return true;
  if (ArrayBuffer.isView(value)) return false;
  for (const child of Object.values(value))
    if (exceedsLevels(child, levels - 1)) return true;
  return false;
}
