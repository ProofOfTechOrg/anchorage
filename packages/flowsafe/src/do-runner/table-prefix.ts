// SPDX-License-Identifier: Apache-2.0

const TABLE_PREFIX_PATTERN = /^(?:[A-Za-z_][A-Za-z0-9_]*)?$/;
const MASTRA_IDENTIFIER_MAX_LENGTH = 63;
const LONGEST_PREFIXED_MASTRA_TABLE = 'mastra_workflow_snapshot';
// The prefix plus Mastra's longest relevant table name must fit its identifier limit.
const MAX_TABLE_PREFIX_LENGTH =
  MASTRA_IDENTIFIER_MAX_LENGTH - LONGEST_PREFIXED_MASTRA_TABLE.length;

/** Validate the table-prefix contract shared with @mastra/cloudflare-d1. */
export function validateTablePrefix(
  value: string | undefined,
  fieldName = 'tablePrefix',
): string | undefined {
  if (value !== undefined && !TABLE_PREFIX_PATTERN.test(value)) {
    throw new Error(
      `Invalid ${fieldName}: use an empty prefix or start with a letter or underscore and continue with letters, numbers, or underscores.`,
    );
  }
  if (value !== undefined && value.length > MAX_TABLE_PREFIX_LENGTH) {
    throw new Error(
      `Invalid ${fieldName}: must be at most ${MAX_TABLE_PREFIX_LENGTH} characters so prefixed Mastra table names stay within the ${MASTRA_IDENTIFIER_MAX_LENGTH}-character identifier limit.`,
    );
  }
  return value;
}
