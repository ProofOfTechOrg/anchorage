// SPDX-License-Identifier: Apache-2.0

const TABLE_PREFIX_PATTERN = /^(?:[A-Za-z_][A-Za-z0-9_]*)?$/;

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
  return value;
}
