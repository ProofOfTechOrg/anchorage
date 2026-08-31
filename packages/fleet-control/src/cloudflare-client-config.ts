// SPDX-License-Identifier: Apache-2.0

export const CLOUDFLARE_SDK_MAX_RETRIES = 2;
export const CLOUDFLARE_SDK_MAX_ATTEMPTS = CLOUDFLARE_SDK_MAX_RETRIES + 1;
export const CLOUDFLARE_INVENTORY_BOUND = 10_000;

/**
 * Fixed refusal shared by every bounded provider inventory traversal. It lives
 * here because the client, the attachment scanner, and the bounded inventory
 * engine all enforce the same bound and must refuse with the same bytes.
 */
export function inventoryBoundExceeded(label: string, max: number): Error {
  return new Error(
    `${label} exceeded the supported inventory bound of ${max} items`,
  );
}
