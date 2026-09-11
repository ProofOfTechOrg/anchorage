// SPDX-License-Identifier: Apache-2.0

import type { CleanupTerminalReceipt } from '@proofoftech/fleet-control';

export function directCleanupReceiptPreimage(
  receipt: CleanupTerminalReceipt,
): readonly unknown[];

export function directCleanupReceiptDigest(
  receipt: CleanupTerminalReceipt,
): string;
