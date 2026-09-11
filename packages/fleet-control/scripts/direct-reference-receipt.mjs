// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';

export function directCleanupReceiptPreimage(receipt) {
  const evidence = receipt.evidence;
  return [
    receipt.version,
    receipt.operationId,
    receipt.tenantTag,
    receipt.environment,
    receipt.backend,
    receipt.scriptName,
    receipt.databaseId,
    receipt.databaseName,
    receipt.authority,
    receipt.admittedPhase,
    receipt.disposition,
    evidence.eligibility,
    evidence.ingressRemoved,
    evidence.workerAbsent,
    evidence.platformResourcesAbsent,
    evidence.applicationR2Settled,
    evidence.databaseAbsentReadback,
    evidence.scan
      ? [
          evidence.scan.discover.evidenceSha256,
          evidence.scan.discover.evidenceCount,
          evidence.scan.verify.evidenceSha256,
          evidence.scan.verify.evidenceCount,
        ]
      : null,
    receipt.completedAtMs,
  ];
}

export function directCleanupReceiptDigest(receipt) {
  return createHash('sha256')
    .update(JSON.stringify(directCleanupReceiptPreimage(receipt)))
    .digest('hex');
}
