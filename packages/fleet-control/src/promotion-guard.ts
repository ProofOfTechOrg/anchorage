// SPDX-License-Identifier: Apache-2.0

import type { FleetRecord, PromotionGuard } from './types.js';

export function buildPromotionGuard(
  record: FleetRecord,
  targetScriptName: string,
  allowUnrouted = false,
): PromotionGuard {
  const allowedCurrentScriptNames = [
    record.activeRelease?.physicalScriptName,
    record.pendingRelease?.physicalScriptName,
    targetScriptName,
  ].filter(
    (scriptName, index, scriptNames): scriptName is string =>
      scriptName !== undefined && scriptNames.indexOf(scriptName) === index,
  );
  return { allowedCurrentScriptNames, allowUnrouted };
}
