// SPDX-License-Identifier: Apache-2.0

import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';

export function directDeploymentModules(manifest, role, release) {
  if (
    !['a', 'b', 'recovery'].includes(role) ||
    !['initial', 'next', 'failed-recovery'].includes(release) ||
    (release === 'failed-recovery' && role !== 'recovery')
  )
    throw new Error('invalid direct fixture selection');
  const artifact = manifest.tenantModule;
  if (
    new TextEncoder().encode(artifact.source).byteLength !==
      artifact.byteLength ||
    createHash('sha256').update(artifact.source).digest('hex') !==
      artifact.sha256
  )
    throw new Error('invalid direct fixture artifact');
  return [
    {
      name: artifact.name,
      content: artifact.source,
      contentType: artifact.contentType,
    },
    ...manifest.tenantWasm.map((module) => {
      const content = new Uint8Array(Buffer.from(module.base64, 'base64'));
      if (
        content.byteLength !== module.byteLength ||
        createHash('sha256').update(content).digest('hex') !== module.sha256
      )
        throw new Error('invalid direct fixture Wasm artifact');
      return { name: module.name, content, contentType: module.contentType };
    }),
  ];
}
