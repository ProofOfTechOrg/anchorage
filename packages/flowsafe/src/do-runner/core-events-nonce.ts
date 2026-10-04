// SPDX-License-Identifier: Apache-2.0
// Core generates its process nonce at module scope, where workerd refuses
// random values. Core keeps a value already stored under its symbol, and the
// fallback matters only to its Unix socket bus, which cannot run in workerd.

const processNonceKey = Symbol.for(
  '@mastra/core/unix-socket-pubsub/process-nonce',
);
const processGlobals = globalThis as typeof globalThis &
  Record<symbol, unknown> & { crypto: { randomUUID(): string } };

if (processGlobals[processNonceKey] == null) {
  try {
    processGlobals[processNonceKey] = processGlobals.crypto.randomUUID();
  } catch {
    processGlobals[processNonceKey] = 'flowsafe-workerd';
  }
}

export {};
