// SPDX-License-Identifier: Apache-2.0

import type { DirectRunManifest } from '../../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  createDirectReferenceContext,
  type DirectReferenceContext,
  type DirectReferenceEnvironment,
} from '../../scripts/direct-reference-context.js';
import { recoverDirectForceResidual } from '../../scripts/direct-reference-force.js';
import { DirectReferenceExecutionError } from '../../scripts/direct-reference-http.js';

export type ForceBudgetStage = 'fence' | 'empty' | 'bucket';

export async function directForceBudgetProbe(
  manifest: DirectRunManifest,
  environment: DirectReferenceEnvironment,
  stage: ForceBudgetStage,
  fetchRequest: typeof fetch,
  signal: AbortSignal,
) {
  const fillerUrl = 'https://force-budget.fixture.test/fill';
  let nativeFillers = 0;
  const context = await createDirectReferenceContext(manifest, environment, {
    startedAt: performance.now(),
    signal,
    fetch: async (input, init) => {
      const request = new Request(input, init);
      if (request.url === fillerUrl) {
        nativeFillers += 1;
        return new Response(null, { status: 204 });
      }
      return fetchRequest(request);
    },
  });
  const sentinel = new Error(`force-budget-${stage}-sentinel`);
  const plane = context.createForcePlane();
  const restore: Array<() => void> = [];
  let selected = false;
  let expectedFillers = 0;
  let overflowRefused = false;

  async function exhaust(): Promise<never> {
    selected = true;
    const before = context.transport.snapshot();
    const used =
      before.providerAttempts +
      before.maintenanceAttempts +
      before.applicationAttempts;
    expectedFillers = manifest.referenceRuntime.maxProviderRequests - used;
    if (before.failure !== null || expectedFillers < 0) {
      throw new Error('force probe has no remaining allowance');
    }
    for (let count = 0; count < expectedFillers; count++) {
      await context.transport.providerFetch(fillerUrl);
    }
    try {
      await context.transport.providerFetch(fillerUrl);
    } catch (error) {
      if (
        !(error instanceof DirectReferenceExecutionError) ||
        error.code !== 'budget-exhausted'
      )
        throw error;
      overflowRefused = true;
    }
    if (!overflowRefused || nativeFillers !== expectedFillers) {
      throw new Error('force probe overflow reached native fetch');
    }
    throw sentinel;
  }

  let claimsOverride = context.recoveryResidualClaimsPresent.bind(context);
  const wrapped: DirectReferenceContext = {
    ...context,
    createForcePlane: () => plane,
    recoveryResidualClaimsPresent: (...args) => claimsOverride(...args),
  };

  try {
    if (stage === 'fence') {
      const original = plane.store.get;
      const get = original.bind(plane.store);
      let peer: ReturnType<typeof get> | undefined;
      plane.store.get = (...args) => {
        const reading = get(...args);
        peer = reading;
        return reading;
      };
      restore.push(() => {
        plane.store.get = original;
      });
      const claims = context.recoveryResidualClaimsPresent.bind(context);
      claimsOverride = async (...args) => {
        const preceding = peer;
        if (!preceding) throw new Error('force fence peer was not started');
        await Promise.all([preceding, claims(...args)]);
        return exhaust();
      };
    } else if (stage === 'empty') {
      const retained = await context.journal.readForceAfter();
      if (!retained) throw new Error('force probe needs a retained footprint');
      const footprint = JSON.parse(retained.identityJson) as {
        buckets: Array<{ observedCreationDate: string | null }>;
      };
      const participants = footprint.buckets.filter(
        (bucket) => bucket.observedCreationDate !== null,
      ).length;
      if (participants === 0 || participants !== footprint.buckets.length) {
        throw new Error('force probe requires retained application buckets');
      }
      const original = plane.backend.assertApplicationR2Empty;
      const empty = original.bind(plane.backend);
      const peers: Array<ReturnType<typeof empty>> = [];
      let scheduled!: () => void;
      const group = new Promise<void>((resolve) => {
        scheduled = resolve;
      });
      plane.backend.assertApplicationR2Empty = async (...args) => {
        const chosen = peers.length === 0;
        const reading = empty(...args);
        peers.push(reading);
        if (peers.length === participants) scheduled();
        await reading;
        if (!chosen) return;
        await group;
        await Promise.all(peers);
        return exhaust();
      };
      restore.push(() => {
        plane.backend.assertApplicationR2Empty = original;
      });
    } else if (stage === 'bucket') {
      const originalBucket = plane.client.getR2Bucket;
      const originalDatabase = plane.client.getDatabase;
      const originalAttachments = plane.client.listWorkerDatabaseAttachments;
      const originalDetached = plane.backend.assertApplicationR2Detached;
      const bucket = originalBucket.bind(plane.client);
      const database = originalDatabase.bind(plane.client);
      const attachments = originalAttachments.bind(plane.client);
      const detached = originalDetached.bind(plane.backend);
      let bucketRead: ReturnType<typeof bucket> | undefined;
      let databaseRead: ReturnType<typeof database> | undefined;
      let attachmentsRead: ReturnType<typeof attachments> | undefined;
      plane.client.getR2Bucket = (...args) => {
        const reading = bucket(...args);
        bucketRead = reading;
        return reading;
      };
      plane.client.getDatabase = (...args) => {
        const reading = database(...args);
        databaseRead = reading;
        return reading;
      };
      plane.client.listWorkerDatabaseAttachments = (...args) => {
        const reading = attachments(...args);
        attachmentsRead = reading;
        return reading;
      };
      plane.backend.assertApplicationR2Detached = async (...args) => {
        const peers = [bucketRead, databaseRead, attachmentsRead];
        if (peers.some((peer) => peer === undefined))
          throw new Error('force bucket peers were not started');
        await Promise.all([...peers, detached(...args)]);
        return exhaust();
      };
      restore.push(() => {
        plane.client.getR2Bucket = originalBucket;
        plane.client.getDatabase = originalDatabase;
        plane.client.listWorkerDatabaseAttachments = originalAttachments;
        plane.backend.assertApplicationR2Detached = originalDetached;
      });
    } else {
      throw new Error('unknown force budget stage');
    }

    try {
      await recoverDirectForceResidual(wrapped);
      throw new Error('force probe did not refuse');
    } catch (error) {
      return {
        stage,
        selected,
        typed: error instanceof DirectReferenceExecutionError,
        code:
          error instanceof DirectReferenceExecutionError ? error.code : null,
        rawSentinel: error === sentinel,
        nativeFillers,
        expectedFillers,
        overflowRefused,
        snapshot: context.transport.snapshot(),
      };
    }
  } finally {
    for (const undo of restore.reverse()) undo();
  }
}
