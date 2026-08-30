// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/workers-types" />

import { R2DatabaseExportStore } from '../../src/r2-export-store.js';

interface Env {
  readonly EXPORTS: R2Bucket;
}

const RECEIPT_DATABASE_ID = '11111111-1111-1111-1111-111111111111';
const RECEIPT_OPERATION_ID = '22222222-2222-4222-8222-222222222222';

function sequence(length: number, seed: number): Uint8Array {
  return Uint8Array.from({ length }, (_, index) => (index * 31 + seed) % 256);
}

function bodyFrom(value: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(value);
      controller.close();
    },
  });
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function toHex(value: ArrayBuffer): string {
  return Array.from(new Uint8Array(value), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

function keyFromLocation(location: string): string {
  const prefix = 'r2://exports/';
  if (!location.startsWith(prefix)) throw new Error('unexpected location');
  return location.slice(prefix.length);
}

function messageOf(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

function store(env: Env, uuid?: string): R2DatabaseExportStore {
  return new R2DatabaseExportStore({
    bucket: env.EXPORTS,
    bucketName: 'exports',
    keyPrefix: 'exports/',
    streams: {
      DigestStream: crypto.DigestStream,
      FixedLengthStream,
    },
    randomUUID: uuid === undefined ? () => crypto.randomUUID() : () => uuid,
  });
}

async function integrityOf(value: Uint8Array) {
  const digest = new crypto.DigestStream('SHA-256');
  await bodyFrom(value).pipeTo(digest);
  return { size: value.byteLength, sha256: toHex(await digest.digest) };
}

function receiptIdentity(exportStore: R2DatabaseExportStore) {
  return {
    version: 1 as const,
    authority: exportStore.receiptAuthority,
    databaseId: RECEIPT_DATABASE_ID,
    operationId: RECEIPT_OPERATION_ID,
  };
}

function receiptKey(operationId = RECEIPT_OPERATION_ID): string {
  return `exports/receipts/v1/${RECEIPT_DATABASE_ID}/${operationId}.sql`;
}

async function success(env: Env) {
  const source = sequence(1_048_576, 17);
  const result = await store(env).write({
    databaseId: 'success-db',
    fileName: 'export.sqlite3',
    body: bodyFrom(source),
    contentLength: source.byteLength,
  });
  const key = keyFromLocation(result.location);
  const byteObject = await env.EXPORTS.get(key);
  if (byteObject === null) throw new Error('success object missing');
  const readback = await byteObject.bytes();
  const digestObject = await env.EXPORTS.get(key);
  if (digestObject === null) throw new Error('digest object missing');
  const digest = new crypto.DigestStream('SHA-256');
  await digestObject.body.pipeTo(digest);
  const readbackSha256 = toHex(await digest.digest);
  await env.EXPORTS.delete(key);
  return {
    location: result.location,
    size: result.size,
    sha256: result.sha256,
    bytesEqual: equalBytes(readback, source),
    objectSize: byteObject.size,
    readbackSha256,
    cleaned: (await env.EXPORTS.get(key)) === null,
  };
}

async function empty(env: Env) {
  const result = await Promise.allSettled([
    store(env).write({
      databaseId: 'empty-db',
      fileName: 'export.sqlite3',
      body: bodyFrom(new Uint8Array(0)),
      contentLength: 0,
    }),
  ]);
  const state = result[0];
  if (state.status === 'fulfilled') throw new Error('empty export succeeded');
  const listed = await env.EXPORTS.list({ prefix: 'exports/empty-db/' });
  return {
    message: messageOf(state.reason),
    objectCount: listed.objects.length,
  };
}

async function short(env: Env) {
  const result = await Promise.allSettled([
    store(env).write({
      databaseId: 'short-db',
      fileName: 'export.sqlite3',
      body: bodyFrom(sequence(2, 3)),
      contentLength: 4,
    }),
  ]);
  const state = result[0];
  if (state.status === 'fulfilled') throw new Error('short export succeeded');
  const listed = await env.EXPORTS.list({ prefix: 'exports/short-db/' });
  return {
    message: messageOf(state.reason),
    objectCount: listed.objects.length,
  };
}

async function midTransferError(env: Env) {
  const uuid = 'mid-transfer-uuid';
  const prefix = 'exports/mid-transfer-db/';
  let sent = false;
  let pullCount = 0;
  let bytesEnqueued = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      pullCount += 1;
      if (!sent) {
        sent = true;
        const chunk = sequence(4096, 41);
        bytesEnqueued += chunk.byteLength;
        controller.enqueue(chunk);
        return;
      }
      controller.error(new Error('mid-transfer source failure'));
    },
  });
  const result = await Promise.allSettled([
    store(env, uuid).write({
      databaseId: 'mid-transfer-db',
      fileName: 'export.sqlite3',
      body,
      contentLength: 8192,
    }),
  ]);
  const state = result[0];
  if (state.status === 'fulfilled') {
    throw new Error('mid-transfer export succeeded');
  }
  const listed = await env.EXPORTS.list({ prefix });
  return {
    message: messageOf(state.reason),
    objectCount: listed.objects.length,
    pullCount,
    bytesEnqueued,
  };
}

async function collision(env: Env) {
  const fixed = 'collision-uuid';
  const first = sequence(4096, 11);
  const second = sequence(4096, 29);
  const exportStore = store(env, fixed);
  const states = await Promise.allSettled([
    exportStore.write({
      databaseId: 'collision-db',
      fileName: 'export.sqlite3',
      body: bodyFrom(first),
      contentLength: first.byteLength,
    }),
    exportStore.write({
      databaseId: 'collision-db',
      fileName: 'export.sqlite3',
      body: bodyFrom(second),
      contentLength: second.byteLength,
    }),
  ]);
  let fulfilled = 0;
  let rejected = 0;
  let failureMessage: string | undefined;
  for (const state of states) {
    if (state.status === 'fulfilled') {
      fulfilled += 1;
    } else {
      rejected += 1;
      failureMessage = messageOf(state.reason);
    }
  }
  if (fulfilled !== 1 || rejected !== 1 || failureMessage === undefined) {
    throw new Error('collision did not produce one winner');
  }
  const key = `exports/collision-db/${fixed}-export.sqlite3`;
  const object = await env.EXPORTS.get(key);
  if (object === null) throw new Error('collision winner missing');
  const winnerBytes = await object.bytes();
  const winner = equalBytes(winnerBytes, first)
    ? 'first'
    : equalBytes(winnerBytes, second)
      ? 'second'
      : 'unknown';
  await env.EXPORTS.delete(key);
  return {
    fulfilled,
    rejected,
    message: failureMessage,
    winner,
    objectSurvived: winner !== 'unknown',
    cleaned: (await env.EXPORTS.get(key)) === null,
  };
}

async function receiptReplay(env: Env) {
  const source = sequence(16_384, 71);
  const exportStore = store(env);
  const identity = receiptIdentity(exportStore);
  const first = await exportStore.writeReceipt({
    identity,
    body: bodyFrom(source),
    contentLength: source.byteLength,
    expectedIntegrity: integrityOf(source),
  });
  const replay = await exportStore.writeReceipt({
    identity,
    body: bodyFrom(source),
    contentLength: source.byteLength,
    expectedIntegrity: integrityOf(source),
  });
  const key = receiptKey();
  const listed = await env.EXPORTS.list({
    prefix: `exports/receipts/v1/${RECEIPT_DATABASE_ID}/`,
  });
  const object = await env.EXPORTS.get(key);
  if (object === null) throw new Error('receipt replay object missing');
  const stored = await object.bytes();
  const customMetadata = object.customMetadata;
  await env.EXPORTS.delete(key);
  return {
    sameResult:
      first.location === replay.location &&
      first.size === replay.size &&
      first.sha256 === replay.sha256,
    location: first.location,
    objectCount: listed.objects.length,
    bytesEqual: equalBytes(stored, source),
    customMetadata,
    cleaned: (await env.EXPORTS.get(key)) === null,
  };
}

async function receiptConcurrent(env: Env) {
  const source = sequence(12_288, 83);
  const operationId = '33333333-3333-4333-8333-333333333333';
  const exportStore = store(env);
  const identity = { ...receiptIdentity(exportStore), operationId };
  const [first, second] = await Promise.all([
    exportStore.writeReceipt({
      identity,
      body: bodyFrom(source),
      contentLength: source.byteLength,
      expectedIntegrity: integrityOf(source),
    }),
    exportStore.writeReceipt({
      identity,
      body: bodyFrom(source),
      contentLength: source.byteLength,
      expectedIntegrity: integrityOf(source),
    }),
  ]);
  const key = receiptKey(operationId);
  const listed = await env.EXPORTS.list({
    prefix: `exports/receipts/v1/${RECEIPT_DATABASE_ID}/`,
  });
  const object = await env.EXPORTS.get(key);
  if (object === null) throw new Error('concurrent receipt object missing');
  const stored = await object.bytes();
  await env.EXPORTS.delete(key);
  return {
    sameResult:
      first.location === second.location &&
      first.size === second.size &&
      first.sha256 === second.sha256,
    location: first.location,
    objectCount: listed.objects.length,
    bytesEqual: equalBytes(stored, source),
    cleaned: (await env.EXPORTS.get(key)) === null,
  };
}

async function receiptMismatch(env: Env) {
  const winner = sequence(8192, 97);
  const challenger = sequence(8192, 101);
  const operationId = '44444444-4444-4444-8444-444444444444';
  const exportStore = store(env);
  const identity = { ...receiptIdentity(exportStore), operationId };
  const committed = await exportStore.writeReceipt({
    identity,
    body: bodyFrom(winner),
    contentLength: winner.byteLength,
    expectedIntegrity: integrityOf(winner),
  });
  const state = await Promise.allSettled([
    exportStore.writeReceipt({
      identity,
      body: bodyFrom(challenger),
      contentLength: challenger.byteLength,
      expectedIntegrity: integrityOf(challenger),
    }),
  ]);
  if (state[0].status === 'fulfilled') {
    throw new Error('mismatched receipt replay succeeded');
  }
  const key = receiptKey(operationId);
  const listed = await env.EXPORTS.list({
    prefix: `exports/receipts/v1/${RECEIPT_DATABASE_ID}/`,
  });
  const object = await env.EXPORTS.get(key);
  if (object === null) throw new Error('mismatched receipt winner missing');
  const stored = await object.bytes();
  await env.EXPORTS.delete(key);
  return {
    location: committed.location,
    message: messageOf(state[0].reason),
    objectCount: listed.objects.length,
    winnerPreserved: equalBytes(stored, winner),
    challengerRejected: !equalBytes(stored, challenger),
    cleaned: (await env.EXPORTS.get(key)) === null,
  };
}

async function dispatch(action: string, env: Env): Promise<Response> {
  switch (action) {
    case 'success':
      return Response.json(await success(env));
    case 'empty':
      return Response.json(await empty(env));
    case 'short':
      return Response.json(await short(env));
    case 'mid-transfer-error':
      return Response.json(await midTransferError(env));
    case 'collision':
      return Response.json(await collision(env));
    case 'receipt-replay':
      return Response.json(await receiptReplay(env));
    case 'receipt-concurrent':
      return Response.json(await receiptConcurrent(env));
    case 'receipt-mismatch':
      return Response.json(await receiptMismatch(env));
    default:
      return Response.json({ error: 'unknown action' }, { status: 400 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method !== 'POST' || url.pathname !== '/r2-export') {
      return new Response('not found', { status: 404 });
    }
    const payload: unknown = await request.json();
    if (typeof payload !== 'object' || payload === null) {
      return Response.json({ error: 'invalid request' }, { status: 400 });
    }
    const action = Reflect.get(payload, 'action');
    if (typeof action !== 'string') {
      return Response.json({ error: 'invalid action' }, { status: 400 });
    }
    try {
      return await dispatch(action, env);
    } catch (error) {
      return Response.json({ error: messageOf(error) }, { status: 500 });
    }
  },
} satisfies ExportedHandler<Env>;
