// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type {
  DigestStreamConstructor,
  FixedLengthStreamConstructor,
  R2DatabaseExportStoreStreamPrimitives,
} from '../../src/r2-export-store.js';

export class NodeDigestStream extends WritableStream<
  ArrayBuffer | ArrayBufferView
> {
  readonly digest: Promise<ArrayBuffer>;
  readonly #byteCount: { value: number };

  constructor(_algorithm: 'SHA-256') {
    const hash = createHash('sha256');
    const byteCount = { value: 0 };
    let resolveDigest: (value: ArrayBuffer) => void = () => undefined;
    let rejectDigest: (reason: unknown) => void = () => undefined;
    const digest = new Promise<ArrayBuffer>((resolve, reject) => {
      resolveDigest = resolve;
      rejectDigest = reject;
    });
    super({
      write(chunk) {
        const bytes = ArrayBuffer.isView(chunk)
          ? new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
          : new Uint8Array(chunk);
        hash.update(bytes);
        byteCount.value += bytes.byteLength;
      },
      close() {
        const result = hash.digest();
        const bytes = new Uint8Array(result.byteLength);
        bytes.set(result);
        resolveDigest(bytes.buffer);
      },
      abort(reason) {
        rejectDigest(reason);
      },
    });
    this.digest = digest;
    this.#byteCount = byteCount;
  }

  get bytesWritten(): number {
    return this.#byteCount.value;
  }
}

export class NodeFixedLengthStream extends TransformStream<
  Uint8Array,
  Uint8Array
> {
  constructor(expectedLength: number) {
    let bytesWritten = 0;
    super(
      {
        transform(chunk, controller) {
          bytesWritten += chunk.byteLength;
          if (bytesWritten > expectedLength) {
            throw new Error(
              'Attempt to write too many bytes through a FixedLengthStream.',
            );
          }
          controller.enqueue(chunk);
        },
        flush() {
          if (bytesWritten !== expectedLength) {
            throw new Error(
              'FixedLengthStream did not see all expected bytes before close().',
            );
          }
        },
      },
      undefined,
      {
        highWaterMark: Math.max(expectedLength, 1),
        size: (chunk) => chunk.byteLength,
      },
    );
  }
}

// These casts adapt Node stream objects to
// workers-types at the injection boundary.
export const nodeWorkerStreams: R2DatabaseExportStoreStreamPrimitives = {
  DigestStream: NodeDigestStream as unknown as DigestStreamConstructor,
  FixedLengthStream:
    NodeFixedLengthStream as unknown as FixedLengthStreamConstructor,
};
