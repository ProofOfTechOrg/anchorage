// SPDX-License-Identifier: Apache-2.0

export type BoundedBodyResult =
  | { ok: true; bytes: Uint8Array; text: string }
  | {
      ok: false;
      reason: 'payload-too-large' | 'invalid-utf8';
      bytesRead: number;
    };

export type BoundedBytesResult =
  | { ok: true; bytes: Uint8Array }
  | { ok: false; reason: 'payload-too-large'; bytesRead: number };

export async function readBoundedBytes(
  request: Request,
  maxBytes: number,
  cancelReason = 'request body exceeds limit',
): Promise<BoundedBytesResult> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new RangeError('maxBytes must be a nonnegative safe integer');
  }
  const contentLength = request.headers.get('content-length');
  if (
    contentLength !== null &&
    /^\d+$/.test(contentLength) &&
    Number(contentLength) > maxBytes
  ) {
    return {
      ok: false,
      reason: 'payload-too-large',
      bytesRead: maxBytes + 1,
    };
  }

  const reader = request.body?.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  if (reader) {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      length += next.value.byteLength;
      if (length > maxBytes) {
        try {
          await reader.cancel(cancelReason);
        } catch {
          // Cancellation is cleanup; the size verdict is already authoritative.
        }
        return { ok: false, reason: 'payload-too-large', bytesRead: length };
      }
      chunks.push(next.value);
    }
  }

  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { ok: true, bytes };
}

export async function readBoundedBody(
  request: Request,
  maxBytes: number,
  cancelReason = 'request body exceeds limit',
): Promise<BoundedBodyResult> {
  const raw = await readBoundedBytes(request, maxBytes, cancelReason);
  if (!raw.ok) return raw;
  try {
    return {
      ok: true,
      bytes: raw.bytes,
      text: new TextDecoder('utf-8', {
        fatal: true,
        ignoreBOM: false,
      }).decode(raw.bytes),
    };
  } catch {
    return {
      ok: false,
      reason: 'invalid-utf8',
      bytesRead: raw.bytes.byteLength,
    };
  }
}
