// SPDX-License-Identifier: Apache-2.0
// Bounded tail buffer for captured process output. Package-internal: the
// './agent-cli' subpath export maps to index.js only, so nothing here is
// consumer-reachable; it lives in its own module so tests can drive the
// chunk-boundary geometry directly (pipe 'data' events don't offer
// deterministic splits).

export interface TextDecoderLike {
  decode(input: Uint8Array): string;
}
export interface TextEncoderLike {
  encode(input: string): Uint8Array;
}

// The global lookups are injected (default-exec.ts wraps them in safe runtime
// failures); tests pass the bare globals.
export interface TextCodecLookups {
  encoder(): TextEncoderLike;
  decoder(): TextDecoderLike;
}

// A UTF-8 continuation byte (0b10xxxxxx): the middle/tail of a multi-byte
// codepoint, never a valid position to start decoding from.
export function isUtf8ContinuationByte(byte: number): boolean {
  return (byte & 0b1100_0000) === 0b1000_0000;
}

function concatBytes(
  chunks: readonly Uint8Array[],
  totalBytes: number,
): Uint8Array {
  if (chunks.length === 1) return chunks[0] ?? new Uint8Array(0);
  const out = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

// Audit fix (2026-07-11): bounds retained output per stream by true UTF-8
// BYTES, not UTF-16 code units — the prior char-length cap measured
// `string.length`, which over-counts non-Latin1 text (CJK output measured
// 3.00x over maxOutputBytes) and can cut a string mid-surrogate-pair,
// leaving an orphaned surrogate at the retained head. Chunks are kept as raw
// bytes and concatenated + decoded ONCE in value(), so the final string is
// never sliced mid-codepoint; the TAIL is kept (the CLI's final answer text
// arrives last) and truncation is marked, instead of growing unboundedly on
// a runaway process.
export function tailAccumulator(
  maxBytes: number,
  lookups: TextCodecLookups,
): {
  push(chunk: unknown): void;
  value(): string;
} {
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  let truncated = false;
  let encoder: TextEncoderLike | undefined;
  return {
    push(chunk: unknown) {
      let bytes: Uint8Array;
      if (chunk instanceof Uint8Array) {
        bytes = chunk;
      } else {
        encoder ??= lookups.encoder();
        bytes = encoder.encode(String(chunk));
      }
      if (bytes.length === 0) return;
      chunks.push(bytes);
      totalBytes += bytes.length;
      if (totalBytes <= maxBytes) return;
      truncated = true;
      // Drop whole leading chunks while the head is entirely excess.
      let head = chunks[0];
      while (
        chunks.length > 1 &&
        head &&
        totalBytes - head.length >= maxBytes
      ) {
        totalBytes -= head.length;
        chunks.shift();
        head = chunks[0];
      }
      // The remaining excess lies inside the (new) head chunk — cut past it,
      // dropping the head entirely when the cut consumes it.
      const excess = totalBytes - maxBytes;
      if (excess > 0 && head) {
        const cut = Math.min(excess, head.length);
        totalBytes -= cut;
        if (cut === head.length) chunks.shift();
        else chunks[0] = head.slice(cut);
      }
      // Skip any continuation bytes now leading the retained data — ACROSS
      // chunk boundaries: a codepoint split across stream 'data' events parks
      // its continuation bytes at the start of the NEXT chunk, and stopping
      // at the boundary would orphan them (decoding as U+FFFD at the retained
      // head — 2026-07-11 QA finding).
      for (let h = chunks[0]; h; h = chunks[0]) {
        let skip = 0;
        while (skip < h.length && isUtf8ContinuationByte(h[skip] ?? 0)) {
          skip += 1;
        }
        if (skip === 0) break;
        totalBytes -= skip;
        if (skip === h.length) {
          chunks.shift();
        } else {
          chunks[0] = h.slice(skip);
          break;
        }
      }
    },
    value: () => {
      const decoded = lookups.decoder().decode(concatBytes(chunks, totalBytes));
      return truncated
        ? `…[truncated to the last ${maxBytes} bytes]…${decoded}`
        : decoded;
    },
  };
}
