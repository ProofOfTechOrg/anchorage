// SPDX-License-Identifier: Apache-2.0

// This module holds safe-read and secret-redaction helpers for Cloudflare SDK
// errors, and the shared isNotFound predicate.
// Its two sanitizer consumers use different members: ordinary-Worker upload
// dispatch calls sanitizeProviderError, while D1 export calls
// readErrorFieldSafely and sanitizedErrorName; isNotFound is shared by the
// client's control, WFP, and ordinary-Worker paths.

import { APIConnectionError, APIError } from 'cloudflare';
import { readField } from './provider-binding-inventory.js';

export const MAX_SANITIZED_ERROR_CAUSE_DEPTH = 8;

export function redactSecretValues(
  value: string,
  secretValues: readonly string[],
): string {
  return secretValues.reduce(
    (redacted, secret) =>
      secret.length > 0 ? redacted.split(secret).join('[redacted]') : redacted,
    value,
  );
}

export function readErrorFieldSafely(
  value: unknown,
  key: 'cause' | 'constructor' | 'message' | 'name' | 'status',
): unknown {
  // A consumer-injected rejection can still be instanceof Error with doctored
  // or throwing fields; the redaction boundaries must not throw while reading.
  try {
    return Reflect.get(value as object, key);
  } catch {
    return undefined;
  }
}

export function isErrorSafely(value: unknown): value is Error {
  // For object values, `value instanceof Error` walks [[GetPrototypeOf]]; a
  // Proxy trap or revoked Proxy can throw and replace the sanitized failure,
  // so every Error check on the foreign cause graph uses this predicate.
  try {
    return value instanceof Error;
  } catch {
    return false;
  }
}

export function sanitizedErrorName(error: unknown): string {
  const name = readErrorFieldSafely(error, 'name');
  const directName = typeof name === 'string' ? name : undefined;
  const nameFromConstructor = readErrorFieldSafely(
    isErrorSafely(error)
      ? readErrorFieldSafely(error, 'constructor')
      : undefined,
    'name',
  );
  const constructorName =
    typeof nameFromConstructor === 'string' ? nameFromConstructor : undefined;
  const candidate =
    directName && directName !== 'Error'
      ? directName
      : (constructorName ?? directName);
  return candidate && /^[A-Za-z][A-Za-z0-9]*Error$/.test(candidate)
    ? candidate
    : 'unknown';
}

export function sanitizedErrorCause(
  error: Error,
  secretValues: readonly string[],
  depth = 0,
  seen = new WeakMap<Error, string>(),
): Error {
  // This boundary never throws on the values it receives: cycles, depth,
  // non-string messages, throwing accessors, and hostile prototypes all
  // degrade to safe values. Fresh Errors copy safe values.
  const name = sanitizedErrorName(error);
  seen.set(error, name);
  const cause = readErrorFieldSafely(error, 'cause');
  const message = readErrorFieldSafely(error, 'message');
  let sanitizedCause: Error | { name: string } | undefined;
  if (isErrorSafely(cause)) {
    const memoized = seen.get(cause);
    sanitizedCause =
      memoized !== undefined
        ? { name: memoized }
        : depth + 1 >= MAX_SANITIZED_ERROR_CAUSE_DEPTH
          ? { name: sanitizedErrorName(cause) }
          : sanitizedErrorCause(cause, secretValues, depth + 1, seen);
  }
  const sanitized = new Error(
    typeof message === 'string'
      ? redactSecretValues(message, secretValues)
      : '',
    sanitizedCause === undefined ? undefined : { cause: sanitizedCause },
  );
  sanitized.name = name;
  return sanitized;
}

export function sanitizeProviderError(
  error: unknown,
  secretValues: readonly string[],
): unknown {
  // Wrapped paths supply an SDK-constructed operand. A value that escapes when
  // the SDK itself throws on the rejection before wrapping it (castToError's
  // instanceof, internal/errors.mjs:11; the String()/in coercions at
  // client.mjs:389-390) is outside this boundary's no-throw promise.
  if (!(error instanceof APIError)) return error;
  const sanitized = new Error('Cloudflare Worker upload failed');
  sanitized.name = 'CloudflareProviderError';
  Object.defineProperties(sanitized, {
    status: { enumerable: true, value: error.status },
    errors: {
      enumerable: true,
      value: (Array.isArray(error.errors) ? error.errors : []).flatMap(
        (entry) => {
          if (!entry || typeof entry !== 'object') return [];
          const code = readField(entry, 'code');
          const message = readField(entry, 'message');
          return [
            {
              ...(typeof code === 'number' || typeof code === 'string'
                ? { code }
                : {}),
              ...(typeof message === 'string'
                ? { message: redactSecretValues(message, secretValues) }
                : {}),
            },
          ];
        },
      ),
    },
    cause: {
      enumerable: false,
      // APIConnectionError keeps a cause chain because, for a rejected fetch,
      // that chain is the fence or transport failure the adapter classifies
      // through. The SDK drops the underlying error on its timeout arm
      // (APIConnectionTimeoutError is constructed without a cause,
      // core/error.mjs:81-85), so that chain is one constant level. Every other
      // APIError (a provider response or the SDK's own abort error) collapses
      // to { name }.
      value:
        error instanceof APIConnectionError
          ? sanitizedErrorCause(error, secretValues)
          : { name: sanitizedErrorName(error) },
    },
  });
  return sanitized;
}

export function isNotFound(error: unknown): boolean {
  return Boolean(
    error &&
      typeof error === 'object' &&
      'status' in error &&
      error.status === 404,
  );
}
