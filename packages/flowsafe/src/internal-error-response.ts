// SPDX-License-Identifier: Apache-2.0

function errorText(error: unknown): string {
  try {
    return String(error instanceof Error ? error.message : error);
  } catch {
    return 'unreadable error';
  }
}

/**
 * Package-internal: log a failure that an HTTP answer omits, and return the
 * message the answer carries in its place.
 */
export function internalErrorMessage(route: string, error: unknown): string {
  try {
    console.error(
      JSON.stringify({
        type: 'route-internal-error',
        route,
        error: errorText(error),
      }),
      error,
    );
  } catch {
    // Diagnostic failure cannot prevent the HTTP response.
  }
  return 'internal error';
}

/** Package-internal catch-all for public HTTP routes; `status` is a 5xx. */
export function internalErrorResponse(
  route: string,
  error: unknown,
  status = 500,
): Response {
  return new Response(
    JSON.stringify({ error: internalErrorMessage(route, error) }),
    {
      status,
      headers: {
        'content-type': 'application/json',
        'cache-control': 'no-store',
      },
    },
  );
}
