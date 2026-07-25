// SPDX-License-Identifier: Apache-2.0

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Package-internal catch-all for public HTTP routes. */
export function internalErrorResponse(
  route: string,
  error: unknown,
  status: 500 | 502 = 500,
): Response {
  console.error(
    JSON.stringify({
      type: 'route-internal-error',
      route,
      error: errorText(error),
    }),
  );
  return new Response(JSON.stringify({ error: 'internal error' }), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}
