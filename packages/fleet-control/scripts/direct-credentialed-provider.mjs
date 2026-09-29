// SPDX-License-Identifier: Apache-2.0

import { validateHeaderValue } from 'node:http';
import { cancelBodyWithoutAwait } from './direct-credentialed-body-cancel.mjs';

const API_BASE = 'https://api.cloudflare.com/client/v4';
export const DIRECT_PROVIDER_MAX_REQUESTS = 512;
const MAX_DURATION_MS = 300_000;
const MAX_JSON_BYTES = 8 * 1024 * 1024;
// The SDK's repeated type query parameters return no rows from the live API.
const ZONE_TYPES = Object.freeze(['full', 'partial', 'secondary', 'internal']);
const ERROR_CODES = new Set([
  'invalid-input',
  'provider-unavailable',
  'observation-mismatch',
  'budget-exhausted',
  'forbidden',
]);

export class DirectProviderError extends Error {
  constructor(code = 'invalid-input') {
    const accepted = ERROR_CODES.has(code) ? code : 'invalid-input';
    super(accepted);
    this.name = 'DirectProviderError';
    this.code = accepted;
  }
}

function refuse(code = 'observation-mismatch') {
  throw new DirectProviderError(code);
}
function object(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) refuse();
  return value;
}

export function identifier(value, max = 128) {
  if (
    typeof value !== 'string' ||
    !value ||
    value !== value.trim() ||
    value.length > max ||
    [...value].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  )
    refuse();
  return value;
}

export function validateProviderAuth(value) {
  if (typeof value !== 'string' || !value || value !== value.trim())
    refuse('invalid-input');
  const header = `Bearer ${value}`;
  validateHeaderValue('Authorization', header);
  if (new Headers({ Authorization: header }).get('authorization') !== header)
    refuse('invalid-input');
}

function providerTransport(fetchRequest, timeoutMs) {
  const expiresAt = performance.now() + MAX_DURATION_MS;
  const active = new Set();
  let attempts = 0;
  let failure;
  const assertBudget = () => {
    if (
      performance.now() >= expiresAt ||
      attempts >= DIRECT_PROVIDER_MAX_REQUESTS
    ) {
      failure = 'budget-exhausted';
      refuse(failure);
    }
  };
  return {
    assertBudget,
    failure: () => failure,
    close() {
      for (const finish of active) finish();
    },
    async fetch(input, init, rawByteLimit) {
      assertBudget();
      const url = new URL(
        typeof input === 'string' || input instanceof URL ? input : input.url,
      );
      if (
        url.origin !== 'https://api.cloudflare.com' ||
        !url.pathname.startsWith('/client/v4/') ||
        url.username ||
        url.password ||
        url.hash
      )
        refuse('provider-unavailable');
      attempts += 1;
      const controller = new AbortController();
      const signal = AbortSignal.any([
        controller.signal,
        ...(init?.signal ? [init.signal] : []),
      ]);
      const deadline = Math.min(timeoutMs, expiresAt - performance.now());
      let response;
      let bounded;
      let abort;
      let timer;
      let rawReader;
      // `reason` identifies the refusal ending this request. The deadline timer
      // aborts without a reason because a budget is not a refusal.
      const finish = (reason) => {
        clearTimeout(timer);
        signal.removeEventListener('abort', abort);
        controller.abort(reason);
        cancelBodyWithoutAwait((bounded ?? response)?.body, reason);
        if (rawReader) {
          void rawReader.cancel(reason).catch(() => {});
          rawReader = undefined;
        }
        active.delete(finish);
      };
      active.add(finish);
      const aborted = new Promise((_, reject) => {
        abort = () => reject(new DirectProviderError('provider-unavailable'));
        signal.addEventListener('abort', abort, { once: true });
      });
      timer = setTimeout(() => controller.abort(), Math.max(1, deadline));
      try {
        signal.throwIfAborted();
        if (rawByteLimit !== undefined) {
          const headers = new Headers(
            init?.headers ??
              (input instanceof Request ? input.headers : undefined),
          );
          // The export observation refuses compression even if the platform ignores identity.
          headers.set('Accept-Encoding', 'identity');
          init = { ...init, headers };
        }
        const exchange = Promise.resolve(
          fetchRequest(input, { ...init, signal, redirect: 'manual' }),
        ).then((value) => {
          if (signal.aborted) {
            cancelBodyWithoutAwait(value?.body, signal.reason);
            signal.throwIfAborted();
          }
          return value;
        });
        response = await Promise.race([exchange, aborted]);
        if (
          response.redirected ||
          (response.status >= 300 && response.status < 400)
        )
          refuse('provider-unavailable');
        let bytes = 0;
        if (rawByteLimit !== undefined && response.status === 200) {
          rawReader = response.body?.getReader();
          if (!rawReader) refuse('provider-unavailable');
          const reader = rawReader;
          bounded = new Response(
            new ReadableStream({
              async pull(output) {
                try {
                  const chunk = await Promise.race([reader.read(), aborted]);
                  if (chunk.done) {
                    output.close();
                    finish();
                    return;
                  }
                  if (!(chunk.value instanceof Uint8Array))
                    refuse('provider-unavailable');
                  bytes += chunk.value.byteLength;
                  if (bytes > rawByteLimit) refuse('observation-mismatch');
                  output.enqueue(chunk.value);
                } catch (error) {
                  output.error(error);
                  finish(error);
                }
              },
              cancel(reason) {
                finish(reason);
              },
            }),
            {
              status: response.status,
              headers: response.headers,
            },
          );
          return bounded;
        }
        const body = response.body?.pipeThrough(
          new TransformStream({
            transform(chunk, output) {
              bytes += chunk.byteLength;
              if (bytes > MAX_JSON_BYTES) refuse('provider-unavailable');
              output.enqueue(chunk);
            },
          }),
          { signal },
        );
        bounded = new Response(body, {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers,
        });
        return new Proxy(bounded, {
          get(target, property) {
            const value = Reflect.get(target, property, target);
            if (
              ['json', 'text', 'arrayBuffer', 'blob', 'formData'].includes(
                property,
              )
            )
              return async (...args) => {
                try {
                  return await Promise.race([
                    value.apply(target, args),
                    aborted,
                  ]);
                } finally {
                  finish();
                }
              };
            return typeof value === 'function' ? value.bind(target) : value;
          },
        });
      } catch (error) {
        finish(error);
        throw error;
      }
    },
  };
}

function validateEnvelope(value) {
  object(value);
  const cursor = value.result_info?.cursor;
  // Cloudflare returns errors: null on successful pages.
  if (
    value.success !== true ||
    (value.errors !== undefined &&
      value.errors !== null &&
      (!Array.isArray(value.errors) || value.errors.length !== 0)) ||
    (typeof cursor === 'string' && cursor.length > 0)
  )
    refuse('provider-unavailable');
}

// `SinglePage` copies only the rows, so a caller cannot reach the `result_info`
// the provider sent. The transport records that attestation against the row
// array the page carries.
const singlePageAttestations = new WeakMap();

function proofFetch(transport, shape, bound) {
  return async (input, init) => {
    const response = await transport.fetch(input, init);
    if (!response.ok || shape === 'status') return response;
    const contentType = response.headers.get('content-type') ?? '';
    if (
      !/^application\/(?:json|[a-z0-9.+-]+\+json)(?:\s*;.*)?$/iu.test(
        contentType,
      ) ||
      (shape !== 'object' && response.status !== 200) ||
      // The SDK skips its parser, and so this validation, on an empty body.
      (shape === 'settled' && response.headers.get('content-length') === '0')
    ) {
      cancelBodyWithoutAwait(response.body);
      refuse('provider-unavailable');
    }
    // SDK parser selection must reach the validated JSON method.
    response.headers.set(
      'content-type',
      contentType.replace(/^[^;]+/u, (mediaType) => mediaType.toLowerCase()),
    );
    const parse = response.json.bind(response);
    const json = async () => {
      const value = await parse();
      validateEnvelope(value);
      if (shape === 'object') object(value.result);
      else if (shape === 'settled') {
        if (value.result !== null) object(value.result);
      } else {
        const rows = value.result;
        if (!Array.isArray(rows) || rows.length > bound)
          refuse('provider-unavailable');
        rows.forEach(object);
        const info = value.result_info;
        // Successful Cloudflare pages can omit optional metadata with null.
        if (info !== undefined && info !== null) object(info);
        const {
          cursor,
          total_pages: totalPages,
          total_count: totalCount,
          per_page: perPage,
          page,
          count,
        } = info ?? {};
        const url = new URL(
          typeof input === 'string' || input instanceof URL ? input : input.url,
        );
        const requestedPage = Number(url.searchParams.get('page') ?? '1');
        if (
          (cursor !== undefined &&
            cursor !== null &&
            typeof cursor !== 'string') ||
          [totalPages, totalCount, perPage, page, count].some(
            (number) =>
              number !== undefined &&
              (!Number.isSafeInteger(number) || number < 0),
          ) ||
          (page !== undefined && page !== requestedPage) ||
          (count !== undefined && count !== rows.length) ||
          (perPage !== undefined && rows.length > perPage) ||
          (totalCount !== undefined && rows.length > totalCount) ||
          (totalPages === 0 && rows.length > 0)
        )
          refuse('provider-unavailable');
        if (shape === 'single') {
          if (
            (totalPages !== undefined && totalPages > 1) ||
            (totalCount !== undefined && totalCount !== rows.length)
          )
            refuse('provider-unavailable');
          // A positive rule, stated independently of the refusals above it: the
          // attestation records what the provider corroborated rather than what
          // survived a refusal, so a relaxed refusal downgrades the page to
          // `exhaustive: false` instead of silently attesting it.
          singlePageAttestations.set(
            rows,
            totalCount === rows.length ||
              totalPages === 1 ||
              (totalPages === 0 && rows.length === 0),
          );
        } else if (
          rows.length === 0 &&
          ((totalPages !== undefined && totalPages > requestedPage) ||
            (totalCount > 0 &&
              (requestedPage === 1 ||
                (perPage > 0 && totalCount / perPage > requestedPage - 1))))
        )
          refuse('provider-unavailable');
      }
      return value;
    };
    return new Proxy(response, {
      get(target, property) {
        if (property === 'json') return json;
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
  };
}

export async function inventory(pages, identity, bound) {
  const rows = [];
  const seen = new Set();
  let expectedCount = 0;
  let expectedPages = 0;
  let pageCount = 0;
  for await (const page of (await pages).iterPages()) {
    pageCount += 1;
    expectedCount = Math.max(expectedCount, page.result_info?.total_count ?? 0);
    expectedPages = Math.max(expectedPages, page.result_info?.total_pages ?? 0);
    for (const row of page.result) {
      const keys = identity(row);
      if (keys.some((key) => seen.has(key)) || rows.length >= bound)
        refuse('provider-unavailable');
      for (const key of keys) seen.add(key);
      rows.push(row);
    }
  }
  if (rows.length < expectedCount || pageCount < expectedPages)
    refuse('provider-unavailable');
  return rows;
}

function zoneKeys(row, accountId) {
  identifier(row.id);
  identifier(row.type);
  const name = identifier(row.name, 253);
  if (
    row.account?.id !== accountId ||
    name
      .split('.')
      .some((label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u.test(label))
  )
    refuse();
  return [`id:${row.id}`, `name:${row.name}`];
}

export async function resolveDirectZone({
  sdk,
  numbered,
  accountId,
  ownedHostname,
  bound,
}) {
  const zones = await inventory(
    numbered.zones.list({ account: { id: accountId }, per_page: 50 }),
    (row) => zoneKeys(row, accountId),
    bound,
  );
  const matches = zones
    .filter((row) => ZONE_TYPES.includes(row.type))
    .filter(
      (row) =>
        ownedHostname === row.name || ownedHostname.endsWith(`.${row.name}`),
    )
    .sort((a, b) => b.name.length - a.name.length);
  if (
    !matches[0] ||
    (matches[1] && matches[0].name.length === matches[1].name.length)
  )
    refuse();
  const selected = await sdk.zones.get({ zone_id: matches[0].id });
  zoneKeys(selected, accountId);
  if (
    selected.id !== matches[0].id ||
    selected.name !== matches[0].name ||
    !ZONE_TYPES.includes(selected.type)
  )
    refuse();
  return selected;
}

export async function classifyDispatchNamespaces(single, selectors, bound) {
  const { APIError } = await import('cloudflare');
  try {
    const page =
      await single.workersForPlatforms.dispatch.namespaces.list(selectors);
    const namespaces = await inventory(
      page,
      (row) => [
        `id:${identifier(row.namespace_id)}`,
        `name:${identifier(row.namespace_name)}`,
      ],
      bound,
    );
    return {
      kind: namespaces.length ? 'enumerated' : 'empty',
      count: namespaces.length,
      names: namespaces.map((row) => row.namespace_name),
      exhaustive: singlePageAttestations.get(page.result) ?? false,
    };
  } catch (error) {
    // A 404 is no page the provider sent, so the empty reading it stands for
    // is unattested.
    if (error instanceof APIError && error.status === 404)
      return { kind: 'first-page-404', count: 0, names: [], exhaustive: false };
    throw error;
  }
}

export async function singlePage(promise) {
  const rows = (await promise).result;
  if (!Array.isArray(rows)) refuse('provider-unavailable');
  return { rows, exhaustive: singlePageAttestations.get(rows) ?? false };
}

export async function bucketPages({ sdk, selectors, jurisdiction, bound }) {
  const rows = [];
  let startAfter;
  // Only an empty page proves the end: a full page that happens to be last is
  // indistinguishable from a truncated one.
  for (;;) {
    const page = await sdk.r2.buckets.list({
      ...selectors,
      per_page: 100,
      order: 'name',
      direction: 'asc',
      jurisdiction,
      ...(startAfter === undefined ? {} : { start_after: startAfter }),
    });
    const buckets = object(page).buckets;
    if (!Array.isArray(buckets)) refuse('provider-unavailable');
    if (buckets.length === 0) return rows;
    for (const row of buckets) {
      object(row);
      if (
        typeof row.name !== 'string' ||
        !row.name ||
        (startAfter !== undefined && row.name <= startAfter)
      )
        refuse('provider-unavailable');
      startAfter = row.name;
      rows.push(row);
      if (rows.length > bound) refuse('provider-unavailable');
    }
  }
}

// The SDK reports a transport rejection as an APIConnectionError carrying the
// original as `cause`, so a refusal raised here reaches callers wrapped.
export function providerErrorFrom(error) {
  let current = error;
  for (let depth = 0; current && depth < 4; depth += 1) {
    if (current instanceof DirectProviderError) return current;
    current = current.cause;
  }
  return null;
}

export async function probeAbsent(promise) {
  const { APIError } = await import('cloudflare');
  try {
    const value = await promise;
    if (value instanceof Response) cancelBodyWithoutAwait(value.body);
    return 'present';
  } catch (error) {
    const raised = providerErrorFrom(error);
    if (raised) throw raised;
    if (error instanceof APIError) {
      if (error.status === 404) return 'absent';
      if (error.status === 401 || error.status === 403) refuse('forbidden');
    }
    refuse('provider-unavailable');
  }
}

export async function openDirectProviderSession({
  apiToken,
  fetchRequest,
  timeoutMs,
}) {
  delete process.env.CLOUDFLARE_CUSTOM_HEADERS;
  delete process.env.CLOUDFLARE_LOG;
  delete process.env.CLOUDFLARE_BASE_URL;
  const transport = providerTransport(fetchRequest, timeoutMs);
  try {
    const { default: Cloudflare, APIError } = await import('cloudflare');
    const { CLOUDFLARE_INVENTORY_BOUND: bound } = await import(
      '../src/cloudflare-client-config.ts'
    );
    const sdk = new Cloudflare({
      baseURL: API_BASE,
      apiToken,
      apiKey: null,
      apiEmail: null,
      userServiceKey: null,
      logLevel: 'off',
      timeout: timeoutMs,
      maxRetries: 0,
      fetch: proofFetch(transport, 'object', bound),
    });
    const numbered = sdk.withOptions({
      fetch: proofFetch(transport, 'numbered', bound),
    });
    const single = sdk.withOptions({
      fetch: proofFetch(transport, 'single', bound),
    });
    const status = sdk.withOptions({
      fetch: proofFetch(transport, 'status', bound),
    });
    const settled = sdk.withOptions({
      fetch: proofFetch(transport, 'settled', bound),
    });
    return {
      sdk,
      numbered,
      single,
      status,
      settled,
      APIError,
      bound,
      transport,
      exportReader(expectedSize) {
        return sdk.withOptions({
          fetch: (input, init) => transport.fetch(input, init, expectedSize),
        });
      },
    };
  } catch (error) {
    transport.close();
    throw error;
  }
}
