// SPDX-License-Identifier: Apache-2.0

import { validateHeaderValue } from 'node:http';

const API_BASE = 'https://api.cloudflare.com/client/v4';
const MAX_ATTEMPTS = 512;
const MAX_DURATION_MS = 300_000;
const MAX_JSON_BYTES = 8 * 1024 * 1024;
const ERROR_CODES = new Set([
  'invalid-input',
  'provider-unavailable',
  'observation-mismatch',
  'budget-exhausted',
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

export function validateProviderAuth(value) {
  if (typeof value !== 'string' || !value || value !== value.trim())
    refuse('invalid-input');
  const header = `Bearer ${value}`;
  validateHeaderValue('Authorization', header);
  if (new Headers({ Authorization: header }).get('authorization') !== header)
    refuse('invalid-input');
}

function cancel(response) {
  try {
    void response?.body?.cancel().catch(() => {});
  } catch {
    /* The abortable pipe owns a locked source. */
  }
}

function providerTransport(fetchRequest, timeoutMs) {
  const expiresAt = performance.now() + MAX_DURATION_MS;
  const active = new Set();
  let attempts = 0;
  let failure;
  const assertBudget = () => {
    if (performance.now() >= expiresAt || attempts >= MAX_ATTEMPTS) {
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
      const finish = () => {
        clearTimeout(timer);
        signal.removeEventListener('abort', abort);
        controller.abort();
        cancel(bounded ?? response);
        if (rawReader) {
          void rawReader.cancel().catch(() => {});
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
        const exchange = Promise.resolve(
          fetchRequest(input, { ...init, signal, redirect: 'manual' }),
        ).then((value) => {
          if (signal.aborted) {
            cancel(value);
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
                  finish();
                }
              },
              cancel() {
                finish();
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
        finish();
        throw error;
      }
    },
  };
}

function validateEnvelope(value) {
  object(value);
  if (
    value.success !== true ||
    (value.errors !== undefined &&
      (!Array.isArray(value.errors) || value.errors.length !== 0))
  )
    refuse('provider-unavailable');
}

function proofFetch(transport, shape, bound) {
  return async (input, init) => {
    const response = await transport.fetch(input, init);
    if (!response.ok || shape === 'status') return response;
    const contentType = response.headers.get('content-type') ?? '';
    if (
      !/^application\/(?:json|[a-z0-9.+-]+\+json)(?:\s*;.*)?$/iu.test(
        contentType,
      ) ||
      (shape !== 'object' && response.status !== 200)
    ) {
      cancel(response);
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
      else {
        const rows = value.result;
        if (!Array.isArray(rows) || rows.length > bound)
          refuse('provider-unavailable');
        rows.forEach(object);
        const info = value.result_info;
        if (info !== undefined) object(info);
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
          (totalPages === 0 && rows.length > 0) ||
          (typeof cursor === 'string' && cursor.length > 0)
        )
          refuse('provider-unavailable');
        if (shape === 'single') {
          if (
            (totalPages !== undefined && totalPages > 1) ||
            (totalCount !== undefined && totalCount !== rows.length)
          )
            refuse('provider-unavailable');
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
    return {
      sdk,
      numbered,
      single,
      status,
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
