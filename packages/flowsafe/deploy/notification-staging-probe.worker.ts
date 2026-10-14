import type { Fetcher } from '@cloudflare/workers-types';
import {
  assertDeploymentIdentitySecret,
  credentialsMatch,
} from '../src/do-runner/deployment-identity.js';
import { readBoundedBody } from '../src/http-body.js';

const MAX_FORWARD_BODY_BYTES = 4_096;

interface Env {
  SUBJECT: Fetcher;
  STAGING_PROBE_SECRET: string;
  MAINTENANCE_ADMIN_SECRET?: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.hostname !== '127.0.0.1' && url.hostname !== 'localhost')
      return new Response(null, { status: 404 });
    if (url.search || url.hash) return new Response(null, { status: 404 });
    const staging =
      request.method === 'POST' &&
      ['/ensure', '/start', '/complete'].includes(url.pathname);
    const admin =
      (request.method === 'GET' &&
        ['/admin/execution-fence', '/admin/maintenance-status'].includes(
          url.pathname,
        )) ||
      (request.method === 'POST' && url.pathname === '/admin/execution-fence');
    if (!staging && !admin) return new Response(null, { status: 404 });
    try {
      assertDeploymentIdentitySecret(
        env.STAGING_PROBE_SECRET,
        'staging probe credential',
      );
    } catch {
      return new Response(null, { status: 503 });
    }
    const authorization = request.headers.get('authorization');
    if (
      !authorization?.startsWith('Bearer ') ||
      !(await credentialsMatch(
        authorization.slice('Bearer '.length),
        env.STAGING_PROBE_SECRET,
      ))
    )
      return new Response(null, { status: 403 });
    if (admin) {
      try {
        assertDeploymentIdentitySecret(
          env.MAINTENANCE_ADMIN_SECRET,
          'staging maintenance admin credential',
        );
      } catch {
        return new Response(null, { status: 503 });
      }
      if (
        await credentialsMatch(
          env.STAGING_PROBE_SECRET,
          env.MAINTENANCE_ADMIN_SECRET as string,
        )
      )
        return new Response(null, { status: 503 });
    }
    const hasBody =
      (staging && url.pathname === '/complete') ||
      (admin && request.method === 'POST');
    const raw = await readBoundedBody(
      request,
      hasBody ? MAX_FORWARD_BODY_BYTES : 0,
    );
    if (!raw.ok)
      return new Response(null, {
        status: raw.reason === 'payload-too-large' ? 413 : 400,
      });
    return env.SUBJECT.fetch(
      `http://subject${admin ? '' : '/__staging'}${url.pathname}`,
      {
        method: request.method,
        headers: {
          ...(admin
            ? { authorization: `Bearer ${env.MAINTENANCE_ADMIN_SECRET}` }
            : { 'x-flowsafe-staging-probe': env.STAGING_PROBE_SECRET }),
          ...(hasBody ? { 'content-type': 'application/json' } : {}),
        },
        ...(hasBody ? { body: raw.text } : {}),
      },
    ) as Promise<Response>;
  },
};
