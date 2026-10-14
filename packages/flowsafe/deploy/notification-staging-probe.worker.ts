import type { Fetcher } from '@cloudflare/workers-types';
import {
  assertDeploymentIdentitySecret,
  credentialsMatch,
} from '../src/do-runner/deployment-identity.js';

interface Env {
  SUBJECT: Fetcher;
  STAGING_PROBE_SECRET: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.hostname !== '127.0.0.1' && url.hostname !== 'localhost')
      return new Response(null, { status: 404 });
    if (
      request.method !== 'POST' ||
      !['/ensure', '/start', '/complete'].includes(url.pathname)
    )
      return new Response(null, { status: 404 });
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
    return env.SUBJECT.fetch(`http://subject/__staging${url.pathname}`, {
      method: 'POST',
      headers: {
        'x-flowsafe-staging-probe': env.STAGING_PROBE_SECRET,
        ...(url.pathname === '/complete'
          ? { 'content-type': 'application/json' }
          : {}),
      },
      ...(url.pathname === '/complete' ? { body: await request.text() } : {}),
    }) as Promise<Response>;
  },
};
