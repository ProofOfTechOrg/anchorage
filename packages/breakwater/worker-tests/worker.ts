// SPDX-License-Identifier: Apache-2.0

export default {
  fetch(request: Request): Response {
    if (new URL(request.url).pathname === '/redirect') {
      return new Response(null, {
        status: 302,
        headers: { Location: 'https://other.example/x' },
      });
    }
    return new Response('breakwater worker test fixture');
  },
} satisfies ExportedHandler<Cloudflare.Env>;
