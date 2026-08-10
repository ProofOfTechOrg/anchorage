// SPDX-License-Identifier: Apache-2.0

export default {
  fetch(): Response {
    return new Response('breakwater worker test fixture');
  },
} satisfies ExportedHandler<Cloudflare.Env>;
