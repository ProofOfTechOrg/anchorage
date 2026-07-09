// Subdomain <-> tenant cross-check for client-per-subdomain commercial hosts
// (<tenant>.example.com). DEFENSE IN DEPTH ONLY: the subdomain identifies the
// tenant for ROUTING; authorization is the token's verified tenant claim and
// the bound stores (INV-2). What this closes is the confused-deputy UX bug —
// tenant A's user pasting their token into tenant B's subdomain and quietly
// operating on A's data while LOOKING at B's branded host.
//
// The Host header is client-controllable at the edge of the internet but not
// through Cloudflare's routing to this worker (requests reach the zone's
// worker only for hosts the zone serves) — still, this check treats it as a
// cross-check, never the authorization boundary.

import type { TenantContext, TenantResolver } from '../approval-api/index.js';
import { TenantResolutionError } from '../approval-api/index.js';
import { RESERVED_TENANT_SLUGS } from './tenant-registry.js';

export interface SubdomainCrossCheckOptions {
  /**
   * The apex under which tenant subdomains live, e.g. 'example.com'. Hosts
   * NOT under it (workers.dev previews, localhost) skip the check.
   */
  apexDomain: string;
  /** Subdomains that are shared infrastructure, never tenants. */
  reserved?: readonly string[];
}

/**
 * The tenant slug a hostname addresses: `<slug>.<apex>` => slug; the apex
 * itself, hosts outside the apex, multi-level subdomains, and reserved slugs
 * => undefined (not tenant-addressed).
 */
export function subdomainTenantOf(
  hostname: string,
  options: SubdomainCrossCheckOptions,
): string | undefined {
  // DNS-equivalent normalization: 'bravo.example.com.' (the FQDN root-dot
  // form browsers pass through Host and URL untouched) names the same host
  // as 'bravo.example.com' — without stripping it, the dotted form would
  // silently SKIP the cross-check this module exists to apply. The WHOLE
  // trailing run goes, not one dot: multi-dot forms are not valid DNS, but
  // stripping them errs toward APPLYING the check (fail closed), and a
  // fat-fingered apex config ('example.com..') keeps matching instead of
  // silently disabling the check for every host.
  const apex = stripTrailingDots(options.apexDomain.toLowerCase());
  const host = stripTrailingDots(hostname.toLowerCase());
  if (host === apex || !host.endsWith(`.${apex}`)) return undefined;
  const label = host.slice(0, -(apex.length + 1));
  if (label.includes('.')) return undefined; // deeper levels are not tenants
  if ((options.reserved ?? RESERVED_TENANT_SLUGS).includes(label)) {
    return undefined;
  }
  return label;
}

function stripTrailingDots(name: string): string {
  return name.replace(/\.+$/, '');
}

/**
 * Decorate a TenantResolver: when the request addresses a tenant subdomain,
 * the resolved token tenant must BE that tenant — mismatch throws
 * TenantResolutionError (the routers' 403). Non-tenant hosts pass through.
 */
export function withSubdomainCrossCheck(
  resolve: TenantResolver,
  options: SubdomainCrossCheckOptions,
): TenantResolver {
  return async (request: Request): Promise<TenantContext | undefined> => {
    const tenant = await resolve(request);
    if (!tenant) return undefined;
    const hostTenant = subdomainTenantOf(
      new URL(request.url).hostname,
      options,
    );
    if (hostTenant !== undefined && hostTenant !== tenant.tenantId) {
      throw new TenantResolutionError(
        `token tenant '${tenant.tenantId}' does not match this host's tenant subdomain — sign in on your own tenant's host`,
      );
    }
    return tenant;
  };
}
