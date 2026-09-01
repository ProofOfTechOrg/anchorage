// SPDX-License-Identifier: Apache-2.0

/**
 * GENERATED FILE — DO NOT EDIT BY HAND.
 *
 * Written by `scripts/record-audit-baseline.mjs` from the hand-authored world
 * in `fleet-audit-world.ts`. It freezes the observable behavior of
 * `auditFleetDrift()` (src/fleet.ts) before it is decomposed into bounded
 * stages, so the decomposition can be proven byte-equivalent. Verify with
 * `node scripts/record-audit-baseline.mjs --check`; any required change to
 * these literals is a compatibility break, not a fixture update.
 */

import type { DriftFinding } from '../../src/fleet.js';
import type { AuditOpLogEntry } from './fleet-audit-world.js';

/** Every finding `auditFleetDrift()` returned, in order. */
export const AUDIT_BASELINE_FINDINGS = [
  {
    tenantTag: 'seed-provider',
    environment: 'production',
    kind: 'stale-route',
    detail: 'seeded provider finding carried through the golden baseline',
  },
  {
    tenantTag: 'ghost-registration',
    environment: 'production',
    kind: 'orphan-deployment',
    detail:
      "registered script 'ghost-registered-script' has no live fleet owner",
  },
  {
    tenantTag: 'ghost-live',
    environment: 'production',
    kind: 'orphan-deployment',
    detail: "unregistered script 'ghost-live-script'",
  },
  {
    tenantTag: 'livedup',
    environment: 'production',
    kind: 'duplicate-deployment',
    detail: "expected lifecycle Worker 'livedup-worker' appears 2 times",
  },
  {
    tenantTag: 'missingdeploy',
    environment: 'production',
    kind: 'missing-deployment',
    detail:
      "expected lifecycle Worker 'missingdeploy-worker' is absent from provider inventory",
  },
  {
    tenantTag: 'stalenotready',
    environment: 'production',
    kind: 'missing-deployment',
    detail:
      "expected lifecycle Worker 'stalenotready-worker' is absent from provider inventory",
  },
  {
    tenantTag: 'unknown',
    environment: 'unknown',
    kind: 'orphan-database',
    detail: "unregistered fleet database 'db-orphan-ghost'",
  },
  {
    tenantTag: 'orphan-route-owner',
    environment: 'production',
    kind: 'orphan-route',
    detail:
      "route 'orphan-route.example.test' points to unregistered mapping 'nonexistent-script'",
  },
  {
    tenantTag: 'routedup',
    environment: 'production',
    kind: 'orphan-route',
    detail:
      "route 'routedup.example.test' points to unregistered mapping 'route-dup-ghost-script'",
  },
  {
    tenantTag: 'unknown',
    environment: 'unknown',
    kind: 'orphan-namespace',
    detail: "unregistered Durable Object namespace 'ns-orphan-ghost'",
  },
  {
    tenantTag: 'unknown',
    environment: 'unknown',
    kind: 'orphan-namespace',
    detail: "unregistered Durable Object namespace 'ns-bindingmismatch-wrong'",
  },
  {
    tenantTag: 'recdupb',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-recdupb' is absent from fleet inventory",
  },
  {
    tenantTag: 'dupexpnsb',
    environment: 'production',
    kind: 'duplicate-namespace',
    detail: "namespace 'ns-shared-expected' also bound to dupexpnsa:production",
  },
  {
    tenantTag: 'missingns',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-missing-expected' is absent from fleet inventory",
  },
  {
    tenantTag: 'missingdeploy',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-missingdeploy' is absent from fleet inventory",
  },
  {
    tenantTag: 'dbmismatch',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-dbmismatch' is absent from fleet inventory",
  },
  {
    tenantTag: 'bindingmismatch',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-bindingmismatch' is absent from fleet inventory",
  },
  {
    tenantTag: 'routebroken',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-routebroken' is absent from fleet inventory",
  },
  {
    tenantTag: 'platformdrift',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-platformdrift-missing' is absent from fleet inventory",
  },
  {
    tenantTag: 'platformdrift',
    environment: 'production',
    kind: 'duplicate-namespace',
    detail: "namespace 'ns-shared-expected' also bound to dupexpnsa:production",
  },
  {
    tenantTag: 'stalenotready',
    environment: 'production',
    kind: 'missing-namespace',
    detail:
      "expected Durable Object namespace 'ns-stalenotready' is absent from fleet inventory",
  },
  {
    tenantTag: 'r2dupb',
    environment: 'production',
    kind: 'r2-bucket-drift',
    detail: "R2 bucket 'shared-bucket' is claimed by more than one deployment",
  },
  {
    tenantTag: 'unknown',
    environment: 'unknown',
    kind: 'orphan-r2-bucket',
    detail: "unregistered fleet R2 bucket 'bucket-orphan-ghost'",
  },
  {
    tenantTag: 'r2missing',
    environment: 'production',
    kind: 'missing-r2-bucket',
    detail:
      "expected R2 bucket 'bucket-missing' is absent from fleet inventory",
  },
  {
    tenantTag: 'r2drift',
    environment: 'production',
    kind: 'r2-bucket-drift',
    detail: "R2 bucket 'bucket-drift' changed its persisted creation identity",
  },
  {
    tenantTag: 'recdupa',
    environment: 'production',
    kind: 'duplicate-deployment',
    detail: "script 'shared-record-script' is registered 2 times",
  },
  {
    tenantTag: 'recdupb',
    environment: 'production',
    kind: 'duplicate-deployment',
    detail: "script 'shared-record-script' is registered 2 times",
  },
  {
    tenantTag: 'dbmismatch',
    environment: 'production',
    kind: 'database-mismatch',
    detail:
      "fleet inventory does not contain exactly database 'db-dbmismatch' for 'dbmismatch-worker'",
  },
  {
    tenantTag: 'bindingmismatch',
    environment: 'production',
    kind: 'binding-drift',
    detail:
      'expected RUNNER:Runner:ns-bindingmismatch, found RUNNER:Runner:ns-bindingmismatch-wrong',
  },
  {
    tenantTag: 'routebroken',
    environment: 'production',
    kind: 'route-drift',
    detail:
      "deployment inventory does not contain exactly route 'routebroken.example.test'",
  },
  {
    tenantTag: 'routebroken',
    environment: 'production',
    kind: 'route-drift',
    detail: "route 'routebroken.example.test' is missing or mismatched",
  },
  {
    tenantTag: 'routedup',
    environment: 'production',
    kind: 'duplicate-route',
    detail: "route 'routedup.example.test' appears 2 times",
  },
  {
    tenantTag: 'platformdrift',
    environment: 'production',
    kind: 'version-drift',
    detail:
      "trusted Worker 'platform-drift-state-worker' has drifted ownership or artifact metadata",
  },
  {
    tenantTag: 'platformdrift',
    environment: 'production',
    kind: 'binding-drift',
    detail:
      "trusted state Worker 'platform-drift-state-worker' has drifted database, Durable Object, or egress bindings",
  },
  {
    tenantTag: 'platformdrift',
    environment: 'production',
    kind: 'version-drift',
    detail:
      "trusted Worker 'platform-drift-egress-worker' has drifted ownership or artifact metadata",
  },
  {
    tenantTag: 'platformdrift',
    environment: 'production',
    kind: 'binding-drift',
    detail:
      "trusted egress Worker 'platform-drift-egress-worker' has drifted policy or attribution bindings",
  },
  {
    tenantTag: 'channeldrift',
    environment: 'production',
    kind: 'binding-drift',
    detail:
      "release 'channeldrift-worker' has drifted trusted channel bindings",
  },
  {
    tenantTag: 'inspectabsent',
    environment: 'production',
    kind: 'missing-deployment',
    detail: "script 'inspectabsent-worker' is absent",
  },
  {
    tenantTag: 'maintstale',
    environment: 'production',
    kind: 'maintenance-stale',
    detail: 'maintenance scheduler is not armed',
  },
  {
    tenantTag: 'rearmfail',
    environment: 'production',
    kind: 'maintenance-stale',
    detail: 'maintenance scheduler is not armed',
  },
  {
    tenantTag: 'rearmfail',
    environment: 'production',
    kind: 'audit-error',
    detail: 'maintenance re-arm failed: Error: maintenance re-arm failed',
  },
  {
    tenantTag: 'stalenotready',
    environment: 'production',
    kind: 'incomplete-provisioning',
    detail: "phase 'worker-deployed' has not advanced",
  },
  {
    tenantTag: 'wfprelease',
    environment: 'production',
    kind: 'version-drift',
    detail:
      "lifecycle release 'wfp-release-pending' does not match its persisted identity, artifact, schema, and spec digest",
  },
  {
    tenantTag: 'wfprelease',
    environment: 'production',
    kind: 'audit-error',
    detail:
      "lifecycle release 'wfp-release-pending' has no durable binding topology",
  },
  {
    tenantTag: 'wfprelease',
    environment: 'production',
    kind: 'version-drift',
    detail:
      "lifecycle release 'wfp-release-active' does not match its persisted identity, artifact, schema, and spec digest",
  },
  {
    tenantTag: 'wfprelease',
    environment: 'production',
    kind: 'database-mismatch',
    detail:
      "lifecycle release 'wfp-release-active' is not bound exactly to database 'db-wfprelease'",
  },
  {
    tenantTag: 'wfprelease',
    environment: 'production',
    kind: 'binding-drift',
    detail:
      "lifecycle release 'wfp-release-active' has drifted Durable Object, service, queue, application variable, R2, or secret topology",
  },
] as const satisfies readonly DriftFinding[];

/**
 * Every `withDeploymentLease`/`get`/`put`/`inspect`/`ensureMaintenance`
 * call, every `resolver:<kind>` invocation, and every `lease.assertOwned()`
 * call `auditFleetDrift()` made, in order. `list`/`renew`/`delete` are in
 * `AuditOpLogEntry`'s vocabulary but never appear here (defensive, unused by
 * this pre-decomposition world).
 */
export const AUDIT_BASELINE_OPS = [
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'withDeploymentLease',
  'get',
  'put',
  'assertOwned:maintstale:production',
  'ensureMaintenance',
  'resolver:backendFor',
  'resolver:specFor',
  'resolver:maintenanceSecretFor',
  'inspect',
  'withDeploymentLease',
  'get',
  'put',
  'assertOwned:rearmfail:production',
  'ensureMaintenance',
] as const satisfies readonly AuditOpLogEntry[];
