// SPDX-License-Identifier: Apache-2.0
import type { DeploymentIdentityDatabase } from '../src/do-runner/deployment-identity.js';
import { DEPLOYMENT_IDENTITY_HEADER } from '../src/do-runner/deployment-identity.js';
import { openSqlite, sqliteUnitDatabase } from './sqlite.js';

export const TEST_DEPLOYMENT_IDENTITY_SECRET =
  'test-deployment-identity-secret-0001';

export function deploymentIdentityDatabase(
  tag = 'acme',
): DeploymentIdentityDatabase {
  const db = openSqlite();
  db.exec(`
    CREATE TABLE flowsafe_deployment (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      tenant_tag TEXT NOT NULL,
      provisioned_at TEXT NOT NULL
    );
  `);
  db.prepare(
    'INSERT INTO flowsafe_deployment (id, tenant_tag, provisioned_at) VALUES (1, ?, ?)',
  ).run(tag, '2026-08-10T00:00:00.000Z');
  return sqliteUnitDatabase(db) as DeploymentIdentityDatabase;
}

export function deploymentIdentityRequest(
  input: string | URL | Request,
  init?: RequestInit,
  secret = TEST_DEPLOYMENT_IDENTITY_SECRET,
): Request {
  const request = new Request(input, init);
  request.headers.set(DEPLOYMENT_IDENTITY_HEADER, secret);
  return request;
}
