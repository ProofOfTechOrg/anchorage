// SPDX-License-Identifier: Apache-2.0

interface Env {
  DB: D1Database;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
  RUNNER: DurableObjectNamespace<import('./worker.js').StarterRunner>;
  MAINTENANCE: DurableObjectNamespace;
  HUB: DurableObjectNamespace<import('./worker.js').StarterHub>;
  THREAD: DurableObjectNamespace<import('./worker.js').StarterThread>;
  SIGNAL_PROVIDER_HOST: DurableObjectNamespace<
    import('./worker.js').StarterSignalProviderHost
  >;
  BACKGROUND_TASKS: DurableObjectNamespace<
    import('./worker.js').StarterBackgroundTasks
  >;
  AUTH_JWT_ISSUER: string;
  AUTH_JWT_AUDIENCE: string;
  MODEL_ID: string;
  APPROVAL_SLA_SECONDS: string;
  RUN_RETENTION_DAYS: string;
  APPROVAL_RETENTION_DAYS: string;
  THREAD_RETENTION_DAYS: string;
  NOTIFICATION_RETENTION_DAYS: string;
  THREAD_STATE_RETENTION_DAYS: string;
  SCHEDULE_TRIGGER_RETENTION_DAYS: string;
  SIGNAL_ATTRIBUTE_ALLOWLIST: string;
  GITHUB_RESOURCE_ALLOWLIST: string;
  AUTH_HMAC_SECRET?: string;
  MAINTENANCE_ADMIN_SECRET?: string;
  STREAM_TICKET_SECRET?: string;
  MODEL_API_KEY?: string;
  MODEL_BASE_URL?: string;
  GITHUB_WEBHOOK_SECRET?: string;
}
