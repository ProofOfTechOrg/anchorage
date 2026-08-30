// SPDX-License-Identifier: Apache-2.0

import type { WorkerModule } from '../../src/types.js';

interface SqliteStatement {
  all(...params: unknown[]): unknown[];
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
}

interface RecordedStatement {
  readonly sql: string;
  readonly bindings: readonly string[];
  readonly mode: 'prepare' | 'exec';
}

function openSqlite(): SqliteDatabase {
  // getBuiltinModule avoids vite's resolver, which cannot resolve node:sqlite;
  // node:sqlite has been unflagged since Node 22.13.
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error('node:sqlite unavailable — tests require node >= 22.13');
  }
  const sqlite = getBuiltin('node:sqlite') as {
    DatabaseSync: new (path: string) => SqliteDatabase;
  };
  return new sqlite.DatabaseSync(':memory:');
}

export class D1State {
  readonly #database = openSqlite();
  readonly #statementLog: RecordedStatement[] = [];

  queryDatabase(
    sql: string,
    bindings: readonly string[] = [],
  ): readonly Readonly<Record<string, unknown>>[] {
    const rows = this.#database.prepare(sql).all(...bindings);
    this.#statementLog.push({ sql, bindings: [...bindings], mode: 'prepare' });
    return rows.flatMap((row) =>
      row && typeof row === 'object'
        ? [row as Readonly<Record<string, unknown>>]
        : [],
    );
  }

  batchDatabase(
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly string[];
    }[],
  ): void {
    const recorded: RecordedStatement[] = [];
    this.#database.exec('BEGIN');
    try {
      for (const statement of statements) {
        const bindings = statement.bindings ?? [];
        if (bindings.length === 0) {
          this.#database.exec(statement.sql);
          recorded.push({
            sql: statement.sql,
            bindings: [],
            mode: 'exec',
          });
        } else {
          this.#database.prepare(statement.sql).all(...bindings);
          recorded.push({
            sql: statement.sql,
            bindings: [...bindings],
            mode: 'prepare',
          });
        }
      }
      this.#database.exec('COMMIT');
      this.#statementLog.push(...recorded);
    } catch (error) {
      this.#database.exec('ROLLBACK');
      throw error;
    }
  }

  clone(): D1State {
    const cloned = new D1State();
    for (const statement of this.#statementLog) {
      if (statement.mode === 'exec') {
        cloned.#database.exec(statement.sql);
      } else {
        cloned.#database.prepare(statement.sql).all(...statement.bindings);
      }
      cloned.#statementLog.push({
        ...statement,
        bindings: [...statement.bindings],
      });
    }
    return cloned;
  }
}

export interface ProviderVersion {
  readonly versionId: string;
  readonly tag: string | undefined;
  readonly bindings: readonly unknown[];
  readonly mainModule: string;
  readonly modules: readonly WorkerModule[];
}

export interface ProviderScript {
  present: boolean;
  versions: ProviderVersion[];
  deployment?: Array<{ versionId: string; percentage: number }>;
  subdomain: { enabled: boolean; previewsEnabled: boolean };
  secretNames: Set<string>;
}

export interface ProviderDatabase {
  readonly databaseId: string;
  readonly name: string;
  readonly d1: D1State;
}

export type ProviderDatabaseIdMode = 'sequence' | 'uuid';

export interface ProviderUpload {
  readonly scriptName: string;
  readonly mode: 'initial' | 'staged';
  readonly tag: string | undefined;
  readonly bindings: readonly unknown[];
  readonly mainModule: string;
  readonly modules: readonly WorkerModule[];
  readonly publicAccess?: {
    readonly workersDevEnabled: boolean;
    readonly previewUrlsEnabled: boolean;
  };
}

/** A one-shot provider failure at a logical backend operation boundary. */
export interface ProviderFailure {
  /**
   * Whether the selected request commits before its response is lost. A
   * non-dispatched failure settles without its own handler recording a
   * mutation. The CLI projection throws a supplied `error` from every
   * operation it serves; on the REST projection only the two upload handlers
   * do, because the upload dispatch is the sanitizer's one call site
   * (`sanitizeProviderError` inside `dispatchOrdinaryWorkerUpload`) and a
   * returned 400 would erase the injected message there. The other REST
   * handlers uniformly answer a non-retryable 400, because an endpoint on the
   * client's default `maxRetries` budget would otherwise retry a thrown error
   * and commit the mutation on the retry, with the one-shot hook already
   * consumed.
   */
  readonly dispatched: boolean;
  readonly duplicate?: boolean;
  readonly error?: Error;
  /** REST-only; honoured by upload handlers and rejected by the CLI projection. */
  readonly response?: Response;
  /**
   * Selects where a DISPATCHED REST initial upload settles: the public-access
   * POST by default (both mutations committed, the response lost) or `script`
   * (the script PUT committed, public access left unchanged). The initial
   * script PUT refuses `{ dispatched: false, at: 'public-access' }` and
   * settles any other non-dispatched failure before it records the upload.
   * The staged versions POST never reads this selector and settles a
   * non-dispatched failure before it records the version; the direct client
   * may already have converged public access for that staged upload. A
   * supplied `error` is thrown from the fetch at `script`, at the dispatched
   * staged versions POST, and at both non-dispatched upload sites; each of
   * those throws happens inside `send()` with `maxRetries: 0`, so the SDK
   * wraps it as `APIConnectionError`, whose cause chain the sanitizer
   * rebuilds rather than collapsing. It is refused at the public-access POST,
   * which is outside the sanitizer and keeps the SDK's retries. The CLI lane
   * ignores this selector because one deploy command commits the complete
   * operation.
   */
  readonly at?: 'script' | 'public-access';
}

export interface WorkerRoute {
  readonly zoneId: string;
  readonly id: string;
  readonly pattern: string;
  readonly script: string;
}

type AfterEffect = (world: ProviderWorld) => void | Promise<void>;

class WorldAllocators {
  #counters = new Map<string, number>();

  versionId(world: ProviderWorld): string {
    return this.#next(
      'version',
      new Set(
        [...world.scripts.values()].flatMap(({ versions }) =>
          versions.map(({ versionId }) => versionId),
        ),
      ),
    );
  }

  namespaceId(world: ProviderWorld): string {
    return this.#next(
      'namespace',
      new Set(world.durableObjectNamespaces.map(({ id }) => id)),
    );
  }

  databaseId(world: ProviderWorld, mode: ProviderDatabaseIdMode): string {
    const occupied = new Set(
      world.databases.map(({ databaseId }) => databaseId),
    );
    if (mode === 'sequence') return this.#next('database', occupied);
    for (;;) {
      const next = (this.#counters.get('database') ?? 0) + 1;
      this.#counters.set('database', next);
      const candidate = `00000000-0000-4000-8000-${String(next).padStart(12, '0')}`;
      if (!occupied.has(candidate)) return candidate;
    }
  }

  domainId(world: ProviderWorld): string {
    return this.#next(
      'domain',
      new Set(world.customDomains.map(({ id }) => id)),
    );
  }

  clone(): WorldAllocators {
    const cloned = new WorldAllocators();
    cloned.#counters = new Map(this.#counters);
    return cloned;
  }

  #next(prefix: string, occupied: Set<string>): string {
    for (;;) {
      const next = (this.#counters.get(prefix) ?? 0) + 1;
      this.#counters.set(prefix, next);
      const candidate = `${prefix}-${next}`;
      if (!occupied.has(candidate)) return candidate;
    }
  }
}

function cloneModule(module: WorkerModule): WorkerModule {
  return {
    ...module,
    content:
      module.content instanceof Uint8Array
        ? new Uint8Array(module.content)
        : module.content,
  };
}

function cloneBinding(binding: unknown): unknown {
  return structuredClone(binding);
}

function stringField(value: unknown, field: string): string | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const candidate = Reflect.get(value, field);
  return typeof candidate === 'string' ? candidate : undefined;
}

function bindingType(value: unknown): string | undefined {
  return stringField(value, 'type');
}

function secretNamesFrom(bindings: readonly unknown[]): Set<string> {
  return new Set(
    bindings.flatMap((binding) => {
      const name = stringField(binding, 'name');
      return bindingType(binding) === 'secret_text' && name ? [name] : [];
    }),
  );
}

export class ProviderWorld {
  readonly scripts = new Map<string, ProviderScript>();
  readonly databases: ProviderDatabase[] = [];
  readonly customDomains: Array<{
    id: string;
    hostname: string;
    service: string;
  }> = [];
  readonly zones: Array<{ id: string }> = [];
  readonly routes: WorkerRoute[] = [];
  readonly durableObjectNamespaces: Array<{
    id: string;
    script: string;
    className: string;
  }> = [];
  readonly dispatchNamespaces: Array<{
    name: string;
    scripts: Array<{ name: string; bindings: readonly unknown[] }>;
  }> = [];
  readonly exports = new Map<string, Uint8Array>();
  readonly mutationLog: string[] = [];
  maintenanceOrigin = 'https://control-acme.example.test';
  routeOrigin = 'https://acme.example.test';
  #allocators = new WorldAllocators();
  readonly #failures = new Map<string, ProviderFailure>();
  readonly #afterEffects = new Map<string, AfterEffect>();
  readonly #deferredFailures = new Set<string>();

  constructor(readonly databaseIdMode: ProviderDatabaseIdMode = 'sequence') {}

  failNext(operation: string, failure: ProviderFailure): void {
    if (this.#failures.has(operation)) {
      throw new Error(`failure already registered for '${operation}'`);
    }
    this.#failures.set(operation, { ...failure });
  }

  afterNext(operation: string, effect: AfterEffect): void {
    if (this.#afterEffects.has(operation)) {
      throw new Error(`after-effect already registered for '${operation}'`);
    }
    this.#afterEffects.set(operation, effect);
  }

  consumeFailure(operation: string): ProviderFailure | undefined {
    const failure = this.#failures.get(operation);
    this.#failures.delete(operation);
    this.#deferredFailures.delete(operation);
    return failure;
  }

  peekFailure(operation: string): ProviderFailure | undefined {
    return this.#failures.get(operation);
  }

  deferFailure(operation: string): void {
    if (!this.#failures.has(operation)) {
      throw new Error(`cannot defer missing failure '${operation}'`);
    }
    this.#deferredFailures.add(operation);
  }

  consumeDeferredFailure(operation: string): ProviderFailure | undefined {
    if (!this.#deferredFailures.has(operation)) return undefined;
    return this.consumeFailure(operation);
  }

  pendingHookNames(): readonly string[] {
    return [
      ...[...this.#failures.keys()].map((name) => `failure:${name}`),
      ...[...this.#afterEffects.keys()].map((name) => `after:${name}`),
    ].sort();
  }

  async applyAfter(operation: string): Promise<void> {
    const effect = this.#afterEffects.get(operation);
    this.#afterEffects.delete(operation);
    await effect?.(this);
  }

  createDatabase(
    name: string,
    databaseId = this.#allocators.databaseId(this, this.databaseIdMode),
  ) {
    const database = this.#insertDatabase(name, databaseId);
    this.mutationLog.push(`create-database:${databaseId}`);
    return database;
  }

  seedDatabase(
    name: string,
    options: {
      readonly databaseId?: string;
      readonly exportBytes?: Uint8Array;
    } = {},
  ): ProviderDatabase {
    const database = this.#insertDatabase(
      name,
      options.databaseId ??
        this.#allocators.databaseId(this, this.databaseIdMode),
    );
    if (options.exportBytes) {
      this.exports.set(
        database.databaseId,
        new Uint8Array(options.exportBytes),
      );
    }
    return database;
  }

  seedScript(
    scriptName: string,
    script: Omit<ProviderScript, 'present' | 'secretNames'> &
      Partial<Pick<ProviderScript, 'present' | 'secretNames'>>,
  ): ProviderScript {
    const seeded: ProviderScript = {
      present: script.present ?? true,
      versions: script.versions.map((version) => ({
        ...version,
        bindings: version.bindings.map(cloneBinding),
        modules: version.modules.map(cloneModule),
      })),
      ...(script.deployment
        ? { deployment: script.deployment.map((version) => ({ ...version })) }
        : {}),
      subdomain: { ...script.subdomain },
      secretNames:
        script.secretNames ??
        secretNamesFrom(script.versions[0]?.bindings ?? []),
    };
    this.scripts.set(scriptName, seeded);
    return seeded;
  }

  applyUpload(upload: ProviderUpload, options: { duplicate?: boolean } = {}) {
    const script = this.scripts.get(upload.scriptName) ?? {
      present: true,
      versions: [],
      subdomain: { enabled: false, previewsEnabled: false },
      secretNames: new Set<string>(),
    };
    script.present = true;
    const count = options.duplicate ? 2 : 1;
    const versions: ProviderVersion[] = [];
    for (let index = 0; index < count; index += 1) {
      const bindings = upload.bindings.map((binding) => {
        const cloned = cloneBinding(binding);
        if (
          bindingType(cloned) !== 'durable_object_namespace' ||
          !cloned ||
          typeof cloned !== 'object'
        ) {
          return cloned;
        }
        const className = stringField(cloned, 'class_name');
        if (!className) return cloned;
        let namespace = this.durableObjectNamespaces.find(
          (candidate) =>
            candidate.script === upload.scriptName &&
            candidate.className === className,
        );
        if (!namespace) {
          namespace = {
            id: this.#allocators.namespaceId(this),
            script: upload.scriptName,
            className,
          };
          this.durableObjectNamespaces.push(namespace);
        }
        Reflect.set(cloned, 'namespace_id', namespace.id);
        return cloned;
      });
      const version = {
        versionId: this.#allocators.versionId(this),
        tag: upload.tag,
        bindings,
        mainModule: upload.mainModule,
        modules: upload.modules.map(cloneModule),
      };
      script.versions.unshift(version);
      versions.push(version);
    }
    script.secretNames = secretNamesFrom(versions[0]?.bindings ?? []);
    if (upload.mode === 'initial') {
      script.deployment = [
        { versionId: versions[0]?.versionId ?? '', percentage: 100 },
      ];
    }
    if (upload.publicAccess) {
      // wrangler deploy applies workers_dev and preview_urls from its config in
      // the same command, so only uploads carrying that config write this state.
      script.subdomain = {
        enabled: upload.publicAccess.workersDevEnabled,
        previewsEnabled: upload.publicAccess.previewUrlsEnabled,
      };
    }
    this.scripts.set(upload.scriptName, script);
    this.mutationLog.push(`upload:${upload.scriptName}`);
    return versions;
  }

  allocateDomainId(): string {
    return this.#allocators.domainId(this);
  }

  deleteScript(scriptName: string): void {
    const script = this.scripts.get(scriptName);
    if (script) {
      script.present = false;
      script.versions.length = 0;
      delete script.deployment;
      script.secretNames.clear();
      script.subdomain = { enabled: false, previewsEnabled: false };
    }
    this.removeScriptNamespaces(scriptName);
    this.mutationLog.push(`delete-script:${scriptName}`);
  }

  applyDeployment(
    scriptName: string,
    deployment: readonly {
      readonly versionId: string;
      readonly percentage: number;
    }[],
  ): 'deploy' | 'deploy-candidate' {
    const script = this.scripts.get(scriptName);
    if (!script) throw new Error(`cannot deploy absent Worker '${scriptName}'`);
    script.deployment = deployment.map((version) => ({ ...version }));
    const operation =
      deployment.length === 1 && deployment[0]?.percentage === 100
        ? 'deploy'
        : 'deploy-candidate';
    this.mutationLog.push(`${operation}:${scriptName}`);
    return operation;
  }

  removeScriptNamespaces(scriptName: string): void {
    for (
      let index = this.durableObjectNamespaces.length - 1;
      index >= 0;
      index -= 1
    ) {
      if (this.durableObjectNamespaces[index]?.script === scriptName) {
        this.durableObjectNamespaces.splice(index, 1);
      }
    }
  }

  clone(): ProviderWorld {
    const cloned = new ProviderWorld(this.databaseIdMode);
    cloned.maintenanceOrigin = this.maintenanceOrigin;
    cloned.routeOrigin = this.routeOrigin;
    cloned.#allocators = this.#allocators.clone();
    for (const [name, script] of this.scripts) {
      cloned.seedScript(name, {
        present: script.present,
        versions: script.versions,
        ...(script.deployment ? { deployment: script.deployment } : {}),
        subdomain: script.subdomain,
        secretNames: new Set(script.secretNames),
      });
    }
    cloned.databases.push(
      ...this.databases.map(({ databaseId, name, d1 }) => ({
        databaseId,
        name,
        d1: d1.clone(),
      })),
    );
    cloned.customDomains.push(
      ...this.customDomains.map((domain) => ({ ...domain })),
    );
    cloned.zones.push(...this.zones.map((zone) => ({ ...zone })));
    cloned.routes.push(...this.routes.map((route) => ({ ...route })));
    cloned.durableObjectNamespaces.push(
      ...this.durableObjectNamespaces.map((namespace) => ({ ...namespace })),
    );
    cloned.dispatchNamespaces.push(
      ...this.dispatchNamespaces.map((namespace) => ({
        name: namespace.name,
        scripts: namespace.scripts.map((script) => ({
          name: script.name,
          bindings: script.bindings.map(cloneBinding),
        })),
      })),
    );
    for (const [databaseId, bytes] of this.exports) {
      cloned.exports.set(databaseId, new Uint8Array(bytes));
    }
    cloned.mutationLog.push(...this.mutationLog);
    for (const [operation, failure] of this.#failures) {
      cloned.#failures.set(operation, { ...failure });
    }
    for (const [operation, effect] of this.#afterEffects) {
      cloned.#afterEffects.set(operation, effect);
    }
    for (const operation of this.#deferredFailures) {
      cloned.#deferredFailures.add(operation);
    }
    return cloned;
  }

  #insertDatabase(name: string, databaseId: string): ProviderDatabase {
    const database = { databaseId, name, d1: new D1State() };
    this.databases.push(database);
    this.exports.set(
      databaseId,
      new TextEncoder().encode('CREATE TABLE example (id TEXT PRIMARY KEY);\n'),
    );
    return database;
  }
}

export function providerWorld(
  databaseIdMode: ProviderDatabaseIdMode = 'sequence',
): ProviderWorld {
  return new ProviderWorld(databaseIdMode);
}

function versionDigest(
  version: ProviderVersion | undefined,
): string | undefined {
  return version?.bindings.flatMap((binding) =>
    bindingType(binding) === 'plain_text' &&
    stringField(binding, 'name') === 'FLEET_SPEC_DIGEST'
      ? [stringField(binding, 'text')]
      : [],
  )[0];
}

function maintenanceBody(digest: string | undefined): Response {
  return Response.json({
    alarmAt: 2_000,
    lastSweepAt: 1_000,
    lastPurgeAt: 1_000,
    ...(digest ? { deploymentSpecDigest: digest } : {}),
  });
}

export async function maintenanceResponder(
  world: ProviderWorld,
  request: {
    readonly method: string;
    readonly url: string;
    readonly headers: Headers;
  },
): Promise<Response | undefined> {
  const target = new URL(request.url);
  if (target.origin !== world.maintenanceOrigin) return undefined;
  const override = request.headers.get('Cloudflare-Workers-Version-Overrides');
  const match = override?.match(/^([^=]+)="([^"]+)"$/u);
  const routedScript = world.customDomains.find(
    ({ hostname }) => hostname === new URL(world.routeOrigin).hostname,
  )?.service;
  const scriptName =
    match?.[1] ??
    routedScript ??
    [...world.scripts.entries()].find(
      ([, candidate]) => candidate.present,
    )?.[0];
  const state = scriptName ? world.scripts.get(scriptName) : undefined;
  if (!scriptName || !state?.present) return maintenanceBody(undefined);
  if (
    request.method === 'POST' &&
    target.pathname === '/admin/ensure-maintenance'
  ) {
    const failure = world.consumeFailure('ensureMaintenance');
    if (failure && !failure.dispatched) {
      return new Response(
        failure.error?.message ?? 'injected maintenance failure',
        { status: 400 },
      );
    }
    const version =
      match?.[1] === scriptName
        ? state.versions.find(({ versionId }) => versionId === match[2])
        : undefined;
    await world.applyAfter('ensureMaintenance');
    if (failure) {
      return new Response(
        failure.error?.message ?? 'injected maintenance failure',
        { status: 400 },
      );
    }
    return maintenanceBody(versionDigest(version));
  }
  if (
    request.method === 'GET' &&
    target.pathname === '/admin/maintenance-status'
  ) {
    const activeId = state.deployment?.find(
      ({ percentage }) => percentage === 100,
    )?.versionId;
    const active = state.versions.find(({ versionId }) =>
      match?.[1] === scriptName
        ? versionId === match[2]
        : versionId === activeId,
    );
    await world.applyAfter('readMaintenance');
    return maintenanceBody(versionDigest(active));
  }
  return new Response('maintenance endpoint refused', { status: 404 });
}
