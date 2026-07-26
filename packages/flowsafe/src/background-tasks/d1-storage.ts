// SPDX-License-Identifier: Apache-2.0
// Track B (M-003) background-tasks storage seam.
//
// Cloudflare's adapter owns the D1 schemas. Flowsafe subclasses its workflow and
// background-task domains to add the guarantees core's global domains do not
// provide: serialized partial workflow updates inside one DO, tenant-filtered
// task access and recovery, and deletion of the unsalted internal workflow
// snapshot before its task row. `createBackgroundTaskD1Domains` composes the two
// subclasses as one inseparable execution configuration.

import type { D1Database } from '@cloudflare/workers-types';
import {
  BackgroundTasksStorageD1,
  type D1DomainConfig,
  WorkflowsStorageD1,
} from '@mastra/cloudflare-d1';
import type {
  BackgroundTask,
  TaskFilter,
  TaskListResult,
  UpdateBackgroundTask,
} from '@mastra/core/background-tasks';
import type { Mastra } from '@mastra/core/mastra';
import {
  type BackgroundTasksStorage,
  type MastraStorageDomains,
  mergeWorkflowStepResult,
  type UpdateWorkflowStateOptions,
} from '@mastra/core/storage';
import type { StepResult, WorkflowRunState } from '@mastra/core/workflows';

import {
  assertMintableTenantId,
  type D1DatabaseBinding,
  tenantOwnsSaltedId,
} from '../do-runner/index.js';

export type {
  PurgeExpiredBackgroundTasksOptions,
  PurgeExpiredBackgroundTasksResult,
} from '../do-runner/index.js';
export {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  purgeExpiredBackgroundTasks,
} from '../do-runner/index.js';

/**
 * The D1 background-tasks storage domain the manager persists to, reached
 * through the manager's own access path — `mastra.getStorage()` then
 * `getStore('backgroundTasks')`. The store lookup is asynchronous, so this
 * helper awaits it. It fails closed with a clear message
 * when a host wired a Mastra with no storage, or a storage adapter that does not
 * implement the `backgroundTasks` domain, rather than surfacing core's terser
 * "Background tasks storage is not available" only once the first task
 * dispatches.
 */
export async function backgroundTasksStore(
  mastra: Mastra,
): Promise<BackgroundTasksStorage> {
  const storage = mastra.getStorage();
  if (!storage) {
    throw new Error(
      'background-tasks: the hosting Mastra has no storage configured — background tasks need a D1 (or equivalent) composite store',
    );
  }
  const store = await storage.getStore('backgroundTasks');
  if (!store) {
    throw new Error(
      "background-tasks: the storage adapter does not implement the 'backgroundTasks' domain (use @mastra/cloudflare-d1, which ships BackgroundTasksStorageD1)",
    );
  }
  return store;
}

export const SERIALIZED_WORKFLOWS_D1: unique symbol = Symbol(
  'flowsafe.serializedWorkflowsD1',
);

/** Serialized D1 workflow updates for one single-tenant DO owner. */
export class DurableObjectWorkflowsStorageD1 extends WorkflowsStorageD1 {
  readonly [SERIALIZED_WORKFLOWS_D1] = true as const;
  readonly #tails = new Map<string, Promise<unknown>>();

  override supportsConcurrentUpdates(): boolean {
    return true;
  }

  #locked<T>(
    workflowName: string,
    runId: string,
    work: () => Promise<T>,
  ): Promise<T> {
    const key = `${workflowName}\0${runId}`;
    const previous = this.#tails.get(key) ?? Promise.resolve();
    const current = previous.then(work, work);
    const settled = current.then(
      () => undefined,
      () => undefined,
    );
    this.#tails.set(key, settled);
    return current.finally(() => {
      if (this.#tails.get(key) === settled) this.#tails.delete(key);
    });
  }

  #persistUnlocked(
    args: Parameters<WorkflowsStorageD1['persistWorkflowSnapshot']>[0],
  ): Promise<void> {
    return super.persistWorkflowSnapshot(args);
  }

  override persistWorkflowSnapshot(
    args: Parameters<WorkflowsStorageD1['persistWorkflowSnapshot']>[0],
  ): Promise<void> {
    return this.#locked(args.workflowName, args.runId, () =>
      this.#persistUnlocked(args),
    );
  }

  override updateWorkflowResults(args: {
    workflowName: string;
    runId: string;
    stepId: string;
    result: StepResult<unknown, unknown, unknown, unknown>;
    requestContext: Record<string, unknown>;
  }): Promise<Record<string, StepResult<unknown, unknown, unknown, unknown>>> {
    return this.#locked(args.workflowName, args.runId, async () => {
      const run = await super.getWorkflowRunById({
        workflowName: args.workflowName,
        runId: args.runId,
      });
      if (!run) return {};
      const snapshot =
        typeof run.snapshot === 'string'
          ? (JSON.parse(run.snapshot) as WorkflowRunState)
          : run.snapshot;
      const context = mergeWorkflowStepResult({
        snapshot,
        stepId: args.stepId,
        result: args.result,
        requestContext: args.requestContext,
      });
      await this.#persistUnlocked({
        workflowName: args.workflowName,
        runId: args.runId,
        resourceId: run.resourceId,
        snapshot,
      });
      return context;
    });
  }

  override updateWorkflowState(args: {
    workflowName: string;
    runId: string;
    opts: UpdateWorkflowStateOptions;
  }): Promise<WorkflowRunState | undefined> {
    return this.#locked(args.workflowName, args.runId, async () => {
      const run = await super.getWorkflowRunById({
        workflowName: args.workflowName,
        runId: args.runId,
      });
      if (!run) return undefined;
      const current =
        typeof run.snapshot === 'string'
          ? (JSON.parse(run.snapshot) as WorkflowRunState)
          : run.snapshot;
      const snapshot: WorkflowRunState = { ...current, ...args.opts };
      await this.#persistUnlocked({
        workflowName: args.workflowName,
        runId: args.runId,
        resourceId: run.resourceId,
        snapshot,
      });
      return snapshot;
    });
  }
}

export const TENANT_SCOPED_BACKGROUND_TASKS_D1: unique symbol = Symbol(
  'flowsafe.tenantScopedBackgroundTasksD1',
);

/** Tenant-bound D1 task domain used by one background-task manager DO. */
export class TenantScopedBackgroundTasksStorageD1 extends BackgroundTasksStorageD1 {
  readonly [TENANT_SCOPED_BACKGROUND_TASKS_D1] = true as const;
  readonly tenantId: string;
  readonly #workflows: DurableObjectWorkflowsStorageD1;

  constructor(
    config: D1DomainConfig,
    options: {
      tenantId: string;
      workflows: DurableObjectWorkflowsStorageD1;
    },
  ) {
    super(config);
    assertMintableTenantId(options.tenantId, 'backgroundTasks');
    this.tenantId = options.tenantId;
    this.#workflows = options.workflows;
  }

  #ownsRun(runId: string): boolean {
    return tenantOwnsSaltedId(this.tenantId, runId);
  }

  override async createTask(task: BackgroundTask): Promise<void> {
    if (!this.#ownsRun(task.runId)) {
      throw new Error(
        `background-tasks: parent run '${task.runId}' is not owned by tenant '${this.tenantId}'`,
      );
    }
    await super.createTask(task);
  }

  override async getTask(taskId: string): Promise<BackgroundTask | null> {
    const task = await super.getTask(taskId);
    return task && this.#ownsRun(task.runId) ? task : null;
  }

  override async updateTask(
    taskId: string,
    update: UpdateBackgroundTask,
  ): Promise<void> {
    if (!(await this.getTask(taskId))) {
      throw new Error(
        `background-tasks: task '${taskId}' is not owned by this tenant`,
      );
    }
    await super.updateTask(taskId, update);
  }

  override async listTasks(filter: TaskFilter): Promise<TaskListResult> {
    if (filter.runId !== undefined && !this.#ownsRun(filter.runId)) {
      return { tasks: [], total: 0 };
    }
    const { page, perPage, resourceId, ...unpaged } = filter;
    const all = await super.listTasks(unpaged);
    const owned = all.tasks.filter(
      (task) =>
        this.#ownsRun(task.runId) &&
        (resourceId === undefined || task.resourceId === resourceId),
    );
    if (perPage === undefined) return { tasks: owned, total: owned.length };
    const start = (page ?? 0) * perPage;
    return {
      tasks: owned.slice(start, start + perPage),
      total: owned.length,
    };
  }

  override async deleteTask(taskId: string): Promise<void> {
    const task = await this.getTask(taskId);
    if (!task) return;
    await this.#workflows.deleteWorkflowRunById({
      workflowName: '__background-task',
      runId: task.id,
    });
    await super.deleteTask(taskId);
  }

  override async deleteTasks(filter: TaskFilter): Promise<void> {
    const { tasks } = await this.listTasks({
      ...filter,
      page: undefined,
      perPage: undefined,
    });
    for (const task of tasks) await this.deleteTask(task.id);
  }

  override async getRunningCount(): Promise<number> {
    return (await this.listTasks({ status: 'running' })).total;
  }

  override async getRunningCountByAgent(agentId: string): Promise<number> {
    return (await this.listTasks({ status: 'running', agentId })).total;
  }
}

export interface CreateBackgroundTaskD1DomainsOptions {
  binding: D1DatabaseBinding;
  tenantId: string;
  tablePrefix?: string;
}

/** Coupled workflow/task overrides for one tenant background-task DO. */
export function createBackgroundTaskD1Domains(
  options: CreateBackgroundTaskD1DomainsOptions,
): MastraStorageDomains {
  const config: D1DomainConfig = {
    binding: options.binding as unknown as D1Database,
    ...(options.tablePrefix !== undefined
      ? { tablePrefix: options.tablePrefix }
      : {}),
  };
  const workflows = new DurableObjectWorkflowsStorageD1(config);
  const backgroundTasks = new TenantScopedBackgroundTasksStorageD1(config, {
    tenantId: options.tenantId,
    workflows,
  });
  return { workflows, backgroundTasks };
}
