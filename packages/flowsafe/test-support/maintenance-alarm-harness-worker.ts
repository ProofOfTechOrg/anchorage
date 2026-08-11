// SPDX-License-Identifier: Apache-2.0
import { DurableObject } from 'cloudflare:workers';

import handler, {
  FlowsafeHub,
  FlowsafeMaintenance,
  FlowsafeRunner,
} from '../deploy/worker.js';

const HEALTH_KEY = 'flowsafe:maintenance-health:v1';
const TRACE_KEY = 'flowsafe:test-maintenance-alarm-trace:v1';

interface MaintenanceHealth {
  nextSweepAt: number;
  nextPurgeAt: number;
  lastSweepAttemptAt?: number;
  lastPurgeAttemptAt?: number;
  lastTickAttemptAt?: number;
}

export interface MaintenanceAlarmTrace {
  changedDuties: string[];
  events: string[];
  alarmAt: number | null;
}

type ProductionEnv = ConstructorParameters<typeof FlowsafeMaintenance>[1];

export class HarnessFlowsafeMaintenance extends DurableObject<ProductionEnv> {
  readonly #delegate: InstanceType<typeof FlowsafeMaintenance>;
  #events: string[] | undefined;

  constructor(ctx: DurableObjectState, env: ProductionEnv) {
    super(ctx, env);
    const state = {
      id: ctx.id,
      storage: {
        get: <T>(key: string) => ctx.storage.get<T>(key),
        getAlarm: () => ctx.storage.getAlarm(),
        setAlarm: (scheduledTime: number | Date) =>
          ctx.storage.setAlarm(scheduledTime),
        transaction: <T>(
          closure: (transaction: {
            get<V>(key: string): Promise<V | undefined>;
            put<V>(key: string, value: V): Promise<void>;
            setAlarm(scheduledTime: number | Date): Promise<void>;
          }) => Promise<T>,
        ) =>
          ctx.storage.transaction((transaction) =>
            closure({
              get: <V>(key: string) => transaction.get<V>(key),
              put: async <V>(key: string, value: V) => {
                if (key === HEALTH_KEY && this.#events) {
                  this.#events.push('health-persisted');
                }
                await transaction.put(key, value);
              },
              setAlarm: async (scheduledTime: number | Date) => {
                if (this.#events) this.#events.push('alarm-armed');
                await transaction.setAlarm(scheduledTime);
              },
            }),
          ),
      },
    };
    this.#delegate = new FlowsafeMaintenance(state, env);
  }

  fetch(request: Request): Promise<Response> {
    return this.#delegate.fetch(request);
  }

  async alarm(): Promise<void> {
    const before = await this.ctx.storage.get<MaintenanceHealth>(HEALTH_KEY);
    this.#events = [];
    try {
      await this.#delegate.alarm();
    } finally {
      const events = this.#events;
      this.#events = undefined;
      const after = await this.ctx.storage.get<MaintenanceHealth>(HEALTH_KEY);
      const changedDuties = (
        [
          ['sweep', 'lastSweepAttemptAt'],
          ['purge', 'lastPurgeAttemptAt'],
          ['tick', 'lastTickAttemptAt'],
        ] as const
      )
        .filter(([, key]) => after?.[key] !== before?.[key])
        .map(([duty]) => duty);
      const trace =
        (await this.ctx.storage.get<MaintenanceAlarmTrace[]>(TRACE_KEY)) ?? [];
      trace.push({
        changedDuties,
        events,
        alarmAt: await this.ctx.storage.getAlarm(),
      });
      await this.ctx.storage.put(TRACE_KEY, trace);
    }
  }

  async forceSweepAlarm(): Promise<void> {
    const now = Date.now();
    await this.ctx.storage.transaction(async (transaction) => {
      const health = await transaction.get<MaintenanceHealth>(HEALTH_KEY);
      if (!health) throw new Error('maintenance health is not initialized');
      health.nextSweepAt = now;
      await transaction.put(HEALTH_KEY, health);
      await transaction.setAlarm(now);
    });
    await this.alarm();
  }

  alarmTrace(): Promise<MaintenanceAlarmTrace[] | undefined> {
    return this.ctx.storage.get<MaintenanceAlarmTrace[]>(TRACE_KEY);
  }
}

export { FlowsafeHub, FlowsafeRunner };
export default handler;
