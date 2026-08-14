// SPDX-License-Identifier: Apache-2.0
// Deployment-scoped approval-store factories. Each factory memoizes both the
// store and its schema gate for reuse across request contexts.

import {
  type ApprovalDatabase,
  createApprovalSchema,
  D1ApprovalStore,
} from './d1-store.js';
import {
  createResourceOwnershipSchema,
  D1ResourceOwnershipStore,
  InMemoryResourceOwnershipStore,
  type RecoverableResourceOwnershipStore,
  type ResourceOwnershipStore,
} from './resource-ownership.js';
import { type ApprovalStore, InMemoryApprovalStore } from './store.js';

export interface ApprovalStoreFactory {
  store(): ApprovalStore;
  resources(): ResourceOwnershipStore;
}

export interface D1ApprovalStoreFactoryOptions {
  /** Existing Mastra snapshot table used to atomically fence human decisions. */
  workflowSnapshotTable?: string;
}

export class D1ApprovalStoreFactory implements ApprovalStoreFactory {
  readonly #db: ApprovalDatabase;
  readonly #options: D1ApprovalStoreFactoryOptions;
  #schemaReady?: Promise<void>;
  #resourceSchemaReady?: Promise<void>;
  #store?: ApprovalStore;
  #resources?: RecoverableResourceOwnershipStore;

  constructor(
    db: ApprovalDatabase,
    options: D1ApprovalStoreFactoryOptions = {},
  ) {
    this.#db = db;
    this.#options = options;
  }

  #ready = (): Promise<void> => {
    this.#schemaReady ??= createApprovalSchema(this.#db).catch(
      (error: unknown) => {
        this.#schemaReady = undefined;
        throw error;
      },
    );
    return this.#schemaReady;
  };

  store(): ApprovalStore {
    this.#store ??= new D1ApprovalStore(this.#db, {
      ready: this.#ready,
      ...this.#options,
    });
    return this.#store;
  }

  resources(): RecoverableResourceOwnershipStore {
    const ready = (): Promise<void> => {
      this.#resourceSchemaReady ??= createResourceOwnershipSchema(
        this.#db,
      ).catch((error: unknown) => {
        this.#resourceSchemaReady = undefined;
        throw error;
      });
      return this.#resourceSchemaReady;
    };
    this.#resources ??= new D1ResourceOwnershipStore(this.#db, { ready });
    return this.#resources;
  }
}

export class InMemoryApprovalStoreFactory implements ApprovalStoreFactory {
  readonly #store = new InMemoryApprovalStore();
  readonly #resources = new InMemoryResourceOwnershipStore();

  store(): ApprovalStore {
    return this.#store;
  }

  resources(): RecoverableResourceOwnershipStore {
    return this.#resources;
  }
}
