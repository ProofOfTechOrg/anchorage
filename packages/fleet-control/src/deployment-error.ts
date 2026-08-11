// SPDX-License-Identifier: Apache-2.0

export class WorkerDeploymentError extends Error {
  readonly createdByAttempt: boolean;
  readonly resourceState: 'absent' | 'present' | 'unknown';

  constructor(options: {
    readonly message: string;
    readonly cause: unknown;
    readonly createdByAttempt: boolean;
    readonly resourceState: 'absent' | 'present' | 'unknown';
  }) {
    super(
      options.cause instanceof Error
        ? `${options.message}: ${options.cause.message}`
        : options.message,
      { cause: options.cause },
    );
    this.name = 'WorkerDeploymentError';
    this.createdByAttempt = options.createdByAttempt;
    this.resourceState = options.resourceState;
  }
}
