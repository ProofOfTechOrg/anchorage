// SPDX-License-Identifier: Apache-2.0

export interface DurableDatabaseExportStore {
  write(input: {
    readonly databaseId: string;
    readonly fileName: string;
    readonly body: ReadableStream<Uint8Array>;
    readonly contentLength?: number;
  }): Promise<{
    readonly location: string;
    readonly size: number;
    readonly sha256: string;
  }>;
}
