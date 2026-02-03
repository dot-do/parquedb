/**
 * Shared types for E2E Worker tests
 *
 * Extends the canonical ParqueDBDOStub with test-specific methods that are
 * exposed by the DO but not part of the public API contract.
 */

import type { ParqueDBDOStub } from '../../src/types/worker'

// Re-export the canonical stub type
export type { ParqueDBDOStub }

/**
 * Test environment bindings from wrangler.jsonc
 */
export interface TestEnv {
  BUCKET: R2Bucket
  PARQUEDB: DurableObjectNamespace
  ENVIRONMENT: string
}

/**
 * Extended DO stub interface for testing
 *
 * Includes internal methods exposed by ParqueDBDO that are useful for testing
 * but not part of the public API contract.
 */
export interface ParqueDBDOTestStub extends ParqueDBDOStub {
  // WAL operations (event batching)
  appendEventWithSeq(ns: string, event: {
    ts: number
    op: string
    target: string
    before?: unknown | undefined
    after?: unknown | undefined
    actor: string
  }): Promise<string>
  flushNsEventBatch(ns: string): Promise<void>
  flushAllNsEventBatches(): Promise<void>
  flushEventBatch(): Promise<void>

  // Counter/state methods (sync - requires prior async call to initialize)
  getSequenceCounter(ns: string): number
  getNsBufferState(ns: string): { eventCount: number; firstSeq: number; lastSeq: number; sizeBytes: number } | null

  // Query methods for WAL
  getUnflushedEventCount(): Promise<number>
  getUnflushedWalEventCount(ns: string): Promise<number>
  getTotalUnflushedWalEventCount(): Promise<number>
  getUnflushedWalBatchCount(): Promise<number>
  readUnflushedWalEvents(ns: string): Promise<Array<{
    id: string
    ts: number
    op: string
    target: string
    actor: string
  }>>
  deleteWalBatches(ns: string, upToSeq: number): Promise<void>

  // Pending row group methods (bulk operations)
  getPendingRowGroups(ns: string): Promise<Array<{
    id: string
    path: string
    rowCount: number
    firstSeq: number
    lastSeq: number
    createdAt: string
  }>>
  deletePendingRowGroups(ns: string, upToSeq: number): Promise<void>
  flushPendingToCommitted(ns: string): Promise<number>

  // Cache methods
  clearEntityCache(): void
  isEntityCached(ns: string, id: string): boolean

  // Transaction methods
  beginTransaction(): string
  commitTransaction(): Promise<void>
  rollbackTransaction(): Promise<void>
  isInTransaction(): boolean
}

/**
 * Helper to cast DO stub to test stub type
 */
export function asDOTestStub(stub: DurableObjectStub): ParqueDBDOTestStub {
  return stub as unknown as ParqueDBDOTestStub
}
