/**
 * MergeTree Engine Test Worker
 *
 * Minimal Cloudflare Worker + Durable Object used exclusively by
 * vitest-pool-workers to exercise SqliteWal and DOCompactor with real
 * DO SQLite + R2 bindings.
 *
 * This is separate from the production ParqueDBDO so engine tests
 * remain isolated and fast.
 */

import { DurableObject } from 'cloudflare:workers'
import { SqliteWal } from './sqlite-wal'
import { DOCompactor } from './do-compactor'
import { DOReadPath } from './do-read-path'
import { encodeDataToParquet, encodeRelsToParquet, encodeEventsToParquet } from './parquet-encoders'
import type { DataLine, RelLine } from './types'
import type { AnyEventLine } from './merge-events'

// ---------------------------------------------------------------------------
// Env
// ---------------------------------------------------------------------------

interface Env {
  BUCKET: R2Bucket
}

// ---------------------------------------------------------------------------
// MergeTreeDO — thin wrapper that exposes SqliteWal + DOCompactor methods via RPC
// ---------------------------------------------------------------------------

export class MergeTreeDO extends DurableObject {
  private wal: SqliteWal
  private compactor: DOCompactor
  private readPath: DOReadPath
  private bucket: R2Bucket

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.wal = new SqliteWal(ctx.storage.sql)
    this.compactor = new DOCompactor(this.wal, env.BUCKET)
    this.readPath = new DOReadPath(this.wal, env.BUCKET)
    this.bucket = env.BUCKET
  }

  // -- Append ---------------------------------------------------------------

  async append(table: string, line: Record<string, unknown>): Promise<void> {
    this.wal.append(table, line)
  }

  async appendBatch(table: string, lines: Record<string, unknown>[]): Promise<void> {
    this.wal.appendBatch(table, lines)
  }

  // -- Read -----------------------------------------------------------------

  async getBatches(
    table: string,
  ): Promise<Array<{ id: number; ts: number; batch: string; row_count: number }>> {
    return this.wal.getBatches(table)
  }

  async getAllBatches(): Promise<
    Array<{ id: number; ts: number; kind: string; batch: string; row_count: number }>
  > {
    return this.wal.getAllBatches()
  }

  // -- Flush / cleanup ------------------------------------------------------

  async markFlushed(batchIds: number[]): Promise<void> {
    this.wal.markFlushed(batchIds)
  }

  async cleanup(): Promise<void> {
    this.wal.cleanup()
  }

  // -- Counts / metadata ----------------------------------------------------

  async getUnflushedCount(): Promise<number> {
    return this.wal.getUnflushedCount()
  }

  async getUnflushedCountForTable(table: string): Promise<number> {
    return this.wal.getUnflushedCountForTable(table)
  }

  async getUnflushedTables(): Promise<string[]> {
    return this.wal.getUnflushedTables()
  }

  // -- Replay ---------------------------------------------------------------

  async replayUnflushed(table: string): Promise<Record<string, unknown>[]> {
    return this.wal.replayUnflushed(table)
  }

  // -- DOCompactor methods --------------------------------------------------

  async compact(
    table: string,
  ): Promise<{ count: number; flushed: number } | null> {
    return this.compactor.compactTable(table)
  }

  async compactRels(): Promise<{ count: number; flushed: number } | null> {
    return this.compactor.compactRels()
  }

  async compactEvents(): Promise<{ count: number; flushed: number } | null> {
    return this.compactor.compactEvents()
  }

  async compactAll(): Promise<{
    tables: Record<string, number>
    rels: number | null
    events: number | null
  }> {
    const result = await this.compactor.compactAll()
    // Convert Map to plain object for RPC serialization
    const tables: Record<string, number> = {}
    for (const [key, value] of result.tables) {
      tables[key] = value
    }
    return { tables, rels: result.rels, events: result.events }
  }

  async shouldCompact(threshold?: number): Promise<boolean> {
    return this.compactor.shouldCompact(threshold)
  }

  // -- DOReadPath methods ----------------------------------------------------

  async find(table: string, filter?: Record<string, unknown>): Promise<DataLine[]> {
    return this.readPath.find(table, filter)
  }

  async getById(table: string, id: string): Promise<DataLine | null> {
    return this.readPath.getById(table, id)
  }

  async findRels(fromId?: string): Promise<RelLine[]> {
    return this.readPath.findRels(fromId)
  }

  async findEvents(): Promise<AnyEventLine[]> {
    return this.readPath.findEvents()
  }

  // -- R2 seeding helpers (for tests) ----------------------------------------

  async seedR2Data(table: string, data: DataLine[]): Promise<void> {
    const buffer = await encodeDataToParquet(data)
    await this.bucket.put(`data/${table}.parquet`, buffer)
  }

  async seedR2Rels(rels: RelLine[]): Promise<void> {
    const buffer = await encodeRelsToParquet(rels)
    await this.bucket.put('rels/rels.parquet', buffer)
  }

  async seedR2Events(events: AnyEventLine[]): Promise<void> {
    const buffer = await encodeEventsToParquet(events)
    await this.bucket.put('events/events.parquet', buffer)
  }
}

// ---------------------------------------------------------------------------
// Default fetch handler (required by Workers runtime)
// ---------------------------------------------------------------------------

export default {
  fetch(): Response {
    return new Response('MergeTree test worker')
  },
}
