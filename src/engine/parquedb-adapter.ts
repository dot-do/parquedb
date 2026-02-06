/**
 * ParqueDB Bridge Adapter
 *
 * Wraps the ParqueEngine with a MongoDB-style API surface, providing:
 * - Proxy-based collection access: db.Users.find() -> db.collection('users').find()
 * - EngineCollection with CRUD methods: create, find, findOne, get, update, delete, count
 * - Lifecycle methods: init(), compact(), close()
 *
 * This adapter is the bridge between the ParqueDB MongoDB-style API and the
 * low-level MergeTree engine. It mirrors the Proxy-based collection access
 * pattern used by the main ParqueDB class.
 */

import { ParqueEngine } from './engine'
import type { EngineConfig } from './engine'
import type { DataLine } from './types'
import type { FindOptions, UpdateOps } from './engine'

// =============================================================================
// Configuration
// =============================================================================

export interface EngineDBConfig extends EngineConfig {}

// =============================================================================
// EngineDB — MongoDB-style wrapper around ParqueEngine
// =============================================================================

export class EngineDB {
  readonly engine: ParqueEngine

  constructor(config: EngineDBConfig) {
    this.engine = new ParqueEngine(config)
    return new Proxy(this, {
      get(target, prop: string, receiver) {
        if (prop in target || typeof prop === 'symbol') {
          return Reflect.get(target, prop, receiver)
        }
        const tableName = prop.charAt(0).toLowerCase() + prop.slice(1)
        return target.collection(tableName)
      },
    })
  }

  async init(): Promise<void> {
    await this.engine.init()
  }

  collection(name: string): EngineCollection {
    return new EngineCollection(this.engine, name)
  }

  async compact(): Promise<void> {
    await this.engine.compactAll()
  }

  async close(): Promise<void> {
    await this.engine.close()
  }

  get tables(): string[] {
    return this.engine.tables
  }
}

// =============================================================================
// EngineCollection — per-table CRUD API
// =============================================================================

export class EngineCollection {
  constructor(
    private engine: ParqueEngine,
    private table: string,
  ) {}

  async create(data: Record<string, unknown>): Promise<DataLine> {
    return this.engine.create(this.table, data)
  }

  async createMany(items: Record<string, unknown>[]): Promise<DataLine[]> {
    return this.engine.createMany(this.table, items)
  }

  async find(filter?: Record<string, unknown>, options?: FindOptions): Promise<DataLine[]> {
    return this.engine.find(this.table, filter, options)
  }

  async findOne(filter?: Record<string, unknown>): Promise<DataLine | null> {
    return this.engine.findOne(this.table, filter)
  }

  async get(id: string): Promise<DataLine | null> {
    return this.engine.get(this.table, id)
  }

  async getMany(ids: string[]): Promise<(DataLine | null)[]> {
    return this.engine.getMany(this.table, ids)
  }

  async update(id: string, ops: UpdateOps): Promise<DataLine> {
    return this.engine.update(this.table, id, ops)
  }

  async delete(id: string): Promise<void> {
    return this.engine.delete(this.table, id)
  }

  async count(filter?: Record<string, unknown>): Promise<number> {
    return this.engine.count(this.table, filter)
  }
}
