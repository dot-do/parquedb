/**
 * Schema Registry for the MergeTree Engine
 *
 * Manages schema definitions, schema evolution (migrations), and time-travel
 * queries over schema history. Every schema change produces a SchemaLine event
 * that can be serialized to the events.jsonl log.
 *
 * Key capabilities:
 * - define(): Register a new schema for a table
 * - evolve(): Apply a migration (add, drop, rename, change) to an existing schema
 * - getAt(): Time-travel query to retrieve schema at a specific timestamp
 * - replayEvent(): Rebuild state from serialized SchemaLine events on startup
 * - inferType/inferSchema: Utility methods for schema inference from data
 */

import type { SchemaLine, Migration } from './types'

// =============================================================================
// Types
// =============================================================================

/**
 * A versioned schema snapshot, stored in the history timeline.
 */
export interface SchemaVersion {
  /** The full schema at this point in time */
  schema: Record<string, string>
  /** Epoch milliseconds timestamp */
  ts: number
  /** The original SchemaLine event */
  event: SchemaLine
}

// =============================================================================
// SchemaRegistry
// =============================================================================

export class SchemaRegistry {
  /** Current schema per table */
  private current: Map<string, Record<string, string>>
  /** Full history per table (for time-travel), ordered by timestamp */
  private history: Map<string, SchemaVersion[]>

  constructor() {
    this.current = new Map()
    this.history = new Map()
  }

  // ===========================================================================
  // define()
  // ===========================================================================

  /**
   * Define a schema for a table. If the table already has a schema, it is
   * overwritten. Returns the SchemaLine event that should be appended to
   * the event log.
   */
  define(table: string, schema: Record<string, string>): SchemaLine {
    const event: SchemaLine = {
      id: this.generateId(),
      ts: Date.now(),
      op: 's',
      ns: table,
      schema: { ...schema },
    }

    this.applyEvent(event)
    return event
  }

  // ===========================================================================
  // evolve()
  // ===========================================================================

  /**
   * Evolve an existing schema by applying a migration. Returns the SchemaLine
   * event with the full updated schema and migration metadata.
   *
   * Supported migration operations:
   * - added: Add new fields (type inferred as 'string' by default)
   * - dropped: Remove existing fields
   * - renamed: Rename fields (preserving type)
   * - changed: Change field types
   *
   * @throws Error if the table has no schema to evolve
   * @throws Error if adding a field that already exists
   * @throws Error if renaming a field that does not exist
   * @throws Error if dropping a field that does not exist
   */
  evolve(table: string, migration: Migration): SchemaLine {
    const currentSchema = this.current.get(table)
    if (!currentSchema) {
      throw new Error(`Cannot evolve schema for table '${table}': no schema defined`)
    }

    // Build new schema by applying migration to a copy
    const newSchema = { ...currentSchema }

    // Validate and apply: added
    if (migration.added) {
      for (const field of migration.added) {
        if (field in newSchema) {
          throw new Error(`Cannot add field '${field}' to table '${table}': field already exists`)
        }
        // Infer type from default value if provided, otherwise default to 'string'
        if (migration.default && field in migration.default) {
          newSchema[field] = SchemaRegistry.inferType(migration.default[field])
        } else {
          newSchema[field] = 'string'
        }
      }
    }

    // Validate and apply: renamed
    if (migration.renamed) {
      for (const [oldName, newName] of Object.entries(migration.renamed)) {
        if (!(oldName in newSchema)) {
          throw new Error(`Cannot rename field '${oldName}' in table '${table}': field does not exist`)
        }
        const fieldType = newSchema[oldName]
        delete newSchema[oldName]
        newSchema[newName] = fieldType
      }
    }

    // Validate and apply: dropped
    if (migration.dropped) {
      for (const field of migration.dropped) {
        if (!(field in newSchema)) {
          throw new Error(`Cannot drop field '${field}' from table '${table}': field does not exist`)
        }
        delete newSchema[field]
      }
    }

    // Validate and apply: changed
    if (migration.changed) {
      for (const [field, newType] of Object.entries(migration.changed)) {
        // changed allows changing the type even if the field doesn't exist yet
        // (it could have been added in the same migration or exist already)
        newSchema[field] = newType
      }
    }

    // Build the migration metadata to record (only include defined fields)
    const migrationRecord: Migration = {}
    if (migration.added) migrationRecord.added = migration.added
    if (migration.dropped) migrationRecord.dropped = migration.dropped
    if (migration.renamed) migrationRecord.renamed = migration.renamed
    if (migration.changed) migrationRecord.changed = migration.changed
    if (migration.default) migrationRecord.default = migration.default

    const event: SchemaLine = {
      id: this.generateId(),
      ts: Date.now(),
      op: 's',
      ns: table,
      schema: newSchema,
      migration: migrationRecord,
    }

    this.applyEvent(event)
    return event
  }

  // ===========================================================================
  // get() / has()
  // ===========================================================================

  /**
   * Get the current schema for a table. Returns undefined if the table
   * has no schema registered.
   */
  get(table: string): Record<string, string> | undefined {
    const schema = this.current.get(table)
    if (!schema) return undefined
    return { ...schema }
  }

  /**
   * Check whether a table has a schema registered.
   */
  has(table: string): boolean {
    return this.current.has(table)
  }

  // ===========================================================================
  // getAt() - time-travel
  // ===========================================================================

  /**
   * Get the schema for a table at a specific point in time. Uses binary search
   * over the history timeline to find the latest schema version at or before
   * the given timestamp.
   *
   * Returns undefined if the table had no schema defined at that time.
   */
  getAt(table: string, ts: number): Record<string, string> | undefined {
    const versions = this.history.get(table)
    if (!versions || versions.length === 0) return undefined

    // Binary search: find the last version where version.ts <= ts
    let lo = 0
    let hi = versions.length - 1
    let result: SchemaVersion | undefined

    while (lo <= hi) {
      const mid = (lo + hi) >>> 1
      if (versions[mid].ts <= ts) {
        result = versions[mid]
        lo = mid + 1
      } else {
        hi = mid - 1
      }
    }

    return result ? { ...result.schema } : undefined
  }

  // ===========================================================================
  // getHistory()
  // ===========================================================================

  /**
   * Get the full schema version history for a table, ordered by timestamp.
   * Returns an empty array if the table has no history.
   */
  getHistory(table: string): SchemaVersion[] {
    return this.history.get(table) ?? []
  }

  // ===========================================================================
  // replayEvent()
  // ===========================================================================

  /**
   * Apply a SchemaLine event to rebuild state. Used on startup when replaying
   * events from the events.jsonl log. This updates both the current schema
   * and the history timeline.
   */
  replayEvent(event: SchemaLine): void {
    this.applyEvent(event)
  }

  // ===========================================================================
  // Static utility: inferType()
  // ===========================================================================

  /**
   * Infer the type string for a JavaScript value.
   *
   * Returns one of: 'string', 'int', 'float', 'boolean', 'object', 'array', 'null'
   */
  static inferType(value: unknown): string {
    if (value === null || value === undefined) return 'null'
    if (typeof value === 'string') return 'string'
    if (typeof value === 'boolean') return 'boolean'
    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'int' : 'float'
    }
    if (Array.isArray(value)) return 'array'
    if (typeof value === 'object') return 'object'
    return 'string'
  }

  // ===========================================================================
  // Static utility: inferSchema()
  // ===========================================================================

  /**
   * Examine an array of entity objects and produce a schema by inferring
   * the type of each field. When multiple entities have the same field with
   * different types, the last non-null type encountered wins.
   */
  static inferSchema(entities: Record<string, unknown>[]): Record<string, string> {
    const schema: Record<string, string> = {}

    for (const entity of entities) {
      for (const [key, value] of Object.entries(entity)) {
        const inferred = SchemaRegistry.inferType(value)
        // Only set the type if we haven't seen this field yet or if
        // the new value is non-null (prefer concrete types over null)
        if (!(key in schema) || (inferred !== 'null' && schema[key] === 'null')) {
          schema[key] = inferred
        }
        // If we already have a concrete type, don't overwrite with null
        // But do overwrite with a different concrete type (last wins)
        if (inferred !== 'null' && schema[key] !== inferred && key in schema) {
          schema[key] = inferred
        }
      }
    }

    return schema
  }

  // ===========================================================================
  // Private helpers
  // ===========================================================================

  /**
   * Generate a unique ID: base-36 timestamp + random suffix.
   */
  private generateId(): string {
    const ts = Date.now().toString(36).padStart(9, '0')
    const rand = Math.random().toString(36).substring(2, 8)
    return ts + rand
  }

  /**
   * Apply a SchemaLine event to internal state (current + history).
   * Used by both define()/evolve() and replayEvent().
   */
  private applyEvent(event: SchemaLine): void {
    // Update current schema
    this.current.set(event.ns, { ...event.schema })

    // Append to history
    if (!this.history.has(event.ns)) {
      this.history.set(event.ns, [])
    }
    const versions = this.history.get(event.ns)!
    versions.push({
      schema: { ...event.schema },
      ts: event.ts,
      event,
    })

    // Keep history sorted by timestamp (in case of out-of-order replay)
    if (versions.length > 1 && versions[versions.length - 2].ts > event.ts) {
      versions.sort((a, b) => a.ts - b.ts)
    }
  }
}
