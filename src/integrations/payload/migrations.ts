/**
 * Migration management for Payload CMS adapter
 *
 * Stores migration state in a dedicated ParqueDB collection.
 */

import type { ParqueDB } from '../../ParqueDB'
import type { MigrationRecord, ResolvedAdapterConfig } from './types'
import { toParqueDBInput, extractLocalId } from './transform'

/**
 * Migration manager for Payload CMS
 */
export class MigrationManager {
  constructor(
    private db: ParqueDB,
    private config: ResolvedAdapterConfig
  ) {}

  /**
   * Get the migrations collection name
   */
  get collection(): string {
    return this.config.migrationCollection
  }

  /**
   * Create a migration record
   */
  async createMigration(args: {
    name: string
    batch?: number | undefined
  }): Promise<MigrationRecord> {
    const { name, batch = 1 } = args

    const input = toParqueDBInput(
      {
        migrationName: name,
        batch,
        executedAt: new Date(),
      },
      { collection: this.collection, actor: this.config.defaultActor }
    )

    input.name = name
    input.$type = 'Migration'

    const entity = await this.db.create(this.collection, input, {
      actor: this.config.defaultActor,
    })

    return {
      name,
      batch,
      createdAt: entity.createdAt,
    }
  }

  /**
   * Get all executed migrations
   */
  async getMigrations(): Promise<MigrationRecord[]> {
    const result = await this.db.find(
      this.collection,
      {},
      { sort: { batch: 1, createdAt: 1 } }
    )

    return result.items.map(entity => ({
      name: entity.name,
      batch: (entity as Record<string, unknown>)['batch'] as number ?? 1,
      createdAt: entity.createdAt,
    }))
  }

  /**
   * Get the latest batch number
   */
  async getLatestBatch(): Promise<number> {
    const result = await this.db.find(
      this.collection,
      {},
      { sort: { batch: -1 }, limit: 1 }
    )

    if (result.items.length === 0) {
      return 0
    }

    return (result.items[0] as Record<string, unknown>)['batch'] as number ?? 0
  }

  /**
   * Check if a migration has been run
   */
  async hasMigration(name: string): Promise<boolean> {
    const result = await this.db.find(
      this.collection,
      { migrationName: name },
      { limit: 1 }
    )

    return result.items.length > 0
  }

  /**
   * Delete a migration record (for rollback)
   */
  async deleteMigration(name: string): Promise<boolean> {
    const result = await this.db.find(
      this.collection,
      { migrationName: name },
      { limit: 1 }
    )

    if (result.items.length === 0) {
      return false
    }

    const localId = extractLocalId(result.items[0]!.$id)
    await this.db.delete(this.collection, localId, {
      actor: this.config.defaultActor,
    })

    return true
  }

  /**
   * Delete all migrations in a batch (for batch rollback)
   */
  async deleteBatch(batch: number): Promise<number> {
    const result = await this.db.find(
      this.collection,
      { batch },
      { limit: 10000 }
    )

    let deleted = 0

    for (const entity of result.items) {
      const localId = extractLocalId(entity.$id)
      await this.db.delete(this.collection, localId, {
        actor: this.config.defaultActor,
      })
      deleted++
    }

    return deleted
  }

  /**
   * Delete all migrations (for fresh migration)
   */
  async deleteAllMigrations(): Promise<number> {
    const result = await this.db.find(
      this.collection,
      {},
      { limit: 10000 }
    )

    let deleted = 0

    for (const entity of result.items) {
      const localId = extractLocalId(entity.$id)
      await this.db.delete(this.collection, localId, {
        actor: this.config.defaultActor,
      })
      deleted++
    }

    return deleted
  }
}

/**
 * Create a migration manager instance
 */
export function createMigrationManager(
  db: ParqueDB,
  config: ResolvedAdapterConfig
): MigrationManager {
  return new MigrationManager(db, config)
}
