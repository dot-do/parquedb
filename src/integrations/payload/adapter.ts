/**
 * PayloadAdapter - Main adapter class implementing Payload CMS database interface
 *
 * This adapter bridges Payload CMS to ParqueDB's storage layer,
 * providing all required database operations.
 */

import { ParqueDB } from '../../ParqueDB'
import type { EntityId } from '../../types'
import { logger } from '../../utils/logger'
import type {
  PayloadAdapterConfig,
  ResolvedAdapterConfig,
  CreateArgs,
  FindArgs,
  FindOneArgs,
  UpdateOneArgs,
  UpdateManyArgs,
  DeleteOneArgs,
  DeleteManyArgs,
  CountArgs,
  DistinctArgs,
  QueryDraftsArgs,
  UpsertArgs,
  CreateVersionArgs,
  FindVersionsArgs,
  DeleteVersionsArgs,
  FindGlobalArgs,
  CreateGlobalVersionArgs,
  FindGlobalVersionsArgs,
  PayloadPaginatedDocs,
  DeleteResult,
  UpdateManyResult,
  PayloadWhere,
} from './types'
import { TransactionManager, createTransactionManager } from './transactions'
import { MigrationManager, createMigrationManager } from './migrations'
import * as operations from './operations'

/**
 * Payload CMS database adapter for ParqueDB
 */
export class PayloadAdapter {
  /** Adapter name for Payload */
  readonly name = 'parquedb'

  /** Package name for Payload */
  readonly packageName = 'parquedb'

  /** Default ID type */
  readonly defaultIDType: 'text' | 'number' = 'text'

  /** Allow setting ID on create */
  readonly allowIDOnCreate = true

  /** Run bulk operations in single transaction */
  readonly bulkOperationsSingleTransaction = false

  /** Directory for migrations */
  readonly migrationDir: string

  /** Resolved configuration */
  private config: ResolvedAdapterConfig

  /** ParqueDB instance */
  private db: ParqueDB

  /** Transaction manager */
  private transactions: TransactionManager

  /** Migration manager */
  private migrations: MigrationManager

  /** Payload instance (set during init) */
  payload: unknown = null

  constructor(userConfig: PayloadAdapterConfig) {
    // Resolve config with defaults
    // Note: ParqueDB doesn't allow namespaces starting with underscore,
    // so we use 'payload' prefix instead
    this.config = {
      storage: userConfig.storage,
      migrationCollection: userConfig.migrationCollection ?? 'payload_migrations',
      globalsCollection: userConfig.globalsCollection ?? 'payload_globals',
      versionsSuffix: userConfig.versionsSuffix ?? '_versions',
      defaultActor: userConfig.defaultActor ?? ('system/payload' as EntityId),
      debug: userConfig.debug ?? false,
    }

    // Initialize ParqueDB
    this.db = new ParqueDB({ storage: this.config.storage })

    // Initialize managers
    this.transactions = createTransactionManager(this.db, this.config)
    this.migrations = createMigrationManager(this.db, this.config)

    // Set migration directory (Payload expects this)
    this.migrationDir = './migrations'
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  /**
   * Initialize the adapter (called by Payload)
   */
  async init(): Promise<void> {
    if (this.config.debug) {
      logger.info('[PayloadAdapter] Initialized with ParqueDB storage')
    }
  }

  /**
   * Connect to the database
   */
  async connect(): Promise<void> {
    // ParqueDB doesn't require explicit connection
    if (this.config.debug) {
      logger.info('[PayloadAdapter] Connected')
    }
  }

  /**
   * Destroy/disconnect the adapter
   */
  async destroy(): Promise<void> {
    // Clean up any stale transactions
    this.transactions.cleanupStaleTransactions()

    if (this.config.debug) {
      logger.info('[PayloadAdapter] Destroyed')
    }
  }

  // ===========================================================================
  // Transaction Methods
  // ===========================================================================

  /**
   * Begin a new transaction
   */
  async beginTransaction(): Promise<string | number> {
    return this.transactions.beginTransaction()
  }

  /**
   * Commit a transaction
   */
  async commitTransaction(id: string | number): Promise<void> {
    return this.transactions.commitTransaction(id)
  }

  /**
   * Rollback a transaction
   */
  async rollbackTransaction(id: string | number): Promise<void> {
    return this.transactions.rollbackTransaction(id)
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  /**
   * Create a new document
   */
  async create(args: CreateArgs): Promise<Record<string, unknown>> {
    return operations.create(this.db, this.config, args)
  }

  /**
   * Find documents with pagination
   */
  async find<T = Record<string, unknown>>(args: FindArgs): Promise<PayloadPaginatedDocs<T>> {
    return operations.find<T>(this.db, this.config, args)
  }

  /**
   * Find a single document
   */
  async findOne<T = Record<string, unknown>>(args: FindOneArgs): Promise<T | null> {
    return operations.findOne<T>(this.db, this.config, args)
  }

  /**
   * Update a single document
   */
  async updateOne<T = Record<string, unknown>>(args: UpdateOneArgs): Promise<T | null> {
    return operations.updateOne<T>(this.db, this.config, args)
  }

  /**
   * Update multiple documents
   */
  async updateMany(args: UpdateManyArgs): Promise<UpdateManyResult> {
    return operations.updateMany(this.db, this.config, args)
  }

  /**
   * Delete a single document
   */
  async deleteOne(args: DeleteOneArgs): Promise<DeleteResult> {
    return operations.deleteOne(this.db, this.config, args)
  }

  /**
   * Delete multiple documents
   */
  async deleteMany(args: DeleteManyArgs): Promise<DeleteResult> {
    return operations.deleteMany(this.db, this.config, args)
  }

  /**
   * Count documents
   */
  async count(args: CountArgs): Promise<number> {
    return operations.count(this.db, this.config, args)
  }

  /**
   * Find distinct values
   */
  async findDistinct<T = unknown>(args: DistinctArgs): Promise<T[]> {
    return operations.findDistinct<T>(this.db, this.config, args)
  }

  /**
   * Upsert a document
   */
  async upsert<T = Record<string, unknown>>(args: UpsertArgs): Promise<T | null> {
    return operations.upsert<T>(this.db, this.config, args)
  }

  /**
   * Query drafts
   */
  async queryDrafts<T = Record<string, unknown>>(args: QueryDraftsArgs): Promise<PayloadPaginatedDocs<T>> {
    return operations.queryDrafts<T>(this.db, this.config, args)
  }

  // ===========================================================================
  // Version Operations
  // ===========================================================================

  /**
   * Create a version
   */
  async createVersion(args: CreateVersionArgs): Promise<Record<string, unknown>> {
    return operations.createVersion(this.db, this.config, args)
  }

  /**
   * Find versions
   */
  async findVersions<T = Record<string, unknown>>(args: FindVersionsArgs): Promise<PayloadPaginatedDocs<T>> {
    return operations.findVersions<T>(this.db, this.config, args)
  }

  /**
   * Update a version
   */
  async updateVersion<T = Record<string, unknown>>(args: {
    collection: string
    id: string | number
    versionData?: Record<string, unknown> | undefined
    locale?: string | undefined
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
  }): Promise<T | null> {
    return operations.updateVersion<T>(this.db, this.config, args)
  }

  /**
   * Delete versions
   */
  async deleteVersions(args: DeleteVersionsArgs): Promise<DeleteResult> {
    return operations.deleteVersions(this.db, this.config, args)
  }

  /**
   * Count versions
   */
  async countVersions(args: { collection: string; where?: PayloadWhere | undefined }): Promise<number> {
    return operations.countVersions(this.db, this.config, args)
  }

  // ===========================================================================
  // Global Operations
  // ===========================================================================

  /**
   * Find a global document
   */
  async findGlobal<T = Record<string, unknown>>(args: FindGlobalArgs): Promise<T | null> {
    return operations.findGlobal<T>(this.db, this.config, args)
  }

  /**
   * Create a global document
   */
  async createGlobal(args: {
    slug: string
    data: Record<string, unknown>
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
    draft?: boolean | undefined
  }): Promise<Record<string, unknown>> {
    return operations.createGlobal(this.db, this.config, args)
  }

  /**
   * Update a global document
   */
  async updateGlobal<T = Record<string, unknown>>(args: {
    slug: string
    data: Record<string, unknown>
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
    draft?: boolean | undefined
    locale?: string | undefined
    select?: Record<string, boolean> | undefined
  }): Promise<T | null> {
    return operations.updateGlobal<T>(this.db, this.config, args)
  }

  /**
   * Create a global version
   */
  async createGlobalVersion(args: CreateGlobalVersionArgs): Promise<Record<string, unknown>> {
    return operations.createGlobalVersion(this.db, this.config, args)
  }

  /**
   * Find global versions
   */
  async findGlobalVersions<T = Record<string, unknown>>(args: FindGlobalVersionsArgs): Promise<PayloadPaginatedDocs<T>> {
    return operations.findGlobalVersions<T>(this.db, this.config, args)
  }

  /**
   * Update a global version
   */
  async updateGlobalVersion<T = Record<string, unknown>>(args: {
    slug: string
    id: string | number
    versionData?: Record<string, unknown> | undefined
    locale?: string | undefined
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
  }): Promise<T | null> {
    return operations.updateGlobalVersion<T>(this.db, this.config, args)
  }

  /**
   * Delete global versions
   */
  async deleteGlobalVersions(args: {
    slug: string
    where: PayloadWhere
    req?: { transactionID?: string | number | undefined; user?: Record<string, unknown> | undefined } | undefined
  }): Promise<DeleteResult> {
    return operations.deleteGlobalVersions(this.db, this.config, args)
  }

  /**
   * Count global versions
   */
  async countGlobalVersions(args: { slug: string; where?: PayloadWhere | undefined }): Promise<number> {
    return operations.countGlobalVersions(this.db, this.config, args)
  }

  // ===========================================================================
  // Migration Operations
  // ===========================================================================

  /**
   * Create a migration record
   */
  async createMigration(args: { name: string; batch?: number | undefined }): Promise<void> {
    await this.migrations.createMigration(args)
  }

  /**
   * Run migrations
   */
  async migrate(_args?: { migrations?: unknown[] | undefined }): Promise<void> {
    // Payload handles the actual migration execution
    // We just need to track which migrations have run
    if (this.config.debug) {
      logger.info('[PayloadAdapter] Migration triggered')
    }
  }

  /**
   * Roll back the last batch of migrations
   */
  async migrateDown(): Promise<void> {
    const latestBatch = await this.migrations.getLatestBatch()
    if (latestBatch > 0) {
      await this.migrations.deleteBatch(latestBatch)
    }
  }

  /**
   * Fresh migration (drop all and re-run)
   */
  async migrateFresh(_args: { forceAcceptWarning?: boolean | undefined }): Promise<void> {
    await this.migrations.deleteAllMigrations()
  }

  /**
   * Refresh migrations (rollback all and re-run)
   */
  async migrateRefresh(): Promise<void> {
    await this.migrations.deleteAllMigrations()
  }

  /**
   * Reset migrations (rollback all)
   */
  async migrateReset(): Promise<void> {
    await this.migrations.deleteAllMigrations()
  }

  /**
   * Get migration status
   */
  async migrateStatus(): Promise<void> {
    const migrations = await this.migrations.getMigrations()

    logger.info('[PayloadAdapter] Migration status:')
    for (const m of migrations) {
      logger.info(`  - ${m.name} (batch ${m.batch}) at ${m.createdAt.toISOString()}`)
    }
  }

  // ===========================================================================
  // Update Jobs (for Payload's job queue)
  // ===========================================================================

  /**
   * Update job documents
   * This is used by Payload's internal job queue
   */
  async updateJobs(args: {
    input: Array<{
      id: string
      data: Record<string, unknown>
    }>
  }): Promise<void> {
    for (const job of args.input) {
      await this.db.update('payload_jobs', job.id, { $set: job.data }, {
        actor: this.config.defaultActor,
        upsert: true,
      })
    }
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Get the underlying ParqueDB instance
   */
  getDB(): ParqueDB {
    return this.db
  }

  /**
   * Get the resolved configuration
   */
  getConfig(): ResolvedAdapterConfig {
    return this.config
  }
}
