/**
 * Backend Migration CLI Command
 *
 * Migrate data between storage backend formats (Native, Iceberg, Delta Lake)
 *
 * @example
 * ```bash
 * # Auto-detect and migrate to Iceberg
 * parquedb migrate-backend --to=iceberg
 *
 * # Migrate specific namespaces from native to delta
 * parquedb migrate-backend --from=native --to=delta --namespaces=users,posts
 *
 * # Dry run to see what would be migrated
 * parquedb migrate-backend --to=iceberg --dry-run
 * ```
 */

import type { StorageBackend } from '../../types/storage'
import {
  migrateBackend,
  detectExistingFormat,
  discoverNamespaces,
  type MigrationConfig,
  type BackendType,
} from '../../backends'
import { logger } from '../../utils/logger'

export interface MigrateBackendArgs {
  /** Target format */
  to: BackendType
  /** Source format (or 'auto' to detect) */
  from?: BackendType | 'auto' | undefined
  /** Specific namespaces to migrate (comma-separated) */
  namespaces?: string | undefined
  /** Batch size for migration */
  batchSize?: number | undefined
  /** Delete source data after migration */
  deleteSource?: boolean | undefined
  /** Show what would be migrated without actually migrating */
  dryRun?: boolean | undefined
  /** Storage backend */
  storage: StorageBackend
}

export async function migrateBackendCommand(args: MigrateBackendArgs): Promise<void> {
  const {
    to,
    from = 'auto',
    namespaces: namespacesArg,
    batchSize = 1000,
    deleteSource = false,
    dryRun = false,
    storage,
  } = args

  console.log(`\n🔄 Backend Migration: ${from} → ${to}\n`)

  // Parse namespaces
  const namespaces = namespacesArg
    ? namespacesArg.split(',').map(n => n.trim())
    : '*'

  // Discover what exists
  const allNamespaces = namespaces === '*'
    ? await discoverNamespaces(storage)
    : namespaces as string[]

  if (allNamespaces.length === 0) {
    console.log('No namespaces found in storage.')
    return
  }

  console.log(`Found ${allNamespaces.length} namespace(s):`)

  // Check each namespace
  const migrationPlan: Array<{
    namespace: string
    currentFormat: BackendType | null
    willMigrate: boolean
    reason: string
  }> = []

  for (const ns of allNamespaces) {
    const { formats, primary } = await detectExistingFormat(storage, ns)

    let willMigrate = false
    let reason = ''

    if (!primary) {
      reason = 'No data found'
    } else if (primary === to) {
      reason = `Already in ${to} format`
    } else if (from !== 'auto' && primary !== from) {
      reason = `Not in ${from} format (found ${primary})`
    } else {
      willMigrate = true
      reason = `${primary} → ${to}`
    }

    migrationPlan.push({
      namespace: ns,
      currentFormat: primary,
      willMigrate,
      reason,
    })

    const icon = willMigrate ? '📦' : '⏭️'
    console.log(`  ${icon} ${ns}: ${reason}`)
  }

  const toMigrate = migrationPlan.filter(p => p.willMigrate)

  if (toMigrate.length === 0) {
    console.log('\n✅ Nothing to migrate.')
    return
  }

  console.log(`\n📊 Migration Summary:`)
  console.log(`   Namespaces to migrate: ${toMigrate.length}`)
  console.log(`   Target format: ${to}`)
  console.log(`   Delete source: ${deleteSource}`)

  if (dryRun) {
    console.log('\n🔍 Dry run - no changes made.')
    return
  }

  console.log('\n⏳ Starting migration...\n')

  const config: MigrationConfig = {
    storage,
    from,
    to,
    namespaces: toMigrate.map(p => p.namespace),
    batchSize,
    deleteSource,
    onProgress: (progress) => {
      const pct = progress.totalEntities > 0
        ? Math.round((progress.entitiesMigrated / progress.totalEntities) * 100)
        : 0

      if (progress.phase === 'migrating') {
        process.stdout.write(`\r   ${progress.namespace}: ${progress.entitiesMigrated}/${progress.totalEntities} (${pct}%)`)
      } else if (progress.phase === 'complete') {
        console.log(`\r   ✅ ${progress.namespace}: ${progress.entitiesMigrated} entities migrated`)
      }
    },
  }

  const result = await migrateBackend(config)

  console.log('\n' + '─'.repeat(50))
  console.log(`\n${result.success ? '✅ Migration Complete' : '⚠️ Migration Completed with Errors'}`)
  console.log(`   Namespaces: ${result.namespacesProcessed.length}`)
  console.log(`   Entities: ${result.entitiesMigrated}`)
  console.log(`   Duration: ${result.durationMs}ms`)

  if (result.errors.length > 0) {
    console.log(`\n❌ Errors:`)
    for (const error of result.errors) {
      console.log(`   - ${error}`)
    }
  }
}

/**
 * Show current backend status for all namespaces
 */
export async function showBackendStatus(storage: StorageBackend): Promise<void> {
  console.log('\n📊 Backend Status\n')

  const namespaces = await discoverNamespaces(storage)

  if (namespaces.length === 0) {
    console.log('No namespaces found.')
    return
  }

  const formatCounts = { native: 0, iceberg: 0, delta: 0, unknown: 0 }

  for (const ns of namespaces) {
    const { formats, primary } = await detectExistingFormat(storage, ns)

    if (primary) {
      formatCounts[primary]++
    } else {
      formatCounts.unknown++
    }

    const formatStr = formats.length > 0 ? formats.join(', ') : 'none'
    const primaryStr = primary ? ` (primary: ${primary})` : ''
    console.log(`  ${ns}: ${formatStr}${primaryStr}`)
  }

  console.log('\n' + '─'.repeat(40))
  console.log('Summary:')
  console.log(`  Native: ${formatCounts.native}`)
  console.log(`  Iceberg: ${formatCounts.iceberg}`)
  console.log(`  Delta: ${formatCounts.delta}`)
  if (formatCounts.unknown > 0) {
    console.log(`  Unknown: ${formatCounts.unknown}`)
  }
}
