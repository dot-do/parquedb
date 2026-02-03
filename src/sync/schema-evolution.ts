/**
 * Schema Evolution - Detect and analyze schema changes
 *
 * Provides tools for detecting breaking changes and generating
 * migration hints when schema evolves over time.
 */

import type { SchemaChanges, SchemaChange } from './schema-snapshot'

/**
 * Breaking change with severity and impact information
 */
export interface BreakingChange extends SchemaChange {
  severity: 'critical' | 'high' | 'medium'
  impact: string
  migrationHint?: string
}

/**
 * Detect breaking changes from schema differences
 *
 * Analyzes changes and provides severity levels and migration guidance
 *
 * @param changes Schema changes to analyze
 * @returns Array of breaking changes with details
 */
export function detectBreakingChanges(changes: SchemaChanges): BreakingChange[] {
  const breakingChanges: BreakingChange[] = []

  for (const change of changes.breakingChanges) {
    let severity: 'critical' | 'high' | 'medium' = 'medium'
    let impact = ''
    let migrationHint: string | undefined

    switch (change.type) {
      case 'DROP_COLLECTION':
        severity = 'critical'
        impact = `All data in collection '${change.collection}' will be inaccessible. Existing queries will fail.`
        migrationHint = `If data should be preserved, consider:\n` +
          `  1. Export data: parquedb export ${change.collection} backup.json\n` +
          `  2. Migrate to new collection if renamed\n` +
          `  3. Keep old collection with different name if deprecating`
        break

      case 'REMOVE_FIELD':
        severity = 'high'
        impact = `Field '${change.collection}.${change.field}' will be removed. Queries referencing this field will fail.`
        migrationHint = `Consider:\n` +
          `  1. Update all queries to remove references to '${change.field}'\n` +
          `  2. If data should be preserved, migrate to a new field name\n` +
          `  3. Add a migration script to transform existing data`
        break

      case 'CHANGE_TYPE':
        severity = 'critical'
        impact = `Field '${change.collection}.${change.field}' type changed from ${change.before} to ${change.after}. ` +
          `Existing data may not be compatible with new type.`
        migrationHint = `Required actions:\n` +
          `  1. Write migration script to convert existing values\n` +
          `  2. Test conversion on backup data first\n` +
          `  3. Update application code to handle new type\n` +
          `  Example migration:\n` +
          `    const items = await db.${change.collection}.find({})\n` +
          `    for (const item of items) {\n` +
          `      await db.${change.collection}.update(item.$id, {\n` +
          `        ${change.field}: convertType(item.${change.field})\n` +
          `      })\n` +
          `    }`
        break

      case 'CHANGE_REQUIRED':
        if (change.after === true) {
          severity = 'high'
          impact = `Field '${change.collection}.${change.field}' is now required. ` +
            `Existing records without this field will need to be updated.`
          migrationHint = `Required actions:\n` +
            `  1. Set default value for existing records:\n` +
            `    await db.${change.collection}.updateMany(\n` +
            `      { ${change.field}: { $exists: false } },\n` +
            `      { $set: { ${change.field}: <default_value> } }\n` +
            `    )\n` +
            `  2. Update application code to always provide this field`
        }
        break

      case 'ADD_FIELD':
        if (change.after && typeof change.after === 'object' && 'required' in change.after && change.after.required) {
          severity = 'high'
          impact = `New required field '${change.collection}.${change.field}' added. ` +
            `All create operations must now include this field.`
          migrationHint = `Required actions:\n` +
            `  1. Update all create calls to include '${change.field}'\n` +
            `  2. Consider if existing records need this field populated`
        }
        break
    }

    breakingChanges.push({
      ...change,
      severity,
      impact,
      migrationHint
    })
  }

  return breakingChanges
}

/**
 * Generate migration hints from schema changes
 *
 * Creates actionable migration steps for both breaking and non-breaking changes
 *
 * @param changes Schema changes to analyze
 * @returns Array of migration hint strings
 */
export function generateMigrationHints(changes: SchemaChanges): string[] {
  const hints: string[] = []

  // Breaking changes first
  const breaking = detectBreakingChanges(changes)
  if (breaking.length > 0) {
    hints.push('⚠️  BREAKING CHANGES DETECTED')
    hints.push('')
    hints.push('The following changes may break existing code:')
    hints.push('')

    for (const change of breaking) {
      hints.push(`${getSeverityIcon(change.severity)} ${change.description}`)
      hints.push(`   Impact: ${change.impact}`)
      if (change.migrationHint) {
        hints.push('')
        hints.push('   Migration steps:')
        for (const line of change.migrationHint.split('\n')) {
          hints.push(`   ${line}`)
        }
      }
      hints.push('')
    }
  }

  // Non-breaking changes
  const nonBreaking = changes.changes.filter(c => !c.breaking)
  if (nonBreaking.length > 0) {
    hints.push('ℹ️  Non-breaking changes:')
    hints.push('')

    for (const change of nonBreaking) {
      hints.push(`  ✓ ${change.description}`)

      // Add specific hints for certain non-breaking changes
      if (change.type === 'ADD_COLLECTION') {
        hints.push(`    No action required. New collection '${change.collection}' is available.`)
      } else if (change.type === 'ADD_FIELD') {
        hints.push(`    No action required. Field is optional.`)
      } else if (change.type === 'ADD_INDEX') {
        hints.push(`    Index will improve query performance for '${change.collection}.${change.field}'.`)
      }
    }
    hints.push('')
  }

  // Summary and recommendations
  if (breaking.length > 0) {
    hints.push('📋 Recommended workflow:')
    hints.push('')
    hints.push('  1. Review all breaking changes above')
    hints.push('  2. Create a backup: parquedb export --all backup/')
    hints.push('  3. Write and test migration scripts')
    hints.push('  4. Update application code to handle changes')
    hints.push('  5. Deploy schema changes to staging first')
    hints.push('  6. Run migration scripts')
    hints.push('  7. Verify application behavior')
    hints.push('  8. Deploy to production')
    hints.push('')
  }

  // Type generation hint
  hints.push('💡 Generate updated types:')
  hints.push('   parquedb types generate')
  hints.push('')

  return hints
}

/**
 * Get icon for severity level
 */
function getSeverityIcon(severity: 'critical' | 'high' | 'medium'): string {
  switch (severity) {
    case 'critical':
      return '🔴'
    case 'high':
      return '🟠'
    case 'medium':
      return '🟡'
  }
}

/**
 * Check if a schema change set is safe to apply
 *
 * @param changes Schema changes to validate
 * @returns true if safe, false if breaking changes exist
 */
export function isSafeToApply(changes: SchemaChanges): boolean {
  return changes.compatible && changes.breakingChanges.length === 0
}

/**
 * Categorize changes by type
 *
 * @param changes Schema changes
 * @returns Map of change type to changes
 */
export function categorizeChanges(changes: SchemaChanges): Map<string, SchemaChange[]> {
  const categories = new Map<string, SchemaChange[]>()

  for (const change of changes.changes) {
    const category = getCategoryForChangeType(change.type)
    const list = categories.get(category) ?? []
    list.push(change)
    categories.set(category, list)
  }

  return categories
}

/**
 * Get category name for change type
 */
function getCategoryForChangeType(type: string): string {
  if (type.includes('COLLECTION')) return 'Collections'
  if (type.includes('FIELD')) return 'Fields'
  if (type.includes('INDEX')) return 'Indexes'
  if (type.includes('TYPE')) return 'Type Changes'
  return 'Other'
}
