/**
 * Studio Configuration Utilities
 *
 * Extracts and merges studio/layout configuration from schemas and global config.
 */

import type { DBSchema, CollectionSchemaWithLayout } from '../db'
import type { StudioConfig, CollectionStudioConfig } from './loader'

/**
 * Extract studio configuration for a collection from its schema
 *
 * Parses $layout, $sidebar, and $studio from the schema definition.
 */
export function extractCollectionStudio(
  _collectionName: string,
  schema: CollectionSchemaWithLayout
): CollectionStudioConfig {
  const config: CollectionStudioConfig = {}

  // Extract layout (array = rows, object = tabs)
  if (schema.$layout) {
    config.layout = schema.$layout
  }

  // Extract sidebar
  if (schema.$sidebar) {
    config.sidebar = schema.$sidebar
  }

  // Extract $studio config
  if (schema.$studio) {
    const studio = schema.$studio

    // Collection-level settings
    if (studio.label) config.label = studio.label
    if (studio.useAsTitle) config.useAsTitle = studio.useAsTitle
    if (studio.defaultColumns) config.defaultColumns = studio.defaultColumns as string[]
    if (studio.group) config.group = studio.group

    // Field-level settings
    config.fields = {}
    for (const [key, value] of Object.entries(studio)) {
      // Skip collection-level keys
      if (['label', 'useAsTitle', 'defaultColumns', 'group'].includes(key)) {
        continue
      }

      // Handle field config
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        config.fields[key] = value
      }
    }

    // Clean up empty fields
    if (Object.keys(config.fields).length === 0) {
      delete config.fields
    }
  }

  return config
}

/**
 * Extract studio configuration from an entire schema
 */
export function extractSchemaStudio(schema: DBSchema): Record<string, CollectionStudioConfig> {
  const result: Record<string, CollectionStudioConfig> = {}

  for (const [name, collectionSchema] of Object.entries(schema)) {
    if (collectionSchema === 'flexible') continue

    const config = extractCollectionStudio(name, collectionSchema)
    if (Object.keys(config).length > 0) {
      result[name.toLowerCase()] = config
    }
  }

  return result
}

/**
 * Merge global studio config with schema-extracted config
 *
 * Schema-level config takes precedence over global config.
 */
export function mergeStudioConfig(
  global: StudioConfig | undefined,
  schemaStudio: Record<string, CollectionStudioConfig>
): StudioConfig {
  const merged: StudioConfig = {
    theme: global?.theme ?? 'auto',
    defaultSidebar: global?.defaultSidebar ?? ['$id', 'createdAt', 'updatedAt'],
    port: global?.port ?? 3000,
    collections: {},
  }

  // Start with global collection configs
  if (global?.collections) {
    for (const [name, config] of Object.entries(global.collections)) {
      merged.collections![name] = { ...config }
    }
  }

  // Merge schema-extracted configs (takes precedence)
  for (const [name, config] of Object.entries(schemaStudio)) {
    if (merged.collections![name]) {
      // Deep merge
      merged.collections![name] = {
        ...merged.collections![name],
        ...config,
        fields: {
          ...merged.collections![name]!.fields,
          ...config.fields,
        },
      }
    } else {
      merged.collections![name] = config
    }
  }

  return merged
}

/**
 * Get field names from a collection schema (excluding $ prefixed keys)
 */
export function getSchemaFields(schema: CollectionSchemaWithLayout): string[] {
  return Object.keys(schema).filter(
    (key) => !key.startsWith('$') && typeof schema[key] === 'string'
  )
}

/**
 * Check if a schema key is a field definition (not layout/studio config)
 */
export function isFieldDefinition(key: string, value: unknown): value is string {
  return !key.startsWith('$') && typeof value === 'string'
}

/**
 * Convert simple string options to label/value pairs
 *
 * @example
 * normalizeOptions(['draft', 'published'])
 * // => [{ label: 'Draft', value: 'draft' }, { label: 'Published', value: 'published' }]
 */
export function normalizeOptions(
  options: string[] | Array<{ label: string; value: string }>
): Array<{ label: string; value: string }> {
  if (options.length === 0) return []

  // Already normalized
  if (typeof options[0] === 'object') {
    return options as Array<{ label: string; value: string }>
  }

  // Convert string array
  return (options as string[]).map((value) => ({
    label: value.charAt(0).toUpperCase() + value.slice(1).replace(/_/g, ' '),
    value,
  }))
}

/**
 * Check if layout has tabs (object) vs just rows (array)
 */
export function layoutHasTabs(
  layout: (string | string[])[] | Record<string, (string | string[])[]>
): layout is Record<string, (string | string[])[]> {
  return !Array.isArray(layout)
}

/**
 * Normalize a row entry to array format
 * 'field' -> ['field']
 * ['a', 'b'] -> ['a', 'b']
 */
export function normalizeRow(row: string | string[]): string[] {
  return Array.isArray(row) ? row : [row]
}
