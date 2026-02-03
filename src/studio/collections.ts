/**
 * Payload Collection Generation
 *
 * Generates Payload CMS collection configurations from discovered
 * Parquet schemas and optional UI metadata.
 */

import type {
  DiscoveredCollection,
  DiscoveredField,
  CollectionUIMetadata,
  FieldUIMetadata,
  PayloadFieldType,
} from './types'
import { findTitleField, findDefaultColumns } from './discovery'

// =============================================================================
// Types
// =============================================================================

/**
 * Payload field configuration (subset of actual Payload types)
 */
export interface PayloadFieldConfig {
  name: string
  type: PayloadFieldType
  label?: string | undefined
  required?: boolean | undefined
  admin?: {
    readOnly?: boolean | undefined
    hidden?: boolean | undefined
    position?: 'sidebar' | undefined
    width?: string | undefined
    description?: string | undefined
    condition?: unknown | undefined
  } | undefined
  // Type-specific options
  options?: Array<{ label: string; value: string }> | undefined
  min?: number | undefined
  max?: number | undefined
  minLength?: number | undefined
  maxLength?: number | undefined
  relationTo?: string | string[] | undefined
  hasMany?: boolean | undefined
  fields?: PayloadFieldConfig[] | undefined
}

/**
 * Payload collection configuration (subset of actual Payload types)
 */
export interface PayloadCollectionConfig {
  slug: string
  labels?: {
    singular?: string | undefined
    plural?: string | undefined
  } | undefined
  admin?: {
    useAsTitle?: string | undefined
    defaultColumns?: string[] | undefined
    description?: string | undefined
    group?: string | undefined
    hidden?: boolean | undefined
    preview?: boolean | undefined
  } | undefined
  fields: PayloadFieldConfig[]
  timestamps?: boolean | undefined
  access?: Record<string, unknown> | undefined
}

// =============================================================================
// Collection Generation
// =============================================================================

/**
 * Generate a Payload collection configuration from a discovered collection
 *
 * @param collection - Discovered collection metadata
 * @param uiMetadata - Optional UI metadata for customization
 * @param options - Generation options
 * @returns Payload collection configuration
 */
export function generateCollection(
  collection: DiscoveredCollection,
  uiMetadata?: CollectionUIMetadata,
  options: { readOnly?: boolean | undefined } = {}
): PayloadCollectionConfig {
  const { slug, label, fields, isParqueDB } = collection
  const { readOnly = false } = options

  // Generate field configurations
  const payloadFields = fields
    .filter((f) => !shouldSkipField(f, isParqueDB))
    .map((f) => generateField(f, uiMetadata?.fields?.[f.name], readOnly))

  // Determine title field
  const titleField = uiMetadata?.admin?.useAsTitle ?? findTitleField(fields)

  // Determine default columns
  const defaultColumns = uiMetadata?.admin?.defaultColumns ??
                         findDefaultColumns(fields, isParqueDB)

  return {
    slug,
    labels: {
      singular: uiMetadata?.labelSingular ?? label.replace(/s$/, ''),
      plural: uiMetadata?.label ?? label,
    },
    admin: {
      useAsTitle: titleField,
      defaultColumns,
      description: uiMetadata?.description,
      group: uiMetadata?.admin?.group,
      hidden: uiMetadata?.admin?.hidden,
      preview: uiMetadata?.admin?.preview,
    },
    fields: payloadFields,
    timestamps: isParqueDB,
    ...(readOnly && {
      access: {
        create: () => false,
        update: () => false,
        delete: () => false,
      },
    }),
  }
}

/**
 * Generate a Payload field configuration from a discovered field
 *
 * @param field - Discovered field
 * @param uiMetadata - Optional field UI metadata
 * @param readOnly - Whether the field should be read-only
 * @returns Payload field configuration
 */
export function generateField(
  field: DiscoveredField,
  uiMetadata?: FieldUIMetadata,
  readOnly = false
): PayloadFieldConfig {
  const { name, payloadType, optional, isArray } = field

  const config: PayloadFieldConfig = {
    name,
    type: payloadType,
    label: uiMetadata?.label ?? formatFieldLabel(name),
    required: !optional,
  }

  // Admin configuration
  const admin: NonNullable<PayloadFieldConfig['admin']> = {}

  if (readOnly || uiMetadata?.readOnly) {
    admin.readOnly = true
  }

  if (uiMetadata?.hideInForm) {
    admin.hidden = true
  }

  if (uiMetadata?.description) {
    admin.description = uiMetadata.description
  }

  if (uiMetadata?.admin?.position) {
    admin.position = uiMetadata.admin.position
  }

  if (uiMetadata?.admin?.width) {
    admin.width = uiMetadata.admin.width
  }

  if (Object.keys(admin).length > 0) {
    config.admin = admin
  }

  // Type-specific configuration
  switch (payloadType) {
    case 'number':
      if (uiMetadata?.min !== undefined) config.min = uiMetadata.min
      if (uiMetadata?.max !== undefined) config.max = uiMetadata.max
      break

    case 'text':
    case 'textarea':
      if (uiMetadata?.minLength !== undefined) config.minLength = uiMetadata.minLength
      if (uiMetadata?.maxLength !== undefined) config.maxLength = uiMetadata.maxLength
      break

    case 'select':
      config.options = uiMetadata?.options ?? [
        { label: 'Option 1', value: 'option1' },
        { label: 'Option 2', value: 'option2' },
      ]
      break

    case 'relationship':
      config.relationTo = uiMetadata?.relationTo ?? 'users'
      config.hasMany = uiMetadata?.hasMany ?? isArray
      break

    case 'array':
      // For array fields, we need nested field definitions
      config.fields = [
        {
          name: 'value',
          type: 'text',
          label: 'Value',
        },
      ]
      break

    case 'json':
      // JSON fields are handled as code blocks
      config.type = 'code'
      break
  }

  return config
}

/**
 * Generate all collections from discovered collections
 *
 * @param collections - Array of discovered collections
 * @param metadataMap - Map of slug to UI metadata
 * @param options - Generation options
 * @returns Array of Payload collection configurations
 */
export function generateCollections(
  collections: DiscoveredCollection[],
  metadataMap: Record<string, CollectionUIMetadata> = {},
  options: { readOnly?: boolean | undefined } = {}
): PayloadCollectionConfig[] {
  return collections.map((collection) =>
    generateCollection(collection, metadataMap[collection.slug], options)
  )
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Determine if a field should be skipped in the Payload schema
 *
 * @param field - Discovered field
 * @param isParqueDB - Whether this is a ParqueDB-managed file
 * @returns True if the field should be skipped
 */
function shouldSkipField(field: DiscoveredField, isParqueDB: boolean): boolean {
  const { name } = field

  // Skip internal ParqueDB fields (handled automatically)
  if (isParqueDB) {
    const skipFields = new Set([
      '$type',
      '$data',
      'createdBy',
      'updatedBy',
      'deletedAt',
      'deletedBy',
      'version',
    ])

    if (skipFields.has(name)) {
      return true
    }
  }

  return false
}

/**
 * Format a field name as a human-readable label
 *
 * @param name - Field name (e.g., 'user_name', 'firstName', '$id')
 * @returns Formatted label (e.g., 'User Name', 'First Name', 'ID')
 */
export function formatFieldLabel(name: string): string {
  // Handle special prefixes
  if (name.startsWith('$')) {
    name = name.slice(1)
  }

  return name
    // Handle snake_case
    .replace(/_/g, ' ')
    // Handle camelCase
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    // Handle kebab-case
    .replace(/-/g, ' ')
    // Capitalize each word
    .replace(/\b\w/g, (char) => char.toUpperCase())
    // Handle common abbreviations
    .replace(/\bId\b/g, 'ID')
    .replace(/\bUrl\b/g, 'URL')
    .replace(/\bApi\b/g, 'API')
    .trim()
}

/**
 * Infer relationships between collections
 *
 * Looks for field names that match other collection slugs
 * (e.g., 'author' field might relate to 'users' collection)
 *
 * @param collections - Array of discovered collections
 * @returns Map of field paths to related collection slugs
 */
export function inferRelationships(
  collections: DiscoveredCollection[]
): Map<string, string> {
  const relationships = new Map<string, string>()
  const slugs = new Set(collections.map((c) => c.slug))

  // Common relationship field name patterns
  const patterns: Array<[RegExp, string]> = [
    [/^author$/i, 'users'],
    [/^creator$/i, 'users'],
    [/^owner$/i, 'users'],
    [/^user$/i, 'users'],
    [/^parent$/i, ''], // Same collection
    [/^category$/i, 'categories'],
    [/^categories$/i, 'categories'],
    [/^tag$/i, 'tags'],
    [/^tags$/i, 'tags'],
  ]

  for (const collection of collections) {
    for (const field of collection.fields) {
      const fieldPath = `${collection.slug}.${field.name}`

      // Check if field name matches a collection slug directly
      const singularName = field.name.replace(/s$/, '')
      if (slugs.has(field.name)) {
        relationships.set(fieldPath, field.name)
      } else if (slugs.has(singularName)) {
        relationships.set(fieldPath, singularName)
      } else if (slugs.has(`${field.name}s`)) {
        relationships.set(fieldPath, `${field.name}s`)
      }

      // Check patterns
      for (const [pattern, target] of patterns) {
        if (pattern.test(field.name)) {
          const actualTarget = target || collection.slug
          if (slugs.has(actualTarget)) {
            relationships.set(fieldPath, actualTarget)
          }
          break
        }
      }

      // Check if field ends with 'Id' or '_id' and matches a collection
      const idMatch = field.name.match(/^(.+?)(?:Id|_id)$/i)
      if (idMatch) {
        const baseName = idMatch[1]!.toLowerCase()
        for (const slug of slugs) {
          if (slug.toLowerCase().startsWith(baseName)) {
            relationships.set(fieldPath, slug)
            break
          }
        }
      }
    }
  }

  return relationships
}
