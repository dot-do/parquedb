/**
 * UI Metadata Management
 *
 * Handles loading, saving, and managing UI metadata for collections.
 * Metadata is stored in .studio/metadata.json separate from the data.
 */

import type { StorageBackend } from '../types/storage'
import type {
  StudioMetadata,
  CollectionUIMetadata,
  FieldUIMetadata,
  DiscoveredCollection,
} from './types'
import { findTitleField, findDefaultColumns } from './discovery'
import { logger } from '../utils/logger'

// =============================================================================
// Constants
// =============================================================================

const METADATA_VERSION = '1.0' as const
const METADATA_FILENAME = 'metadata.json'

// =============================================================================
// Metadata Loading
// =============================================================================

/**
 * Load studio metadata from disk
 *
 * @param storage - Storage backend
 * @param metadataDir - Directory containing metadata (default: '.studio')
 * @returns Studio metadata or default if not found
 */
export async function loadMetadata(
  storage: StorageBackend,
  metadataDir: string = '.studio'
): Promise<StudioMetadata> {
  const path = `${metadataDir}/${METADATA_FILENAME}`

  try {
    const exists = await storage.exists(path)
    if (!exists) {
      return createDefaultMetadata()
    }

    const data = await storage.read(path)
    const text = new TextDecoder().decode(data)
    const parsed = JSON.parse(text)

    // Validate version
    if (parsed.version !== METADATA_VERSION) {
      logger.warn(`Metadata version mismatch: ${parsed.version}, expected ${METADATA_VERSION}`)
    }

    return parsed as StudioMetadata
  } catch (error) {
    logger.warn(`Failed to load metadata from ${path}:`, error)
    return createDefaultMetadata()
  }
}

/**
 * Save studio metadata to disk
 *
 * @param storage - Storage backend
 * @param metadata - Metadata to save
 * @param metadataDir - Directory for metadata (default: '.studio')
 */
export async function saveMetadata(
  storage: StorageBackend,
  metadata: StudioMetadata,
  metadataDir: string = '.studio'
): Promise<void> {
  const path = `${metadataDir}/${METADATA_FILENAME}`

  // Ensure directory exists
  const dirExists = await storage.exists(metadataDir)
  if (!dirExists) {
    await storage.mkdir(metadataDir)
  }

  // Write metadata
  const text = JSON.stringify(metadata, null, 2)
  const data = new TextEncoder().encode(text)
  await storage.write(path, data)
}

/**
 * Create default empty metadata
 */
export function createDefaultMetadata(): StudioMetadata {
  return {
    version: METADATA_VERSION,
    collections: {},
  }
}

// =============================================================================
// Metadata Generation
// =============================================================================

/**
 * Generate initial metadata for a discovered collection
 *
 * Creates sensible defaults based on the schema.
 *
 * @param collection - Discovered collection
 * @returns Collection UI metadata
 */
export function generateCollectionMetadata(
  collection: DiscoveredCollection
): CollectionUIMetadata {
  const { label, fields, isParqueDB } = collection

  // Generate field metadata
  const fieldMetadata: Record<string, FieldUIMetadata> = {}

  for (const field of fields) {
    const meta = generateFieldMetadata(field.name, field.payloadType, isParqueDB)
    if (Object.keys(meta).length > 0) {
      fieldMetadata[field.name] = meta
    }
  }

  return {
    label,
    labelSingular: label.replace(/s$/, ''),
    admin: {
      useAsTitle: findTitleField(fields),
      defaultColumns: findDefaultColumns(fields, isParqueDB),
    },
    fields: fieldMetadata,
  }
}

/**
 * Generate field metadata based on common patterns
 */
function generateFieldMetadata(
  name: string,
  _type: string,
  isParqueDB: boolean
): FieldUIMetadata {
  const meta: FieldUIMetadata = {}

  // Handle ParqueDB system fields
  if (isParqueDB) {
    switch (name) {
      case '$id':
        meta.label = 'ID'
        meta.readOnly = true
        meta.admin = { position: 'sidebar' }
        break
      case 'name':
        meta.label = 'Name'
        break
      case 'createdAt':
        meta.label = 'Created'
        meta.readOnly = true
        meta.admin = { position: 'sidebar' }
        break
      case 'updatedAt':
        meta.label = 'Updated'
        meta.readOnly = true
        meta.admin = { position: 'sidebar' }
        break
    }
  }

  // Handle common field patterns
  const nameLower = name.toLowerCase()

  if (nameLower === 'email') {
    meta.description = 'Email address'
  } else if (nameLower === 'password') {
    meta.hideInList = true
  } else if (nameLower === 'description' || nameLower === 'bio') {
    meta.description = 'A detailed description'
  } else if (nameLower.includes('url') || nameLower.includes('link')) {
    meta.description = 'Enter a valid URL'
  } else if (nameLower === 'status') {
    meta.options = [
      { label: 'Draft', value: 'draft' },
      { label: 'Published', value: 'published' },
      { label: 'Archived', value: 'archived' },
    ]
  }

  return meta
}

/**
 * Merge new collections into existing metadata
 *
 * Preserves existing customizations while adding new collections.
 *
 * @param existing - Existing metadata
 * @param collections - Newly discovered collections
 * @returns Merged metadata
 */
export function mergeMetadata(
  existing: StudioMetadata,
  collections: DiscoveredCollection[]
): StudioMetadata {
  const merged = { ...existing }

  for (const collection of collections) {
    if (!merged.collections[collection.slug]) {
      // New collection - generate defaults
      merged.collections[collection.slug] = generateCollectionMetadata(collection)
    } else {
      // Existing collection - preserve customizations, maybe add new fields
      const existingMeta = merged.collections[collection.slug]!
      const existingFieldNames = new Set(
        Object.keys(existingMeta.fields ?? {})
      )

      // Add metadata for new fields
      for (const field of collection.fields) {
        if (!existingFieldNames.has(field.name)) {
          if (!existingMeta.fields) {
            existingMeta.fields = {}
          }
          const fieldMeta = generateFieldMetadata(
            field.name,
            field.payloadType,
            collection.isParqueDB
          )
          if (Object.keys(fieldMeta).length > 0) {
            existingMeta.fields[field.name] = fieldMeta
          }
        }
      }
    }
  }

  return merged
}

// =============================================================================
// Metadata Updates
// =============================================================================

/**
 * Update collection metadata
 *
 * @param metadata - Current metadata
 * @param slug - Collection slug
 * @param updates - Partial updates to apply
 * @returns Updated metadata
 */
export function updateCollectionMetadata(
  metadata: StudioMetadata,
  slug: string,
  updates: Partial<CollectionUIMetadata>
): StudioMetadata {
  return {
    ...metadata,
    collections: {
      ...metadata.collections,
      [slug]: {
        ...metadata.collections[slug],
        ...updates,
      },
    },
  }
}

/**
 * Update field metadata
 *
 * @param metadata - Current metadata
 * @param slug - Collection slug
 * @param fieldName - Field name
 * @param updates - Partial updates to apply
 * @returns Updated metadata
 */
export function updateFieldMetadata(
  metadata: StudioMetadata,
  slug: string,
  fieldName: string,
  updates: Partial<FieldUIMetadata>
): StudioMetadata {
  const collection = metadata.collections[slug] ?? {}

  return {
    ...metadata,
    collections: {
      ...metadata.collections,
      [slug]: {
        ...collection,
        fields: {
          ...(collection.fields ?? {}),
          [fieldName]: {
            ...(collection.fields?.[fieldName] ?? {}),
            ...updates,
          },
        },
      },
    },
  }
}

// =============================================================================
// Metadata Validation
// =============================================================================

/**
 * Validate metadata against discovered collections
 *
 * @param metadata - Metadata to validate
 * @param collections - Discovered collections
 * @returns Validation result
 */
export function validateMetadata(
  metadata: StudioMetadata,
  collections: DiscoveredCollection[]
): { valid: boolean; warnings: string[] } {
  const warnings: string[] = []
  const collectionSlugs = new Set(collections.map((c) => c.slug))

  // Check for metadata for non-existent collections
  for (const slug of Object.keys(metadata.collections)) {
    if (!collectionSlugs.has(slug)) {
      warnings.push(`Metadata for unknown collection: ${slug}`)
    }
  }

  // Check field references
  for (const collection of collections) {
    const collectionMeta = metadata.collections[collection.slug]
    if (!collectionMeta) continue

    const fieldNames = new Set(collection.fields.map((f) => f.name))

    // Check useAsTitle field exists
    if (collectionMeta.admin?.useAsTitle) {
      if (!fieldNames.has(collectionMeta.admin.useAsTitle)) {
        warnings.push(
          `${collection.slug}: useAsTitle references unknown field: ${collectionMeta.admin.useAsTitle}`
        )
      }
    }

    // Check defaultColumns fields exist
    if (collectionMeta.admin?.defaultColumns) {
      for (const col of collectionMeta.admin.defaultColumns) {
        if (!fieldNames.has(col)) {
          warnings.push(
            `${collection.slug}: defaultColumns references unknown field: ${col}`
          )
        }
      }
    }

    // Check field metadata references
    if (collectionMeta.fields) {
      for (const fieldName of Object.keys(collectionMeta.fields)) {
        if (!fieldNames.has(fieldName)) {
          warnings.push(
            `${collection.slug}: field metadata for unknown field: ${fieldName}`
          )
        }
      }
    }
  }

  return {
    valid: warnings.length === 0,
    warnings,
  }
}
