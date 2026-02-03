/**
 * Schema Discovery
 *
 * Discovers Parquet files and extracts schema information to generate
 * Payload CMS collections automatically.
 */

import { parquetMetadataAsync } from 'hyparquet'
import type { SchemaElement } from 'hyparquet'
import type { StorageBackend } from '../types/storage'
import { initializeAsyncBuffer } from '../parquet/reader'
import { logger } from '../utils/logger'
import type {
  DiscoveredCollection,
  DiscoveredField,
  PayloadFieldType,
} from './types'

// =============================================================================
// Type Mapping
// =============================================================================

/**
 * Map Parquet physical types to Payload field types
 */
const PARQUET_TO_PAYLOAD: Record<string, PayloadFieldType> = {
  // String types
  'BYTE_ARRAY': 'text',
  'STRING': 'text',
  'UTF8': 'text',

  // Numeric types
  'INT32': 'number',
  'INT64': 'number',
  'INT96': 'number',
  'FLOAT': 'number',
  'DOUBLE': 'number',

  // Boolean
  'BOOLEAN': 'checkbox',

  // Date/Time
  'DATE': 'date',
  'TIME_MILLIS': 'text', // No time-only field in Payload
  'TIME_MICROS': 'text',
  'TIMESTAMP_MILLIS': 'date',
  'TIMESTAMP_MICROS': 'date',

  // Binary
  'FIXED_LEN_BYTE_ARRAY': 'text',
}

/**
 * Map Parquet converted/logical types to Payload field types
 */
const CONVERTED_TYPE_TO_PAYLOAD: Record<string, PayloadFieldType> = {
  'UTF8': 'text',
  'DATE': 'date',
  'TIME_MILLIS': 'text',
  'TIME_MICROS': 'text',
  'TIMESTAMP_MILLIS': 'date',
  'TIMESTAMP_MICROS': 'date',
  'INT_8': 'number',
  'INT_16': 'number',
  'INT_32': 'number',
  'INT_64': 'number',
  'UINT_8': 'number',
  'UINT_16': 'number',
  'UINT_32': 'number',
  'UINT_64': 'number',
  'DECIMAL': 'number',
  'JSON': 'json',
  'BSON': 'json',
  'ENUM': 'select',
  'UUID': 'text',
  'MAP': 'json',
  'LIST': 'array',
}

// =============================================================================
// Discovery Functions
// =============================================================================

/**
 * Discover all Parquet files in a directory
 *
 * @param storage - Storage backend
 * @param baseDir - Base directory to search (e.g., '.db' or 'data')
 * @returns Array of discovered collections
 */
export async function discoverCollections(
  storage: StorageBackend,
  baseDir: string
): Promise<DiscoveredCollection[]> {
  const collections: DiscoveredCollection[] = []

  // List all files in the directory
  const result = await storage.list(baseDir, { delimiter: '/' })

  // Check for parquet files directly in baseDir
  for (const file of result.files) {
    if (file.endsWith('.parquet')) {
      try {
        const name = file.split('/').pop() ?? file
        const slug = name.replace('.parquet', '')
        const collection = await discoverCollection(storage, file, slug)
        collections.push(collection)
      } catch (error) {
        logger.warn(`Failed to discover ${file}:`, error)
      }
    }
  }

  // Check for nested directories (namespace/data.parquet pattern)
  for (const prefix of result.prefixes ?? []) {
    const nestedResult = await storage.list(prefix)
    const dataFile = nestedResult.files.find((f) => f.endsWith('/data.parquet') || f === 'data.parquet')

    if (dataFile) {
      try {
        // Extract slug from prefix (e.g., '.db/posts/' -> 'posts')
        const slug = prefix.replace(/\/$/, '').split('/').pop() ?? prefix
        const collection = await discoverCollection(storage, dataFile, slug)
        collections.push(collection)
      } catch (error) {
        logger.warn(`Failed to discover ${dataFile}:`, error)
      }
    }
  }

  return collections
}

/**
 * Discover a single collection from a Parquet file
 *
 * @param storage - Storage backend
 * @param path - Path to the Parquet file
 * @param slug - Collection slug (namespace name)
 * @returns Discovered collection metadata
 */
export async function discoverCollection(
  storage: StorageBackend,
  path: string,
  slug: string
): Promise<DiscoveredCollection> {
  // Read file metadata
  const stat = await storage.stat(path)
  if (!stat) {
    throw new Error(`File not found: ${path}`)
  }

  // Read Parquet metadata
  const asyncBuffer = await initializeAsyncBuffer(storage, path)
  const metadata = await parquetMetadataAsync(asyncBuffer)

  // Extract schema fields
  const fields = extractFields(metadata.schema ?? [])

  // Check if this is a ParqueDB-managed file
  const isParqueDB = fields.some((f) => f.name === '$id') &&
                     fields.some((f) => f.name === '$type')

  // Generate human-readable label
  const label = slugToLabel(slug)

  return {
    slug,
    label,
    path,
    rowCount: Number(metadata.num_rows ?? 0),
    fileSize: stat.size,
    fields,
    isParqueDB,
    lastModified: stat.mtime,
  }
}

/**
 * Extract fields from Parquet schema elements
 *
 * @param schema - Array of Parquet schema elements
 * @returns Array of discovered fields
 */
export function extractFields(schema: SchemaElement[]): DiscoveredField[] {
  const fields: DiscoveredField[] = []

  // Skip the root element (first element is usually the message/root)
  const fieldElements = schema.slice(1)

  for (const element of fieldElements) {
    // Skip nested struct elements (we flatten for now)
    if (element.num_children !== undefined && element.num_children > 0) {
      continue
    }

    const field = schemaElementToField(element)
    if (field) {
      fields.push(field)
    }
  }

  return fields
}

/**
 * Convert a Parquet schema element to a discovered field
 *
 * @param element - Parquet schema element
 * @returns Discovered field or null if not applicable
 */
export function schemaElementToField(element: SchemaElement): DiscoveredField | null {
  const name = element.name
  if (!name) return null

  // Determine the Parquet type
  const physicalType = element.type as string | undefined
  const convertedType = element.converted_type as string | undefined
  const logicalType = element.logical_type as { type?: string } | undefined

  // Determine optionality from repetition type
  // 'REQUIRED', 'OPTIONAL', 'REPEATED'
  const repetitionType = element.repetition_type
  const optional = repetitionType !== 'REQUIRED'
  const isArray = repetitionType === 'REPEATED'

  // Get the Parquet type string for display
  let parquetType = physicalType ?? 'UNKNOWN'
  if (convertedType) {
    parquetType = `${parquetType} (${convertedType})`
  }

  // Determine Payload field type
  let payloadType: PayloadFieldType = 'text'

  // Try converted type first (more specific)
  if (convertedType && CONVERTED_TYPE_TO_PAYLOAD[convertedType]) {
    payloadType = CONVERTED_TYPE_TO_PAYLOAD[convertedType]!
  }
  // Try logical type
  else if (logicalType?.type && CONVERTED_TYPE_TO_PAYLOAD[logicalType.type]) {
    payloadType = CONVERTED_TYPE_TO_PAYLOAD[logicalType.type]!
  }
  // Fall back to physical type
  else if (physicalType && PARQUET_TO_PAYLOAD[physicalType]) {
    payloadType = PARQUET_TO_PAYLOAD[physicalType]!
  }

  // Handle arrays
  if (isArray && payloadType !== 'array') {
    payloadType = 'array'
  }

  // Build type info
  const typeInfo: Record<string, unknown> = {}
  if (element.scale !== undefined) typeInfo['scale'] = element.scale
  if (element.precision !== undefined) typeInfo['precision'] = element.precision
  if (element.type_length !== undefined) typeInfo['typeLength'] = element.type_length

  return {
    name,
    parquetType,
    payloadType,
    optional,
    isArray,
    typeInfo: Object.keys(typeInfo).length > 0 ? typeInfo : undefined,
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Convert a slug to a human-readable label
 *
 * @param slug - Collection slug (e.g., 'user_profiles')
 * @returns Human-readable label (e.g., 'User Profiles')
 */
export function slugToLabel(slug: string): string {
  return slug
    // Handle snake_case
    .replace(/_/g, ' ')
    // Handle camelCase
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    // Handle kebab-case
    .replace(/-/g, ' ')
    // Capitalize each word
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim()
}

/**
 * Determine if a field should be used as the title field
 *
 * @param fields - Array of discovered fields
 * @returns Name of the recommended title field
 */
export function findTitleField(fields: DiscoveredField[]): string {
  // Common title field names in priority order
  const titleCandidates = [
    'name',
    'title',
    'label',
    'displayName',
    'display_name',
    'fullName',
    'full_name',
    'username',
    'email',
    '$id',
  ]

  for (const candidate of titleCandidates) {
    const field = fields.find(
      (f) => f.name.toLowerCase() === candidate.toLowerCase()
    )
    if (field && field.payloadType === 'text') {
      return field.name
    }
  }

  // Fall back to first text field
  const textField = fields.find((f) => f.payloadType === 'text')
  if (textField) {
    return textField.name
  }

  // Ultimate fallback
  return fields[0]?.name ?? 'id'
}

/**
 * Determine recommended default columns for list view
 *
 * @param fields - Array of discovered fields
 * @param isParqueDB - Whether this is a ParqueDB-managed file
 * @returns Array of field names for default columns
 */
export function findDefaultColumns(
  fields: DiscoveredField[],
  isParqueDB: boolean
): string[] {
  const columns: string[] = []

  // For ParqueDB files, start with $id
  if (isParqueDB) {
    const idField = fields.find((f) => f.name === '$id')
    if (idField) columns.push('$id')
  }

  // Add name/title field
  const titleField = findTitleField(fields)
  if (titleField && !columns.includes(titleField)) {
    columns.push(titleField)
  }

  // Add other common display fields
  const displayCandidates = [
    'status',
    'type',
    'category',
    'createdAt',
    'updatedAt',
    'created_at',
    'updated_at',
  ]

  for (const candidate of displayCandidates) {
    if (columns.length >= 5) break // Max 5 columns

    const field = fields.find(
      (f) => f.name.toLowerCase() === candidate.toLowerCase()
    )
    if (field && !columns.includes(field.name)) {
      columns.push(field.name)
    }
  }

  // Fill remaining slots with text fields
  for (const field of fields) {
    if (columns.length >= 5) break
    if (
      field.payloadType === 'text' &&
      !columns.includes(field.name) &&
      !field.name.startsWith('$')
    ) {
      columns.push(field.name)
    }
  }

  return columns
}
