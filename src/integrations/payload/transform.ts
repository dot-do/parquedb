/**
 * Document transformation between Payload CMS and ParqueDB
 *
 * Handles the bidirectional conversion of documents:
 * - Payload → ParqueDB: Converts Payload documents to ParqueDB entity format
 * - ParqueDB → Payload: Converts ParqueDB entities back to Payload format
 */

import type { Entity, EntityId, CreateInput } from '../../types'
import type { ToParqueDBOptions, ToPayloadOptions } from './types'
import { entityId } from '../../types/entity'

// =============================================================================
// Payload → ParqueDB Transform
// =============================================================================

/**
 * Transform a Payload document to a ParqueDB entity input
 *
 * @example
 * // Payload: { title: 'Hello', content: 'World', status: 'published' }
 * // ParqueDB: { $type: 'Post', name: 'Hello', title: 'Hello', content: 'World', status: 'published' }
 */
export function toParqueDBInput(
  data: Record<string, unknown>,
  options: ToParqueDBOptions
): CreateInput {
  const { collection, existingEntity } = options

  // Determine entity type from collection (capitalize first letter)
  const entityType = capitalizeFirst(collection)

  // Extract or derive name field
  const name = deriveName(data, existingEntity)

  // Build the entity input
  const input: CreateInput = {
    $type: entityType,
    name,
  }

  // Copy all data fields, excluding Payload-specific fields
  const excludeFields = new Set(['id', 'createdAt', 'updatedAt', '_status'])

  for (const [key, value] of Object.entries(data)) {
    if (!excludeFields.has(key)) {
      input[key] = transformValueToParqueDB(value)
    }
  }

  // Handle draft status
  if ('_status' in data) {
    input['_status'] = data['_status']
  }

  return input
}

/**
 * Transform a Payload document to a ParqueDB update input
 */
export function toParqueDBUpdate(
  data: Record<string, unknown>,
  _options: ToParqueDBOptions
): Record<string, unknown> {
  const update: Record<string, unknown> = {}

  // Build $set operations for each changed field
  const setOps: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(data)) {
    // Skip id and audit fields - they're managed by ParqueDB
    if (['id', 'createdAt', 'updatedAt'].includes(key)) {
      continue
    }

    setOps[key] = transformValueToParqueDB(value)
  }

  if (Object.keys(setOps).length > 0) {
    update['$set'] = setOps
  }

  return update
}

/**
 * Derive a name field from the document data
 */
function deriveName(data: Record<string, unknown>, existingEntity?: Entity): string {
  // Try common name fields in order of preference
  const nameFields = ['name', 'title', 'label', 'slug', 'email', 'username']

  for (const field of nameFields) {
    if (field in data && typeof data[field] === 'string') {
      return data[field] as string
    }
  }

  // Use existing entity's name if available
  if (existingEntity?.name) {
    return existingEntity.name
  }

  // Fallback to id if available
  if ('id' in data) {
    return String(data['id'])
  }

  // Generate a placeholder name
  return `Untitled`
}

/**
 * Transform a value for storage in ParqueDB
 */
function transformValueToParqueDB(value: unknown): unknown {
  if (value === null || value === undefined) {
    return value
  }

  // Handle Date objects
  if (value instanceof Date) {
    return value
  }

  // Handle date strings
  if (typeof value === 'string' && isISODateString(value)) {
    return new Date(value)
  }

  // Handle arrays
  if (Array.isArray(value)) {
    return value.map(transformValueToParqueDB)
  }

  // Handle relationship references (Payload stores as ID strings or objects)
  if (typeof value === 'object' && value !== null) {
    const obj = value as Record<string, unknown>

    // Check if this is a relationship object with just an id
    if ('id' in obj && Object.keys(obj).length === 1) {
      // Keep as-is, will be resolved during populate
      return obj
    }

    // Recursively transform nested objects
    const transformed: Record<string, unknown> = {}
    for (const [key, val] of Object.entries(obj)) {
      transformed[key] = transformValueToParqueDB(val)
    }
    return transformed
  }

  return value
}

// =============================================================================
// ParqueDB → Payload Transform
// =============================================================================

/**
 * Transform a ParqueDB entity to a Payload document
 *
 * @example
 * // ParqueDB: { $id: 'posts/123', $type: 'Post', name: 'Hello', title: 'Hello', ... }
 * // Payload: { id: '123', title: 'Hello', createdAt: '2024-01-01T00:00:00.000Z', ... }
 */
export function toPayloadDoc<T = Record<string, unknown>>(
  entity: Entity | null | undefined,
  options: ToPayloadOptions
): T | null {
  if (!entity) {
    return null
  }

  const { select } = options

  // Extract local ID from entity ID (format: ns/id)
  const localId = extractLocalId(entity.$id)

  // Build the Payload document
  const doc: Record<string, unknown> = {
    id: localId,
  }

  // Copy data fields, excluding ParqueDB-specific fields
  const excludeFields = new Set(['$id', '$type', 'name', 'version', 'createdBy', 'updatedBy', 'deletedAt', 'deletedBy'])

  for (const [key, value] of Object.entries(entity)) {
    if (excludeFields.has(key)) {
      continue
    }

    // Apply field selection if specified
    if (select && !select[key]) {
      continue
    }

    doc[key] = transformValueToPayload(value)
  }

  // Ensure standard Payload fields are present
  if (!('createdAt' in doc) && entity.createdAt) {
    doc['createdAt'] = toISOString(entity.createdAt)
  }
  if (!('updatedAt' in doc) && entity.updatedAt) {
    doc['updatedAt'] = toISOString(entity.updatedAt)
  }

  return doc as T
}

/**
 * Transform multiple ParqueDB entities to Payload documents
 */
export function toPayloadDocs<T = Record<string, unknown>>(
  entities: Entity[],
  options: ToPayloadOptions
): T[] {
  return entities
    .map(entity => toPayloadDoc<T>(entity, options))
    .filter((doc): doc is T => doc !== null)
}

/**
 * Transform a value for return to Payload
 */
function transformValueToPayload(value: unknown): unknown {
  if (value === null || value === undefined) {
    return value
  }

  // Handle Date objects - convert to ISO string for Payload
  if (value instanceof Date) {
    return toISOString(value)
  }

  // Handle arrays
  if (Array.isArray(value)) {
    return value.map(transformValueToPayload)
  }

  // Handle nested objects
  if (typeof value === 'object' && value !== null) {
    const obj = value as Record<string, unknown>
    const transformed: Record<string, unknown> = {}

    for (const [key, val] of Object.entries(obj)) {
      transformed[key] = transformValueToPayload(val)
    }

    return transformed
  }

  return value
}

// =============================================================================
// Version Transform
// =============================================================================

/**
 * Transform a ParqueDB entity to a Payload version document
 */
export function toPayloadVersion<T = Record<string, unknown>>(
  entity: Entity,
  options: ToPayloadOptions & { parent?: string | undefined }
): Record<string, unknown> {
  const doc = toPayloadDoc<T>(entity, options) || {}

  return {
    id: extractLocalId(entity.$id),
    parent: options.parent || (entity as Record<string, unknown>)['parent'],
    version: doc,
    createdAt: toISOString(entity.createdAt),
    updatedAt: toISOString(entity.updatedAt),
    latest: (entity as Record<string, unknown>)['latest'] ?? false,
    autosave: (entity as Record<string, unknown>)['autosave'] ?? false,
    publishedLocale: (entity as Record<string, unknown>)['publishedLocale'],
    snapshot: (entity as Record<string, unknown>)['snapshot'],
  }
}

/**
 * Build version input for creating a version in ParqueDB
 */
export function buildVersionInput(
  parent: string,
  versionData: Record<string, unknown>,
  options: {
    collection: string
    autosave?: boolean | undefined
    publishedLocale?: string | undefined
    snapshot?: boolean | undefined
  }
): CreateInput {
  const entityType = `${capitalizeFirst(options.collection)}Version`

  return {
    $type: entityType,
    name: `Version of ${parent}`,
    parent,
    version: versionData,
    latest: true,
    autosave: options.autosave ?? false,
    publishedLocale: options.publishedLocale,
    snapshot: options.snapshot ?? false,
  }
}

// =============================================================================
// Global Transform
// =============================================================================

/**
 * Build global document input
 */
export function buildGlobalInput(
  slug: string,
  data: Record<string, unknown>
): CreateInput {
  return {
    $type: 'Global',
    name: slug,
    slug,
    ...data,
  }
}

/**
 * Transform global entity to Payload format
 */
export function toPayloadGlobal<T = Record<string, unknown>>(
  entity: Entity | null,
  _slug: string
): T | null {
  if (!entity) {
    return null
  }

  const doc: Record<string, unknown> = {}

  // Copy all data except ParqueDB internal fields
  const excludeFields = new Set(['$id', '$type', 'name', 'slug', 'version', 'createdBy', 'updatedBy', 'deletedAt', 'deletedBy'])

  for (const [key, value] of Object.entries(entity)) {
    if (!excludeFields.has(key)) {
      doc[key] = transformValueToPayload(value)
    }
  }

  // Ensure audit fields
  if (entity.createdAt) {
    doc['createdAt'] = toISOString(entity.createdAt)
  }
  if (entity.updatedAt) {
    doc['updatedAt'] = toISOString(entity.updatedAt)
  }

  return doc as T
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Extract local ID from a full entity ID
 * @example extractLocalId('posts/123') => '123'
 */
export function extractLocalId(fullId: EntityId | string): string {
  const idStr = String(fullId)
  const slashIndex = idStr.indexOf('/')
  if (slashIndex === -1) {
    return idStr
  }
  return idStr.slice(slashIndex + 1)
}

/**
 * Build a full entity ID from collection and local ID
 */
export function buildEntityId(collection: string, localId: string): EntityId {
  return entityId(collection, localId)
}

/**
 * Capitalize the first letter of a string
 */
function capitalizeFirst(str: string): string {
  if (!str) return str
  return str.charAt(0).toUpperCase() + str.slice(1)
}

/**
 * Check if a string is an ISO date string
 */
function isISODateString(value: string): boolean {
  // Basic ISO 8601 date pattern
  const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?(Z|[+-]\d{2}:\d{2})?)?$/
  return isoPattern.test(value)
}

/**
 * Convert a Date to ISO string, handling various input types
 */
function toISOString(value: Date | string | number | unknown): string {
  if (value instanceof Date) {
    return value.toISOString()
  }
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'number') {
    return new Date(value).toISOString()
  }
  return new Date().toISOString()
}

/**
 * Build pagination info for Payload response
 */
export function buildPaginationInfo(
  totalDocs: number,
  limit: number,
  page: number
): {
  hasNextPage: boolean
  hasPrevPage: boolean
  limit: number
  nextPage: number | null
  page: number
  pagingCounter: number
  prevPage: number | null
  totalDocs: number
  totalPages: number
} {
  const totalPages = Math.ceil(totalDocs / limit) || 1
  const hasNextPage = page < totalPages
  const hasPrevPage = page > 1

  return {
    hasNextPage,
    hasPrevPage,
    limit,
    nextPage: hasNextPage ? page + 1 : null,
    page,
    pagingCounter: (page - 1) * limit + 1,
    prevPage: hasPrevPage ? page - 1 : null,
    totalDocs,
    totalPages,
  }
}
