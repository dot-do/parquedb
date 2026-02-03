/**
 * Entity ID parsing utilities
 *
 * Provides consistent parsing of EntityId strings (format: "namespace/localId")
 * to replace fragile split logic repeated throughout the codebase.
 *
 * @module utils/entity-id
 */

import type { EntityId, Namespace } from '../types/entity'

/**
 * Result of parsing an EntityId
 */
export interface ParsedEntityId {
  /** The namespace portion of the ID (before the first '/') */
  ns: Namespace
  /** The local ID portion (after the first '/', may contain additional '/' characters) */
  localId: string
}

/**
 * Parse an EntityId into its namespace and local ID components.
 *
 * EntityIds are in the format "namespace/localId" where:
 * - namespace: The collection/namespace name (cannot contain '/')
 * - localId: The unique identifier within the namespace (may contain '/')
 *
 * @param id - The EntityId to parse
 * @returns An object containing the namespace and localId
 * @throws Error if the ID does not contain a '/' separator
 *
 * @example
 * const { ns, localId } = parseEntityId('users/user-123')
 * // ns: 'users', localId: 'user-123'
 *
 * @example
 * // Handles IDs with multiple slashes
 * const { ns, localId } = parseEntityId('files/path/to/file.txt')
 * // ns: 'files', localId: 'path/to/file.txt'
 */
export function parseEntityId(id: EntityId | string): ParsedEntityId {
  const slashIndex = id.indexOf('/')

  if (slashIndex === -1) {
    throw new Error(`Invalid EntityId: "${id}" (must contain '/' separator)`)
  }

  if (slashIndex === 0) {
    throw new Error(`Invalid EntityId: "${id}" (namespace cannot be empty)`)
  }

  const ns = id.slice(0, slashIndex) as Namespace
  const localId = id.slice(slashIndex + 1)

  if (localId.length === 0) {
    throw new Error(`Invalid EntityId: "${id}" (localId cannot be empty)`)
  }

  return { ns, localId }
}

/**
 * Parse an EntityId, returning null if invalid instead of throwing.
 *
 * @param id - The EntityId to parse
 * @returns An object containing the namespace and localId, or null if invalid
 *
 * @example
 * const result = tryParseEntityId('users/user-123')
 * if (result) {
 *   console.log(result.ns, result.localId)
 * }
 */
export function tryParseEntityId(id: EntityId | string | unknown): ParsedEntityId | null {
  if (typeof id !== 'string') {
    return null
  }

  const slashIndex = id.indexOf('/')

  if (slashIndex <= 0 || slashIndex === id.length - 1) {
    return null
  }

  return {
    ns: id.slice(0, slashIndex) as Namespace,
    localId: id.slice(slashIndex + 1),
  }
}

/**
 * Create a user actor EntityId from a user ID.
 *
 * This is a convenience function for the common pattern of creating
 * EntityIds in the 'users' namespace for actor identification.
 *
 * If the userId already contains a '/' (i.e., is already a full EntityId),
 * it is returned as-is. Otherwise, it's prefixed with 'users/'.
 *
 * @param userId - The user's ID (either a raw ID or a full EntityId)
 * @returns An EntityId in the format 'users/{userId}' or the original if already an EntityId
 *
 * @example
 * userActorId('user-123')           // 'users/user-123'
 * userActorId('users/user-123')     // 'users/user-123' (unchanged)
 * userActorId('admins/admin-1')     // 'admins/admin-1' (unchanged)
 */
export function userActorId(userId: string): EntityId {
  // If it's already a full EntityId, return as-is
  if (userId.includes('/')) {
    return userId as EntityId
  }
  return `users/${userId}` as EntityId
}
