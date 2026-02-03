/**
 * Shared utilities for ParqueDB integrations
 *
 * This module provides common utility functions used across multiple integration modules.
 */

import type { EntityId } from '../types'
import { userActorId } from '../utils/entity-id'

/**
 * Request context for extracting actor information
 */
export interface RequestContext {
  user?: Record<string, unknown> | undefined
}

/**
 * Config with default actor fallback
 */
export interface ActorConfig {
  defaultActor: EntityId
}

/**
 * Get actor from request context
 *
 * Extracts the user ID from the request and formats it as an EntityId.
 * If the user ID already contains a slash (i.e., is a full entity ID),
 * it's returned as-is. Otherwise, it's prefixed with 'users/'.
 * Falls back to the config's defaultActor if no user is present.
 *
 * @example
 * ```typescript
 * const actor = getActor(req, config)
 * // If req.user.id is 'user-123', returns 'users/user-123'
 * // If req.user.id is 'admins/admin-1', returns 'admins/admin-1'
 * // If no user, returns config.defaultActor
 * ```
 */
export function getActor(
  req: RequestContext | undefined,
  config: ActorConfig
): EntityId {
  if (req?.user && typeof req.user === 'object' && 'id' in req.user) {
    return userActorId(String(req.user.id))
  }
  return config.defaultActor
}

/**
 * Capitalize the first letter of a string
 *
 * @example
 * ```typescript
 * capitalize('hello')  // 'Hello'
 * capitalize('HELLO')  // 'HELLO'
 * capitalize('')       // ''
 * ```
 */
export function capitalize(str: string): string {
  if (!str) return str
  return str.charAt(0).toUpperCase() + str.slice(1)
}

/**
 * Alias for capitalize - capitalizes the first letter of a string
 *
 * @example
 * ```typescript
 * capitalizeFirst('hello')  // 'Hello'
 * ```
 */
export const capitalizeFirst = capitalize

/**
 * Generate a simple name for entities without one
 *
 * Creates a timestamp-based name that is unique enough for temporary use.
 *
 * @example
 * ```typescript
 * generateName()  // 'item-1699900000000'
 * ```
 */
export function generateName(): string {
  return `item-${Date.now()}`
}
