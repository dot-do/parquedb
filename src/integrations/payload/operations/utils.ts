/**
 * Shared utilities for Payload CMS adapter operations
 */

import type { EntityId } from '../../../types'
import { userActorId } from '../../../utils/entity-id'
import type { ResolvedAdapterConfig } from '../types'

/**
 * Get actor from request context
 *
 * Extracts the user ID from the request and formats it as an EntityId.
 * If the user ID already contains a slash (i.e., is a full entity ID),
 * it's returned as-is. Otherwise, it's prefixed with 'users/'.
 * Falls back to the config's defaultActor if no user is present.
 */
export function getActor(
  req: { user?: Record<string, unknown> | undefined } | undefined,
  config: ResolvedAdapterConfig
): EntityId {
  if (req?.user && typeof req.user === 'object' && 'id' in req.user) {
    return userActorId(String(req.user.id))
  }
  return config.defaultActor
}
