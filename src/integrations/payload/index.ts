/**
 * Payload CMS Database Adapter for ParqueDB
 *
 * This module provides a database adapter that enables Payload CMS to use
 * ParqueDB as its storage backend. It supports all major Payload operations
 * including CRUD, versioning, globals, and transactions.
 *
 * @example Basic usage
 * ```typescript
 * import { buildConfig } from 'payload/config'
 * import { parquedbAdapter } from 'parquedb/payload'
 * import { MemoryBackend } from 'parquedb'
 *
 * export default buildConfig({
 *   db: parquedbAdapter({
 *     storage: new MemoryBackend(),
 *   }),
 *   collections: [
 *     {
 *       slug: 'posts',
 *       fields: [
 *         { name: 'title', type: 'text', required: true },
 *         { name: 'content', type: 'richText' },
 *       ],
 *     },
 *   ],
 * })
 * ```
 *
 * @example With filesystem storage (local development)
 * ```typescript
 * import { parquedbAdapter } from 'parquedb/payload'
 * import { FsBackend } from 'parquedb'
 *
 * export default buildConfig({
 *   db: parquedbAdapter({
 *     storage: new FsBackend('./data'),
 *   }),
 *   // ... collections
 * })
 * ```
 *
 * @example With R2 storage (Cloudflare Workers)
 * ```typescript
 * import { parquedbAdapter } from 'parquedb/payload'
 * import { R2Backend } from 'parquedb'
 *
 * export default buildConfig({
 *   db: parquedbAdapter({
 *     storage: new R2Backend(env.MY_BUCKET),
 *   }),
 *   // ... collections
 * })
 * ```
 *
 * @packageDocumentation
 */

import { PayloadAdapter } from './adapter'
import type { PayloadAdapterConfig } from './types'

/**
 * Create a Payload CMS database adapter for ParqueDB
 *
 * @param config - Adapter configuration
 * @returns Database adapter object for Payload
 */
export function parquedbAdapter(config: PayloadAdapterConfig) {
  // Return the adapter factory object that Payload expects
  return {
    name: 'parquedb',
    defaultIDType: 'text' as const,
    allowIDOnCreate: true,

    /**
     * Initialize the adapter
     * Called by Payload during startup
     */
    init: (args: { payload: unknown }) => {
      const adapter = new PayloadAdapter(config)
      adapter.payload = args.payload
      return adapter
    },
  }
}

// Re-export types
export type { PayloadAdapterConfig } from './types'

// Re-export the adapter class for advanced usage
export { PayloadAdapter } from './adapter'

// Re-export utilities that might be useful for custom integrations
export {
  translatePayloadFilter,
  translatePayloadSort,
  convertLikeToRegex,
} from './filter'

export {
  toParqueDBInput,
  toParqueDBUpdate,
  toPayloadDoc,
  toPayloadDocs,
  extractLocalId,
  buildEntityId,
  buildPaginationInfo,
} from './transform'

// OAuth.do authentication
export {
  oauthUsers,
  createOAuthMiddleware,
  createOAuthActorResolver,
  extractToken,
  verifyOAuthToken,
  canAccessPayloadAdmin,
  getPayloadRole,
  type OAuthConfig,
  type OAuthUser,
  type OAuthJWTPayload,
} from './auth'
