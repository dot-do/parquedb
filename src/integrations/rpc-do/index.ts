/**
 * rpc.do Integration for ParqueDB
 *
 * Provides RPC client with batching capabilities for efficient batch loading
 * of relationships, solving the N+1 query problem.
 *
 * @example
 * ```typescript
 * import { createParqueDBRPCClient } from 'parquedb/integrations/rpc-do'
 *
 * const client = createParqueDBRPCClient({
 *   url: 'https://my-parquedb.workers.dev/rpc',
 *   batchingOptions: {
 *     windowMs: 10,
 *     maxBatchSize: 50
 *   }
 * })
 *
 * // Batch load relationships
 * const authorsWithPosts = await client.batchGetRelated([
 *   { type: 'users', id: 'user-1', relation: 'posts' },
 *   { type: 'users', id: 'user-2', relation: 'posts' },
 * ])
 * ```
 *
 * @packageDocumentation
 */

export {
  createParqueDBRPCClient,
  createBatchLoaderDB,
  type ParqueDBRPCClientOptions,
  type ParqueDBRPCClient,
  type RPCCollection,
  type BatchRelatedRequest,
  type BatchingOptions,
  type BatchedRequest,
  type BatchedResponse,
  type Transport,
  type RPCBatchLoaderDB,
} from './client'
