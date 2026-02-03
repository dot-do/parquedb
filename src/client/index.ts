/**
 * ParqueDB Client Module
 *
 * Provides RPC client for interacting with ParqueDB workers.
 *
 * Uses capnweb patterns for efficient RPC:
 * - RpcPromise: Chain method calls without awaiting
 * - RpcTarget: Pass-by-reference objects
 * - .map(): Server-side iteration
 *
 * @see https://github.com/nicholascelestin/capnweb
 */

// Main client exports
export {
  ParqueDBClient,
  createParqueDBClient,
  type ParqueDBService,
  type Collection,
  type ParqueDBClientOptions,
  type TypedCollection,
  type EntityOf,
  type CreateInputOf,
} from './ParqueDBClient'

// RPC Promise exports for capnweb patterns
export {
  createRpcPromise,
  isRpcPromise,
  batchRpc,
  resolvedRpcPromise,
  RpcError,
  type RpcPromiseChain,
} from './rpc-promise'

// Service binding adapter
export {
  ServiceBindingAdapter,
  createServiceAdapter,
  isServiceBinding,
  type Service,
} from './service-binding'

// Re-export collection client for advanced use cases
export { CollectionClient, type CollectionCreateInput } from './collection'

// Remote database client for public/unlisted databases
export {
  openRemoteDB,
  checkRemoteDB,
  listPublicDatabases,
  type RemoteDB,
  type RemoteCollection,
  type RemoteDBInfo,
  type OpenRemoteDBOptions,
} from './remote'
