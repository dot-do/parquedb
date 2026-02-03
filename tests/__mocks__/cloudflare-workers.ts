/**
 * Mock for cloudflare:workers module
 *
 * This mock provides stub implementations of Cloudflare Workers APIs
 * for running unit tests in Node.js environment.
 */

/**
 * Mock DurableObject base class
 */
export class DurableObject<Env = unknown> {
  protected ctx: DurableObjectState
  protected env: Env

  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx
    this.env = env
  }

  async fetch(_request: Request): Promise<Response> {
    return new Response('Mock DurableObject', { status: 501 })
  }
}

/**
 * Mock DurableObjectState
 */
export interface DurableObjectState {
  id: DurableObjectId
  storage: DurableObjectStorage
  waitUntil: (promise: Promise<unknown>) => void
  blockConcurrencyWhile: <T>(callback: () => Promise<T>) => Promise<T>
}

/**
 * Mock DurableObjectStorage
 */
export interface DurableObjectStorage {
  get<T>(key: string): Promise<T | undefined>
  get<T>(keys: string[]): Promise<Map<string, T>>
  put<T>(key: string, value: T): Promise<void>
  put<T>(entries: Record<string, T>): Promise<void>
  delete(key: string): Promise<boolean>
  delete(keys: string[]): Promise<number>
  deleteAll(): Promise<void>
  list<T>(options?: { prefix?: string; start?: string; end?: string; limit?: number; reverse?: boolean }): Promise<Map<string, T>>
  sql: SqlStorage
}

/**
 * Mock SqlStorage for Durable Objects
 */
export interface SqlStorage {
  exec<T = Record<string, SqlStorageValue>>(query: string, ...params: unknown[]): SqlStorageCursor<T>
}

/**
 * Mock SqlStorageCursor
 */
export interface SqlStorageCursor<T> {
  toArray(): T[]
  one(): T | null
  raw(): unknown[][]
  columnNames: string[]
  rowsRead: number
  rowsWritten: number
}

/**
 * Mock SqlStorageValue type
 */
export type SqlStorageValue = string | number | null | ArrayBuffer

/**
 * Mock DurableObjectId
 */
export interface DurableObjectId {
  toString(): string
  equals(other: DurableObjectId): boolean
}

/**
 * Mock WorkerEntrypoint base class
 */
export class WorkerEntrypoint<Env = unknown> {
  protected env: Env
  protected ctx: ExecutionContext

  constructor(ctx: ExecutionContext, env: Env) {
    this.ctx = ctx
    this.env = env
  }
}

/**
 * Mock ExecutionContext
 */
export interface ExecutionContext {
  waitUntil: (promise: Promise<unknown>) => void
  passThroughOnException: () => void
}

/**
 * Mock RpcTarget for RPC functionality
 */
export class RpcTarget {
  // Base class for RPC targets
}

/**
 * Re-export types that might be needed
 */
export type {
  DurableObjectState as DurableObjectStateType,
  DurableObjectStorage as DurableObjectStorageType,
  DurableObjectId as DurableObjectIdType,
  ExecutionContext as ExecutionContextType,
}
