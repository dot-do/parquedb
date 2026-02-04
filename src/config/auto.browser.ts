/**
 * Auto-configured ParqueDB Instance (Browser)
 *
 * Browser-safe version that uses MemoryBackend only.
 * Does not attempt to detect filesystem or Workers bindings.
 */

import { DB, type DBInstance } from '../db'
import { MemoryBackend } from '../storage/MemoryBackend'

// Module-scoped instance cache
let _db: DBInstance | null = null

/**
 * Initialize the database with memory storage
 *
 * In browser environments, only MemoryBackend is available.
 */
export async function initializeDB(): Promise<DBInstance> {
  if (_db) return _db

  _db = DB({ schema: 'flexible' }, { storage: new MemoryBackend() })
  return _db
}

/**
 * Get the auto-configured database instance
 */
export async function getDB(): Promise<DBInstance> {
  return initializeDB()
}

/**
 * Reset the auto-configured database instance
 */
export function resetDB(): void {
  _db = null
}

/**
 * Lazy-initialized database proxy
 */
export const db: DBInstance = new Proxy({} as DBInstance, {
  get(_target, prop) {
    if (prop === 'then') return undefined // Not a Promise
    if (!_db) {
      throw new Error('Database not initialized. Call initializeDB() first or use await getDB().')
    }
    return (_db as unknown as Record<string | symbol, unknown>)[prop]
  },
})

/**
 * SQL template tag for browser
 */
export const sql = async (strings: TemplateStringsArray, ...values: unknown[]) => {
  const database = await getDB()
  return database.sql(strings, ...values)
}
