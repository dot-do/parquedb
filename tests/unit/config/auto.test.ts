/**
 * Auto-configuration Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  detectRuntime,
  isServer,
  isWorkers,
  isBrowser,
} from '../../../src/config/runtime'
import {
  initializeDB,
  resetDB,
} from '../../../src/config/auto'

describe('Runtime Detection', () => {
  it('detects Node.js runtime', () => {
    const runtime = detectRuntime()
    // In vitest, we're running in Node.js
    expect(runtime).toBe('node')
  })

  it('isServer returns true in Node.js', () => {
    expect(isServer()).toBe(true)
  })

  it('isWorkers returns false in Node.js', () => {
    expect(isWorkers()).toBe(false)
  })

  it('isBrowser returns false in Node.js', () => {
    expect(isBrowser()).toBe(false)
  })
})

describe('Auto-configured DB', () => {
  beforeEach(() => {
    resetDB()
  })

  afterEach(() => {
    resetDB()
  })

  it('initializes with MemoryBackend by default in tests', async () => {
    const db = await initializeDB()
    expect(db).toBeDefined()
    expect(db.sql).toBeDefined()
  })

  it('returns same instance on multiple calls', async () => {
    const db1 = await initializeDB()
    const db2 = await initializeDB()
    expect(db1).toBe(db2)
  })

  it('can create and query collections', async () => {
    const db = await initializeDB()
    const collection = db.collection('users')

    await collection.create({ name: 'Alice', status: 'active' })
    const result = await collection.find({ name: 'Alice' })

    expect(result.items).toHaveLength(1)
    expect(result.items[0].name).toBe('Alice')
  })

  it('sql executor works', async () => {
    const db = await initializeDB()
    const collection = db.collection('posts')

    await collection.create({ title: 'Hello', status: 'published' })
    await collection.create({ title: 'World', status: 'draft' })

    const result = await db.sql`SELECT * FROM posts WHERE status = ${'published'}`

    expect(result.rows).toHaveLength(1)
    expect(result.command).toBe('SELECT')
  })
})

describe('Lazy Proxy', () => {
  beforeEach(() => {
    resetDB()
  })

  afterEach(() => {
    resetDB()
  })

  it('db proxy lazily initializes', async () => {
    // Import the lazy db
    const { db } = await import('../../../src/config/auto')

    // First access triggers initialization
    const collection = db.Users
    expect(collection).toBeDefined()

    // Can call methods
    const result = await collection.find({})
    expect(result).toBeDefined()
  })

  it('sql executor works via initializeDB', async () => {
    const db = await initializeDB()

    // db.sql should be the SQL executor function
    expect(db.sql).toBeDefined()
    expect(typeof db.sql).toBe('function')

    // Template tag works
    const result = await db.sql`SELECT * FROM test`
    expect(result).toBeDefined()
    expect(result.command).toBe('SELECT')
  })
})
