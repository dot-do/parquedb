/**
 * SQL Integration Tests
 *
 * Tests the SQL template tag, parser, translator, and ORM adapters.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ParqueDB } from '../../../src/ParqueDB.js'
import { MemoryBackend } from '../../../src/storage/memory.js'
import {
  createSQL,
  createDrizzleProxy,
  createPrismaAdapter,
  parseSQL,
  translateSelect,
  translateWhere,
  whereToFilter,
} from '../../../src/integrations/sql/index.js'

describe('SQL Parser', () => {
  describe('SELECT statements', () => {
    it('parses simple SELECT *', () => {
      const stmt = parseSQL('SELECT * FROM users')
      expect(stmt.type).toBe('SELECT')
      expect((stmt as any).columns).toBe('*')
      expect((stmt as any).from).toBe('users')
    })

    it('parses SELECT with columns', () => {
      const stmt = parseSQL('SELECT id, name, email FROM users')
      expect(stmt.type).toBe('SELECT')
      expect((stmt as any).columns).toHaveLength(3)
      expect((stmt as any).columns[0].name).toBe('id')
    })

    it('parses SELECT with WHERE clause', () => {
      const stmt = parseSQL("SELECT * FROM users WHERE status = 'active'")
      expect(stmt.type).toBe('SELECT')
      expect((stmt as any).where).toBeDefined()
      expect((stmt as any).where.type).toBe('condition')
    })

    it('parses SELECT with AND conditions', () => {
      const stmt = parseSQL("SELECT * FROM users WHERE status = 'active' AND age > 18")
      expect(stmt.type).toBe('SELECT')
      expect((stmt as any).where.type).toBe('and')
      expect((stmt as any).where.conditions).toHaveLength(2)
    })

    it('parses SELECT with OR conditions', () => {
      const stmt = parseSQL("SELECT * FROM users WHERE status = 'active' OR status = 'pending'")
      expect(stmt.type).toBe('SELECT')
      expect((stmt as any).where.type).toBe('or')
    })

    it('parses SELECT with ORDER BY', () => {
      const stmt = parseSQL('SELECT * FROM users ORDER BY created_at DESC')
      expect((stmt as any).orderBy).toHaveLength(1)
      expect((stmt as any).orderBy[0].column).toBe('created_at')
      expect((stmt as any).orderBy[0].direction).toBe('DESC')
    })

    it('parses SELECT with LIMIT and OFFSET', () => {
      const stmt = parseSQL('SELECT * FROM users LIMIT 10 OFFSET 20')
      expect((stmt as any).limit).toBe(10)
      expect((stmt as any).offset).toBe(20)
    })

    it('parses SELECT with parameters', () => {
      const stmt = parseSQL('SELECT * FROM users WHERE id = $1 AND status = $2')
      expect(stmt.type).toBe('SELECT')
    })

    it('parses SELECT with IN clause', () => {
      const stmt = parseSQL("SELECT * FROM users WHERE status IN ('active', 'pending')")
      expect(stmt.type).toBe('SELECT')
    })

    it('parses SELECT with IS NULL', () => {
      const stmt = parseSQL('SELECT * FROM users WHERE deleted_at IS NULL')
      expect(stmt.type).toBe('SELECT')
    })
  })

  describe('INSERT statements', () => {
    it('parses simple INSERT', () => {
      const stmt = parseSQL("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')")
      expect(stmt.type).toBe('INSERT')
      expect((stmt as any).into).toBe('users')
      expect((stmt as any).columns).toEqual(['name', 'email'])
    })

    it('parses INSERT with parameters', () => {
      const stmt = parseSQL('INSERT INTO users (name, email) VALUES ($1, $2)')
      expect(stmt.type).toBe('INSERT')
    })

    it('parses INSERT with RETURNING', () => {
      const stmt = parseSQL('INSERT INTO users (name) VALUES ($1) RETURNING *')
      expect(stmt.type).toBe('INSERT')
      expect((stmt as any).returning).toBe('*')
    })
  })

  describe('UPDATE statements', () => {
    it('parses simple UPDATE', () => {
      const stmt = parseSQL("UPDATE users SET status = 'inactive' WHERE id = $1")
      expect(stmt.type).toBe('UPDATE')
      expect((stmt as any).table).toBe('users')
      expect((stmt as any).set).toHaveProperty('status')
    })

    it('parses UPDATE with multiple SET values', () => {
      const stmt = parseSQL("UPDATE users SET name = $1, email = $2 WHERE id = $3")
      expect(stmt.type).toBe('UPDATE')
      expect(Object.keys((stmt as any).set)).toHaveLength(2)
    })
  })

  describe('DELETE statements', () => {
    it('parses simple DELETE', () => {
      const stmt = parseSQL('DELETE FROM users WHERE id = $1')
      expect(stmt.type).toBe('DELETE')
      expect((stmt as any).from).toBe('users')
    })

    it('parses DELETE with RETURNING', () => {
      const stmt = parseSQL('DELETE FROM users WHERE id = $1 RETURNING *')
      expect(stmt.type).toBe('DELETE')
      expect((stmt as any).returning).toBe('*')
    })
  })
})

describe('SQL Translator', () => {
  describe('WHERE to Filter', () => {
    it('translates equality', () => {
      const filter = whereToFilter("status = 'active'")
      expect(filter).toEqual({ status: 'active' })
    })

    it('translates with parameters', () => {
      const filter = whereToFilter('status = $1', ['active'])
      expect(filter).toEqual({ status: 'active' })
    })

    it('translates greater than', () => {
      const filter = whereToFilter('age > $1', [25])
      expect(filter).toEqual({ age: { $gt: 25 } })
    })

    it('translates less than or equal', () => {
      const filter = whereToFilter('age <= $1', [30])
      expect(filter).toEqual({ age: { $lte: 30 } })
    })

    it('translates IN clause', () => {
      const filter = whereToFilter("status IN ('active', 'pending')")
      expect(filter).toEqual({ status: { $in: ['active', 'pending'] } })
    })

    it('translates NOT IN clause', () => {
      const filter = whereToFilter("status NOT IN ('deleted', 'banned')")
      expect(filter).toEqual({ status: { $nin: ['deleted', 'banned'] } })
    })

    it('translates IS NULL', () => {
      const filter = whereToFilter('deleted_at IS NULL')
      expect(filter).toEqual({ deleted_at: { $exists: false } })
    })

    it('translates IS NOT NULL', () => {
      const filter = whereToFilter('deleted_at IS NOT NULL')
      expect(filter).toEqual({ deleted_at: { $exists: true } })
    })

    it('translates LIKE pattern', () => {
      const filter = whereToFilter("name LIKE '%john%'")
      expect(filter).toHaveProperty('name')
      expect(filter.name).toHaveProperty('$regex')
    })

    it('translates AND conditions', () => {
      const filter = whereToFilter('status = $1 AND age > $2', ['active', 25])
      expect(filter).toEqual({ status: 'active', age: { $gt: 25 } })
    })

    it('translates OR conditions', () => {
      const filter = whereToFilter('status = $1 OR status = $2', ['active', 'pending'])
      expect(filter).toEqual({
        $or: [{ status: 'active' }, { status: 'pending' }],
      })
    })
  })

  describe('SELECT translation', () => {
    it('translates simple SELECT', () => {
      const stmt = parseSQL('SELECT * FROM users') as any
      const query = translateSelect(stmt, [])
      expect(query.collection).toBe('users')
      expect(query.filter).toEqual({})
    })

    it('translates SELECT with columns', () => {
      const stmt = parseSQL('SELECT id, name FROM users') as any
      const query = translateSelect(stmt, [])
      expect(query.columns).toEqual(['id', 'name'])
    })

    it('translates SELECT with ORDER BY', () => {
      const stmt = parseSQL('SELECT * FROM users ORDER BY created_at DESC') as any
      const query = translateSelect(stmt, [])
      expect(query.orderBy).toBe('created_at')
      expect(query.desc).toBe(true)
    })

    it('translates SELECT with LIMIT/OFFSET', () => {
      const stmt = parseSQL('SELECT * FROM users LIMIT 10 OFFSET 5') as any
      const query = translateSelect(stmt, [])
      expect(query.limit).toBe(10)
      expect(query.offset).toBe(5)
    })
  })
})

describe('SQL Template Tag', () => {
  let db: ParqueDB

  beforeEach(async () => {
    const backend = new MemoryBackend()
    db = new ParqueDB({ storage: backend })
  })

  it('executes SELECT query', async () => {
    // Create some test data
    const collection = db.collection('users')
    await collection.create({ name: 'Alice', status: 'active' })
    await collection.create({ name: 'Bob', status: 'inactive' })
    await collection.create({ name: 'Charlie', status: 'active' })

    const sql = createSQL(db)
    const result = await sql`SELECT * FROM users WHERE status = ${'active'}`

    expect(result.rows).toHaveLength(2)
    expect(result.rowCount).toBe(2)
    expect(result.command).toBe('SELECT')
  })

  it('executes INSERT query', async () => {
    const sql = createSQL(db)
    const result = await sql`INSERT INTO users (name, email) VALUES (${'John'}, ${'john@test.com'})`

    expect(result.rows).toHaveLength(1)
    expect(result.rowCount).toBe(1)
    expect(result.command).toBe('INSERT')

    // Verify insert
    const collection = db.collection('users')
    const users = await collection.find({})
    expect(users).toHaveLength(1)
  })

  it('executes UPDATE query', async () => {
    // Create test data
    const collection = db.collection('users')
    await collection.create({ name: 'Alice', status: 'active' })

    const sql = createSQL(db)
    const result = await sql`UPDATE users SET status = ${'inactive'} WHERE name = ${'Alice'}`

    expect(result.rowCount).toBe(1)
    expect(result.command).toBe('UPDATE')

    // Verify update
    const users = await collection.find({ name: 'Alice' })
    expect(users[0].status).toBe('inactive')
  })

  it('executes DELETE query', async () => {
    // Create test data
    const collection = db.collection('users')
    await collection.create({ name: 'Alice', status: 'active' })
    await collection.create({ name: 'Bob', status: 'active' })

    const sql = createSQL(db)
    const result = await sql`DELETE FROM users WHERE name = ${'Alice'}`

    expect(result.rowCount).toBe(1)
    expect(result.command).toBe('DELETE')

    // Verify delete
    const users = await collection.find({})
    expect(users).toHaveLength(1)
    expect(users[0].name).toBe('Bob')
  })

  it('supports raw queries with parameters array', async () => {
    const collection = db.collection('users')
    await collection.create({ name: 'Alice', age: 25 })
    await collection.create({ name: 'Bob', age: 30 })

    const sql = createSQL(db)
    const result = await sql.raw('SELECT * FROM users WHERE age > $1', [20])

    expect(result.rows).toHaveLength(2)
  })
})

describe('Drizzle Proxy', () => {
  let db: ParqueDB

  beforeEach(async () => {
    const backend = new MemoryBackend()
    db = new ParqueDB({ storage: backend })
  })

  it('creates a proxy callback', () => {
    const proxy = createDrizzleProxy(db)
    expect(typeof proxy).toBe('function')
  })

  it('executes SELECT via proxy', async () => {
    // Create test data
    const collection = db.collection('users')
    await collection.create({ name: 'Alice', status: 'active' })

    const proxy = createDrizzleProxy(db)
    const result = await proxy("SELECT * FROM users WHERE status = $1", ['active'], 'all')

    expect(result.rows).toBeDefined()
    expect(Array.isArray(result.rows)).toBe(true)
  })

  it('supports debug mode', async () => {
    const logs: string[] = []
    const originalLog = console.log
    console.log = (...args) => logs.push(args.join(' '))

    const proxy = createDrizzleProxy(db, { debug: true })
    await proxy('SELECT * FROM users', [], 'all')

    console.log = originalLog
    expect(logs.some((l) => l.includes('[drizzle-parquedb]'))).toBe(true)
  })
})

describe('Prisma Adapter', () => {
  let db: ParqueDB

  beforeEach(async () => {
    const backend = new MemoryBackend()
    db = new ParqueDB({ storage: backend })
  })

  it('creates adapter with correct properties', () => {
    const adapter = createPrismaAdapter(db)
    expect(adapter.provider).toBe('sqlite')
    expect(adapter.adapterName).toBe('parquedb')
  })

  it('executes queryRaw', async () => {
    // Create test data
    const collection = db.collection('users')
    await collection.create({ name: 'Alice', status: 'active' })

    const adapter = createPrismaAdapter(db)
    const result = await adapter.queryRaw({
      sql: "SELECT * FROM users WHERE status = $1",
      args: ['active'],
    })

    expect(result.rows).toBeDefined()
    expect(result.columns).toBeDefined()
  })

  it('executes executeRaw for INSERT', async () => {
    const adapter = createPrismaAdapter(db)
    const count = await adapter.executeRaw({
      sql: "INSERT INTO users (name, email) VALUES ($1, $2)",
      args: ['John', 'john@test.com'],
    })

    expect(count).toBe(1)

    // Verify insert
    const collection = db.collection('users')
    const users = await collection.find({})
    expect(users).toHaveLength(1)
  })

  it('starts a transaction', async () => {
    const adapter = createPrismaAdapter(db)
    const transaction = await adapter.startTransaction()

    expect(transaction).toBeDefined()
    expect(typeof transaction.queryRaw).toBe('function')
    expect(typeof transaction.executeRaw).toBe('function')
    expect(typeof transaction.commit).toBe('function')
    expect(typeof transaction.rollback).toBe('function')
  })
})
