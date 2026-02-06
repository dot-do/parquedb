/**
 * DOReadPath Filter Tests -- Verify operator-based filters work through DOReadPath.find()
 *
 * Issue: parquedb-zou5.1
 *
 * The local matchesFilter in do-read-path.ts only supports equality checks,
 * silently returning wrong results for $gt, $lt, $in, $or, $and, $regex, etc.
 * The shared matchesFilter from filter.ts supports all these operators.
 *
 * These tests demonstrate the bug by exercising operator filters through
 * DOReadPath.find() using the same mock pattern as do-workflow.test.ts.
 *
 * Run with: pnpm test tests/engine/do-read-path-filter
 */

import { describe, it, expect } from 'vitest'
import { DOReadPath } from '@/engine/do-read-path'
import { SqliteWal } from '@/engine/sqlite-wal'

// =============================================================================
// Mock SqlStorage (same pattern as do-workflow.test.ts)
// =============================================================================

class MockSqlStorage {
  private rows: Array<{
    id: number
    ts: number
    kind: string
    batch: string
    row_count: number
    flushed: number
  }> = []
  private nextId = 1

  exec(query: string, ...bindings: unknown[]): { toArray(): unknown[] } {
    const q = query.trim()

    if (q.startsWith('CREATE TABLE')) {
      return { toArray: () => [] }
    }

    if (q.startsWith('INSERT INTO wal')) {
      const [ts, kind, batch, row_count] = bindings as [number, string, string, number]
      this.rows.push({
        id: this.nextId++,
        ts,
        kind,
        batch,
        row_count,
        flushed: 0,
      })
      return { toArray: () => [] }
    }

    if (q.includes('SELECT id, ts, batch, row_count FROM wal WHERE kind = ?')) {
      const kind = bindings[0] as string
      const result = this.rows
        .filter((r) => r.kind === kind && r.flushed === 0)
        .sort((a, b) => a.id - b.id)
        .map((r) => ({ id: r.id, ts: r.ts, batch: r.batch, row_count: r.row_count }))
      return { toArray: () => result }
    }

    if (q.includes('SELECT id, ts, kind, batch, row_count FROM wal WHERE flushed = 0')) {
      const result = this.rows
        .filter((r) => r.flushed === 0)
        .sort((a, b) => a.id - b.id)
        .map((r) => ({ id: r.id, ts: r.ts, kind: r.kind, batch: r.batch, row_count: r.row_count }))
      return { toArray: () => result }
    }

    if (q.includes('SUM(row_count)') && q.includes('kind = ?')) {
      const kind = bindings[0] as string
      const count = this.rows
        .filter((r) => r.kind === kind && r.flushed === 0)
        .reduce((sum, r) => sum + r.row_count, 0)
      return { toArray: () => [{ count }] }
    }

    if (q.includes('SUM(row_count)') && !q.includes('kind = ?')) {
      const count = this.rows
        .filter((r) => r.flushed === 0)
        .reduce((sum, r) => sum + r.row_count, 0)
      return { toArray: () => [{ count }] }
    }

    if (q.includes('SELECT DISTINCT kind')) {
      const kinds = [...new Set(this.rows.filter((r) => r.flushed === 0).map((r) => r.kind))].sort()
      return { toArray: () => kinds.map((kind) => ({ kind })) }
    }

    if (q.startsWith('UPDATE wal SET flushed = 1')) {
      const ids = bindings as number[]
      for (const row of this.rows) {
        if (ids.includes(row.id)) {
          row.flushed = 1
        }
      }
      return { toArray: () => [] }
    }

    if (q.startsWith('DELETE FROM wal WHERE flushed = 1')) {
      this.rows = this.rows.filter((r) => r.flushed !== 1)
      return { toArray: () => [] }
    }

    return { toArray: () => [] }
  }
}

// =============================================================================
// Mock R2Bucket (same pattern as do-workflow.test.ts)
// =============================================================================

class MockR2Bucket {
  private objects = new Map<string, ArrayBuffer>()

  async get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null> {
    const data = this.objects.get(key)
    if (!data) return null
    return {
      arrayBuffer: async () => data,
    }
  }

  async put(key: string, value: ArrayBuffer): Promise<void> {
    this.objects.set(key, value)
  }

  async list(): Promise<{ objects: Array<{ key: string }> }> {
    return {
      objects: [...this.objects.keys()].map((key) => ({ key })),
    }
  }

  async delete(key: string): Promise<void> {
    this.objects.delete(key)
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

function createTestContext() {
  const sql = new MockSqlStorage()
  const bucket = new MockR2Bucket() as unknown as R2Bucket
  const wal = new SqliteWal(sql as never)
  const reader = new DOReadPath(wal, bucket)
  return { wal, reader }
}

// =============================================================================
// Tests: operator-based filters through DOReadPath.find()
// =============================================================================

describe('DOReadPath.find() with operator-based filters', () => {
  /**
   * Seed WAL with a variety of entities for filter testing.
   */
  function seedTestData(wal: SqliteWal) {
    wal.append('users', { $id: 'u1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice', age: 30, role: 'admin' })
    wal.append('users', { $id: 'u2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob', age: 25, role: 'user' })
    wal.append('users', { $id: 'u3', $op: 'c', $v: 1, $ts: 1002, name: 'Charlie', age: 35, role: 'admin' })
    wal.append('users', { $id: 'u4', $op: 'c', $v: 1, $ts: 1003, name: 'Diana', age: 28, role: 'mod' })
    wal.append('users', { $id: 'u5', $op: 'c', $v: 1, $ts: 1004, name: 'Eve', age: 22, role: 'user' })
  }

  // ---------------------------------------------------------------------------
  // $gt operator
  // ---------------------------------------------------------------------------
  it('$gt: returns entities where field is greater than value', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // age > 28 should match Alice (30) and Charlie (35)
    const results = await reader.find('users', { age: { $gt: 28 } })
    expect(results).toHaveLength(2)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie'])
  })

  // ---------------------------------------------------------------------------
  // $gte operator
  // ---------------------------------------------------------------------------
  it('$gte: returns entities where field is greater than or equal to value', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // age >= 30 should match Alice (30) and Charlie (35)
    const results = await reader.find('users', { age: { $gte: 30 } })
    expect(results).toHaveLength(2)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie'])
  })

  // ---------------------------------------------------------------------------
  // $lt operator
  // ---------------------------------------------------------------------------
  it('$lt: returns entities where field is less than value', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // age < 25 should match Eve (22) only
    const results = await reader.find('users', { age: { $lt: 25 } })
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Eve')
  })

  // ---------------------------------------------------------------------------
  // $lte operator
  // ---------------------------------------------------------------------------
  it('$lte: returns entities where field is less than or equal to value', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // age <= 25 should match Bob (25) and Eve (22)
    const results = await reader.find('users', { age: { $lte: 25 } })
    expect(results).toHaveLength(2)
    expect(results.map((r) => r.name).sort()).toEqual(['Bob', 'Eve'])
  })

  // ---------------------------------------------------------------------------
  // $in operator
  // ---------------------------------------------------------------------------
  it('$in: returns entities where field is in the given array', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // role in ['admin', 'mod'] should match Alice, Charlie, Diana
    const results = await reader.find('users', { role: { $in: ['admin', 'mod'] } })
    expect(results).toHaveLength(3)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie', 'Diana'])
  })

  // ---------------------------------------------------------------------------
  // $nin operator
  // ---------------------------------------------------------------------------
  it('$nin: returns entities where field is NOT in the given array', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // role not in ['admin'] should match Bob, Diana, Eve
    const results = await reader.find('users', { role: { $nin: ['admin'] } })
    expect(results).toHaveLength(3)
    expect(results.map((r) => r.name).sort()).toEqual(['Bob', 'Diana', 'Eve'])
  })

  // ---------------------------------------------------------------------------
  // $ne operator
  // ---------------------------------------------------------------------------
  it('$ne: returns entities where field is not equal to value', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // role != 'user' should match Alice, Charlie, Diana
    const results = await reader.find('users', { role: { $ne: 'user' } })
    expect(results).toHaveLength(3)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie', 'Diana'])
  })

  // ---------------------------------------------------------------------------
  // $or operator
  // ---------------------------------------------------------------------------
  it('$or: returns entities matching any sub-filter', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // name is 'Alice' OR name is 'Bob'
    const results = await reader.find('users', {
      $or: [{ name: 'Alice' }, { name: 'Bob' }],
    })
    expect(results).toHaveLength(2)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Bob'])
  })

  // ---------------------------------------------------------------------------
  // $and operator
  // ---------------------------------------------------------------------------
  it('$and: returns entities matching all sub-filters', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // role is 'admin' AND age > 30
    const results = await reader.find('users', {
      $and: [{ role: 'admin' }, { age: { $gt: 30 } }],
    })
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Charlie')
  })

  // ---------------------------------------------------------------------------
  // $regex operator
  // ---------------------------------------------------------------------------
  it('$regex: returns entities where string field matches regex', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // name starts with 'A' or 'B'
    const results = await reader.find('users', { name: { $regex: '^[AB]' } })
    expect(results).toHaveLength(2)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Bob'])
  })

  // ---------------------------------------------------------------------------
  // $exists operator
  // ---------------------------------------------------------------------------
  it('$exists: returns entities where field exists or does not exist', async () => {
    const { wal, reader } = createTestContext()

    // Seed entities with and without an optional field
    wal.append('profiles', { $id: 'p1', $op: 'c', $v: 1, $ts: 1000, name: 'Alice', bio: 'Developer' })
    wal.append('profiles', { $id: 'p2', $op: 'c', $v: 1, $ts: 1001, name: 'Bob' })
    wal.append('profiles', { $id: 'p3', $op: 'c', $v: 1, $ts: 1002, name: 'Charlie', bio: 'Designer' })

    // $exists: true should match p1 and p3 (have bio)
    const withBio = await reader.find('profiles', { bio: { $exists: true } })
    expect(withBio).toHaveLength(2)
    expect(withBio.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie'])

    // $exists: false should match p2 (no bio)
    const withoutBio = await reader.find('profiles', { bio: { $exists: false } })
    expect(withoutBio).toHaveLength(1)
    expect(withoutBio[0].name).toBe('Bob')
  })

  // ---------------------------------------------------------------------------
  // Range queries (combined $gte + $lt)
  // ---------------------------------------------------------------------------
  it('range query: $gte + $lt on same field', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // 25 <= age < 31 should match Alice (30), Bob (25), Diana (28)
    const results = await reader.find('users', { age: { $gte: 25, $lt: 31 } })
    expect(results).toHaveLength(3)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Bob', 'Diana'])
  })

  // ---------------------------------------------------------------------------
  // Combined $or with comparison operators
  // ---------------------------------------------------------------------------
  it('$or with comparison operators inside sub-filters', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    // age < 23 OR age > 34 -> should match Eve (22), Charlie (35)
    const results = await reader.find('users', {
      $or: [{ age: { $lt: 23 } }, { age: { $gt: 34 } }],
    })
    expect(results).toHaveLength(2)
    expect(results.map((r) => r.name).sort()).toEqual(['Charlie', 'Eve'])
  })

  // ---------------------------------------------------------------------------
  // Equality still works (regression check)
  // ---------------------------------------------------------------------------
  it('simple equality filter still works after fix', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    const results = await reader.find('users', { role: 'admin' })
    expect(results).toHaveLength(2)
    expect(results.map((r) => r.name).sort()).toEqual(['Alice', 'Charlie'])
  })

  // ---------------------------------------------------------------------------
  // Empty filter returns all entities (regression check)
  // ---------------------------------------------------------------------------
  it('empty filter returns all entities', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    const results = await reader.find('users', {})
    expect(results).toHaveLength(5)
  })

  // ---------------------------------------------------------------------------
  // No filter returns all entities (regression check)
  // ---------------------------------------------------------------------------
  it('undefined filter returns all entities', async () => {
    const { wal, reader } = createTestContext()
    seedTestData(wal)

    const results = await reader.find('users')
    expect(results).toHaveLength(5)
  })
})
