/**
 * Concurrency Benchmarks for ParqueDB
 *
 * Tests concurrent operation patterns:
 * - Parallel reads
 * - Parallel writes
 * - Read/write contention
 * - Transaction throughput
 */

import { describe, bench, beforeAll, beforeEach } from 'vitest'
import { Collection } from '../../src/Collection'
import type { Entity, EntityId, Filter } from '../../src/types'
import {
  randomElement,
  randomInt,
  randomString,
  randomSubset,
  randomDate,
  calculateStats,
  formatStats,
  getMemoryUsage,
  formatBytes,
  Timer,
  startTimer,
  measure,
} from './setup'

// =============================================================================
// Types for Concurrency Testing
// =============================================================================

interface Counter {
  value: number
  lastUpdatedBy: string
  lastUpdatedAt: Date
}

interface Account {
  balance: number
  currency: string
  status: 'active' | 'frozen' | 'closed'
  transactions: number
  lastTransaction?: Date | undefined
}

interface Inventory {
  sku: string
  quantity: number
  reserved: number
  available: number
  lastRestocked?: Date | undefined
}

interface Session {
  userId: string
  token: string
  createdAt: Date
  expiresAt: Date
  lastActivity: Date
  ipAddress: string
  userAgent: string
  active: boolean
}

interface Task {
  title: string
  status: 'pending' | 'processing' | 'completed' | 'failed'
  priority: number
  attempts: number
  lastAttempt?: Date | undefined
  workerId?: string | undefined
  result?: string | undefined
}

// =============================================================================
// Data Generators
// =============================================================================

function generateCounter(index: number): Counter & { $type: string; name: string } {
  return {
    $type: 'Counter',
    name: `Counter ${index}`,
    value: 0,
    lastUpdatedBy: 'init',
    lastUpdatedAt: new Date(),
  }
}

function generateAccount(index: number): Account & { $type: string; name: string } {
  return {
    $type: 'Account',
    name: `Account ${index}`,
    balance: randomInt(100, 10000),
    currency: 'USD',
    status: 'active',
    transactions: 0,
    lastTransaction: undefined,
  }
}

function generateInventory(index: number): Inventory & { $type: string; name: string } {
  const quantity = randomInt(0, 1000)
  const reserved = randomInt(0, Math.min(quantity, 100))
  return {
    $type: 'Inventory',
    name: `SKU-${index.toString().padStart(6, '0')}`,
    sku: `SKU-${index.toString().padStart(6, '0')}`,
    quantity,
    reserved,
    available: quantity - reserved,
    lastRestocked: randomDate(),
  }
}

function generateSession(index: number, userId: string): Session & { $type: string; name: string } {
  const createdAt = randomDate()
  return {
    $type: 'Session',
    name: `Session ${index}`,
    userId,
    token: `tok_${randomString(32)}`,
    createdAt,
    expiresAt: new Date(createdAt.getTime() + 24 * 60 * 60 * 1000),
    lastActivity: new Date(),
    ipAddress: `${randomInt(1, 255)}.${randomInt(1, 255)}.${randomInt(1, 255)}.${randomInt(1, 255)}`,
    userAgent: `Browser/${randomInt(1, 100)}.0`,
    active: true,
  }
}

function generateTask(index: number): Task & { $type: string; name: string } {
  return {
    $type: 'Task',
    name: `Task ${index}`,
    title: `Task ${index}: ${randomString(20)}`,
    status: 'pending',
    priority: randomInt(1, 10),
    attempts: 0,
    lastAttempt: undefined,
    workerId: undefined,
    result: undefined,
  }
}

// =============================================================================
// Concurrency Benchmarks
// =============================================================================

describe('Concurrency Benchmarks', () => {
  // ===========================================================================
  // Parallel Reads
  // ===========================================================================

  describe('Parallel Reads', () => {
    let readCollection: Collection<Account>
    let accountIds: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()
      readCollection = new Collection<Account>(`parallel-read-${suffix}`)

      // Seed 1000 accounts
      for (let i = 0; i < 1000; i++) {
        const account = await readCollection.create(generateAccount(i))
        accountIds.push(account.$id as string)
      }
    })

    bench('sequential reads (10 ops)', async () => {
      for (let i = 0; i < 10; i++) {
        const id = randomElement(accountIds).split('/')[1]
        await readCollection.get(id)
      }
    })

    bench('parallel reads (10 concurrent)', async () => {
      const ops = Array.from({ length: 10 }, () => {
        const id = randomElement(accountIds).split('/')[1]
        return readCollection.get(id)
      })
      await Promise.all(ops)
    })

    bench('parallel reads (50 concurrent)', async () => {
      const ops = Array.from({ length: 50 }, () => {
        const id = randomElement(accountIds).split('/')[1]
        return readCollection.get(id)
      })
      await Promise.all(ops)
    })

    bench('parallel reads (100 concurrent)', async () => {
      const ops = Array.from({ length: 100 }, () => {
        const id = randomElement(accountIds).split('/')[1]
        return readCollection.get(id)
      })
      await Promise.all(ops)
    })

    bench('parallel find queries (10 concurrent)', async () => {
      const statuses = ['active', 'frozen', 'closed']
      const ops = Array.from({ length: 10 }, () =>
        readCollection.find({ status: randomElement(statuses) }, { limit: 20 })
      )
      await Promise.all(ops)
    })

    bench('parallel find queries (50 concurrent)', async () => {
      const statuses = ['active', 'frozen', 'closed']
      const ops = Array.from({ length: 50 }, () =>
        readCollection.find({ status: randomElement(statuses) }, { limit: 20 })
      )
      await Promise.all(ops)
    })

    bench('parallel mixed reads (get + find, 50 concurrent)', async () => {
      const ops = Array.from({ length: 50 }, (_, i) => {
        if (i % 2 === 0) {
          const id = randomElement(accountIds).split('/')[1]
          return readCollection.get(id)
        } else {
          return readCollection.find({ status: 'active' }, { limit: 10 })
        }
      })
      await Promise.all(ops)
    })

    bench('parallel count queries (20 concurrent)', async () => {
      const ops = Array.from({ length: 20 }, () =>
        readCollection.count({ status: 'active' })
      )
      await Promise.all(ops)
    })

    bench('parallel aggregations (10 concurrent)', async () => {
      const ops = Array.from({ length: 10 }, () =>
        readCollection.aggregate([
          { $group: { _id: '$status', count: { $sum: 1 }, total: { $sum: '$balance' } } },
        ])
      )
      await Promise.all(ops)
    })
  })

  // ===========================================================================
  // Parallel Writes
  // ===========================================================================

  describe('Parallel Writes', () => {
    let writeCollection: Collection<Account>
    let writeNamespace: string

    beforeEach(() => {
      writeNamespace = `parallel-write-${Date.now()}-${Math.random().toString(36).slice(2)}`
      writeCollection = new Collection<Account>(writeNamespace)
    })

    bench('sequential creates (10 ops)', async () => {
      for (let i = 0; i < 10; i++) {
        await writeCollection.create(generateAccount(i))
      }
    })

    bench('parallel creates (10 concurrent)', async () => {
      const ops = Array.from({ length: 10 }, (_, i) =>
        writeCollection.create(generateAccount(i))
      )
      await Promise.all(ops)
    })

    bench('parallel creates (50 concurrent)', async () => {
      const ops = Array.from({ length: 50 }, (_, i) =>
        writeCollection.create(generateAccount(i))
      )
      await Promise.all(ops)
    })

    bench('parallel creates (100 concurrent)', async () => {
      const ops = Array.from({ length: 100 }, (_, i) =>
        writeCollection.create(generateAccount(i))
      )
      await Promise.all(ops)
    })

    bench('sequential updates (10 ops)', async () => {
      // First create entities
      const ids: string[] = []
      for (let i = 0; i < 10; i++) {
        const account = await writeCollection.create(generateAccount(i))
        ids.push(account.$id as string)
      }

      // Then update
      for (const id of ids) {
        await writeCollection.update(id.split('/')[1], { $inc: { balance: 100 } })
      }
    })

    bench('parallel updates (10 concurrent, different entities)', async () => {
      // First create entities
      const ids: string[] = []
      for (let i = 0; i < 10; i++) {
        const account = await writeCollection.create(generateAccount(i))
        ids.push(account.$id as string)
      }

      // Parallel updates to different entities
      const ops = ids.map((id) =>
        writeCollection.update(id.split('/')[1], { $inc: { balance: 100 } })
      )
      await Promise.all(ops)
    })

    bench('parallel updates (50 concurrent, different entities)', async () => {
      // First create entities
      const ids: string[] = []
      for (let i = 0; i < 50; i++) {
        const account = await writeCollection.create(generateAccount(i))
        ids.push(account.$id as string)
      }

      // Parallel updates
      const ops = ids.map((id) =>
        writeCollection.update(id.split('/')[1], { $inc: { balance: 100 } })
      )
      await Promise.all(ops)
    })

    bench('parallel deletes (20 concurrent)', async () => {
      // First create entities
      const ids: string[] = []
      for (let i = 0; i < 20; i++) {
        const account = await writeCollection.create(generateAccount(i))
        ids.push(account.$id as string)
      }

      // Parallel deletes
      const ops = ids.map((id) =>
        writeCollection.delete(id.split('/')[1])
      )
      await Promise.all(ops)
    })
  })

  // ===========================================================================
  // Read/Write Contention
  // ===========================================================================

  describe('Read/Write Contention', () => {
    let contentionCollection: Collection<Counter>
    let counterIds: string[] = []

    beforeEach(async () => {
      const namespace = `contention-${Date.now()}-${Math.random().toString(36).slice(2)}`
      contentionCollection = new Collection<Counter>(namespace)
      counterIds = []

      // Create 10 counters
      for (let i = 0; i < 10; i++) {
        const counter = await contentionCollection.create(generateCounter(i))
        counterIds.push(counter.$id as string)
      }
    })

    bench('concurrent read + write (same entity, 10 ops)', async () => {
      const id = counterIds[0].split('/')[1]

      const ops = Array.from({ length: 10 }, (_, i) => {
        if (i % 2 === 0) {
          return contentionCollection.get(id)
        } else {
          return contentionCollection.update(id, {
            $inc: { value: 1 },
            $set: { lastUpdatedBy: `worker-${i}`, lastUpdatedAt: new Date() },
          })
        }
      })

      await Promise.all(ops)
    })

    bench('concurrent read + write (same entity, 50 ops)', async () => {
      const id = counterIds[0].split('/')[1]

      const ops = Array.from({ length: 50 }, (_, i) => {
        if (i % 2 === 0) {
          return contentionCollection.get(id)
        } else {
          return contentionCollection.update(id, {
            $inc: { value: 1 },
            $set: { lastUpdatedBy: `worker-${i}`, lastUpdatedAt: new Date() },
          })
        }
      })

      await Promise.all(ops)
    })

    bench('heavy read, light write (90/10, 100 ops)', async () => {
      const ops = Array.from({ length: 100 }, (_, i) => {
        const id = randomElement(counterIds).split('/')[1]
        if (i % 10 !== 0) {
          // 90% reads
          return contentionCollection.get(id)
        } else {
          // 10% writes
          return contentionCollection.update(id, { $inc: { value: 1 } })
        }
      })

      await Promise.all(ops)
    })

    bench('balanced read/write (50/50, 100 ops)', async () => {
      const ops = Array.from({ length: 100 }, (_, i) => {
        const id = randomElement(counterIds).split('/')[1]
        if (i % 2 === 0) {
          return contentionCollection.get(id)
        } else {
          return contentionCollection.update(id, { $inc: { value: 1 } })
        }
      })

      await Promise.all(ops)
    })

    bench('heavy write, light read (10/90, 100 ops)', async () => {
      const ops = Array.from({ length: 100 }, (_, i) => {
        const id = randomElement(counterIds).split('/')[1]
        if (i % 10 === 0) {
          // 10% reads
          return contentionCollection.get(id)
        } else {
          // 90% writes
          return contentionCollection.update(id, { $inc: { value: 1 } })
        }
      })

      await Promise.all(ops)
    })

    bench('hot spot: all ops on single entity (50 ops)', async () => {
      const hotId = counterIds[0].split('/')[1]

      const ops = Array.from({ length: 50 }, (_, i) => {
        if (i % 3 === 0) {
          return contentionCollection.get(hotId)
        } else {
          return contentionCollection.update(hotId, { $inc: { value: 1 } })
        }
      })

      await Promise.all(ops)
    })

    bench('distributed: ops spread across entities (50 ops)', async () => {
      const ops = Array.from({ length: 50 }, (_, i) => {
        const id = counterIds[i % counterIds.length].split('/')[1]
        if (i % 3 === 0) {
          return contentionCollection.get(id)
        } else {
          return contentionCollection.update(id, { $inc: { value: 1 } })
        }
      })

      await Promise.all(ops)
    })
  })

  // ===========================================================================
  // Transaction Throughput Simulation
  // ===========================================================================

  describe('Transaction Throughput', () => {
    let inventoryCollection: Collection<Inventory>
    let inventoryIds: string[] = []

    beforeEach(async () => {
      const namespace = `txn-${Date.now()}-${Math.random().toString(36).slice(2)}`
      inventoryCollection = new Collection<Inventory>(namespace)
      inventoryIds = []

      // Create 100 inventory items
      for (let i = 0; i < 100; i++) {
        const item = await inventoryCollection.create(generateInventory(i))
        inventoryIds.push(item.$id as string)
      }
    })

    bench('simulated transaction: reserve inventory (single)', async () => {
      const id = randomElement(inventoryIds).split('/')[1]

      // Read current state
      const item = await inventoryCollection.get(id)
      const available = (item.available as number) || 0

      if (available >= 1) {
        // Update to reserve
        await inventoryCollection.update(id, {
          $inc: { reserved: 1, available: -1 },
        })
      }
    })

    bench('simulated transaction: reserve inventory (batch of 10)', async () => {
      for (let i = 0; i < 10; i++) {
        const id = randomElement(inventoryIds).split('/')[1]
        const item = await inventoryCollection.get(id)
        const available = (item.available as number) || 0

        if (available >= 1) {
          await inventoryCollection.update(id, {
            $inc: { reserved: 1, available: -1 },
          })
        }
      }
    })

    bench('simulated transaction: parallel reserves (10 concurrent)', async () => {
      const ops = Array.from({ length: 10 }, async () => {
        const id = randomElement(inventoryIds).split('/')[1]
        const item = await inventoryCollection.get(id)
        const available = (item.available as number) || 0

        if (available >= 1) {
          await inventoryCollection.update(id, {
            $inc: { reserved: 1, available: -1 },
          })
        }
      })

      await Promise.all(ops)
    })

    bench('simulated transaction: transfer between accounts (single)', async () => {
      // Get two different items
      const fromIdx = randomInt(0, inventoryIds.length - 1)
      const toIdx = (fromIdx + 1) % inventoryIds.length
      const fromId = inventoryIds[fromIdx].split('/')[1]
      const toId = inventoryIds[toIdx].split('/')[1]

      // Read both
      const from = await inventoryCollection.get(fromId)
      const to = await inventoryCollection.get(toId)

      const available = (from.available as number) || 0
      const transferAmount = Math.min(available, 10)

      if (transferAmount > 0) {
        // Update both (simulated transaction)
        await inventoryCollection.update(fromId, {
          $inc: { quantity: -transferAmount, available: -transferAmount },
        })
        await inventoryCollection.update(toId, {
          $inc: { quantity: transferAmount, available: transferAmount },
        })
      }
    })

    bench('simulated transaction: batch transfer (5 parallel pairs)', async () => {
      const ops = Array.from({ length: 5 }, async () => {
        const fromIdx = randomInt(0, inventoryIds.length - 1)
        const toIdx = (fromIdx + randomInt(1, 10)) % inventoryIds.length
        const fromId = inventoryIds[fromIdx].split('/')[1]
        const toId = inventoryIds[toIdx].split('/')[1]

        const from = await inventoryCollection.get(fromId)
        const available = (from.available as number) || 0
        const transferAmount = Math.min(available, 5)

        if (transferAmount > 0) {
          await inventoryCollection.update(fromId, {
            $inc: { quantity: -transferAmount, available: -transferAmount },
          })
          await inventoryCollection.update(toId, {
            $inc: { quantity: transferAmount, available: transferAmount },
          })
        }
      })

      await Promise.all(ops)
    })

    bench('throughput: max ops per second (100 simple updates)', async () => {
      const ops = Array.from({ length: 100 }, () => {
        const id = randomElement(inventoryIds).split('/')[1]
        return inventoryCollection.update(id, { $inc: { quantity: 1 } })
      })

      await Promise.all(ops)
    })

    bench('throughput: max ops per second (100 reads)', async () => {
      const ops = Array.from({ length: 100 }, () => {
        const id = randomElement(inventoryIds).split('/')[1]
        return inventoryCollection.get(id)
      })

      await Promise.all(ops)
    })

    bench('throughput: realistic workload (50 reads, 30 updates, 20 creates)', async () => {
      const reads = Array.from({ length: 50 }, () => {
        const id = randomElement(inventoryIds).split('/')[1]
        return inventoryCollection.get(id)
      })

      const updates = Array.from({ length: 30 }, () => {
        const id = randomElement(inventoryIds).split('/')[1]
        return inventoryCollection.update(id, { $inc: { quantity: 1 } })
      })

      const creates = Array.from({ length: 20 }, (_, i) =>
        inventoryCollection.create(generateInventory(1000 + i))
      )

      await Promise.all([...reads, ...updates, ...creates])
    })
  })

  // ===========================================================================
  // Task Queue Pattern
  // ===========================================================================

  describe('Task Queue Pattern', () => {
    let taskCollection: Collection<Task>
    let taskIds: string[] = []

    beforeEach(async () => {
      const namespace = `tasks-${Date.now()}-${Math.random().toString(36).slice(2)}`
      taskCollection = new Collection<Task>(namespace)
      taskIds = []

      // Create 100 pending tasks
      for (let i = 0; i < 100; i++) {
        const task = await taskCollection.create(generateTask(i))
        taskIds.push(task.$id as string)
      }
    })

    bench('claim task (find + update)', async () => {
      const workerId = `worker-${randomInt(1, 10)}`

      // Find unclaimed pending task
      const tasks = await taskCollection.find(
        { status: 'pending', workerId: { $exists: false } },
        { limit: 1, sort: { priority: -1 } }
      )

      if (tasks.length > 0) {
        // Claim it
        await taskCollection.update(tasks[0].$id.split('/')[1], {
          $set: {
            status: 'processing',
            workerId,
            lastAttempt: new Date(),
          },
          $inc: { attempts: 1 },
        })
      }
    })

    bench('parallel task claim (5 workers competing)', async () => {
      const ops = Array.from({ length: 5 }, async (_, i) => {
        const workerId = `worker-${i}`

        const tasks = await taskCollection.find(
          { status: 'pending', workerId: { $exists: false } },
          { limit: 1, sort: { priority: -1 } }
        )

        if (tasks.length > 0) {
          await taskCollection.update(tasks[0].$id.split('/')[1], {
            $set: {
              status: 'processing',
              workerId,
              lastAttempt: new Date(),
            },
            $inc: { attempts: 1 },
          })
        }
      })

      await Promise.all(ops)
    })

    bench('parallel task claim (10 workers competing)', async () => {
      const ops = Array.from({ length: 10 }, async (_, i) => {
        const workerId = `worker-${i}`

        const tasks = await taskCollection.find(
          { status: 'pending', workerId: { $exists: false } },
          { limit: 1, sort: { priority: -1 } }
        )

        if (tasks.length > 0) {
          await taskCollection.update(tasks[0].$id.split('/')[1], {
            $set: {
              status: 'processing',
              workerId,
              lastAttempt: new Date(),
            },
            $inc: { attempts: 1 },
          })
        }
      })

      await Promise.all(ops)
    })

    bench('complete task + enqueue new', async () => {
      // Complete a random processing task
      const processingTasks = await taskCollection.find(
        { status: 'processing' },
        { limit: 1 }
      )

      if (processingTasks.length > 0) {
        await taskCollection.update(processingTasks[0].$id.split('/')[1], {
          $set: {
            status: 'completed',
            result: 'success',
          },
        })
      }

      // Enqueue a new task
      await taskCollection.create(generateTask(Date.now()))
    })

    bench('task stats aggregation', async () => {
      await taskCollection.aggregate([
        {
          $group: {
            _id: '$status',
            count: { $sum: 1 },
            avgPriority: { $avg: '$priority' },
          },
        },
      ])
    })

    bench('retry failed tasks', async () => {
      // Find failed tasks
      const failedTasks = await taskCollection.find(
        { status: 'failed', attempts: { $lt: 3 } },
        { limit: 10 }
      )

      // Reset them for retry
      const ops = failedTasks.map((task) =>
        taskCollection.update(task.$id.split('/')[1], {
          $set: { status: 'pending', workerId: undefined },
        })
      )

      await Promise.all(ops)
    })
  })

  // ===========================================================================
  // Session Management Pattern
  // ===========================================================================

  describe('Session Management Pattern', () => {
    let sessionCollection: Collection<Session>
    let sessionIds: string[] = []
    let userIds: string[] = Array.from({ length: 100 }, (_, i) => `user-${i}`)

    beforeEach(async () => {
      const namespace = `sessions-${Date.now()}-${Math.random().toString(36).slice(2)}`
      sessionCollection = new Collection<Session>(namespace)
      sessionIds = []

      // Create sessions for 100 users
      for (let i = 0; i < 100; i++) {
        const session = await sessionCollection.create(
          generateSession(i, userIds[i % userIds.length])
        )
        sessionIds.push(session.$id as string)
      }
    })

    bench('validate session (get by token simulation)', async () => {
      const session = await sessionCollection.find(
        { active: true },
        { limit: 1 }
      )

      if (session.length > 0) {
        // Update last activity
        await sessionCollection.update(session[0].$id.split('/')[1], {
          $set: { lastActivity: new Date() },
        })
      }
    })

    bench('parallel session validations (20 concurrent)', async () => {
      const ops = Array.from({ length: 20 }, async () => {
        const id = randomElement(sessionIds).split('/')[1]
        const session = await sessionCollection.get(id)

        if (session.active) {
          await sessionCollection.update(id, {
            $set: { lastActivity: new Date() },
          })
        }
      })

      await Promise.all(ops)
    })

    bench('find user sessions', async () => {
      const userId = randomElement(userIds)
      await sessionCollection.find(
        { userId, active: true },
        { sort: { lastActivity: -1 } }
      )
    })

    bench('invalidate all user sessions', async () => {
      const userId = randomElement(userIds)
      const sessions = await sessionCollection.find({ userId, active: true })

      const ops = sessions.map((s) =>
        sessionCollection.update(s.$id.split('/')[1], {
          $set: { active: false },
        })
      )

      await Promise.all(ops)
    })

    bench('cleanup expired sessions (batch)', async () => {
      const expiredBefore = new Date(Date.now() - 24 * 60 * 60 * 1000)

      const expired = await sessionCollection.find(
        { expiresAt: { $lt: expiredBefore } },
        { limit: 50 }
      )

      const ops = expired.map((s) =>
        sessionCollection.delete(s.$id.split('/')[1])
      )

      await Promise.all(ops)
    })

    bench('concurrent login (create session) + validation', async () => {
      const ops: Promise<unknown>[] = []

      // 5 new logins
      for (let i = 0; i < 5; i++) {
        ops.push(sessionCollection.create(generateSession(Date.now() + i, randomElement(userIds))))
      }

      // 15 validations
      for (let i = 0; i < 15; i++) {
        const id = randomElement(sessionIds).split('/')[1]
        ops.push(
          sessionCollection.get(id).then((session) => {
            if (session.active) {
              return sessionCollection.update(id, {
                $set: { lastActivity: new Date() },
              })
            }
          })
        )
      }

      await Promise.all(ops)
    })
  })
})
