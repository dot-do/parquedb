/**
 * MemoryBackend Append Race Condition Tests
 *
 * Tests to verify thread-safety of the append operation.
 *
 * Issue: parquedb-70ol
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

// Helper to create test data
function createTestData(content: string): Uint8Array {
  return new TextEncoder().encode(content)
}

// Helper to decode test data
function decodeData(data: Uint8Array): string {
  return new TextDecoder().decode(data)
}

describe('MemoryBackend append thread-safety', () => {
  let backend: MemoryBackend

  beforeEach(() => {
    backend = new MemoryBackend()
  })

  describe('basic append functionality', () => {
    it('should append to an existing file', async () => {
      await backend.write('test.bin', new Uint8Array([1, 2, 3]))
      await backend.append('test.bin', new Uint8Array([4, 5, 6]))

      const result = await backend.read('test.bin')
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]))
    })

    it('should create file if it does not exist', async () => {
      await backend.append('new.bin', new Uint8Array([7, 8, 9]))

      const result = await backend.read('new.bin')
      expect(result).toEqual(new Uint8Array([7, 8, 9]))
    })
  })

  describe('concurrent append operations', () => {
    it('should preserve all data when multiple appends occur concurrently on existing file', async () => {
      // Start with initial data
      await backend.write('race.bin', new Uint8Array([0]))

      // Launch many concurrent appends
      const numAppends = 20
      const appends: Promise<void>[] = []
      for (let i = 1; i <= numAppends; i++) {
        appends.push(backend.append('race.bin', new Uint8Array([i])))
      }

      // All should complete
      await Promise.all(appends)

      // Read final result
      const result = await backend.read('race.bin')

      // Should have all values 0 through numAppends
      // If there's a race condition, some appends will be lost
      expect(result.length).toBe(numAppends + 1)

      const resultSet = new Set(Array.from(result))
      for (let i = 0; i <= numAppends; i++) {
        expect(resultSet.has(i)).toBe(true)
      }
    })

    it('should preserve all text data when multiple appends occur concurrently', async () => {
      // Start with initial data
      await backend.write('log.txt', createTestData('start|'))

      // Launch many concurrent appends
      const numAppends = 10
      const appends: Promise<void>[] = []
      for (let i = 1; i <= numAppends; i++) {
        appends.push(backend.append('log.txt', createTestData(`${i}|`)))
      }

      // All should complete
      await Promise.all(appends)

      // Read final result
      const result = decodeData(await backend.read('log.txt'))

      // Should contain 'start' and all numbers 1-10
      expect(result).toContain('start|')
      for (let i = 1; i <= numAppends; i++) {
        expect(result).toContain(`${i}|`)
      }
    })

    it('should handle rapid sequential appends correctly', async () => {
      // Start with empty file
      await backend.write('seq.bin', new Uint8Array([0]))

      // Rapidly append 10 values
      const appends: Promise<void>[] = []
      for (let i = 1; i <= 10; i++) {
        appends.push(backend.append('seq.bin', new Uint8Array([i])))
      }

      await Promise.all(appends)

      const result = await backend.read('seq.bin')

      // All values 0-10 must be present
      expect(result.length).toBe(11)
      const resultSet = new Set(Array.from(result))
      for (let i = 0; i <= 10; i++) {
        expect(resultSet.has(i)).toBe(true)
      }
    })

    it('should handle two concurrent appends to non-existent file', async () => {
      // Both appends try to create the file at the same time
      const append1 = backend.append('newrace.bin', new Uint8Array([1, 2]))
      const append2 = backend.append('newrace.bin', new Uint8Array([3, 4]))

      // Both should succeed
      await Promise.all([append1, append2])

      // Final result should contain both appends
      const result = await backend.read('newrace.bin')

      // Both [1,2] and [3,4] must be present, order depends on race winner
      expect(result.length).toBe(4)
      const resultArray = Array.from(result)
      expect(resultArray).toContain(1)
      expect(resultArray).toContain(2)
      expect(resultArray).toContain(3)
      expect(resultArray).toContain(4)
    })
  })

  describe('data integrity', () => {
    it('should maintain append ordering within sequential operations', async () => {
      // Sequential appends should maintain order
      await backend.write('order.bin', new Uint8Array([1]))
      await backend.append('order.bin', new Uint8Array([2]))
      await backend.append('order.bin', new Uint8Array([3]))
      await backend.append('order.bin', new Uint8Array([4]))

      const result = await backend.read('order.bin')
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4]))
    })

    it('should handle stress test with many concurrent appends', async () => {
      // Stress test: many concurrent appends
      await backend.write('stress.bin', new Uint8Array([0]))

      // Launch many concurrent appends
      const numAppends = 50
      const appends: Promise<void>[] = []
      for (let i = 1; i <= numAppends; i++) {
        appends.push(backend.append('stress.bin', new Uint8Array([i % 256])))
      }

      // All should complete
      await Promise.all(appends)

      // Read final result - should have numAppends + 1 bytes
      const result = await backend.read('stress.bin')
      expect(result.length).toBe(numAppends + 1)
    })
  })
})
