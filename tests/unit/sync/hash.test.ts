import { describe, it, expect } from 'vitest'
import { sha256, hashObject } from '../../../src/sync/hash'

describe('hash', () => {
  describe('sha256', () => {
    it('should hash string data', () => {
      const hash = sha256('hello world')
      expect(hash).toBe('b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9')
    })

    it('should hash Uint8Array data', () => {
      const data = new TextEncoder().encode('hello world')
      const hash = sha256(data)
      expect(hash).toBe('b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9')
    })

    it('should produce different hashes for different inputs', () => {
      const hash1 = sha256('hello')
      const hash2 = sha256('world')
      expect(hash1).not.toBe(hash2)
    })

    it('should be deterministic', () => {
      const hash1 = sha256('test data')
      const hash2 = sha256('test data')
      expect(hash1).toBe(hash2)
    })
  })

  describe('hashObject', () => {
    it('should hash simple objects', () => {
      const obj = { name: 'test', value: 42 }
      const hash = hashObject(obj)
      expect(hash).toHaveLength(64) // SHA256 produces 64 hex chars
    })

    it('should be deterministic for same object', () => {
      const obj = { name: 'test', value: 42 }
      const hash1 = hashObject(obj)
      const hash2 = hashObject(obj)
      expect(hash1).toBe(hash2)
    })

    it('should produce same hash regardless of key order', () => {
      const obj1 = { a: 1, b: 2, c: 3 }
      const obj2 = { c: 3, a: 1, b: 2 }
      const hash1 = hashObject(obj1)
      const hash2 = hashObject(obj2)
      expect(hash1).toBe(hash2)
    })

    it('should produce different hashes for different objects', () => {
      const obj1 = { name: 'test1' }
      const obj2 = { name: 'test2' }
      const hash1 = hashObject(obj1)
      const hash2 = hashObject(obj2)
      expect(hash1).not.toBe(hash2)
    })

    it('should handle nested objects', () => {
      const obj = {
        level1: {
          level2: {
            value: 'nested'
          }
        }
      }
      const hash = hashObject(obj)
      expect(hash).toHaveLength(64)
    })

    it('should handle arrays', () => {
      const obj = { items: [1, 2, 3] }
      const hash = hashObject(obj)
      expect(hash).toHaveLength(64)
    })

    it('should be sensitive to array order', () => {
      const obj1 = { items: [1, 2, 3] }
      const obj2 = { items: [3, 2, 1] }
      const hash1 = hashObject(obj1)
      const hash2 = hashObject(obj2)
      expect(hash1).not.toBe(hash2)
    })
  })
})
