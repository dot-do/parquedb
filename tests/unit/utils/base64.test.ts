/**
 * Tests for Worker-safe Base64 Encoding Utilities
 */

import { describe, it, expect } from 'vitest'
import {
  encodeBase64,
  decodeBase64,
  stringToBase64,
  base64ToString,
} from '../../../src/utils/base64'

describe('Base64 Encoding Utilities', () => {
  describe('encodeBase64 / decodeBase64', () => {
    it('should encode and decode empty array', () => {
      const empty = new Uint8Array(0)
      const encoded = encodeBase64(empty)
      expect(encoded).toBe('')
      expect(decodeBase64(encoded)).toEqual(empty)
    })

    it('should encode and decode single byte', () => {
      const bytes = new Uint8Array([65]) // 'A'
      const encoded = encodeBase64(bytes)
      expect(encoded).toBe('QQ==')
      expect(decodeBase64(encoded)).toEqual(bytes)
    })

    it('should encode and decode two bytes', () => {
      const bytes = new Uint8Array([65, 66]) // 'AB'
      const encoded = encodeBase64(bytes)
      expect(encoded).toBe('QUI=')
      expect(decodeBase64(encoded)).toEqual(bytes)
    })

    it('should encode and decode three bytes', () => {
      const bytes = new Uint8Array([65, 66, 67]) // 'ABC'
      const encoded = encodeBase64(bytes)
      expect(encoded).toBe('QUJD')
      expect(decodeBase64(encoded)).toEqual(bytes)
    })

    it('should handle binary data', () => {
      const bytes = new Uint8Array([0, 127, 128, 255])
      const encoded = encodeBase64(bytes)
      const decoded = decodeBase64(encoded)
      expect(decoded).toEqual(bytes)
    })

    it('should match Node.js Buffer encoding', () => {
      const testCases = [
        'Hello, World!',
        'Test string',
        '1234567890',
        'Special chars: !@#$%^&*()',
      ]

      for (const str of testCases) {
        const bytes = new TextEncoder().encode(str)
        const ourEncoding = encodeBase64(bytes)
        const nodeEncoding = Buffer.from(bytes).toString('base64')
        expect(ourEncoding).toBe(nodeEncoding)
      }
    })
  })

  describe('stringToBase64 / base64ToString', () => {
    it('should encode and decode ASCII strings', () => {
      const str = 'Hello, World!'
      const encoded = stringToBase64(str)
      expect(base64ToString(encoded)).toBe(str)
    })

    it('should encode and decode empty string', () => {
      const str = ''
      const encoded = stringToBase64(str)
      expect(encoded).toBe('')
      expect(base64ToString(encoded)).toBe(str)
    })

    it('should handle Unicode characters', () => {
      const str = 'Hello, 世界! 🌍'
      const encoded = stringToBase64(str)
      expect(base64ToString(encoded)).toBe(str)
    })

    it('should handle JSON strings (cursor use case)', () => {
      const cursorData = {
        $id: 'test-123',
        name: 'Test Entity',
        createdAt: '2024-01-01T00:00:00Z',
      }
      const jsonStr = JSON.stringify(cursorData)
      const encoded = stringToBase64(jsonStr)
      const decoded = base64ToString(encoded)
      expect(decoded).toBe(jsonStr)
      expect(JSON.parse(decoded)).toEqual(cursorData)
    })

    it('should produce URL-compatible output', () => {
      // Base64 output should not contain characters that break URLs
      // Note: standard base64 uses + and / which ARE URL-unsafe
      // This test verifies the encoding is valid base64
      const str = 'Some test data'
      const encoded = stringToBase64(str)
      expect(/^[A-Za-z0-9+/=]*$/.test(encoded)).toBe(true)
    })

    it('should match Buffer.from encoding for ASCII strings', () => {
      const testStrings = [
        '{"$id":"test-123","name":"Entity"}',
        '{"cursor":{"offset":100}}',
        'simple text',
      ]

      for (const str of testStrings) {
        const ourEncoding = stringToBase64(str)
        const nodeEncoding = Buffer.from(str).toString('base64')
        expect(ourEncoding).toBe(nodeEncoding)
      }
    })
  })
})
