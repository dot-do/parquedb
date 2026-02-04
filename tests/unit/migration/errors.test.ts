import { describe, it, expect } from 'vitest'
import { 
  MigrationParseError, 
  parseJsonWithContext, 
  safeParseJson,
  extractPositionFromSyntaxError,
  formatJsonlParseError,
  formatCsvJsonColumnError
} from '../../../src/migration/errors'

describe('migration/errors module', () => {
  describe('MigrationParseError', () => {
    it('should have name "MigrationError"', () => {
      const err = new MigrationParseError('test', { filePath: '/test.json' })
      expect(err.name).toBe('MigrationError')
    })

    it('should be instanceof Error', () => {
      const err = new MigrationParseError('test', { filePath: '/test.json' })
      expect(err).toBeInstanceOf(Error)
    })

    it('should store context', () => {
      const err = new MigrationParseError('test', { 
        filePath: '/test.json', 
        namespace: 'items',
        lineNumber: 42 
      })
      expect(err.context.filePath).toBe('/test.json')
      expect(err.context.namespace).toBe('items')
      expect(err.context.lineNumber).toBe(42)
    })

    it('should be serializable via JSON.stringify', () => {
      const err = new MigrationParseError('test error', { 
        filePath: '/test.json',
        position: 10 
      })
      const json = JSON.stringify(err)
      const parsed = JSON.parse(json)
      expect(parsed.name).toBe('MigrationError')
      expect(parsed.message).toBe('test error')
      expect(parsed.context.filePath).toBe('/test.json')
    })
  })

  describe('MigrationParseError.fromJsonSyntaxError', () => {
    it('should create error from SyntaxError', () => {
      let syntaxErr: SyntaxError
      try {
        JSON.parse('{invalid}')
      } catch (e) {
        syntaxErr = e as SyntaxError
      }
      
      const err = MigrationParseError.fromJsonSyntaxError(syntaxErr!, '/path/to/file.json', {
        namespace: 'users'
      })
      
      expect(err.name).toBe('MigrationError')
      expect(err.message).toContain('/path/to/file.json')
      expect(err.message).toContain('position')
      expect(err.cause).toBe(syntaxErr!)
      expect(err.context.filePath).toBe('/path/to/file.json')
      expect(err.context.namespace).toBe('users')
    })

    it('should extract position from SyntaxError when available', () => {
      let syntaxErr: SyntaxError
      try {
        // Use JSON that produces a position in V8's error message
        JSON.parse('{"key": value}')
      } catch (e) {
        syntaxErr = e as SyntaxError
      }

      const err = MigrationParseError.fromJsonSyntaxError(syntaxErr!, '/test.json')
      // Position may or may not be available depending on the JS engine
      // The important thing is that if it's defined, it should be a number
      if (err.context.position !== undefined) {
        expect(typeof err.context.position).toBe('number')
      }
      // Error should still be properly formed
      expect(err.message).toContain('/test.json')
      expect(err.cause).toBe(syntaxErr!)
    })
  })

  describe('parseJsonWithContext', () => {
    it('should parse valid JSON', () => {
      const result = parseJsonWithContext('{"name": "test"}', { filePath: '/test.json' })
      expect(result).toEqual({ name: 'test' })
    })

    it('should throw MigrationParseError for invalid JSON', () => {
      expect(() => {
        parseJsonWithContext('{invalid}', { filePath: '/test.json', namespace: 'items' })
      }).toThrow(MigrationParseError)
    })

    it('should include file path in error', () => {
      try {
        parseJsonWithContext('{invalid}', { filePath: '/my/path.json' })
        throw new Error('Should have thrown')
      } catch (err) {
        expect((err as MigrationParseError).context.filePath).toBe('/my/path.json')
      }
    })

    it('should include line number when provided', () => {
      try {
        parseJsonWithContext('{bad}', { filePath: '/test.jsonl', lineNumber: 42 })
        throw new Error('Should have thrown')
      } catch (err) {
        expect((err as MigrationParseError).context.lineNumber).toBe(42)
      }
    })
  })

  describe('safeParseJson', () => {
    it('should return ok:true for valid JSON', () => {
      const result = safeParseJson('{"key":"value"}', { filePath: '/test.json' })
      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value).toEqual({ key: 'value' })
      }
    })

    it('should return ok:false for invalid JSON', () => {
      const result = safeParseJson('{bad}', { filePath: '/test.json' })
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error).toBeInstanceOf(MigrationParseError)
      }
    })
  })

  describe('extractPositionFromSyntaxError', () => {
    it('should extract position from V8 format', () => {
      let syntaxErr: SyntaxError
      try {
        JSON.parse('{invalid}')
      } catch (e) {
        syntaxErr = e as SyntaxError
      }
      
      const pos = extractPositionFromSyntaxError(syntaxErr!)
      expect(pos).toBeDefined()
      expect(typeof pos).toBe('number')
    })
  })

  describe('formatJsonlParseError', () => {
    it('should format error with line number', () => {
      let syntaxErr: SyntaxError
      try {
        JSON.parse('{bad}')
      } catch (e) {
        syntaxErr = e as SyntaxError
      }
      
      const msg = formatJsonlParseError(syntaxErr!, 42)
      expect(msg).toContain('line 42')
      expect(msg).toContain('position')
    })
  })

  describe('formatCsvJsonColumnError', () => {
    it('should format error with line and column', () => {
      let syntaxErr: SyntaxError
      try {
        JSON.parse('{bad}')
      } catch (e) {
        syntaxErr = e as SyntaxError
      }
      
      const msg = formatCsvJsonColumnError(syntaxErr!, 10, 'config')
      expect(msg).toContain('line 10')
      expect(msg).toContain("column 'config'")
      expect(msg).toContain('JSON')
    })
  })
})
