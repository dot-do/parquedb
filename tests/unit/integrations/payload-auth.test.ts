/**
 * Tests for Payload OAuth authentication utilities
 */

import { describe, expect, it } from 'vitest'
import { parseCookies, extractToken } from '../../../src/integrations/payload/auth'

describe('parseCookies', () => {
  describe('basic parsing', () => {
    it('parses simple key-value pairs', () => {
      const result = parseCookies('foo=bar')
      expect(result).toEqual({ foo: 'bar' })
    })

    it('parses multiple cookies', () => {
      const result = parseCookies('foo=bar; baz=qux')
      expect(result).toEqual({ foo: 'bar', baz: 'qux' })
    })

    it('handles spaces around semicolons', () => {
      const result = parseCookies('foo=bar;  baz=qux  ; xyz=123')
      expect(result).toEqual({ foo: 'bar', baz: 'qux', xyz: '123' })
    })

    it('handles empty string', () => {
      const result = parseCookies('')
      expect(result).toEqual({})
    })

    it('handles only whitespace', () => {
      const result = parseCookies('   ')
      expect(result).toEqual({})
    })
  })

  describe('values with equals signs', () => {
    it('preserves equals signs in values', () => {
      const result = parseCookies('token=abc=def=ghi')
      expect(result).toEqual({ token: 'abc=def=ghi' })
    })

    it('handles base64-encoded values with equals padding', () => {
      const result = parseCookies('session=eyJhbGciOiJIUzI1NiJ9==')
      expect(result).toEqual({ session: 'eyJhbGciOiJIUzI1NiJ9==' })
    })
  })

  describe('URL-encoded values', () => {
    it('decodes URL-encoded spaces', () => {
      const result = parseCookies('name=hello%20world')
      expect(result).toEqual({ name: 'hello world' })
    })

    it('decodes URL-encoded special characters', () => {
      const result = parseCookies('value=a%2Bb%3Dc')
      expect(result).toEqual({ value: 'a+b=c' })
    })

    it('decodes URL-encoded semicolons in values', () => {
      // Note: Values with encoded semicolons should decode correctly
      const result = parseCookies('data=foo%3Bbar')
      expect(result).toEqual({ data: 'foo;bar' })
    })

    it('decodes multiple URL-encoded cookies', () => {
      const result = parseCookies('a=hello%20world; b=foo%3Dbar')
      expect(result).toEqual({ a: 'hello world', b: 'foo=bar' })
    })

    it('handles malformed percent-encoding gracefully', () => {
      // Invalid percent encoding should be kept as-is
      const result = parseCookies('bad=hello%2Gworld')
      expect(result).toEqual({ bad: 'hello%2Gworld' })
    })

    it('handles incomplete percent-encoding gracefully', () => {
      const result = parseCookies('bad=hello%2')
      expect(result).toEqual({ bad: 'hello%2' })
    })

    it('handles unicode characters', () => {
      const result = parseCookies('emoji=%F0%9F%8D%95')
      expect(result).toEqual({ emoji: '\u{1F355}' }) // Pizza emoji
    })
  })

  describe('quoted string values (RFC 6265)', () => {
    it('removes surrounding double quotes', () => {
      const result = parseCookies('name="value"')
      expect(result).toEqual({ name: 'value' })
    })

    it('handles quoted values with spaces', () => {
      const result = parseCookies('name="hello world"')
      expect(result).toEqual({ name: 'hello world' })
    })

    it('handles quoted values with equals signs', () => {
      // Note: RFC 6265 specifies semicolons as cookie separators, so values
      // with semicolons should be URL-encoded, not quoted
      const result = parseCookies('data="a=b=c"')
      expect(result).toEqual({ data: 'a=b=c' })
    })

    it('requires URL encoding for semicolons in values', () => {
      // Semicolons in values must be URL-encoded per RFC 6265
      // Quotes alone do NOT protect against semicolon splitting
      const result = parseCookies('data=%22a%3Db%3Bc%3Dd%22')
      expect(result).toEqual({ data: '"a=b;c=d"' })
    })

    it('handles empty quoted string', () => {
      const result = parseCookies('empty=""')
      expect(result).toEqual({ empty: '' })
    })

    it('does not strip quotes from mid-value quotes', () => {
      const result = parseCookies('partial="hello')
      expect(result).toEqual({ partial: '"hello' })
    })

    it('handles quoted values that are also URL-encoded', () => {
      const result = parseCookies('data="%2Ffoo%2Fbar"')
      expect(result).toEqual({ data: '/foo/bar' })
    })

    it('handles multiple quoted cookies', () => {
      const result = parseCookies('a="foo"; b="bar baz"')
      expect(result).toEqual({ a: 'foo', b: 'bar baz' })
    })
  })

  describe('edge cases', () => {
    it('ignores cookies without equals sign', () => {
      const result = parseCookies('novalue; foo=bar')
      expect(result).toEqual({ foo: 'bar' })
    })

    it('ignores cookies with empty key', () => {
      const result = parseCookies('=nokey; foo=bar')
      expect(result).toEqual({ foo: 'bar' })
    })

    it('handles cookies with empty value', () => {
      const result = parseCookies('empty=')
      expect(result).toEqual({ empty: '' })
    })

    it('handles whitespace in keys', () => {
      const result = parseCookies('  foo  =bar')
      expect(result).toEqual({ foo: 'bar' })
    })

    it('handles trailing semicolon', () => {
      const result = parseCookies('foo=bar;')
      expect(result).toEqual({ foo: 'bar' })
    })

    it('handles multiple semicolons', () => {
      const result = parseCookies('foo=bar;;baz=qux')
      expect(result).toEqual({ foo: 'bar', baz: 'qux' })
    })

    it('handles real-world JWT token', () => {
      const jwt = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.POstGetfAytaZS82wHcjoTyoqhMyxXiWdR7Nn7A29DNSl0EiXLdwJ6xC6AfgZWF1bOsS_TuYI3OG85AmiExREkrS6tDfTQ2B3WXlrr-wp5AokiRbz3_oB4OxG-W9KcEEbDRcZc0nH3L7LzYptiy1PtAylQGxHTWZXtGz4ht0bAecBgmpdgXMguEIcoqPJ1n3pIWk_dUZegpqx0Lka21H6XxUTxiy8OcaarA8zdnPUnV6AmNP3ecFawIFYdvJB_cm-GvpCSbr8G8y_Mllj8f4x9nBH8pQux89_6gUY618iYv7tuPWBFfEbLxtF2pZS6YC1aSfLQxeNe8djT9YjpvRZA'
      const result = parseCookies(`auth=${jwt}; session=abc123`)
      expect(result).toEqual({ auth: jwt, session: 'abc123' })
    })
  })

  describe('combined scenarios', () => {
    it('handles mix of quoted, encoded, and plain cookies', () => {
      const result = parseCookies('plain=value; encoded=hello%20world; quoted="foo bar"')
      expect(result).toEqual({
        plain: 'value',
        encoded: 'hello world',
        quoted: 'foo bar',
      })
    })

    it('handles complex real-world cookie header', () => {
      const result = parseCookies(
        '__Host-session=abc123; user_pref=%7B%22theme%22%3A%22dark%22%7D; _ga=GA1.2.123456789.1234567890'
      )
      expect(result).toEqual({
        '__Host-session': 'abc123',
        user_pref: '{"theme":"dark"}',
        _ga: 'GA1.2.123456789.1234567890',
      })
    })
  })
})

describe('extractToken', () => {
  it('extracts token from Authorization header', () => {
    const request = new Request('http://localhost', {
      headers: { Authorization: 'Bearer my-jwt-token' },
    })
    const token = extractToken(request)
    expect(token).toBe('my-jwt-token')
  })

  it('extracts token from cookie when no Authorization header', () => {
    const request = new Request('http://localhost', {
      headers: { Cookie: 'auth=my-cookie-token' },
    })
    const token = extractToken(request)
    expect(token).toBe('my-cookie-token')
  })

  it('extracts token from custom cookie name', () => {
    const request = new Request('http://localhost', {
      headers: { Cookie: 'custom_auth=my-cookie-token' },
    })
    const token = extractToken(request, 'custom_auth')
    expect(token).toBe('my-cookie-token')
  })

  it('prefers Authorization header over cookie', () => {
    const request = new Request('http://localhost', {
      headers: {
        Authorization: 'Bearer header-token',
        Cookie: 'auth=cookie-token',
      },
    })
    const token = extractToken(request)
    expect(token).toBe('header-token')
  })

  it('returns null when no token found', () => {
    const request = new Request('http://localhost')
    const token = extractToken(request)
    expect(token).toBeNull()
  })

  it('returns null for non-Bearer Authorization', () => {
    const request = new Request('http://localhost', {
      headers: { Authorization: 'Basic dXNlcjpwYXNz' },
    })
    const token = extractToken(request)
    expect(token).toBeNull()
  })

  it('handles URL-encoded cookie token', () => {
    const request = new Request('http://localhost', {
      headers: { Cookie: 'auth=token%3Dwith%3Dequals' },
    })
    const token = extractToken(request)
    expect(token).toBe('token=with=equals')
  })

  it('handles quoted cookie token', () => {
    const request = new Request('http://localhost', {
      headers: { Cookie: 'auth="my-quoted-token"' },
    })
    const token = extractToken(request)
    expect(token).toBe('my-quoted-token')
  })
})
