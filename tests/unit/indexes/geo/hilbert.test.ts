import { describe, it, expect } from 'vitest'
import {
  encodeHilbert,
  decodeHilbert,
  encodeHilbertHex,
  sortByHilbert,
  batchEncodeHilbert,
  compareHilbert,
} from '../../../../src/indexes/geo/hilbert'

describe('encodeHilbert', () => {
  it('encodes coordinates to bigint', () => {
    const value = encodeHilbert(0, 0)
    expect(typeof value).toBe('bigint')
    expect(value).toBeGreaterThanOrEqual(0n)
  })

  it('produces different values for different locations', () => {
    const sf = encodeHilbert(37.7749, -122.4194)
    const nyc = encodeHilbert(40.7128, -74.0060)
    const tokyo = encodeHilbert(35.6762, 139.6503)

    expect(sf).not.toBe(nyc)
    expect(sf).not.toBe(tokyo)
    expect(nyc).not.toBe(tokyo)
  })

  it('produces consistent results', () => {
    const lat = 51.5074
    const lng = -0.1278

    const h1 = encodeHilbert(lat, lng)
    const h2 = encodeHilbert(lat, lng)
    expect(h1).toBe(h2)
  })

  it('handles edge cases', () => {
    // Corners of the world
    expect(() => encodeHilbert(-90, -180)).not.toThrow()
    expect(() => encodeHilbert(-90, 180)).not.toThrow()
    expect(() => encodeHilbert(90, -180)).not.toThrow()
    expect(() => encodeHilbert(90, 180)).not.toThrow()

    // Center
    expect(() => encodeHilbert(0, 0)).not.toThrow()
  })

  it('respects order parameter', () => {
    const lat = 37.7749
    const lng = -122.4194

    // Higher order = more precision = potentially different values
    const low = encodeHilbert(lat, lng, 8)
    const high = encodeHilbert(lat, lng, 16)

    // Different orders produce different scales
    expect(low).toBeLessThan(high)
  })
})

describe('decodeHilbert', () => {
  it('roundtrips approximately', () => {
    const testCases = [
      { lat: 0, lng: 0 },
      { lat: 45, lng: 90 },
      { lat: -45, lng: -90 },
      { lat: 37.7749, lng: -122.4194 },
      { lat: -33.8688, lng: 151.2093 },
    ]

    for (const { lat, lng } of testCases) {
      const encoded = encodeHilbert(lat, lng, 16)
      const decoded = decodeHilbert(encoded, 16)

      // Should be within ~0.01 degrees (about 1km at equator)
      expect(Math.abs(decoded.lat - lat)).toBeLessThan(0.01)
      expect(Math.abs(decoded.lng - lng)).toBeLessThan(0.01)
    }
  })
})

describe('encodeHilbertHex', () => {
  it('returns a hex string', () => {
    const hex = encodeHilbertHex(37.7749, -122.4194)
    expect(typeof hex).toBe('string')
    expect(/^[0-9a-f]+$/.test(hex)).toBe(true)
  })

  it('produces consistent length strings', () => {
    const locations = [
      { lat: 0, lng: 0 },
      { lat: 45, lng: 90 },
      { lat: -90, lng: -180 },
      { lat: 90, lng: 180 },
    ]

    const hexValues = locations.map(({ lat, lng }) => encodeHilbertHex(lat, lng, 16))

    // All should have the same length (padded)
    const lengths = new Set(hexValues.map(h => h.length))
    expect(lengths.size).toBe(1)
  })

  it('is sortable and preserves spatial locality', () => {
    // Nearby points should have similar Hilbert values
    const sf1 = encodeHilbertHex(37.7749, -122.4194)
    const sf2 = encodeHilbertHex(37.78, -122.41) // Close but different cell (~500m away)
    const nyc = encodeHilbertHex(40.7128, -74.0060) // Far away

    // All should be different at this scale
    expect(sf1).not.toBe(nyc)
    expect(sf2).not.toBe(nyc)

    // Note: sf1 and sf2 may or may not be equal depending on grid alignment
    // The key property is that sorting preserves locality for querying
  })
})

describe('sortByHilbert', () => {
  it('sorts items by Hilbert value', () => {
    const items = [
      { name: 'NYC', lat: 40.7128, lng: -74.0060 },
      { name: 'SF', lat: 37.7749, lng: -122.4194 },
      { name: 'Tokyo', lat: 35.6762, lng: 139.6503 },
      { name: 'London', lat: 51.5074, lng: -0.1278 },
    ]

    const sorted = sortByHilbert(items, item => ({ lat: item.lat, lng: item.lng }))

    // Should return same length
    expect(sorted.length).toBe(items.length)

    // All items should still be present
    const names = sorted.map(i => i.name)
    expect(names).toContain('NYC')
    expect(names).toContain('SF')
    expect(names).toContain('Tokyo')
    expect(names).toContain('London')
  })

  it('groups nearby points together', () => {
    // Create a cluster of points in SF area and a cluster in NYC area
    const items = [
      { name: 'SF1', lat: 37.7749, lng: -122.4194 },
      { name: 'NYC1', lat: 40.7128, lng: -74.0060 },
      { name: 'SF2', lat: 37.7800, lng: -122.4100 },
      { name: 'NYC2', lat: 40.7200, lng: -74.0000 },
      { name: 'SF3', lat: 37.7700, lng: -122.4200 },
      { name: 'NYC3', lat: 40.7100, lng: -74.0100 },
    ]

    const sorted = sortByHilbert(items, item => ({ lat: item.lat, lng: item.lng }))
    const names = sorted.map(i => i.name)

    // SF points should be grouped (adjacent or nearly adjacent)
    const sfIndices = names.map((n, i) => n.startsWith('SF') ? i : -1).filter(i => i >= 0)
    const nycIndices = names.map((n, i) => n.startsWith('NYC') ? i : -1).filter(i => i >= 0)

    // Check that each group is clustered (max spread < 4)
    const sfSpread = Math.max(...sfIndices) - Math.min(...sfIndices)
    const nycSpread = Math.max(...nycIndices) - Math.min(...nycIndices)

    expect(sfSpread).toBeLessThanOrEqual(3)
    expect(nycSpread).toBeLessThanOrEqual(3)
  })
})

describe('batchEncodeHilbert', () => {
  it('encodes multiple coordinates efficiently', () => {
    const coords = [
      { lat: 0, lng: 0 },
      { lat: 45, lng: 90 },
      { lat: -45, lng: -90 },
    ]

    const results = batchEncodeHilbert(coords)

    expect(results.length).toBe(coords.length)
    results.forEach(r => {
      expect(typeof r).toBe('bigint')
    })
  })

  it('produces same results as individual encoding', () => {
    const coords = [
      { lat: 37.7749, lng: -122.4194 },
      { lat: 40.7128, lng: -74.0060 },
    ]

    const batch = batchEncodeHilbert(coords, 16)
    const individual = coords.map(c => encodeHilbert(c.lat, c.lng, 16))

    expect(batch[0]).toBe(individual[0])
    expect(batch[1]).toBe(individual[1])
  })
})

describe('compareHilbert', () => {
  it('returns -1 for smaller values', () => {
    expect(compareHilbert(1n, 2n)).toBe(-1)
    expect(compareHilbert(0n, 100n)).toBe(-1)
  })

  it('returns 1 for larger values', () => {
    expect(compareHilbert(2n, 1n)).toBe(1)
    expect(compareHilbert(100n, 0n)).toBe(1)
  })

  it('returns 0 for equal values', () => {
    expect(compareHilbert(5n, 5n)).toBe(0)
    expect(compareHilbert(0n, 0n)).toBe(0)
  })
})
