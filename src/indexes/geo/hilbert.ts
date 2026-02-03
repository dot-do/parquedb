/**
 * Hilbert Curve Encoding for Spatial Sorting
 *
 * Hilbert curves provide better spatial locality than Z-order (Morton) curves,
 * meaning points that are close in 2D space tend to be close in 1D space.
 * This improves compression and query performance when data is sorted by Hilbert value.
 *
 * Reference: https://en.wikipedia.org/wiki/Hilbert_curve
 */

/**
 * Encode lat/lng to a Hilbert curve value
 *
 * @param lat - Latitude (-90 to 90)
 * @param lng - Longitude (-180 to 180)
 * @param order - Hilbert curve order (bits of precision, default 16 = 65536x65536 grid)
 * @returns Hilbert curve index as bigint
 */
export function encodeHilbert(lat: number, lng: number, order: number = 16): bigint {
  // Normalize coordinates to [0, 1] range
  const x = (lng + 180) / 360
  const y = (lat + 90) / 180

  // Scale to grid size (2^order x 2^order)
  const n = 1 << order
  const ix = Math.min(n - 1, Math.max(0, Math.floor(x * n)))
  const iy = Math.min(n - 1, Math.max(0, Math.floor(y * n)))

  return xy2d(n, ix, iy)
}

/**
 * Decode a Hilbert curve value back to lat/lng
 *
 * @param d - Hilbert curve index
 * @param order - Hilbert curve order (must match encoding order)
 * @returns Object with lat and lng (center of the cell)
 */
export function decodeHilbert(d: bigint, order: number = 16): { lat: number; lng: number } {
  const n = 1 << order
  const { x, y } = d2xy(n, d)

  // Convert back to lat/lng (center of cell)
  const lng = ((x + 0.5) / n) * 360 - 180
  const lat = ((y + 0.5) / n) * 180 - 90

  return { lat, lng }
}

/**
 * Get Hilbert curve value as a hex string (for storage/comparison)
 */
export function encodeHilbertHex(lat: number, lng: number, order: number = 16): string {
  const value = encodeHilbert(lat, lng, order)
  // Pad to consistent length based on order (order * 2 bits = order / 2 hex chars)
  const hexLength = Math.ceil((order * 2) / 4)
  return value.toString(16).padStart(hexLength, '0')
}

/**
 * Convert (x, y) to Hilbert curve distance
 * Based on the algorithm from https://en.wikipedia.org/wiki/Hilbert_curve
 */
function xy2d(n: number, x: number, y: number): bigint {
  let d = 0n
  let rx: number, ry: number
  let s = n >> 1

  while (s > 0) {
    rx = (x & s) > 0 ? 1 : 0
    ry = (y & s) > 0 ? 1 : 0
    d += BigInt(s) * BigInt(s) * BigInt((3 * rx) ^ ry)

    // Rotate quadrant
    if (ry === 0) {
      if (rx === 1) {
        x = s - 1 - x
        y = s - 1 - y
      }
      // Swap x and y
      const t = x
      x = y
      y = t
    }

    s >>= 1
  }

  return d
}

/**
 * Convert Hilbert curve distance to (x, y)
 */
function d2xy(n: number, d: bigint): { x: number; y: number } {
  let x = 0
  let y = 0
  let rx: number, ry: number
  let s = 1
  let t = d

  while (s < n) {
    rx = Number((t / 2n) & 1n)
    ry = Number((t ^ BigInt(rx)) & 1n)

    // Rotate quadrant
    if (ry === 0) {
      if (rx === 1) {
        x = s - 1 - x
        y = s - 1 - y
      }
      // Swap x and y
      const temp = x
      x = y
      y = temp
    }

    x += s * rx
    y += s * ry
    t /= 4n
    s *= 2
  }

  return { x, y }
}

/**
 * Compare two Hilbert values for sorting
 */
export function compareHilbert(a: bigint, b: bigint): number {
  if (a < b) return -1
  if (a > b) return 1
  return 0
}

/**
 * Sort an array of objects with lat/lng by Hilbert curve value
 *
 * @param items - Array of items with lat/lng properties
 * @param getCoords - Function to extract lat/lng from each item
 * @param order - Hilbert curve order
 * @returns Sorted array (mutates in place for efficiency)
 */
export function sortByHilbert<T>(
  items: T[],
  getCoords: (item: T) => { lat: number; lng: number },
  order: number = 16
): T[] {
  // Pre-compute Hilbert values
  const withHilbert = items.map(item => ({
    item,
    hilbert: encodeHilbert(getCoords(item).lat, getCoords(item).lng, order),
  }))

  // Sort by Hilbert value
  withHilbert.sort((a, b) => compareHilbert(a.hilbert, b.hilbert))

  // Return sorted items
  return withHilbert.map(({ item }) => item)
}

/**
 * Batch compute Hilbert values for an array of coordinates
 * More efficient than calling encodeHilbert repeatedly
 */
export function batchEncodeHilbert(
  coords: Array<{ lat: number; lng: number }>,
  order: number = 16
): bigint[] {
  const n = 1 << order
  return coords.map(({ lat, lng }) => {
    const x = Math.min(n - 1, Math.max(0, Math.floor(((lng + 180) / 360) * n)))
    const y = Math.min(n - 1, Math.max(0, Math.floor(((lat + 90) / 180) * n)))
    return xy2d(n, x, y)
  })
}
