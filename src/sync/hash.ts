import { createHash } from 'crypto'

/**
 * Compute SHA256 hash of data
 * @param data String or byte array to hash
 * @returns Hex-encoded hash string
 */
export function sha256(data: string | Uint8Array): string {
  const hash = createHash('sha256')
  hash.update(data)
  return hash.digest('hex')
}

/**
 * Compute deterministic hash of an object
 * Keys are sorted to ensure consistent ordering
 * @param obj Object to hash
 * @returns Hex-encoded SHA256 hash
 */
export function hashObject(obj: object): string {
  // Deterministic JSON serialization with sorted keys
  const json = JSON.stringify(obj, Object.keys(obj).sort())
  return sha256(json)
}
