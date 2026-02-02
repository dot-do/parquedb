/**
 * Utility functions for migration operations
 */

/**
 * Get a nested value from an object using dot notation
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) {
      return undefined
    }
    if (typeof current !== 'object') {
      return undefined
    }
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Check if a file exists
 */
export async function fileExists(path: string): Promise<boolean> {
  try {
    const fs = await import('fs/promises')
    await fs.access(path)
    return true
  } catch {
    // Intentionally ignored: fs.access throws when file doesn't exist
    return false
  }
}

/**
 * Create a read stream for a file
 */
export async function createReadStream(path: string): Promise<NodeJS.ReadableStream> {
  const fs = await import('fs')
  return fs.createReadStream(path, { encoding: 'utf-8' })
}

/**
 * Create a line reader for a file
 */
export async function createLineReader(path: string): Promise<AsyncIterable<string>> {
  const fs = await import('fs')
  const readline = await import('readline')

  const fileStream = fs.createReadStream(path, { encoding: 'utf-8' })
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  })

  return rl
}

/**
 * Infer the type of a string value
 */
export function inferType(value: string): unknown {
  // Trim whitespace
  const trimmed = value.trim()

  // Empty string
  if (trimmed === '') {
    return null
  }

  // Boolean
  if (trimmed.toLowerCase() === 'true') {
    return true
  }
  if (trimmed.toLowerCase() === 'false') {
    return false
  }

  // Null/undefined
  if (trimmed.toLowerCase() === 'null' || trimmed.toLowerCase() === 'undefined') {
    return null
  }

  // Number (integer or float)
  if (/^-?\d+$/.test(trimmed)) {
    const num = parseInt(trimmed, 10)
    if (!isNaN(num) && Number.isSafeInteger(num)) {
      return num
    }
  }
  if (/^-?\d*\.?\d+$/.test(trimmed)) {
    const num = parseFloat(trimmed)
    if (!isNaN(num) && isFinite(num)) {
      return num
    }
  }

  // Date (ISO 8601 format)
  if (/^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?(Z|[+-]\d{2}:\d{2})?)?$/.test(trimmed)) {
    const date = new Date(trimmed)
    if (!isNaN(date.getTime())) {
      return date
    }
  }

  // JSON array or object
  if ((trimmed.startsWith('[') && trimmed.endsWith(']')) ||
      (trimmed.startsWith('{') && trimmed.endsWith('}'))) {
    try {
      return JSON.parse(trimmed)
    } catch {
      // Intentionally ignored: not valid JSON, return original string value
    }
  }

  // Return as string
  return trimmed
}

/**
 * Parse a CSV line respecting quoted fields
 */
export function parseCsvLine(line: string, delimiter: string = ','): string[] {
  const fields: string[] = []
  let current = ''
  let inQuotes = false
  let i = 0

  while (i < line.length) {
    const char = line[i]!

    if (inQuotes) {
      if (char === '"') {
        // Check for escaped quote
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"'
          i += 2
          continue
        }
        // End of quoted field
        inQuotes = false
        i++
        continue
      }
      current += char
      i++
    } else {
      if (char === '"') {
        // Start of quoted field
        inQuotes = true
        i++
        continue
      }
      if (char === delimiter) {
        // End of field
        fields.push(current)
        current = ''
        i++
        continue
      }
      current += char
      i++
    }
  }

  // Add last field
  fields.push(current)

  return fields
}

/**
 * Convert a MongoDB BSON value to a JSON-compatible value
 */
export function convertBsonValue(value: unknown): unknown {
  if (value === null || value === undefined) {
    return value
  }

  // Handle MongoDB Extended JSON formats
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>

    // ObjectId: { "$oid": "..." }
    if ('$oid' in obj && typeof obj.$oid === 'string') {
      return obj.$oid
    }

    // Date: { "$date": "..." } or { "$date": { "$numberLong": "..." } }
    if ('$date' in obj) {
      const dateVal = obj.$date
      if (typeof dateVal === 'string') {
        return new Date(dateVal)
      }
      if (typeof dateVal === 'number') {
        return new Date(dateVal)
      }
      if (typeof dateVal === 'object' && dateVal !== null) {
        const inner = dateVal as Record<string, unknown>
        if ('$numberLong' in inner && typeof inner.$numberLong === 'string') {
          return new Date(parseInt(inner.$numberLong, 10))
        }
      }
    }

    // NumberLong: { "$numberLong": "..." }
    if ('$numberLong' in obj && typeof obj.$numberLong === 'string') {
      return parseInt(obj.$numberLong, 10)
    }

    // NumberDecimal: { "$numberDecimal": "..." }
    if ('$numberDecimal' in obj && typeof obj.$numberDecimal === 'string') {
      return parseFloat(obj.$numberDecimal)
    }

    // NumberInt: { "$numberInt": "..." }
    if ('$numberInt' in obj && typeof obj.$numberInt === 'string') {
      return parseInt(obj.$numberInt, 10)
    }

    // NumberDouble: { "$numberDouble": "..." }
    if ('$numberDouble' in obj && typeof obj.$numberDouble === 'string') {
      return parseFloat(obj.$numberDouble)
    }

    // Binary: { "$binary": { "base64": "...", "subType": "..." } }
    if ('$binary' in obj) {
      const binary = obj.$binary as Record<string, unknown>
      if ('base64' in binary && typeof binary.base64 === 'string') {
        // Return as a marked binary object
        return { type: 'binary', base64: binary.base64, subType: binary.subType }
      }
    }

    // UUID: { "$uuid": "..." }
    if ('$uuid' in obj && typeof obj.$uuid === 'string') {
      return obj.$uuid
    }

    // Regular expression: { "$regex": "...", "$options": "..." }
    if ('$regex' in obj && typeof obj.$regex === 'string') {
      return new RegExp(obj.$regex as string, obj.$options as string | undefined)
    }

    // Recursively convert arrays and objects
    if (Array.isArray(value)) {
      return value.map(convertBsonValue)
    }

    // Regular object - recursively convert
    const result: Record<string, unknown> = {}
    for (const [key, val] of Object.entries(obj)) {
      result[key] = convertBsonValue(val)
    }
    return result
  }

  return value
}

/**
 * Generate a name from a document
 */
export function generateName(doc: Record<string, unknown>, type: string): string {
  // Try common name fields
  const nameFields = ['name', 'title', 'label', 'displayName', 'username', 'email', 'slug']
  for (const field of nameFields) {
    if (doc[field] && typeof doc[field] === 'string') {
      return doc[field] as string
    }
  }

  // Try ID fields
  const idFields = ['_id', 'id', 'uuid', 'key']
  for (const field of idFields) {
    if (doc[field]) {
      const val = doc[field]
      if (typeof val === 'string') {
        return val
      }
      if (typeof val === 'object' && val !== null && '$oid' in (val as Record<string, unknown>)) {
        return (val as Record<string, unknown>).$oid as string
      }
    }
  }

  // Generate a name from type and timestamp
  return `${type}-${Date.now()}`
}
