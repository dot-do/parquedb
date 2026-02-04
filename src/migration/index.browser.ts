/**
 * Migration utilities for ParqueDB (Browser Build)
 *
 * Browser-safe migration module that excludes MongoDB/BSON imports
 * (bson requires Node.js Buffer which isn't available in browsers).
 *
 * Import data from:
 * - JSON/JSONL files
 * - CSV files
 *
 * For MongoDB/BSON import, use the Node.js build.
 *
 * @packageDocumentation
 */

// Types (excluding MongoDB-specific ones)
export type {
  MigrationOptions,
  JsonImportOptions,
  CsvImportOptions,
  MigrationResult,
  MigrationError,
  StreamingDocument,
  StreamingOptions,
  CsvStreamingOptions,
} from './types'

// JSON import
export {
  importFromJson,
  importFromJsonl,
  streamFromJsonl,
  streamFromJson,
} from './json'

// CSV import
export {
  importFromCsv,
  streamFromCsv,
} from './csv'

// Re-export utility functions (excluding bson conversion)
export {
  inferType,
  parseCsvLine,
} from './utils'

// Stub for MongoDB import - throws helpful error in browser
export function importFromMongodb(): Promise<never> {
  throw new Error('MongoDB import is not available in browser builds. Use Node.js build for MongoDB/BSON imports.')
}

export function importFromBson(): Promise<never> {
  throw new Error('BSON import is not available in browser builds. Use Node.js build for MongoDB/BSON imports.')
}

export function convertBsonValue(): never {
  throw new Error('BSON conversion is not available in browser builds. Use Node.js build for MongoDB/BSON imports.')
}

// Export type stubs for type compatibility
export type { BsonImportOptions } from './types'
