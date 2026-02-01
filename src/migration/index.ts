/**
 * Migration utilities for ParqueDB
 *
 * Import data from various sources:
 * - JSON/JSONL files
 * - CSV files
 * - MongoDB Extended JSON / BSON
 *
 * @packageDocumentation
 */

// Types
export type {
  MigrationOptions,
  JsonImportOptions,
  CsvImportOptions,
  BsonImportOptions,
  MigrationResult,
  MigrationError,
} from './types'

// JSON import
export {
  importFromJson,
  importFromJsonl,
} from './json'

// CSV import
export {
  importFromCsv,
} from './csv'

// MongoDB import
export {
  importFromMongodb,
  importFromBson,
} from './mongodb'

// Re-export utility functions that may be useful
export {
  inferType,
  parseCsvLine,
  convertBsonValue,
} from './utils'
