/**
 * Utility functions for ParqueDB
 *
 * @module utils
 */

export {
  deepEqual,
  compareValues,
  getNestedValue,
  deepClone,
  getValueType,
  isNullish,
  isDefined,
  isNotNullish,
} from './comparison'

export {
  type Logger,
  consoleLogger,
  noopLogger,
  logger,
  setLogger,
} from './logger'

export {
  getRandomBytes,
  getRandomInt,
  getSecureRandom,
  getRandomBase36,
  getRandomBase32,
  getRandom48Bit,
  getUUID,
  generateULID,
  generateId,
} from './random'

export {
  type SafeRegexOptions,
  UnsafeRegexError,
  validateRegexPattern,
  createSafeRegex,
  isRegexSafe,
} from './safe-regex'

export {
  encodeBase64,
  decodeBase64,
  stringToBase64,
  base64ToString,
} from './base64'

export {
  // Type guards
  isRecord,
  isArray,
  isString,
  isNumber,
  isBoolean,
  // Error types
  JsonParseError,
  JsonValidationError,
  // Safe parsing (Result-based)
  safeJsonParse,
  parseJsonRecord,
  parseJsonArray,
  // Throwing variants
  parseRecordOrThrow,
  parseArrayOrThrow,
  // Schema validation
  type JsonSchema,
  validateSchema,
  parseWithSchema,
  // Convenience functions
  parseStoredData,
  parseStoredArray,
  tryParseJson,
  parseWithGuard,
} from './json-validation'

export {
  UNSAFE_PATH_SEGMENTS,
  DANGEROUS_KEYS,
  isUnsafePath,
  isDangerousKey,
  validatePath,
  validateKey,
  validateObjectKeys,
  validateObjectKeysDeep,
  sanitizeObject,
} from './path-safety'

export {
  escapeLikePattern,
  isValidTableName,
  validateWhereClause,
} from './sql-security'

export {
  PathValidationError,
  hasDangerousCharacters,
  hasPathTraversal,
  escapesBaseDirectory,
  validateFilePath,
  validateFilePathWithAllowedDirs,
  sanitizeFilePath,
} from './fs-path-safety'

export {
  // Typed proxy helpers
  createTypedProxy,
  createEmptyProxy,
  // Cloudflare DO helpers
  getDOStub,
  getDOStubByName,
  // Hono context helpers
  getContextVar,
  getContextVarOr,
  // Record helpers
  asRecord,
  toRecord,
  // Config parsing
  isConfigLike,
  parseConfig,
  // Array helpers
  asArray,
} from './type-utils'

export {
  type TTLCacheOptions,
  type TTLCacheStats,
  TTLCache,
} from './ttl-cache'
