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
} from './comparison'

export {
  type Logger,
  consoleLogger,
  noopLogger,
  logger,
  setLogger,
} from './logger'
