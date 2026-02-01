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

export {
  getRandomBytes,
  getRandomInt,
  getSecureRandom,
  getRandomBase36,
  getRandomBase32,
  getRandom48Bit,
  getUUID,
} from './random'
