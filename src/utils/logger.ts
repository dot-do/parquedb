/**
 * Logger utility for ParqueDB
 *
 * Provides a consistent logging interface that can be configured
 * at runtime. Defaults to noop logger for production, can be
 * switched to console logger for development/debugging.
 *
 * @module utils/logger
 */

/**
 * Logger interface for consistent logging across the codebase
 */
export interface Logger {
  debug(message: string, ...args: unknown[]): void
  info(message: string, ...args: unknown[]): void
  warn(message: string, ...args: unknown[]): void
  error(message: string, error?: unknown, ...args: unknown[]): void
}

/**
 * Console logger implementation
 * Outputs to console with appropriate log levels
 */
export const consoleLogger: Logger = {
  debug(message: string, ...args: unknown[]): void {
    console.debug(`[DEBUG] ${message}`, ...args)
  },
  info(message: string, ...args: unknown[]): void {
    console.info(`[INFO] ${message}`, ...args)
  },
  warn(message: string, ...args: unknown[]): void {
    console.warn(`[WARN] ${message}`, ...args)
  },
  error(message: string, error?: unknown, ...args: unknown[]): void {
    if (error !== undefined) {
      console.error(`[ERROR] ${message}`, error, ...args)
    } else {
      console.error(`[ERROR] ${message}`, ...args)
    }
  },
}

/**
 * Noop logger implementation
 * Silently discards all log messages (default for production)
 */
export const noopLogger: Logger = {
  debug(): void {},
  info(): void {},
  warn(): void {},
  error(): void {},
}

/**
 * Global logger instance
 * Defaults to noopLogger for production use
 */
export let logger: Logger = noopLogger

/**
 * Set the global logger instance
 *
 * @param l - Logger implementation to use
 *
 * @example
 * ```typescript
 * import { setLogger, consoleLogger } from './utils/logger'
 *
 * // Enable console logging for development
 * setLogger(consoleLogger)
 *
 * // Or use a custom logger
 * setLogger({
 *   debug: (msg) => myDebugFn(msg),
 *   info: (msg) => myInfoFn(msg),
 *   warn: (msg) => myWarnFn(msg),
 *   error: (msg, err) => myErrorFn(msg, err),
 * })
 * ```
 */
export function setLogger(l: Logger): void {
  logger = l
}
