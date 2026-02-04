/**
 * E2E Observability Module
 *
 * Provides tools for E2E monitoring of deployed workers:
 * - Baseline storage for regression detection
 * - Alert management for health failures and regressions
 *
 * @example
 * ```typescript
 * import {
 *   createBaselineStore,
 *   createE2EAlertManagerFromEnv,
 * } from './observability/e2e'
 *
 * // Create baseline store
 * const store = createBaselineStore({ bucket: env.BENCHMARK_BUCKET })
 *
 * // Create alert manager
 * const alerts = createE2EAlertManagerFromEnv(env)
 * await alerts.alertHealthCheckFailed('production', workerUrl, checks, 'unhealthy')
 * ```
 */

// Types
export * from './types'

// Baseline storage
export {
  type BaselineStore,
  type BaselineStoreConfig,
  type SaveResult,
  type ListOptions,
  R2BaselineStore,
  FileBaselineStore,
  HttpBaselineStore,
  createBaselineStore,
  createStoredResult,
} from './baseline-store'

// Alert management
export {
  type E2EAlertEventType,
  type E2EAlertSeverity,
  type E2EAlertEvent,
  type E2EAlertDeliveryResult,
  type E2EAlertChannel,
  type E2EAlertThresholds,
  type E2ESlackConfig,
  type E2EPagerDutyConfig,
  type E2EAlertManagerConfig,
  DEFAULT_E2E_ALERT_THRESHOLDS,
  E2EAlertManager,
  createE2ESlackChannel,
  createE2EPagerDutyChannel,
  createE2EAlertManagerFromEnv,
  shouldAlertPagerDuty,
} from './alerts'
