/**
 * Retention Module for ParqueDB Observability
 *
 * Provides data retention and compaction utilities for materialized views.
 *
 * @module observability/retention
 */

export {
  RetentionManager,
  createRetentionManager,
  type RetentionPolicy,
  type TieredRetentionPolicies,
  type RetentionManagerConfig,
  type ResolvedRetentionConfig,
  type CleanupProgress,
  type CleanupResult,
  type ScheduleOptions,
  type CleanupScheduler,
} from './RetentionManager'
