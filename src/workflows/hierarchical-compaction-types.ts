/**
 * Hierarchical Compaction Types and Pure Functions
 *
 * This module contains types and pure functions for hierarchical (LSM-tree style)
 * compaction that can be tested without the Cloudflare Workers runtime.
 *
 * For Worker-specific classes (LevelStateDO, CompactionPromotionWorkflow),
 * see hierarchical-compaction.ts
 *
 * Level structure:
 * - L0: Raw writer files (hourly window)
 * - L1: Daily compacted (24 L0 files → 1 L1)
 * - L2: Weekly compacted (7 L1 files → 1 L2)
 *
 * File naming by level:
 * - L0: data/{ns}/l0/year=.../hour=.../compacted-{ts}.parquet
 * - L1: data/{ns}/l1/year=.../day=.../compacted-{ts}.parquet
 * - L2: data/{ns}/l2/year=.../week=.../compacted-{ts}.parquet
 */

import type { BackendType } from '../backends'

// =============================================================================
// Types
// =============================================================================

/**
 * Compaction level identifiers
 */
export type CompactionLevel = 'l0' | 'l1' | 'l2'

/**
 * Configuration for hierarchical compaction thresholds
 */
export interface HierarchicalCompactionLevels {
  /** Number of L0 files that trigger promotion to L1 (default: 24) */
  l0ToL1Threshold: number
  /** Number of L1 files that trigger promotion to L2 (default: 7) */
  l1ToL2Threshold: number
}

/**
 * Configuration for hierarchical compaction
 */
export interface HierarchicalCompactionConfig {
  /** Enable hierarchical compaction (default: false) */
  enabled: boolean
  /** Level thresholds */
  levels?: HierarchicalCompactionLevels
  /** Target format for compacted files */
  targetFormat?: BackendType
}

/**
 * Default hierarchical compaction configuration
 */
export const DEFAULT_HIERARCHICAL_CONFIG: Required<HierarchicalCompactionConfig> = {
  enabled: false,
  levels: {
    l0ToL1Threshold: 24, // ~1 day of hourly files
    l1ToL2Threshold: 7,   // ~1 week of daily files
  },
  targetFormat: 'native',
}

/**
 * Metadata for a file at a specific level
 */
export interface LevelFileMetadata {
  /** File path */
  path: string
  /** File size in bytes */
  size: number
  /** Row count */
  rowCount?: number
  /** Time window start for this file */
  windowStart: number
  /** Time window end for this file */
  windowEnd: number
  /** When the file was created */
  createdAt: number
}

/**
 * State for a single level
 */
export interface LevelState {
  /** Level identifier */
  level: CompactionLevel
  /** Files at this level */
  files: LevelFileMetadata[]
  /** Total size of all files at this level */
  totalSize: number
  /** Total row count across all files */
  totalRows: number
}

/**
 * State for all levels in a namespace
 */
export interface NamespaceLevelState {
  /** Namespace this state belongs to */
  namespace: string
  /** State per level */
  levels: Record<CompactionLevel, LevelState>
  /** Last time this state was updated */
  updatedAt: number
}

/**
 * Parameters for the promotion workflow
 */
export interface CompactionPromotionParams {
  /** Namespace to promote */
  namespace: string
  /** Source level */
  fromLevel: CompactionLevel
  /** Target level */
  toLevel: CompactionLevel
  /** Files to compact */
  files: string[]
  /** Target format for output */
  targetFormat: BackendType
  /** Delete source files after successful promotion */
  deleteSource?: boolean
  /** Maximum files to process per step (default: 50) */
  maxFilesPerStep?: number
}

/**
 * Result of a promotion operation
 */
export interface PromotionResult {
  /** Whether the promotion succeeded */
  success: boolean
  /** Namespace that was promoted */
  namespace: string
  /** Source level */
  fromLevel: CompactionLevel
  /** Target level */
  toLevel: CompactionLevel
  /** Number of files processed */
  filesProcessed: number
  /** Output file(s) created */
  outputFiles: string[]
  /** Total rows processed */
  totalRows: number
  /** Bytes read */
  bytesRead: number
  /** Bytes written */
  bytesWritten: number
  /** Duration in milliseconds */
  durationMs: number
  /** Errors encountered */
  errors: string[]
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Get the next level after the given level
 */
export function getNextLevel(level: CompactionLevel): CompactionLevel | null {
  switch (level) {
    case 'l0': return 'l1'
    case 'l1': return 'l2'
    case 'l2': return null // L2 is the final level
    default: return null
  }
}

/**
 * Get the threshold for promoting from a level
 */
export function getPromotionThreshold(
  fromLevel: CompactionLevel,
  config: HierarchicalCompactionLevels
): number {
  switch (fromLevel) {
    case 'l0': return config.l0ToL1Threshold
    case 'l1': return config.l1ToL2Threshold
    case 'l2': return Infinity // L2 is never promoted
    default: return Infinity
  }
}

/**
 * Get ISO week number for a date
 */
export function getISOWeek(date: Date): number {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
  const dayNum = d.getUTCDay() || 7
  d.setUTCDate(d.getUTCDate() + 4 - dayNum)
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1))
  return Math.ceil((((d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7)
}

/**
 * Generate the output path for a level
 *
 * Path patterns:
 * - L0: data/{ns}/l0/year=YYYY/month=MM/day=DD/hour=HH/compacted-{ts}.parquet
 * - L1: data/{ns}/l1/year=YYYY/month=MM/day=DD/compacted-{ts}.parquet
 * - L2: data/{ns}/l2/year=YYYY/week=WW/compacted-{ts}.parquet
 */
export function generateLevelPath(
  namespace: string,
  level: CompactionLevel,
  windowStart: number,
  batchNum: number = 0
): string {
  const date = new Date(windowStart)
  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  const hour = String(date.getUTCHours()).padStart(2, '0')
  const timestamp = windowStart

  switch (level) {
    case 'l0':
      return `data/${namespace}/l0/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
        `compacted-${timestamp}-${batchNum}.parquet`
    case 'l1':
      return `data/${namespace}/l1/year=${year}/month=${month}/day=${day}/` +
        `compacted-${timestamp}-${batchNum}.parquet`
    case 'l2': {
      // ISO week number
      const weekNum = getISOWeek(date)
      return `data/${namespace}/l2/year=${year}/week=${String(weekNum).padStart(2, '0')}/` +
        `compacted-${timestamp}-${batchNum}.parquet`
    }
    default:
      return `data/${namespace}/${level}/compacted-${timestamp}-${batchNum}.parquet`
  }
}

/**
 * Parse level from a file path
 */
export function parseLevelFromPath(path: string): CompactionLevel | null {
  const match = path.match(/\/(l0|l1|l2)\//)
  if (match) {
    return match[1] as CompactionLevel
  }
  // Default to L0 for files in data/ prefix without explicit level, only if path starts with data/
  if (path.startsWith('data/') && !path.includes('/l1/') && !path.includes('/l2/')) {
    return 'l0'
  }
  return null
}

/**
 * Check if promotion is needed based on file count
 */
export function shouldPromote(
  level: CompactionLevel,
  fileCount: number,
  config: HierarchicalCompactionLevels
): boolean {
  const threshold = getPromotionThreshold(level, config)
  return fileCount >= threshold
}

/**
 * Create an empty level state
 */
export function createEmptyLevelState(level: CompactionLevel): LevelState {
  return {
    level,
    files: [],
    totalSize: 0,
    totalRows: 0,
  }
}

/**
 * Create empty namespace level state
 */
export function createEmptyNamespaceLevelState(namespace: string): NamespaceLevelState {
  return {
    namespace,
    levels: {
      l0: createEmptyLevelState('l0'),
      l1: createEmptyLevelState('l1'),
      l2: createEmptyLevelState('l2'),
    },
    updatedAt: Date.now(),
  }
}

/**
 * Add a file to a level state and return the updated state
 * (Pure function for testing)
 */
export function addFileToLevel(
  state: LevelState,
  file: LevelFileMetadata
): LevelState {
  return {
    ...state,
    files: [...state.files, file],
    totalSize: state.totalSize + file.size,
    totalRows: state.totalRows + (file.rowCount ?? 0),
  }
}

/**
 * Remove files from a level state and return the updated state
 * (Pure function for testing)
 */
export function removeFilesFromLevel(
  state: LevelState,
  pathsToRemove: Set<string>
): LevelState {
  const remainingFiles = state.files.filter(f => !pathsToRemove.has(f.path))
  return {
    ...state,
    files: remainingFiles,
    totalSize: remainingFiles.reduce((sum, f) => sum + f.size, 0),
    totalRows: remainingFiles.reduce((sum, f) => sum + (f.rowCount ?? 0), 0),
  }
}

/**
 * Check which levels need promotion in a namespace state
 * (Pure function for testing)
 */
export function getPromotionsNeeded(
  state: NamespaceLevelState,
  config: HierarchicalCompactionLevels
): Array<{
  fromLevel: CompactionLevel
  toLevel: CompactionLevel
  files: string[]
  fileCount: number
}> {
  const promotions: Array<{
    fromLevel: CompactionLevel
    toLevel: CompactionLevel
    files: string[]
    fileCount: number
  }> = []

  for (const level of ['l0', 'l1'] as CompactionLevel[]) {
    const levelState = state.levels[level]
    if (shouldPromote(level, levelState.files.length, config)) {
      const nextLevel = getNextLevel(level)
      if (nextLevel) {
        promotions.push({
          fromLevel: level,
          toLevel: nextLevel,
          files: levelState.files.map(f => f.path),
          fileCount: levelState.files.length,
        })
      }
    }
  }

  return promotions
}
