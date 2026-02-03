/**
 * Conflict Resolution Strategies for Event-Based Merge
 *
 * This module provides strategies for resolving conflicts detected during
 * event stream merges. Strategies include:
 *
 * - 'ours': Always use our value
 * - 'theirs': Always use their value
 * - 'latest': Use the value from the event with the latest timestamp
 * - 'manual': Mark conflict for manual resolution
 * - Custom function: User-provided resolution logic
 */

import type { ConflictInfo } from './conflict-detection'

// =============================================================================
// Types
// =============================================================================

/**
 * Built-in resolution strategy names
 */
export type BuiltinStrategy = 'ours' | 'theirs' | 'latest' | 'manual'

/**
 * Custom resolution function type
 */
export type CustomResolutionFn = (conflict: ConflictInfo) => ConflictResolution

/**
 * Resolution strategy - can be a built-in name or a custom function
 */
export type ResolutionStrategy = BuiltinStrategy | CustomResolutionFn

/**
 * Result of resolving a conflict
 */
export interface ConflictResolution {
  /** The resolved value to use */
  readonly resolvedValue: unknown

  /** The strategy that was used */
  readonly strategy: string

  /** Whether manual resolution is required */
  readonly requiresManualResolution: boolean

  /** Optional explanation of the resolution */
  readonly explanation?: string | undefined

  /** Original conflict information */
  readonly conflict?: ConflictInfo | undefined
}

// =============================================================================
// Resolution Functions
// =============================================================================

/**
 * Resolve a conflict using the specified strategy
 *
 * @param conflict The conflict to resolve
 * @param strategy The resolution strategy to use
 * @returns The resolution result
 */
export function resolveConflict(
  conflict: ConflictInfo,
  strategy: ResolutionStrategy
): ConflictResolution {
  // Handle custom function strategy
  if (typeof strategy === 'function') {
    return strategy(conflict)
  }

  // Handle built-in strategies
  switch (strategy) {
    case 'ours':
      return resolveOurs(conflict)
    case 'theirs':
      return resolveTheirs(conflict)
    case 'latest':
      return resolveLatest(conflict)
    case 'manual':
      return resolveManual(conflict)
    default:
      throw new Error(`Unknown resolution strategy: ${strategy}`)
  }
}

/**
 * Resolve conflict by using our value
 */
function resolveOurs(conflict: ConflictInfo): ConflictResolution {
  return {
    resolvedValue: conflict.ourValue,
    strategy: 'ours',
    requiresManualResolution: false,
    explanation: `Used our value for ${conflict.field || conflict.target}`,
    conflict,
  }
}

/**
 * Resolve conflict by using their value
 */
function resolveTheirs(conflict: ConflictInfo): ConflictResolution {
  return {
    resolvedValue: conflict.theirValue,
    strategy: 'theirs',
    requiresManualResolution: false,
    explanation: `Used their value for ${conflict.field || conflict.target}`,
    conflict,
  }
}

/**
 * Resolve conflict by using the value from the latest event
 */
function resolveLatest(conflict: ConflictInfo): ConflictResolution {
  const ourTs = conflict.ourEvent.ts
  const theirTs = conflict.theirEvent.ts

  if (ourTs >= theirTs) {
    return {
      resolvedValue: conflict.ourValue,
      strategy: 'latest',
      requiresManualResolution: false,
      explanation: `Used our value (timestamp ${ourTs} >= ${theirTs})`,
      conflict,
    }
  } else {
    return {
      resolvedValue: conflict.theirValue,
      strategy: 'latest',
      requiresManualResolution: false,
      explanation: `Used their value (timestamp ${theirTs} > ${ourTs})`,
      conflict,
    }
  }
}

/**
 * Mark conflict for manual resolution
 */
function resolveManual(conflict: ConflictInfo): ConflictResolution {
  return {
    resolvedValue: undefined,
    strategy: 'manual',
    requiresManualResolution: true,
    explanation: `Conflict on ${conflict.field || conflict.target} requires manual resolution`,
    conflict,
  }
}

// =============================================================================
// Bulk Resolution
// =============================================================================

/**
 * Resolve multiple conflicts using the same strategy
 *
 * @param conflicts Array of conflicts to resolve
 * @param strategy The resolution strategy to use
 * @returns Array of resolutions
 */
export function resolveAllConflicts(
  conflicts: ConflictInfo[],
  strategy: ResolutionStrategy
): ConflictResolution[] {
  return conflicts.map(conflict => resolveConflict(conflict, strategy))
}

/**
 * Resolve conflicts with different strategies per conflict type
 *
 * @param conflicts Array of conflicts to resolve
 * @param strategyMap Map of conflict type to resolution strategy
 * @param defaultStrategy Default strategy for unmapped types
 * @returns Array of resolutions
 */
export function resolveConflictsByType(
  conflicts: ConflictInfo[],
  strategyMap: Partial<Record<ConflictInfo['type'], ResolutionStrategy>>,
  defaultStrategy: ResolutionStrategy = 'manual'
): ConflictResolution[] {
  return conflicts.map(conflict => {
    const strategy = strategyMap[conflict.type] ?? defaultStrategy
    return resolveConflict(conflict, strategy)
  })
}

// =============================================================================
// Resolution Helpers
// =============================================================================

/**
 * Check if all resolutions are complete (no manual resolution required)
 */
export function allResolutionsComplete(resolutions: ConflictResolution[]): boolean {
  return resolutions.every(r => !r.requiresManualResolution)
}

/**
 * Get conflicts that require manual resolution
 */
export function getUnresolvedConflicts(resolutions: ConflictResolution[]): ConflictResolution[] {
  return resolutions.filter(r => r.requiresManualResolution)
}

/**
 * Apply a custom resolution to a manual conflict
 *
 * @param resolution The resolution that needs manual handling
 * @param value The value to use
 * @returns Updated resolution
 */
export function applyManualResolution(
  resolution: ConflictResolution,
  value: unknown
): ConflictResolution {
  return {
    ...resolution,
    resolvedValue: value,
    strategy: 'manual-resolved',
    requiresManualResolution: false,
    explanation: 'Manually resolved by user',
  }
}

// =============================================================================
// Strategy Composition
// =============================================================================

/**
 * Create a composite strategy that tries strategies in order
 * Returns the first resolution that doesn't require manual intervention
 *
 * @param strategies Array of strategies to try in order
 * @returns A custom resolution function
 */
export function createFallbackStrategy(
  ...strategies: ResolutionStrategy[]
): CustomResolutionFn {
  return (conflict: ConflictInfo): ConflictResolution => {
    for (const strategy of strategies) {
      const resolution = resolveConflict(conflict, strategy)
      if (!resolution.requiresManualResolution) {
        return resolution
      }
    }
    // If all strategies require manual resolution, return the last result
    return resolveConflict(conflict, strategies[strategies.length - 1] ?? 'manual')
  }
}

/**
 * Create a strategy that uses different sub-strategies based on field name
 *
 * @param fieldStrategies Map of field name to strategy
 * @param defaultStrategy Strategy for unmapped fields
 * @returns A custom resolution function
 */
export function createFieldBasedStrategy(
  fieldStrategies: Record<string, ResolutionStrategy>,
  defaultStrategy: ResolutionStrategy = 'manual'
): CustomResolutionFn {
  return (conflict: ConflictInfo): ConflictResolution => {
    const strategy = conflict.field
      ? (fieldStrategies[conflict.field] ?? defaultStrategy)
      : defaultStrategy
    return resolveConflict(conflict, strategy)
  }
}

/**
 * Create a strategy that prefers certain value types
 * Useful for preferring non-null values
 *
 * @param preferOurs Function that returns true if our value should be preferred
 * @returns A custom resolution function
 */
export function createPreferenceStrategy(
  preferOurs: (ourValue: unknown, theirValue: unknown, conflict: ConflictInfo) => boolean
): CustomResolutionFn {
  return (conflict: ConflictInfo): ConflictResolution => {
    if (preferOurs(conflict.ourValue, conflict.theirValue, conflict)) {
      return {
        resolvedValue: conflict.ourValue,
        strategy: 'preference',
        requiresManualResolution: false,
        explanation: 'Preferred our value based on custom preference function',
        conflict,
      }
    } else {
      return {
        resolvedValue: conflict.theirValue,
        strategy: 'preference',
        requiresManualResolution: false,
        explanation: 'Preferred their value based on custom preference function',
        conflict,
      }
    }
  }
}

/**
 * Create a strategy that prefers non-null/undefined values
 */
export function createNonNullStrategy(): CustomResolutionFn {
  return createPreferenceStrategy((ourValue, theirValue) => {
    if (ourValue !== null && ourValue !== undefined) {
      if (theirValue === null || theirValue === undefined) {
        return true
      }
    }
    return false
  })
}

/**
 * Create a strategy that concatenates string values
 */
export function createConcatenateStrategy(separator = '\n---\n'): CustomResolutionFn {
  return (conflict: ConflictInfo): ConflictResolution => {
    if (typeof conflict.ourValue === 'string' && typeof conflict.theirValue === 'string') {
      return {
        resolvedValue: `${conflict.ourValue}${separator}${conflict.theirValue}`,
        strategy: 'concatenate',
        requiresManualResolution: false,
        explanation: 'Concatenated both string values',
        conflict,
      }
    }
    // Fall back to manual for non-strings
    return resolveManual(conflict)
  }
}

/**
 * Create a strategy that merges array values
 */
export function createArrayMergeStrategy(): CustomResolutionFn {
  return (conflict: ConflictInfo): ConflictResolution => {
    if (Array.isArray(conflict.ourValue) && Array.isArray(conflict.theirValue)) {
      // Concatenate arrays and remove duplicates
      const merged = [...new Set([...conflict.ourValue, ...conflict.theirValue])]
      return {
        resolvedValue: merged,
        strategy: 'array-merge',
        requiresManualResolution: false,
        explanation: 'Merged both arrays',
        conflict,
      }
    }
    // Fall back to manual for non-arrays
    return resolveManual(conflict)
  }
}
