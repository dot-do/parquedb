/**
 * MV Cycle Detection for ParqueDB Materialized Views
 *
 * Provides functions to detect circular dependencies in materialized view definitions.
 * Uses depth-first search (DFS) to find cycles in the dependency graph.
 */

// =============================================================================
// Error Type
// =============================================================================

/**
 * Error thrown when a circular dependency is detected in MV definitions
 */
export class MVCycleError extends Error {
  readonly code = 'CIRCULAR_DEPENDENCY' as const
  readonly cyclePath: string[]

  constructor(cyclePath: string[]) {
    const isSelfRef = cyclePath.length === 2 && cyclePath[0] === cyclePath[1]
    const message = isSelfRef
      ? `Circular dependency detected: MV "${cyclePath[0]}" references itself`
      : `Circular dependency detected in materialized views: ${cyclePath.join(' -> ')}`
    super(message)
    this.name = 'MVCycleError'
    this.cyclePath = cyclePath
  }
}

// =============================================================================
// Dependency Extraction
// =============================================================================

/**
 * Extract MV dependencies from a schema
 *
 * Returns a Map where keys are MV names and values are arrays of their $from sources.
 * Only MVs are included (entries with $from).
 *
 * @param schema - The schema object
 * @returns Map of MV name to dependency list
 *
 * @example
 * const deps = getMVDependencies({
 *   Order: { total: 'number!' },
 *   DailyOrders: { $from: 'Order' },
 *   WeeklySummary: { $from: 'DailyOrders' },
 * })
 * // Map { 'DailyOrders' => ['Order'], 'WeeklySummary' => ['DailyOrders'] }
 */
export function getMVDependencies(schema: Record<string, unknown>): Map<string, string[]> {
  const dependencies = new Map<string, string[]>()

  for (const [name, entry] of Object.entries(schema)) {
    if (typeof entry !== 'object' || entry === null) continue

    const obj = entry as Record<string, unknown>

    if ('$from' in obj && typeof obj.$from === 'string') {
      dependencies.set(name, [obj.$from])
    }
  }

  return dependencies
}

// =============================================================================
// Cycle Detection
// =============================================================================

/**
 * Detect circular dependencies in MV definitions
 *
 * Uses depth-first search to find cycles in the dependency graph.
 * Returns the cycle path if found, or null if no cycles exist.
 *
 * @param schema - The schema object
 * @returns Cycle path (e.g., ['A', 'B', 'C', 'A']) or null if no cycle
 *
 * @example
 * // No cycle
 * detectMVCycles({ A: { $from: 'B' }, B: { data: 'string!' } }) // null
 *
 * // Self-reference cycle
 * detectMVCycles({ A: { $from: 'A' } }) // ['A', 'A']
 *
 * // Multi-level cycle
 * detectMVCycles({
 *   A: { $from: 'B' },
 *   B: { $from: 'C' },
 *   C: { $from: 'A' },
 * }) // ['A', 'B', 'C', 'A']
 */
export function detectMVCycles(schema: Record<string, unknown>): string[] | null {
  const dependencies = getMVDependencies(schema)

  // Track visited nodes and nodes in current recursion stack
  const visited = new Set<string>()
  const recursionStack = new Set<string>()
  const path: string[] = []

  /**
   * DFS helper to detect cycles
   * @returns Cycle path if found, null otherwise
   */
  function dfs(node: string): string[] | null {
    visited.add(node)
    recursionStack.add(node)
    path.push(node)

    const deps = dependencies.get(node) || []

    for (const dep of deps) {
      // Self-reference
      if (dep === node) {
        return [node, node]
      }

      // Cycle found - dep is already in current recursion stack
      if (recursionStack.has(dep)) {
        // Find where the cycle starts in the path
        const cycleStart = path.indexOf(dep)
        const cyclePath = [...path.slice(cycleStart), dep]
        return cyclePath
      }

      // Only continue DFS if dep is an MV (has its own dependencies)
      if (dependencies.has(dep) && !visited.has(dep)) {
        const cycle = dfs(dep)
        if (cycle) return cycle
      }
    }

    // Remove from recursion stack and path when backtracking
    recursionStack.delete(node)
    path.pop()
    return null
  }

  // Run DFS from each unvisited MV
  for (const mvName of dependencies.keys()) {
    if (!visited.has(mvName)) {
      const cycle = dfs(mvName)
      if (cycle) return cycle
    }
  }

  return null
}

/**
 * Validate schema for cycles and throw if found
 *
 * @param schema - The schema object
 * @throws {MVCycleError} If a cycle is detected
 */
export function validateNoCycles(schema: Record<string, unknown>): void {
  const cycle = detectMVCycles(schema)
  if (cycle) {
    throw new MVCycleError(cycle)
  }
}
