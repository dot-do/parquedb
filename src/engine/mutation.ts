/**
 * Mutation Operators — Pure functions for applying update operations.
 *
 * Extracted from engine.ts to isolate the update operator logic
 * ($set, $inc, $unset) from the engine's orchestration concerns.
 *
 * Both applyUpdate() and extractData() are pure functions with no side effects.
 */

import type { DataLine, UpdateOps } from './types'
import { DATA_SYSTEM_FIELDS } from './utils'

// =============================================================================
// extractData
// =============================================================================

/**
 * Extract only the user-data fields from a DataLine.
 * Uses the canonical $data field when available, falling back to iterating
 * top-level keys for backward compatibility with pre-$data DataLines.
 */
export function extractData(entity: DataLine): Record<string, unknown> {
  if (entity.$data && Object.keys(entity.$data).length > 0) {
    return { ...entity.$data }
  }
  // Fallback for legacy DataLines without $data
  const data: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(entity)) {
    if (!DATA_SYSTEM_FIELDS.has(key)) {
      data[key] = value
    }
  }
  return data
}

// =============================================================================
// applyUpdate
// =============================================================================

/**
 * Apply update operators to an existing entity, producing a new DataLine.
 *
 * Operators:
 * - $set: shallow merge fields into entity and $data
 * - $inc: add to numeric fields (default 0 if field missing) in entity and $data
 * - $unset: delete fields from entity and $data
 *
 * System fields ($id, $op, $v, $ts, $data) are protected and cannot be modified
 * through any operator. Updates are applied to both $data (canonical location)
 * and flat top-level keys (backward compatibility).
 *
 * Returns a new DataLine; the original entity is not mutated.
 */
export function applyUpdate(entity: DataLine, ops: UpdateOps): DataLine {
  // Clone the entity and its $data (shallow copy is sufficient for top-level fields).
  // For legacy entities without $data, initialize newData from flat user fields.
  const newData = entity.$data
    ? { ...entity.$data }
    : extractData(entity)
  const result: DataLine = { ...entity, $data: newData }

  // $set: merge fields into $data (and flat for backward compat)
  if (ops.$set) {
    for (const [key, value] of Object.entries(ops.$set)) {
      if (DATA_SYSTEM_FIELDS.has(key)) continue  // Guard: protect system fields
      newData[key] = value
      ;(result as Record<string, unknown>)[key] = value  // flat compat
    }
  }

  // $inc: increment numeric fields in $data (and flat for backward compat)
  if (ops.$inc) {
    for (const [key, amount] of Object.entries(ops.$inc)) {
      if (DATA_SYSTEM_FIELDS.has(key)) continue  // Guard: protect system fields
      const current = newData[key]
      const base = typeof current === 'number' ? current : 0
      const newValue = base + amount
      newData[key] = newValue
      ;(result as Record<string, unknown>)[key] = newValue  // flat compat
    }
  }

  // $unset: remove fields from $data (and flat for backward compat)
  if (ops.$unset) {
    for (const key of Object.keys(ops.$unset)) {
      if (DATA_SYSTEM_FIELDS.has(key)) continue  // Guard: protect system fields
      delete newData[key]
      delete (result as Record<string, unknown>)[key]  // flat compat
    }
  }

  return result
}
