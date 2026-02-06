/**
 * JSONL Serialization & Deserialization for the MergeTree Engine
 *
 * Provides functions to serialize Line objects to JSONL strings and
 * deserialize them back, along with type guard functions to discriminate
 * between the four line types.
 *
 * JSONL format: one JSON object per line, terminated by '\n'.
 * No embedded newlines within the JSON (JSON.stringify handles escaping).
 */

import type { Line, DataLine, RelLine, EventLine, SchemaLine } from './types'

// =============================================================================
// Serialization
// =============================================================================

/**
 * Serialize a Line to a JSONL string (JSON + trailing newline).
 *
 * - Produces valid JSON with no embedded newlines
 * - Omits keys with undefined values (standard JSON.stringify behavior)
 * - Appends a single '\n' terminator
 */
export function serializeLine(line: Line): string {
  return JSON.stringify(line) + '\n'
}

// =============================================================================
// Deserialization
// =============================================================================

/**
 * Deserialize a JSONL string back to a Line object.
 *
 * Accepts strings with or without a trailing newline.
 */
export function deserializeLine(str: string): Line {
  return JSON.parse(str.trimEnd()) as Line
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a Line is a DataLine (entity mutation).
 *
 * DataLines are distinguished by having `$id` and `$v` fields, with
 * `$op` being one of 'c', 'u', 'd'.
 */
export function isDataLine(line: Line): line is DataLine {
  return '$id' in line && '$v' in line && '$op' in line
    && (line.$op === 'c' || line.$op === 'u' || line.$op === 'd')
}

/**
 * Check if a Line is a RelLine (relationship mutation).
 *
 * RelLines are distinguished by having `f`, `p`, `r`, `t` fields, with
 * `$op` being one of 'l', 'u'.
 */
export function isRelLine(line: Line): line is RelLine {
  return 'f' in line && 'p' in line && 'r' in line && 't' in line
    && '$op' in line && (line.$op === 'l' || line.$op === 'u')
}

/**
 * Check if a Line is an EventLine (CDC/audit event).
 *
 * EventLines are distinguished by having `id`, `ns`, `eid` fields, with
 * `op` being one of 'c', 'u', 'd'.
 */
export function isEventLine(line: Line): line is EventLine {
  return 'id' in line && 'ns' in line && 'eid' in line
    && 'op' in line && (line.op === 'c' || line.op === 'u' || line.op === 'd')
}

/**
 * Check if a Line is a SchemaLine (schema definition/migration).
 *
 * SchemaLines are distinguished by having `id`, `ns`, `schema` fields,
 * with `op` being 's'.
 */
export function isSchemaLine(line: Line): line is SchemaLine {
  return 'id' in line && 'ns' in line && 'schema' in line
    && 'op' in line && line.op === 's'
}
