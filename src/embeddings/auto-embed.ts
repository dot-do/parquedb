/**
 * Auto-Embedding Helpers for ParqueDB
 *
 * Provides utilities for automatic embedding generation on create/update
 * when fields have $vector indexes defined.
 */

import type { AIBinding, EmbedOptions } from './workers-ai'
import { WorkersAIEmbeddings, DEFAULT_MODEL } from './workers-ai'
import type { EmbedFieldOptions } from '../types/update'

// =============================================================================
// Types
// =============================================================================

/**
 * Field configuration for auto-embedding
 */
export interface AutoEmbedFieldConfig {
  /** Source field path (e.g., 'description', 'content') */
  sourceField: string
  /** Target field path for embedding vector */
  targetField: string
  /** Model to use for embedding */
  model?: string | undefined
  /** Whether to overwrite existing embedding */
  overwrite?: boolean | undefined
}

/**
 * Schema-based auto-embed configuration
 */
export interface AutoEmbedConfig {
  /** Fields to auto-embed on create/update */
  fields: AutoEmbedFieldConfig[]
  /** Default model for all fields */
  defaultModel?: string | undefined
}

/**
 * Options for processing embeddings
 */
export interface ProcessEmbeddingsOptions {
  /** AI binding for Workers AI */
  ai: AIBinding
  /** Model to use (default: @cf/baai/bge-m3) */
  model?: string | undefined
  /** Skip embedding if target field already exists */
  skipExisting?: boolean | undefined
}

// =============================================================================
// Embedding Processing
// =============================================================================

/**
 * Process $embed operator in an update object
 *
 * Takes an entity data object and $embed configuration, generates embeddings
 * for the specified fields, and returns the updated entity data.
 *
 * @param data - Entity data (mutable)
 * @param embedConfig - $embed operator value from update
 * @param options - Processing options
 * @returns Updated entity data with embeddings
 *
 * @example
 * ```typescript
 * const data = { title: 'Hello', description: 'A greeting' }
 * const embedConfig = { description: 'embedding' }
 *
 * const result = await processEmbedOperator(data, embedConfig, { ai: env.AI })
 * // result.embedding is now a number[] vector
 * ```
 */
export async function processEmbedOperator(
  data: Record<string, unknown>,
  embedConfig: Record<string, string | EmbedFieldOptions>,
  options: ProcessEmbeddingsOptions
): Promise<Record<string, unknown>> {
  const embeddings = new WorkersAIEmbeddings(options.ai, options.model ?? DEFAULT_MODEL)

  // Collect texts to embed in batch
  const embedTasks: Array<{
    sourceField: string
    targetField: string
    text: string
    model?: string | undefined
    overwrite: boolean
  }> = []

  for (const [sourceField, config] of Object.entries(embedConfig)) {
    // Get source text
    const sourceText = getNestedValue(data, sourceField)
    if (typeof sourceText !== 'string' || sourceText.trim() === '') {
      continue // Skip non-string or empty values
    }

    // Parse config
    let targetField: string
    let model: string | undefined
    let overwrite = true

    if (typeof config === 'string') {
      targetField = config
    } else {
      targetField = config.field
      model = config.model
      overwrite = config.overwrite ?? true
    }

    // Check if target already exists and we shouldn't overwrite
    const existingValue = getNestedValue(data, targetField)
    if (existingValue !== undefined && !overwrite && options.skipExisting) {
      continue
    }

    embedTasks.push({
      sourceField,
      targetField,
      text: sourceText,
      model,
      overwrite,
    })
  }

  if (embedTasks.length === 0) {
    return data
  }

  // Group by model for efficient batching
  const tasksByModel = new Map<string, typeof embedTasks>()
  for (const task of embedTasks) {
    const model = task.model ?? embeddings.model
    if (!tasksByModel.has(model)) {
      tasksByModel.set(model, [])
    }
    tasksByModel.get(model)!.push(task)
  }

  // Generate embeddings in batches by model
  for (const [model, tasks] of tasksByModel) {
    const texts = tasks.map(t => t.text)
    const embedOptions: EmbedOptions = model !== embeddings.model ? { model } : {}

    let vectors: number[][]
    if (texts.length === 1) {
      const text = texts[0]
      if (!text) continue
      const vector = await embeddings.embed(text, embedOptions)
      vectors = [vector]
    } else {
      vectors = await embeddings.embedBatch(texts, embedOptions)
    }

    // Set embeddings in data
    for (let i = 0; i < tasks.length; i++) {
      const task = tasks[i]
      if (!task) continue
      const vector = vectors[i]
      setNestedValue(data, task.targetField, vector)
    }
  }

  return data
}

/**
 * Auto-embed fields based on vector index configuration
 *
 * Checks if any fields have associated vector indexes and automatically
 * generates embeddings for them on create/update.
 *
 * @param data - Entity data
 * @param config - Auto-embed configuration from schema/index definitions
 * @param options - Processing options
 * @returns Updated entity data with auto-generated embeddings
 */
export async function autoEmbedFields(
  data: Record<string, unknown>,
  config: AutoEmbedConfig,
  options: ProcessEmbeddingsOptions
): Promise<Record<string, unknown>> {
  const embedConfig: Record<string, string | EmbedFieldOptions> = {}

  for (const field of config.fields) {
    embedConfig[field.sourceField] = {
      field: field.targetField,
      model: field.model ?? config.defaultModel,
      overwrite: field.overwrite ?? true,
    }
  }

  return processEmbedOperator(data, embedConfig, options)
}

/**
 * Check if an update contains $embed operator
 */
export function hasEmbedOperator(update: Record<string, unknown>): boolean {
  return '$embed' in update && update.$embed !== undefined
}

/**
 * Extract $embed operator from update and remove it
 *
 * Returns the $embed config and modifies the update to remove it.
 * This allows the $embed to be processed separately from other update operators.
 *
 * @param update - Update object (will be modified)
 * @returns $embed config or undefined
 */
export function extractEmbedOperator(
  update: Record<string, unknown>
): Record<string, string | EmbedFieldOptions> | undefined {
  if (!hasEmbedOperator(update)) {
    return undefined
  }

  const embedConfig = update.$embed as Record<string, string | EmbedFieldOptions>
  delete update.$embed
  return embedConfig
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Get a nested value from an object using dot notation
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Set a nested value in an object using dot notation
 */
export function setNestedValue(
  obj: Record<string, unknown>,
  path: string,
  value: unknown
): void {
  const parts = path.split('.')
  let current = obj

  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]!
    if (current[part] === undefined || current[part] === null) {
      current[part] = {}
    }
    current = current[part] as Record<string, unknown>
  }

  const lastPart = parts[parts.length - 1]!
  current[lastPart] = value
}

/**
 * Build auto-embed config from vector index definitions
 *
 * @param vectorIndexes - Array of vector index definitions
 * @returns AutoEmbedConfig for auto-embedding
 */
export function buildAutoEmbedConfig(
  vectorIndexes: Array<{
    name: string
    fields: Array<{ path: string }>
    sourceField?: string | undefined
  }>
): AutoEmbedConfig {
  const fields: AutoEmbedFieldConfig[] = []

  for (const index of vectorIndexes) {
    // Vector index field is the target, source field needs to be specified
    // Convention: if sourceField not specified, use field name without '_embedding' suffix
    const targetField = index.fields[0]?.path
    if (!targetField) continue

    let sourceField = index.sourceField
    if (!sourceField) {
      // Try to infer source from target (e.g., 'embedding' -> 'description')
      // This is a heuristic - in practice, the source should be explicitly configured
      if (targetField.endsWith('_embedding')) {
        sourceField = targetField.replace(/_embedding$/, '')
      } else if (targetField === 'embedding') {
        sourceField = 'description' // Common default
      } else {
        continue // Can't infer, skip
      }
    }

    fields.push({
      sourceField,
      targetField,
      overwrite: true,
    })
  }

  return { fields }
}
