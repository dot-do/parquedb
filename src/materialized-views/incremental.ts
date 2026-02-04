/**
 * Incremental Refresh Logic for Materialized Views
 *
 * Provides efficient delta-based updates for materialized views by:
 * 1. Tracking changes since last refresh using event sequence IDs
 * 2. Applying only delta updates to avoid full recomputation
 * 3. Maintaining consistency through optimistic concurrency
 *
 * Incremental refresh is preferred when:
 * - Source operations are INSERT-only (no UPDATE/DELETE)
 * - Aggregates are limited to: SUM, MIN, MAX, COUNT, AVG
 * - No compaction occurred on source tables since last refresh
 */

import type { Event, Variant, VariantValue } from '../types/entity'
import type { EventSource } from '../events/replay'
import type { ManifestManager } from '../events/manifest'
import type { SegmentStorage } from '../events/segment'
import type {
  ViewDefinition,
  ViewQuery,
} from './types'
import type { Filter } from '../types/filter'
import type { AggregationStage } from '../aggregation/types'
import { generateULID, createSafeRegex } from '../utils'

// =============================================================================
// Types
// =============================================================================

/**
 * Lineage information for incremental refresh tracking.
 *
 * This extends the base MVLineage concept from staleness.ts
 * with additional fields specific to incremental refresh operations.
 */
export interface IncrementalLineage {
  /** Last event ID processed for each source */
  lastEventIds: Map<string, string>
  /** Snapshot ID (or event sequence) of each source at last refresh */
  sourceSnapshots: Map<string, string>
  /** Version ID of the MV definition when last refreshed */
  refreshVersionId: string
  /** Timestamp of last refresh */
  lastRefreshTime: number
  /** Number of events processed in last refresh */
  lastEventCount: number
}

/**
 * Result of an incremental refresh operation
 */
export interface IncrementalRefreshResult {
  /** Whether the refresh was successful */
  success: boolean
  /** Number of events processed */
  eventsProcessed: number
  /** Number of rows added to the view */
  rowsAdded: number
  /** Number of rows updated in the view */
  rowsUpdated: number
  /** Number of rows removed from the view */
  rowsRemoved: number
  /** Duration in milliseconds */
  durationMs: number
  /** New lineage after refresh */
  lineage: IncrementalLineage
  /** Whether a full refresh was required */
  wasFullRefresh: boolean
  /** Error message if not successful */
  error?: string | undefined
}

/**
 * Delta change from source
 */
export interface SourceDelta {
  /** Events since last refresh */
  events: Event[]
  /** Minimum timestamp in delta */
  minTs: number
  /** Maximum timestamp in delta */
  maxTs: number
  /** Whether the delta requires full refresh (e.g., contains DELETE operations) */
  requiresFullRefresh: boolean
  /** Reason for requiring full refresh */
  fullRefreshReason?: string | undefined
}

/**
 * Options for incremental refresh
 */
export interface IncrementalRefreshOptions {
  /** Force full refresh even if incremental is possible */
  forceFullRefresh?: boolean | undefined
  /** Maximum events to process in one refresh */
  maxEvents?: number | undefined
  /** Timeout in milliseconds */
  timeoutMs?: number | undefined
  /** Callback for progress updates */
  onProgress?: ((processed: number, total: number) => void) | undefined
}

/**
 * Storage interface for materialized view data
 */
export interface MVStorage {
  /** Append rows to the view */
  append(rows: Variant[]): Promise<void>
  /** Replace all data in the view */
  replace(rows: Variant[]): Promise<void>
  /** Update rows matching filter */
  update(filter: Filter, updates: Variant): Promise<number>
  /** Delete rows matching filter */
  delete(filter: Filter): Promise<number>
  /** Read all current rows */
  readAll(): Promise<Variant[]>
  /** Get current row count */
  count(): Promise<number>
}

/**
 * Aggregate state for incremental computation
 */
export interface AggregateState {
  /** Running sum for $sum and $avg */
  sum?: number | undefined
  /** Running count for $count and $avg */
  count?: number | undefined
  /** Current min for $min */
  min?: number | string | Date | undefined
  /** Current max for $max */
  max?: number | string | Date | undefined
}

// =============================================================================
// IncrementalRefresher Class
// =============================================================================

/**
 * Handles incremental refresh of materialized views.
 *
 * @example
 * ```typescript
 * const refresher = new IncrementalRefresher({
 *   eventSource,
 *   storage: mvStorage,
 *   manifest,
 * })
 *
 * // Check if incremental refresh is possible
 * const canIncremental = await refresher.canRefreshIncrementally(view, lineage)
 *
 * // Perform incremental refresh
 * const result = await refresher.refresh(view, lineage)
 * ```
 */
export class IncrementalRefresher {
  private eventSource: EventSource
  private storage: MVStorage
  // Reserved for future use (event manifest access)
  private manifest: ManifestManager
  // Reserved for future use (event segment storage)
  private segmentStorage: SegmentStorage

  constructor(options: {
    eventSource: EventSource
    storage: MVStorage
    manifest: ManifestManager
    segmentStorage: SegmentStorage
  }) {
    this.eventSource = options.eventSource
    this.storage = options.storage
    this.manifest = options.manifest
    this.segmentStorage = options.segmentStorage
  }

  // Reserved: Access to manifest manager for future segment-level optimizations
  getManifest(): ManifestManager {
    return this.manifest
  }

  // Reserved: Access to segment storage for future segment-level reads
  getSegmentStorage(): SegmentStorage {
    return this.segmentStorage
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Check if a view can be refreshed incrementally
   */
  async canRefreshIncrementally(
    view: ViewDefinition,
    lineage: IncrementalLineage
  ): Promise<{ canIncremental: boolean; reason?: string | undefined }> {
    // Check if view supports incremental refresh
    if (view.options.refreshStrategy === 'full') {
      return { canIncremental: false, reason: 'View is configured for full refresh only' }
    }

    // Check if aggregations are incrementally refreshable
    if (view.query.pipeline) {
      const unsupportedStages = this.findUnsupportedIncrementalStages(view.query.pipeline as unknown as AggregationStage[])
      if (unsupportedStages.length > 0) {
        return {
          canIncremental: false,
          reason: `Aggregation stages not supported for incremental: ${unsupportedStages.join(', ')}`,
        }
      }
    }

    // Get delta to check for UPDATE/DELETE operations
    const delta = await this.getDelta(view.source, lineage)
    if (delta.requiresFullRefresh) {
      return {
        canIncremental: false,
        reason: delta.fullRefreshReason ?? 'Delta contains non-incremental operations',
      }
    }

    return { canIncremental: true }
  }

  /**
   * Perform refresh on a materialized view
   */
  async refresh(
    view: ViewDefinition,
    lineage: IncrementalLineage,
    options: IncrementalRefreshOptions = {}
  ): Promise<IncrementalRefreshResult> {
    const startTime = Date.now()

    try {
      // Check if we can do incremental refresh
      const { canIncremental, reason } = await this.canRefreshIncrementally(view, lineage)

      if (options.forceFullRefresh || !canIncremental) {
        return this.fullRefresh(view, lineage, {
          ...options,
          wasFullRefresh: true,
          fullRefreshReason: reason,
        })
      }

      return this.incrementalRefresh(view, lineage, options)
    } catch (error) {
      return {
        success: false,
        eventsProcessed: 0,
        rowsAdded: 0,
        rowsUpdated: 0,
        rowsRemoved: 0,
        durationMs: Date.now() - startTime,
        lineage,
        wasFullRefresh: false,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Perform incremental refresh using delta changes only
   */
  async incrementalRefresh(
    view: ViewDefinition,
    lineage: IncrementalLineage,
    options: IncrementalRefreshOptions = {}
  ): Promise<IncrementalRefreshResult> {
    const startTime = Date.now()

    // Get delta since last refresh
    const delta = await this.getDelta(view.source, lineage, options.maxEvents)

    if (delta.events.length === 0) {
      // Already up to date
      return {
        success: true,
        eventsProcessed: 0,
        rowsAdded: 0,
        rowsUpdated: 0,
        rowsRemoved: 0,
        durationMs: Date.now() - startTime,
        lineage,
        wasFullRefresh: false,
      }
    }

    // Process delta events
    let rowsAdded = 0
    let rowsUpdated = 0
    let rowsRemoved = 0
    let processed = 0

    for (const event of delta.events) {
      // Check timeout
      if (options.timeoutMs && Date.now() - startTime > options.timeoutMs) {
        break
      }

      // Apply event to view
      const result = await this.applyEventToView(event, view)
      rowsAdded += result.added
      rowsUpdated += result.updated
      rowsRemoved += result.removed
      processed++

      // Report progress
      if (options.onProgress) {
        options.onProgress(processed, delta.events.length)
      }
    }

    // Update lineage
    const newLineage = this.updateLineage(lineage, delta, view)

    return {
      success: true,
      eventsProcessed: processed,
      rowsAdded,
      rowsUpdated,
      rowsRemoved,
      durationMs: Date.now() - startTime,
      lineage: newLineage,
      wasFullRefresh: false,
    }
  }

  /**
   * Perform full refresh by recomputing entire view
   */
  async fullRefresh(
    view: ViewDefinition,
    _lineage: IncrementalLineage,
    options: IncrementalRefreshOptions & { wasFullRefresh?: boolean | undefined; fullRefreshReason?: string | undefined } = {}
  ): Promise<IncrementalRefreshResult> {
    const startTime = Date.now()

    // Get all events for source
    const events = await this.getAllSourceEvents(view.source, options.maxEvents)

    // Process all events to build entity state map (replay properly)
    // This tracks the final state of each entity, handling CREATE/UPDATE/DELETE properly
    const entityStates = new Map<string, Variant | null>()
    let processed = 0

    for (const event of events) {
      const entityId = this.extractEntityId(event.target)

      switch (event.op) {
        case 'CREATE':
        case 'UPDATE':
          if (event.after) {
            entityStates.set(entityId, event.after)
          }
          break
        case 'DELETE':
          entityStates.set(entityId, null) // Mark as deleted
          break
      }
      processed++

      if (options.onProgress) {
        options.onProgress(processed, events.length)
      }
    }

    // Convert entity states to rows, filtering out deleted entities
    const rows: Variant[] = []
    for (const [_entityId, state] of entityStates) {
      if (state !== null) {
        // Apply filter if defined
        if (this.matchesFilter(state, view.query.filter)) {
          // Apply projection
          const projected = this.applyProjection(state, view.query)
          rows.push(projected)
        }
      }
    }

    // If pipeline is defined, execute aggregation
    let finalRows = rows
    if (view.query.pipeline) {
      finalRows = await this.executePipeline(rows, view.query.pipeline as unknown as AggregationStage[])
    }

    // Replace all data in storage
    await this.storage.replace(finalRows)

    // Create new lineage
    const newLineage = this.createFreshLineage(view, events)

    return {
      success: true,
      eventsProcessed: processed,
      rowsAdded: finalRows.length,
      rowsUpdated: 0,
      rowsRemoved: 0,
      durationMs: Date.now() - startTime,
      lineage: newLineage,
      wasFullRefresh: true,
    }
  }

  // ===========================================================================
  // Delta Detection
  // ===========================================================================

  /**
   * Get delta changes since last refresh
   */
  async getDelta(
    source: string,
    lineage: IncrementalLineage,
    maxEvents?: number
  ): Promise<SourceDelta> {
    const lastEventId = lineage.lastEventIds.get(source)
    const minTs = lineage.lastRefreshTime

    // Get events since last refresh
    const events = await this.eventSource.getEventsInRange(minTs, Date.now())

    // Filter to events after the last processed event ID
    let filteredEvents = events
    if (lastEventId) {
      const lastIdx = events.findIndex(e => e.id === lastEventId)
      if (lastIdx >= 0) {
        filteredEvents = events.slice(lastIdx + 1)
      }
    }

    // Filter to source namespace
    filteredEvents = filteredEvents.filter(e => e.target.startsWith(source + ':'))

    // Apply max events limit
    if (maxEvents && filteredEvents.length > maxEvents) {
      filteredEvents = filteredEvents.slice(0, maxEvents)
    }

    // Check for operations that require full refresh
    const hasDeletes = filteredEvents.some(e => e.op === 'DELETE')
    const hasUpdates = filteredEvents.some(e => e.op === 'UPDATE')

    let requiresFullRefresh = false
    let fullRefreshReason: string | undefined

    if (hasDeletes) {
      requiresFullRefresh = true
      fullRefreshReason = 'Delta contains DELETE operations'
    } else if (hasUpdates) {
      // Updates may require full refresh for certain aggregations
      requiresFullRefresh = true
      fullRefreshReason = 'Delta contains UPDATE operations'
    }

    const timestamps = filteredEvents.map(e => e.ts)
    return {
      events: filteredEvents,
      minTs: timestamps.length > 0 ? Math.min(...timestamps) : minTs,
      maxTs: timestamps.length > 0 ? Math.max(...timestamps) : Date.now(),
      requiresFullRefresh,
      fullRefreshReason,
    }
  }

  /**
   * Get all events for a source
   */
  private async getAllSourceEvents(source: string, maxEvents?: number): Promise<Event[]> {
    const events = await this.eventSource.getEventsInRange(0, Date.now())
    let filtered = events.filter(e => e.target.startsWith(source + ':'))

    if (maxEvents && filtered.length > maxEvents) {
      filtered = filtered.slice(0, maxEvents)
    }

    return filtered
  }

  // ===========================================================================
  // Event Application
  // ===========================================================================

  /**
   * Apply a single event to the view
   */
  private async applyEventToView(
    event: Event,
    view: ViewDefinition
  ): Promise<{ added: number; updated: number; removed: number }> {
    switch (event.op) {
      case 'CREATE':
        return this.handleCreate(event, view)
      case 'UPDATE':
        return this.handleUpdate(event, view)
      case 'DELETE':
        return this.handleDelete(event, view)
      default:
        return { added: 0, updated: 0, removed: 0 }
    }
  }

  /**
   * Handle CREATE event
   */
  private async handleCreate(
    event: Event,
    view: ViewDefinition
  ): Promise<{ added: number; updated: number; removed: number }> {
    if (!event.after) {
      return { added: 0, updated: 0, removed: 0 }
    }

    // Check if it matches the view filter
    if (!this.matchesFilter(event.after, view.query.filter)) {
      return { added: 0, updated: 0, removed: 0 }
    }

    // Apply projection
    const projected = this.applyProjection(event.after, view.query)

    // Add to storage
    await this.storage.append([projected])

    return { added: 1, updated: 0, removed: 0 }
  }

  /**
   * Handle UPDATE event
   */
  private async handleUpdate(
    event: Event,
    view: ViewDefinition
  ): Promise<{ added: number; updated: number; removed: number }> {
    const beforeMatches = event.before && this.matchesFilter(event.before, view.query.filter)
    const afterMatches = event.after && this.matchesFilter(event.after, view.query.filter)

    if (!beforeMatches && !afterMatches) {
      // Neither version matches - no change to view
      return { added: 0, updated: 0, removed: 0 }
    }

    if (!beforeMatches && afterMatches) {
      // Now matches - add to view
      const projected = this.applyProjection(event.after!, view.query)
      await this.storage.append([projected])
      return { added: 1, updated: 0, removed: 0 }
    }

    if (beforeMatches && !afterMatches) {
      // No longer matches - remove from view
      const entityId = this.extractEntityId(event.target)
      await this.storage.delete({ $id: entityId })
      return { added: 0, updated: 0, removed: 1 }
    }

    // Both match - update in view
    const entityId = this.extractEntityId(event.target)
    const projected = this.applyProjection(event.after!, view.query)
    await this.storage.update({ $id: entityId }, projected)
    return { added: 0, updated: 1, removed: 0 }
  }

  /**
   * Handle DELETE event
   */
  private async handleDelete(
    event: Event,
    view: ViewDefinition
  ): Promise<{ added: number; updated: number; removed: number }> {
    if (!event.before) {
      return { added: 0, updated: 0, removed: 0 }
    }

    // Check if it was in the view
    if (!this.matchesFilter(event.before, view.query.filter)) {
      return { added: 0, updated: 0, removed: 0 }
    }

    // Remove from storage
    const entityId = this.extractEntityId(event.target)
    const removed = await this.storage.delete({ $id: entityId })

    return { added: 0, updated: 0, removed }
  }

  // ===========================================================================
  // Aggregation Support
  // ===========================================================================

  /**
   * Find aggregation stages that don't support incremental refresh
   */
  private findUnsupportedIncrementalStages(pipeline: AggregationStage[]): string[] {
    const unsupported: string[] = []
    const supportedAggregates = ['$sum', '$count', '$min', '$max', '$avg']

    for (const stage of pipeline) {
      if ('$group' in stage && stage.$group) {
        const group = stage.$group as Record<string, unknown>
        for (const [key, value] of Object.entries(group)) {
          if (key === '_id') continue
          if (typeof value === 'object' && value !== null) {
            const operator = Object.keys(value)[0]
            if (operator && !supportedAggregates.includes(operator)) {
              unsupported.push(operator)
            }
          }
        }
      }
      // $lookup, $unwind, etc. may or may not support incremental
      // For now, be conservative
      if ('$lookup' in stage) {
        unsupported.push('$lookup')
      }
    }

    return [...new Set(unsupported)]
  }

  /**
   * Execute aggregation pipeline on rows
   */
  private async executePipeline(rows: Variant[], pipeline: AggregationStage[]): Promise<Variant[]> {
    // Simplified pipeline execution
    // In a full implementation, this would be more comprehensive
    let result = rows

    for (const stage of pipeline) {
      if ('$match' in stage && stage.$match) {
        result = result.filter(row => this.matchesFilter(row, stage.$match as Filter))
      } else if ('$project' in stage && stage.$project) {
        result = result.map(row => this.applySimpleProjection(row, stage.$project as Record<string, unknown>))
      } else if ('$group' in stage && stage.$group) {
        result = this.executeGroup(result, stage.$group as Record<string, unknown>)
      } else if ('$sort' in stage && stage.$sort) {
        result = this.executeSort(result, stage.$sort as Record<string, 1 | -1>)
      } else if ('$limit' in stage && typeof stage.$limit === 'number') {
        result = result.slice(0, stage.$limit)
      } else if ('$skip' in stage && typeof stage.$skip === 'number') {
        result = result.slice(stage.$skip)
      }
    }

    return result
  }

  /**
   * Execute $group stage
   */
  private executeGroup(rows: Variant[], group: Record<string, unknown>): Variant[] {
    const groups = new Map<string, { key: Variant; accumulators: Record<string, AggregateState> }>()
    const groupId = group._id

    for (const row of rows) {
      // Compute group key
      let keyValue: unknown
      if (typeof groupId === 'string' && groupId.startsWith('$')) {
        keyValue = this.getFieldValue(row, groupId.slice(1))
      } else {
        keyValue = groupId
      }

      const keyStr = JSON.stringify(keyValue)

      let groupData = groups.get(keyStr)
      if (!groupData) {
        groupData = {
          key: { _id: keyValue } as Variant,
          accumulators: {},
        }
        groups.set(keyStr, groupData)
      }

      // Update accumulators
      for (const [field, expr] of Object.entries(group)) {
        if (field === '_id') continue

        if (typeof expr === 'object' && expr !== null) {
          const operator = Object.keys(expr)[0]
          const operand = (expr as Record<string, string>)[operator!]

          if (!groupData.accumulators[field]) {
            groupData.accumulators[field] = {}
          }

          const state = groupData.accumulators[field]!
          const value = operand?.startsWith('$')
            ? this.getFieldValue(row, operand.slice(1))
            : operand

          this.updateAccumulator(state, operator!, value)
        }
      }
    }

    // Convert groups to output rows
    return Array.from(groups.values()).map(({ key, accumulators }) => {
      const result = { ...key }
      for (const [field, state] of Object.entries(accumulators)) {
        result[field] = this.getAccumulatorValue(state)
      }
      return result
    })
  }

  /**
   * Update an accumulator with a new value
   */
  private updateAccumulator(state: AggregateState, operator: string, value: unknown): void {
    const numValue = typeof value === 'number' ? value : 0

    switch (operator) {
      case '$sum':
        state.sum = (state.sum ?? 0) + numValue
        break
      case '$count':
        state.count = (state.count ?? 0) + 1
        break
      case '$min':
        if (state.min === undefined || (value != null && value < state.min)) {
          state.min = value as number | string | Date
        }
        break
      case '$max':
        if (state.max === undefined || (value != null && value > state.max)) {
          state.max = value as number | string | Date
        }
        break
      case '$avg':
        state.sum = (state.sum ?? 0) + numValue
        state.count = (state.count ?? 0) + 1
        break
    }
  }

  /**
   * Get final value from accumulator state
   */
  private getAccumulatorValue(state: AggregateState): VariantValue {
    if (state.count !== undefined && state.sum !== undefined) {
      // This is an $avg
      return state.count > 0 ? state.sum / state.count : 0
    }
    if (state.sum !== undefined) return state.sum
    if (state.count !== undefined) return state.count
    if (state.min !== undefined) return state.min
    if (state.max !== undefined) return state.max
    return null
  }

  /**
   * Execute $sort stage
   */
  private executeSort(rows: Variant[], sort: Record<string, 1 | -1>): Variant[] {
    return [...rows].sort((a, b) => {
      for (const [field, direction] of Object.entries(sort)) {
        const aVal = this.getFieldValue(a, field) as string | number | boolean | null | undefined
        const bVal = this.getFieldValue(b, field) as string | number | boolean | null | undefined

        if (aVal != null && bVal != null && aVal < bVal) return -1 * direction
        if (aVal != null && bVal != null && aVal > bVal) return 1 * direction
      }
      return 0
    })
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /**
   * Check if a value matches a filter
   */
  private matchesFilter(value: Variant, filter?: Filter): boolean {
    if (!filter) return true

    for (const [key, condition] of Object.entries(filter)) {
      const fieldValue = this.getFieldValue(value, key)

      if (typeof condition === 'object' && condition !== null) {
        // Operator-based condition
        for (const [op, opValue] of Object.entries(condition)) {
          if (!this.matchesOperator(fieldValue, op, opValue)) {
            return false
          }
        }
      } else {
        // Direct equality
        if (fieldValue !== condition) {
          return false
        }
      }
    }

    return true
  }

  /**
   * Check if a value matches an operator condition
   */
  private matchesOperator(fieldValue: unknown, operator: string, operand: unknown): boolean {
    switch (operator) {
      case '$eq':
        return fieldValue === operand
      case '$ne':
        return fieldValue !== operand
      case '$gt':
        return fieldValue != null && operand != null && fieldValue > operand
      case '$gte':
        return fieldValue != null && operand != null && fieldValue >= operand
      case '$lt':
        return fieldValue != null && operand != null && fieldValue < operand
      case '$lte':
        return fieldValue != null && operand != null && fieldValue <= operand
      case '$in':
        return Array.isArray(operand) && operand.includes(fieldValue)
      case '$nin':
        return Array.isArray(operand) && !operand.includes(fieldValue)
      case '$exists':
        return operand ? fieldValue !== undefined : fieldValue === undefined
      case '$regex':
        if (typeof fieldValue !== 'string' || typeof operand !== 'string') return false
        try {
          return createSafeRegex(operand).test(fieldValue)
        } catch {
          return false
        }
      default:
        return true
    }
  }

  /**
   * Apply projection to a value
   */
  private applyProjection(value: Variant, query: ViewQuery): Variant {
    if (!query.project) return { ...value }

    return this.applySimpleProjection(value, query.project)
  }

  /**
   * Apply simple projection
   */
  private applySimpleProjection(value: Variant, project: Record<string, unknown>): Variant {
    const result: Variant = {}
    const hasInclusions = Object.values(project).some(v => v === 1 || v === true)

    if (hasInclusions) {
      // Include mode - only specified fields
      for (const [key, include] of Object.entries(project)) {
        if (include === 1 || include === true) {
          const fieldValue = this.getFieldValue(value, key)
          if (fieldValue !== undefined) {
            result[key] = fieldValue as VariantValue
          }
        }
      }
    } else {
      // Exclude mode - all fields except specified
      for (const [key, val] of Object.entries(value)) {
        if (project[key] !== 0 && project[key] !== false) {
          result[key] = val
        }
      }
    }

    return result
  }

  /**
   * Get a field value using dot notation
   */
  private getFieldValue(obj: Variant, path: string): unknown {
    const parts = path.split('.')
    let current: unknown = obj

    for (const part of parts) {
      if (current == null || typeof current !== 'object') {
        return undefined
      }
      current = (current as Record<string, unknown>)[part]
    }

    return current
  }

  /**
   * Extract entity ID from target string
   */
  private extractEntityId(target: string): string {
    const colonIdx = target.indexOf(':')
    return colonIdx >= 0 ? target.slice(colonIdx + 1) : target
  }

  /**
   * Update lineage after processing delta
   */
  private updateLineage(
    oldLineage: IncrementalLineage,
    delta: SourceDelta,
    view: ViewDefinition
  ): IncrementalLineage {
    const newLastEventIds = new Map(oldLineage.lastEventIds)
    const lastEvent = delta.events[delta.events.length - 1]
    if (lastEvent) {
      newLastEventIds.set(view.source, lastEvent.id)
    }

    return {
      lastEventIds: newLastEventIds,
      sourceSnapshots: new Map(oldLineage.sourceSnapshots),
      refreshVersionId: oldLineage.refreshVersionId,
      lastRefreshTime: Date.now(),
      lastEventCount: delta.events.length,
    }
  }

  /**
   * Create fresh lineage after full refresh
   */
  private createFreshLineage(view: ViewDefinition, events: Event[]): IncrementalLineage {
    const lastEvent = events[events.length - 1]
    const lastEventIds = new Map<string, string>()
    if (lastEvent) {
      lastEventIds.set(view.source, lastEvent.id)
    }

    return {
      lastEventIds,
      sourceSnapshots: new Map([[view.source, generateULID()]]),
      refreshVersionId: generateULID(),
      lastRefreshTime: Date.now(),
      lastEventCount: events.length,
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an IncrementalRefresher instance
 */
export function createIncrementalRefresher(options: {
  eventSource: EventSource
  storage: MVStorage
  manifest: ManifestManager
  segmentStorage: SegmentStorage
}): IncrementalRefresher {
  return new IncrementalRefresher(options)
}

/**
 * Create an empty IncrementalLineage for new views
 */
export function createEmptyLineage(): IncrementalLineage {
  return {
    lastEventIds: new Map(),
    sourceSnapshots: new Map(),
    refreshVersionId: generateULID(),
    lastRefreshTime: 0,
    lastEventCount: 0,
  }
}

/**
 * Serialize IncrementalLineage for storage
 */
export function serializeLineage(lineage: IncrementalLineage): string {
  return JSON.stringify({
    lastEventIds: Array.from(lineage.lastEventIds.entries()),
    sourceSnapshots: Array.from(lineage.sourceSnapshots.entries()),
    refreshVersionId: lineage.refreshVersionId,
    lastRefreshTime: lineage.lastRefreshTime,
    lastEventCount: lineage.lastEventCount,
  })
}

/**
 * Deserialize IncrementalLineage from storage
 */
export function deserializeLineage(data: string): IncrementalLineage {
  const parsed = JSON.parse(data)
  return {
    lastEventIds: new Map(parsed.lastEventIds),
    sourceSnapshots: new Map(parsed.sourceSnapshots),
    refreshVersionId: parsed.refreshVersionId,
    lastRefreshTime: parsed.lastRefreshTime,
    lastEventCount: parsed.lastEventCount,
  }
}

// =============================================================================
// In-Memory MV Storage (for testing)
// =============================================================================

/**
 * Simple in-memory storage for materialized view data
 */
export class InMemoryMVStorage implements MVStorage {
  private rows: Variant[] = []

  async append(rows: Variant[]): Promise<void> {
    this.rows.push(...rows)
  }

  async replace(rows: Variant[]): Promise<void> {
    this.rows = [...rows]
  }

  async update(filter: Filter, updates: Variant): Promise<number> {
    let updated = 0
    this.rows = this.rows.map(row => {
      if (this.matchesFilter(row, filter)) {
        updated++
        return { ...row, ...updates }
      }
      return row
    })
    return updated
  }

  async delete(filter: Filter): Promise<number> {
    const before = this.rows.length
    this.rows = this.rows.filter(row => !this.matchesFilter(row, filter))
    return before - this.rows.length
  }

  async readAll(): Promise<Variant[]> {
    return [...this.rows]
  }

  async count(): Promise<number> {
    return this.rows.length
  }

  private matchesFilter(row: Variant, filter: Filter): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (row[key] !== value) return false
    }
    return true
  }

  clear(): void {
    this.rows = []
  }

  getRows(): Variant[] {
    return [...this.rows]
  }
}

/**
 * Create an InMemoryMVStorage instance
 */
export function createInMemoryMVStorage(): InMemoryMVStorage {
  return new InMemoryMVStorage()
}
