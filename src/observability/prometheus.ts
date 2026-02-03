/**
 * Prometheus Metrics Exporter
 *
 * Provides a general-purpose Prometheus metrics collection and export system
 * for ParqueDB. Supports counters, gauges, and histograms with labels.
 *
 * Standard metrics exposed:
 * - parquedb_requests_total (counter, labels: method, namespace, status)
 * - parquedb_request_duration_seconds (histogram)
 * - parquedb_entities_total (gauge, by namespace)
 * - parquedb_storage_bytes (gauge)
 * - parquedb_compaction_runs_total (counter)
 * - parquedb_cache_hits_total / parquedb_cache_misses_total (counters)
 *
 * @module observability/prometheus
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Prometheus metric types
 */
export type PrometheusMetricType = 'counter' | 'gauge' | 'histogram'

/**
 * Metric definition with metadata
 */
export interface MetricDefinition {
  name: string
  help: string
  type: PrometheusMetricType
  labelNames?: string[] | undefined
  buckets?: number[] | undefined // For histograms
}

/**
 * Label set for a metric
 */
export type Labels = Record<string, string>

/**
 * Internal storage for counter/gauge values
 */
interface MetricValue {
  value: number
  labels: Labels
}

/**
 * Internal storage for histogram values
 */
interface HistogramValue {
  sum: number
  count: number
  buckets: Map<number, number> // bucket upper bound -> cumulative count
  labels: Labels
}

/**
 * Configuration for PrometheusMetrics
 */
export interface PrometheusMetricsConfig {
  /** Default labels applied to all metrics */
  defaultLabels?: Labels | undefined
  /** Default histogram buckets (in seconds) */
  defaultBuckets?: number[] | undefined
  /** Metric prefix (default: 'parquedb') */
  prefix?: string | undefined
}

// =============================================================================
// Default Configuration
// =============================================================================

/**
 * Default histogram buckets for request duration (in seconds)
 */
export const DEFAULT_DURATION_BUCKETS = [
  0.001, // 1ms
  0.005, // 5ms
  0.01, // 10ms
  0.025, // 25ms
  0.05, // 50ms
  0.1, // 100ms
  0.25, // 250ms
  0.5, // 500ms
  1, // 1s
  2.5, // 2.5s
  5, // 5s
  10, // 10s
]

/**
 * Standard ParqueDB metric definitions
 */
export const PARQUEDB_METRICS: MetricDefinition[] = [
  {
    name: 'requests_total',
    help: 'Total number of requests',
    type: 'counter',
    labelNames: ['method', 'namespace', 'status'],
  },
  {
    name: 'request_duration_seconds',
    help: 'Request duration in seconds',
    type: 'histogram',
    labelNames: ['method', 'namespace'],
    buckets: DEFAULT_DURATION_BUCKETS,
  },
  {
    name: 'entities_total',
    help: 'Total number of entities',
    type: 'gauge',
    labelNames: ['namespace'],
  },
  {
    name: 'storage_bytes',
    help: 'Storage size in bytes',
    type: 'gauge',
    labelNames: ['namespace', 'type'],
  },
  {
    name: 'compaction_runs_total',
    help: 'Total number of compaction runs',
    type: 'counter',
    labelNames: ['namespace', 'status'],
  },
  {
    name: 'cache_hits_total',
    help: 'Total number of cache hits',
    type: 'counter',
    labelNames: ['cache'],
  },
  {
    name: 'cache_misses_total',
    help: 'Total number of cache misses',
    type: 'counter',
    labelNames: ['cache'],
  },
  {
    name: 'write_operations_total',
    help: 'Total number of write operations',
    type: 'counter',
    labelNames: ['namespace', 'operation'],
  },
  {
    name: 'read_operations_total',
    help: 'Total number of read operations',
    type: 'counter',
    labelNames: ['namespace', 'operation'],
  },
  {
    name: 'errors_total',
    help: 'Total number of errors',
    type: 'counter',
    labelNames: ['type', 'namespace'],
  },
]

// =============================================================================
// Prometheus Metrics Class
// =============================================================================

/**
 * Prometheus metrics collector and exporter
 *
 * Provides methods for recording counters, gauges, and histograms,
 * and exports them in Prometheus text format.
 *
 * @example
 * ```typescript
 * const metrics = new PrometheusMetrics()
 *
 * // Record a request
 * metrics.increment('requests_total', { method: 'GET', namespace: 'users', status: '200' })
 *
 * // Record request duration
 * metrics.observe('request_duration_seconds', 0.125, { method: 'GET', namespace: 'users' })
 *
 * // Set entity count
 * metrics.set('entities_total', 1000, { namespace: 'users' })
 *
 * // Export to Prometheus format
 * const output = metrics.export()
 * ```
 */
export class PrometheusMetrics {
  private counters = new Map<string, MetricValue[]>()
  private gauges = new Map<string, MetricValue[]>()
  private histograms = new Map<string, HistogramValue[]>()
  private definitions = new Map<string, MetricDefinition>()
  private config: Required<PrometheusMetricsConfig>

  constructor(config: PrometheusMetricsConfig = {}) {
    this.config = {
      defaultLabels: config.defaultLabels ?? {},
      defaultBuckets: config.defaultBuckets ?? DEFAULT_DURATION_BUCKETS,
      prefix: config.prefix ?? 'parquedb',
    }

    // Register standard metrics
    for (const def of PARQUEDB_METRICS) {
      this.register(def)
    }
  }

  // ===========================================================================
  // Registration
  // ===========================================================================

  /**
   * Register a metric definition
   *
   * @param definition - Metric definition
   */
  register(definition: MetricDefinition): void {
    const fullName = this.getFullName(definition.name)
    this.definitions.set(fullName, {
      ...definition,
      name: fullName,
      buckets: definition.buckets ?? this.config.defaultBuckets,
    })
  }

  // ===========================================================================
  // Counter Operations
  // ===========================================================================

  /**
   * Increment a counter
   *
   * @param name - Metric name (without prefix)
   * @param labels - Label values
   * @param value - Value to add (default: 1)
   */
  increment(name: string, labels: Labels = {}, value = 1): void {
    const fullName = this.getFullName(name)
    const mergedLabels = this.mergeLabels(labels)
    const key = this.labelsToKey(mergedLabels)

    let values = this.counters.get(fullName)
    if (!values) {
      values = []
      this.counters.set(fullName, values)
    }

    const existing = values.find((v) => this.labelsToKey(v.labels) === key)
    if (existing) {
      existing.value += value
    } else {
      values.push({ value, labels: mergedLabels })
    }
  }

  /**
   * Get current counter value
   *
   * @param name - Metric name (without prefix)
   * @param labels - Label values
   * @returns Current counter value or 0
   */
  getCounter(name: string, labels: Labels = {}): number {
    const fullName = this.getFullName(name)
    const mergedLabels = this.mergeLabels(labels)
    const key = this.labelsToKey(mergedLabels)

    const values = this.counters.get(fullName)
    if (!values) return 0

    const existing = values.find((v) => this.labelsToKey(v.labels) === key)
    return existing?.value ?? 0
  }

  // ===========================================================================
  // Gauge Operations
  // ===========================================================================

  /**
   * Set a gauge value
   *
   * @param name - Metric name (without prefix)
   * @param value - Gauge value
   * @param labels - Label values
   */
  set(name: string, value: number, labels: Labels = {}): void {
    const fullName = this.getFullName(name)
    const mergedLabels = this.mergeLabels(labels)
    const key = this.labelsToKey(mergedLabels)

    let values = this.gauges.get(fullName)
    if (!values) {
      values = []
      this.gauges.set(fullName, values)
    }

    const existing = values.find((v) => this.labelsToKey(v.labels) === key)
    if (existing) {
      existing.value = value
    } else {
      values.push({ value, labels: mergedLabels })
    }
  }

  /**
   * Increment a gauge
   *
   * @param name - Metric name (without prefix)
   * @param labels - Label values
   * @param value - Value to add (default: 1)
   */
  incrementGauge(name: string, labels: Labels = {}, value = 1): void {
    const current = this.getGauge(name, labels)
    this.set(name, current + value, labels)
  }

  /**
   * Decrement a gauge
   *
   * @param name - Metric name (without prefix)
   * @param labels - Label values
   * @param value - Value to subtract (default: 1)
   */
  decrementGauge(name: string, labels: Labels = {}, value = 1): void {
    const current = this.getGauge(name, labels)
    this.set(name, current - value, labels)
  }

  /**
   * Get current gauge value
   *
   * @param name - Metric name (without prefix)
   * @param labels - Label values
   * @returns Current gauge value or 0
   */
  getGauge(name: string, labels: Labels = {}): number {
    const fullName = this.getFullName(name)
    const mergedLabels = this.mergeLabels(labels)
    const key = this.labelsToKey(mergedLabels)

    const values = this.gauges.get(fullName)
    if (!values) return 0

    const existing = values.find((v) => this.labelsToKey(v.labels) === key)
    return existing?.value ?? 0
  }

  // ===========================================================================
  // Histogram Operations
  // ===========================================================================

  /**
   * Observe a histogram value
   *
   * @param name - Metric name (without prefix)
   * @param value - Observed value
   * @param labels - Label values
   */
  observe(name: string, value: number, labels: Labels = {}): void {
    const fullName = this.getFullName(name)
    const mergedLabels = this.mergeLabels(labels)
    const key = this.labelsToKey(mergedLabels)

    const definition = this.definitions.get(fullName)
    const buckets = definition?.buckets ?? this.config.defaultBuckets

    let values = this.histograms.get(fullName)
    if (!values) {
      values = []
      this.histograms.set(fullName, values)
    }

    let existing = values.find((v) => this.labelsToKey(v.labels) === key)
    if (!existing) {
      existing = {
        sum: 0,
        count: 0,
        buckets: new Map(buckets.map((b) => [b, 0])),
        labels: mergedLabels,
      }
      values.push(existing)
    }

    existing.sum += value
    existing.count++

    // Update bucket counts - find the smallest bucket that contains this value
    // Store non-cumulative counts; export will make them cumulative
    const sortedBuckets = [...buckets].sort((a, b) => a - b)
    for (const bucket of sortedBuckets) {
      if (value <= bucket) {
        existing.buckets.set(bucket, (existing.buckets.get(bucket) ?? 0) + 1)
        break // Only count in the first (smallest) matching bucket
      }
    }
  }

  /**
   * Time a function and observe its duration
   *
   * @param name - Histogram metric name
   * @param fn - Function to time
   * @param labels - Label values
   * @returns Function result
   */
  async time<T>(name: string, fn: () => T | Promise<T>, labels: Labels = {}): Promise<T> {
    const start = performance.now()
    try {
      return await fn()
    } finally {
      const duration = (performance.now() - start) / 1000 // Convert to seconds
      this.observe(name, duration, labels)
    }
  }

  /**
   * Create a timer that can be manually ended
   *
   * @param name - Histogram metric name
   * @param labels - Label values
   * @returns Timer object with end() method
   */
  startTimer(name: string, labels: Labels = {}): { end: () => number } {
    const start = performance.now()
    return {
      end: () => {
        const duration = (performance.now() - start) / 1000 // Convert to seconds
        this.observe(name, duration, labels)
        return duration
      },
    }
  }

  // ===========================================================================
  // Export
  // ===========================================================================

  /**
   * Export all metrics in Prometheus text format
   *
   * @returns Prometheus exposition format string
   */
  export(): string {
    const lines: string[] = []
    const timestamp = Date.now()

    // Export counters
    for (const [name, values] of this.counters) {
      const def = this.definitions.get(name)
      if (def) {
        lines.push(`# HELP ${name} ${def.help}`)
        lines.push(`# TYPE ${name} counter`)
      }
      for (const { value, labels } of values) {
        lines.push(this.formatMetricLine(name, labels, value, timestamp))
      }
    }

    // Export gauges
    for (const [name, values] of this.gauges) {
      const def = this.definitions.get(name)
      if (def) {
        lines.push(`# HELP ${name} ${def.help}`)
        lines.push(`# TYPE ${name} gauge`)
      }
      for (const { value, labels } of values) {
        lines.push(this.formatMetricLine(name, labels, value, timestamp))
      }
    }

    // Export histograms
    for (const [name, values] of this.histograms) {
      const def = this.definitions.get(name)
      if (def) {
        lines.push(`# HELP ${name} ${def.help}`)
        lines.push(`# TYPE ${name} histogram`)
      }
      for (const hist of values) {
        // Export bucket values (cumulative)
        const sortedBuckets = [...hist.buckets.entries()].sort((a, b) => a[0] - b[0])
        let cumulative = 0
        for (const [le, count] of sortedBuckets) {
          cumulative += count
          lines.push(this.formatMetricLine(`${name}_bucket`, { ...hist.labels, le: String(le) }, cumulative, timestamp))
        }
        // +Inf bucket
        lines.push(this.formatMetricLine(`${name}_bucket`, { ...hist.labels, le: '+Inf' }, hist.count, timestamp))

        // Sum and count
        lines.push(this.formatMetricLine(`${name}_sum`, hist.labels, hist.sum, timestamp))
        lines.push(this.formatMetricLine(`${name}_count`, hist.labels, hist.count, timestamp))
      }
    }

    return lines.join('\n')
  }

  // ===========================================================================
  // Reset
  // ===========================================================================

  /**
   * Reset all metrics
   */
  reset(): void {
    this.counters.clear()
    this.gauges.clear()
    this.histograms.clear()
  }

  /**
   * Reset a specific metric
   *
   * @param name - Metric name (without prefix)
   */
  resetMetric(name: string): void {
    const fullName = this.getFullName(name)
    this.counters.delete(fullName)
    this.gauges.delete(fullName)
    this.histograms.delete(fullName)
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getFullName(name: string): string {
    if (name.startsWith(this.config.prefix)) {
      return name
    }
    return `${this.config.prefix}_${name}`
  }

  private mergeLabels(labels: Labels): Labels {
    return { ...this.config.defaultLabels, ...labels }
  }

  private labelsToKey(labels: Labels): string {
    const sorted = Object.entries(labels).sort((a, b) => a[0].localeCompare(b[0]))
    return JSON.stringify(sorted)
  }

  private formatMetricLine(name: string, labels: Labels, value: number, _timestamp?: number): string {
    const labelStr = Object.entries(labels)
      .map(([k, v]) => `${k}="${this.escapeLabel(v)}"`)
      .join(',')

    const labelPart = labelStr ? `{${labelStr}}` : ''
    // Note: timestamp is optional and often omitted in Prometheus exports
    return `${name}${labelPart} ${value}`
  }

  private escapeLabel(value: string): string {
    return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')
  }
}

// =============================================================================
// Global Instance
// =============================================================================

/** Global PrometheusMetrics instance */
let globalMetrics: PrometheusMetrics | null = null

/**
 * Get or create the global PrometheusMetrics instance
 *
 * @param config - Optional configuration (only used on first call)
 * @returns Global PrometheusMetrics instance
 */
export function getGlobalMetrics(config?: PrometheusMetricsConfig): PrometheusMetrics {
  if (!globalMetrics) {
    globalMetrics = new PrometheusMetrics(config)
  }
  return globalMetrics
}

/**
 * Reset the global PrometheusMetrics instance
 */
export function resetGlobalMetrics(): void {
  if (globalMetrics) {
    globalMetrics.reset()
  }
  globalMetrics = null
}

/**
 * Create a new PrometheusMetrics instance
 *
 * @param config - Configuration options
 * @returns New PrometheusMetrics instance
 */
export function createPrometheusMetrics(config?: PrometheusMetricsConfig): PrometheusMetrics {
  return new PrometheusMetrics(config)
}

// =============================================================================
// Convenience Functions for Global Metrics
// =============================================================================

/**
 * Increment a counter on the global metrics instance
 */
export function incrementCounter(name: string, labels?: Labels, value?: number): void {
  getGlobalMetrics().increment(name, labels, value)
}

/**
 * Set a gauge on the global metrics instance
 */
export function setGauge(name: string, value: number, labels?: Labels): void {
  getGlobalMetrics().set(name, value, labels)
}

/**
 * Observe a histogram value on the global metrics instance
 */
export function observeHistogram(name: string, value: number, labels?: Labels): void {
  getGlobalMetrics().observe(name, value, labels)
}

/**
 * Start a timer on the global metrics instance
 */
export function startTimer(name: string, labels?: Labels): { end: () => number } {
  return getGlobalMetrics().startTimer(name, labels)
}

/**
 * Export global metrics in Prometheus format
 */
export function exportMetrics(): string {
  return getGlobalMetrics().export()
}
