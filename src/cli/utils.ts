/**
 * CLI Utilities
 *
 * Enhanced CLI utilities powered by @dotdo/cli.
 * Provides spinners, progress bars, syntax highlighting, and output formatting.
 *
 * Uses sub-path imports to avoid loading auth dependencies.
 */

import {
  formatOutput,
  formatOutputSync,
  createTableFormatter,
  createJsonFormatter,
} from '@dotdo/cli/output'

import {
  highlightJson,
  highlightSql,
  highlightTypeScript,
  highlightJavaScript,
} from '@dotdo/cli/highlight'

import {
  createSpinner,
  startSpinner,
  withSpinner,
  spinnerFrames,
} from '@dotdo/cli/spinner'

import type { Spinner } from '@dotdo/cli/spinner'

import {
  createProgress,
  startProgress,
  withProgress,
  MultiProgress,
  createMultiProgress,
} from '@dotdo/cli/progress'

import type { Progress } from '@dotdo/cli/progress'

import {
  colorize,
  colors,
  isColorEnabled,
  isTTY,
  setNoColor,
  getNoColor,
  stripAnsi,
} from '@dotdo/cli/colors'

import {
  BYTES_PER_KB,
  MS_PER_SECOND,
  MS_PER_MINUTE,
  MS_PER_HOUR,
} from '../constants'

// Type definitions for cli.do utilities
// These are inferred from the package exports

/** Output format types supported by formatters */
type OutputFormat = 'json' | 'json5' | 'table' | 'highlighted' | 'raw'

/** Configuration for output formatting */
interface OutputConfig {
  format?: OutputFormat | undefined
  colors?: boolean | undefined
  pretty?: boolean | undefined
}

/** Table column definition */
interface TableColumn {
  key: string
  header: string
  width?: number | undefined
  minWidth?: number | undefined
  maxWidth?: number | undefined
  align?: 'left' | 'right' | 'center' | undefined
}

/** Table configuration */
interface TableConfig {
  columns?: TableColumn[] | undefined
  showHeader?: boolean | undefined
  border?: 'ascii' | 'unicode' | 'none' | undefined
  padding?: number | undefined
}

/** Spinner configuration */
interface SpinnerConfig {
  text?: string | undefined
  frames?: string[] | undefined
  interval?: number | undefined
  color?: string | undefined
  stream?: NodeJS.WriteStream | undefined
  colors?: boolean | undefined
}

/** Progress bar configuration */
interface ProgressConfig {
  total: number
  current?: number | undefined
  width?: number | undefined
  complete?: string | undefined
  incomplete?: string | undefined
  format?: string | undefined
  showETA?: boolean | undefined
  stream?: NodeJS.WriteStream | undefined
  colors?: boolean | undefined
}

// Re-export all utilities
export {
  createSpinner,
  startSpinner,
  withSpinner,
  spinnerFrames,
  createProgress,
  startProgress,
  withProgress,
  MultiProgress,
  createMultiProgress,
  formatOutput,
  formatOutputSync,
  highlightJson,
  highlightSql,
  highlightTypeScript,
  highlightJavaScript,
  colorize,
  colors,
  isColorEnabled,
  isTTY,
  setNoColor,
  getNoColor,
  stripAnsi,
  createTableFormatter,
  createJsonFormatter,
}

// Re-export types
export type {
  SpinnerConfig,
  ProgressConfig,
  OutputFormat,
  OutputConfig,
  Spinner,
  Progress,
  TableColumn,
  TableConfig,
}

// =============================================================================
// ParqueDB-specific CLI Helpers
// =============================================================================

/**
 * Print a colored success message
 */
export function success(message: string): void {
  const prefix = isColorEnabled() ? `${colors.green}OK${colors.reset}` : '[OK]'
  console.log(`${prefix} ${message}`)
}

/**
 * Print a colored error message
 */
export function error(message: string): void {
  const prefix = isColorEnabled() ? `${colors.red}Error${colors.reset}` : '[ERROR]'
  console.error(`${prefix} ${message}`)
}

/**
 * Print a colored warning message
 */
export function warn(message: string): void {
  const prefix = isColorEnabled() ? `${colors.yellow}Warning${colors.reset}` : '[WARN]'
  console.warn(`${prefix} ${message}`)
}

/**
 * Print a colored info message
 */
export function info(message: string): void {
  const prefix = isColorEnabled() ? `${colors.blue}Info${colors.reset}` : '[INFO]'
  console.log(`${prefix} ${message}`)
}

/**
 * Print a dimmed message (for secondary information)
 */
export function dim(message: string): void {
  if (isColorEnabled()) {
    console.log(`${colors.gray}${message}${colors.reset}`)
  } else {
    console.log(message)
  }
}

/**
 * Print a bold header
 */
export function header(message: string): void {
  if (isColorEnabled()) {
    console.log(`${colors.bold}${colors.cyan}${message}${colors.reset}`)
  } else {
    console.log(message)
  }
}

/**
 * Print data in a table format
 */
export async function printTable(data: Record<string, unknown>[] | Record<string, unknown>): Promise<void> {
  const output = await formatOutput(data, 'table')
  console.log(output)
}

/**
 * Print JSON with optional syntax highlighting
 */
export async function printJson(
  data: unknown,
  options: { pretty?: boolean | undefined; highlighted?: boolean | undefined } = {}
): Promise<void> {
  const { pretty = true, highlighted = isTTY() } = options

  if (highlighted) {
    const output = await highlightJson(data)
    console.log(output)
  } else {
    const indent = pretty ? 2 : 0
    console.log(JSON.stringify(data, null, indent))
  }
}

/**
 * Execute an async function with a spinner showing progress
 */
export async function withLoading<T>(
  fn: () => Promise<T>,
  options: {
    text?: string | undefined
    successText?: string | undefined
    failText?: string | undefined
  } = {}
): Promise<T> {
  return withSpinner(fn, {
    text: options.text ?? 'Processing...',
    ...(options.successText !== undefined ? { successText: options.successText } : {}),
    ...(options.failText !== undefined ? { failText: options.failText } : {}),
  })
}

/**
 * Process items with a progress bar
 */
export async function processWithProgress<T>(
  items: T[],
  fn: (item: T, index: number) => Promise<void> | void,
  options: {
    label?: string | undefined
    showEta?: boolean | undefined
  } = {}
): Promise<void> {
  const { label, showEta = true } = options

  if (label) {
    console.log(label)
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  await withProgress(items, fn, {
    config: { showETA: showEta },
  } as any)
}

/**
 * Format bytes to human-readable string
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(BYTES_PER_KB))
  const value = bytes / Math.pow(BYTES_PER_KB, i)

  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

/**
 * Format a duration in milliseconds to human-readable string
 */
export function formatDuration(ms: number): string {
  if (ms < MS_PER_SECOND) return `${ms}ms`
  if (ms < MS_PER_MINUTE) return `${(ms / MS_PER_SECOND).toFixed(1)}s`
  if (ms < MS_PER_HOUR) {
    const mins = Math.floor(ms / MS_PER_MINUTE)
    const secs = Math.floor((ms % MS_PER_MINUTE) / MS_PER_SECOND)
    return `${mins}m ${secs}s`
  }

  const hours = Math.floor(ms / MS_PER_HOUR)
  const mins = Math.floor((ms % MS_PER_HOUR) / MS_PER_MINUTE)
  return `${hours}h ${mins}m`
}

/**
 * Create a styled divider line
 */
export function divider(char: string = '-', length: number = 50): string {
  return char.repeat(length)
}

/**
 * Box styling for important messages
 */
export function box(title: string, content: string[]): void {
  const maxLen = Math.max(title.length, ...content.map(line => line.length))
  const width = maxLen + 4

  const topBorder = isColorEnabled()
    ? `${colors.cyan}+${'-'.repeat(width - 2)}+${colors.reset}`
    : `+${'-'.repeat(width - 2)}+`

  const titleLine = isColorEnabled()
    ? `${colors.cyan}|${colors.reset} ${colors.bold}${title.padEnd(maxLen)}${colors.reset} ${colors.cyan}|${colors.reset}`
    : `| ${title.padEnd(maxLen)} |`

  const contentLines = content.map(line => {
    if (isColorEnabled()) {
      return `${colors.cyan}|${colors.reset} ${line.padEnd(maxLen)} ${colors.cyan}|${colors.reset}`
    }
    return `| ${line.padEnd(maxLen)} |`
  })

  console.log(topBorder)
  console.log(titleLine)
  console.log(topBorder)
  contentLines.forEach(line => console.log(line))
  console.log(topBorder)
}

/**
 * Print statistics as a key-value list
 */
export function printStats(stats: Record<string, string | number | boolean>): void {
  const maxKeyLen = Math.max(...Object.keys(stats).map(k => k.length))

  for (const [key, value] of Object.entries(stats)) {
    const label = key.padEnd(maxKeyLen)
    if (isColorEnabled()) {
      console.log(`  ${colors.gray}${label}${colors.reset}  ${value}`)
    } else {
      console.log(`  ${label}  ${value}`)
    }
  }
}

// =============================================================================
// Enhanced CLI Helpers for Commands
// =============================================================================

/**
 * Run an async operation with a loading spinner and automatic success/failure handling
 */
export async function runWithSpinner<T>(
  operation: () => Promise<T>,
  options: {
    text: string
    successText?: string | undefined
    failText?: string | undefined
    silent?: boolean | undefined
  }
): Promise<T> {
  if (options.silent || !isTTY()) {
    // No spinner in quiet mode or non-TTY
    return operation()
  }

  const spinner = createSpinner({
    text: options.text,
    frames: spinnerFrames.dots,
  })
  spinner.start()

  try {
    const result = await operation()
    spinner.succeed(options.successText ?? options.text)
    return result
  } catch (error) {
    spinner.fail(options.failText ?? `Failed: ${options.text}`)
    throw error
  }
}

/**
 * Process items with a progress bar, showing count and ETA
 */
export async function processItems<T, R>(
  items: T[],
  processor: (item: T, index: number) => Promise<R>,
  options: {
    label?: string | undefined
    silent?: boolean | undefined
    concurrency?: number | undefined
  } = {}
): Promise<R[]> {
  const results: R[] = []
  const { label, silent = false, concurrency = 1 } = options

  if (silent || !isTTY() || items.length === 0) {
    // Sequential processing without progress bar
    for (let i = 0; i < items.length; i++) {
      results.push(await processor(items[i]!, i))
    }
    return results
  }

  if (label) {
    console.log(label)
  }

  const progress = createProgress({ total: items.length, showETA: true })
  progress.start()

  if (concurrency === 1) {
    // Sequential processing
    for (let i = 0; i < items.length; i++) {
      results.push(await processor(items[i]!, i))
      progress.increment()
    }
  } else {
    // Concurrent processing with controlled parallelism
    const chunks: T[][] = []
    for (let i = 0; i < items.length; i += concurrency) {
      chunks.push(items.slice(i, i + concurrency))
    }

    let index = 0
    for (const chunk of chunks) {
      const chunkResults = await Promise.all(
        chunk.map((item, i) => processor(item, index + i))
      )
      results.push(...chunkResults)
      progress.update(Math.min(index + chunk.length, items.length))
      index += chunk.length
    }
  }

  progress.complete()
  return results
}

/**
 * Print data as a formatted table with automatic column detection
 */
export async function printFormattedTable(
  data: Record<string, unknown>[],
  _options: {
    columns?: TableColumn[] | undefined
    showHeader?: boolean | undefined
  } = {}
): Promise<void> {
  if (data.length === 0) {
    dim('No data to display')
    return
  }

  // formatOutput uses default table formatting
  const output = await formatOutput(data, 'table')
  console.log(output)
}

/**
 * Print JSON with syntax highlighting when TTY is available
 */
export async function printHighlightedJson(
  data: unknown,
  options: { pretty?: boolean | undefined } = {}
): Promise<void> {
  const { pretty = true } = options

  if (isTTY()) {
    const highlighted = await highlightJson(data)
    console.log(highlighted)
  } else {
    console.log(JSON.stringify(data, null, pretty ? 2 : 0))
  }
}

/**
 * Print SQL with syntax highlighting when TTY is available
 */
export async function printHighlightedSql(sql: string): Promise<void> {
  if (isTTY()) {
    const highlighted = await highlightSql(sql)
    console.log(highlighted)
  } else {
    console.log(sql)
  }
}

/**
 * Create a section with a header and indented content
 */
export function section(title: string, content: () => void): void {
  header(title)
  console.log()
  content()
  console.log()
}

/**
 * Print a list of items with bullet points
 */
export function bulletList(items: string[], indent: number = 2): void {
  const prefix = ' '.repeat(indent)
  const bullet = isColorEnabled() ? `${colors.gray}-${colors.reset}` : '-'

  for (const item of items) {
    console.log(`${prefix}${bullet} ${item}`)
  }
}

/**
 * Print a numbered list
 */
export function numberedList(items: string[], indent: number = 2): void {
  const prefix = ' '.repeat(indent)

  for (let i = 0; i < items.length; i++) {
    const num = isColorEnabled()
      ? `${colors.cyan}${i + 1}.${colors.reset}`
      : `${i + 1}.`
    console.log(`${prefix}${num} ${items[i]}`)
  }
}

/**
 * Print a key-value pair on a single line
 */
export function keyValue(key: string, value: string | number | boolean): void {
  const formattedKey = isColorEnabled()
    ? `${colors.gray}${key}:${colors.reset}`
    : `${key}:`
  console.log(`  ${formattedKey} ${value}`)
}

/**
 * Format a number with thousand separators
 */
export function formatNumber(num: number): string {
  return num.toLocaleString()
}

/**
 * Format a percentage value
 */
export function formatPercent(value: number, decimals: number = 1): string {
  return `${(value * 100).toFixed(decimals)}%`
}

/**
 * Truncate text to a maximum length with ellipsis
 */
export function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text
  return text.slice(0, maxLength - 3) + '...'
}

/**
 * Pad text to a specific width
 */
export function pad(text: string, width: number, align: 'left' | 'right' | 'center' = 'left'): string {
  const stripped = stripAnsi(text)
  const padLength = Math.max(0, width - stripped.length)

  switch (align) {
    case 'right':
      return ' '.repeat(padLength) + text
    case 'center':
      const left = Math.floor(padLength / 2)
      const right = padLength - left
      return ' '.repeat(left) + text + ' '.repeat(right)
    default:
      return text + ' '.repeat(padLength)
  }
}

/**
 * Create a simple ASCII table from data
 */
export function simpleTable(
  headers: string[],
  rows: (string | number)[][],
  options: { padding?: number | undefined } = {}
): string {
  const { padding = 2 } = options
  const pad = ' '.repeat(padding)

  // Calculate column widths
  const widths = headers.map((h, i) => {
    const cellWidths = rows.map(row => String(row[i] ?? '').length)
    return Math.max(h.length, ...cellWidths)
  })

  // Build header row
  const headerRow = headers
    .map((h, i) => h.padEnd(widths[i]!))
    .join(pad)

  // Build separator
  const separator = widths.map(w => '-'.repeat(w)).join(pad)

  // Build data rows
  const dataRows = rows.map(row =>
    row.map((cell, i) => String(cell).padEnd(widths[i]!)).join(pad)
  )

  return [headerRow, separator, ...dataRows].join('\n')
}
