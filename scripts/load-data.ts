#!/usr/bin/env npx tsx
/**
 * Data Loader CLI
 *
 * Downloads and loads example datasets into Parquet format.
 *
 * Usage:
 *   npx tsx scripts/load-data.ts --dataset onet --output ./data/onet
 *   npx tsx scripts/load-data.ts --dataset imdb --output ./data/imdb
 *   npx tsx scripts/load-data.ts --dataset wiktionary --output ./data/wiktionary
 *   npx tsx scripts/load-data.ts --dataset unspsc --output ./data/unspsc
 *   npx tsx scripts/load-data.ts --dataset wikidata --output ./data/wikidata
 *   npx tsx scripts/load-data.ts --dataset commoncrawl --output ./data/commoncrawl
 */

import { promises as fs } from 'node:fs'
import { createWriteStream, createReadStream, existsSync } from 'node:fs'
import { pipeline } from 'node:stream/promises'
import { Readable, Transform } from 'node:stream'
import { createGunzip, createInflate, inflateRawSync } from 'node:zlib'
import { join, basename, dirname } from 'node:path'
import { parseArgs } from 'node:util'
import { createInterface } from 'node:readline'

// =============================================================================
// Types
// =============================================================================

interface DatasetConfig {
  name: string
  description: string
  url: string
  expectedSize: number // Expected raw size in bytes
  format: 'zip' | 'tsv' | 'csv' | 'json' | 'gzip'
  loader: (config: LoaderContext) => Promise<LoadResult>
}

interface LoaderContext {
  downloadPath: string
  outputDir: string
  onProgress: (progress: ProgressInfo) => void
}

interface ProgressInfo {
  phase: 'download' | 'extract' | 'process' | 'write'
  current: number
  total: number
  message: string
}

interface LoadResult {
  rawSize: number
  parquetSize: number
  compressionRatio: number
  entityCounts: Record<string, number>
  files: string[]
  duration: number
}

interface FileStats {
  name: string
  rowCount: number
  rawSize: number
  parquetSize: number
}

// =============================================================================
// Dataset Configurations
// =============================================================================

const DATASETS: Record<string, DatasetConfig> = {
  onet: {
    name: 'O*NET',
    description: 'Occupational Information Network - job skills, abilities, and tasks',
    url: 'https://www.onetcenter.org/dl_files/database/db_28_3_text.zip',
    expectedSize: 100 * 1024 * 1024, // ~100MB
    format: 'zip',
    loader: loadOnetDataset,
  },
  imdb: {
    name: 'IMDB',
    description: 'Movie and TV show database',
    url: 'https://datasets.imdbws.com/',
    expectedSize: 2 * 1024 * 1024 * 1024, // ~2GB
    format: 'gzip',
    loader: loadImdbDataset,
  },
  wiktionary: {
    name: 'Wiktionary',
    description: 'Multi-language dictionary',
    url: 'https://dumps.wikimedia.org/enwiktionary/latest/enwiktionary-latest-pages-articles.xml.bz2',
    expectedSize: 1 * 1024 * 1024 * 1024, // ~1GB
    format: 'gzip',
    loader: loadWiktionaryDataset,
  },
  unspsc: {
    name: 'UNSPSC',
    description: 'United Nations Standard Products and Services Code',
    url: 'https://www.unspsc.org/', // Requires manual download
    expectedSize: 10 * 1024 * 1024, // ~10MB
    format: 'csv',
    loader: loadUnspscDataset,
  },
  wikidata: {
    name: 'Wikidata',
    description: 'Knowledge graph from Wikimedia',
    url: 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2',
    expectedSize: 100 * 1024 * 1024 * 1024, // ~100GB
    format: 'json',
    loader: loadWikidataDataset,
  },
  commoncrawl: {
    name: 'CommonCrawl',
    description: 'Web crawl data',
    url: 'https://data.commoncrawl.org/',
    expectedSize: 1 * 1024 * 1024 * 1024 * 1024, // ~1TB (sample)
    format: 'gzip',
    loader: loadCommonCrawlDataset,
  },
}

// =============================================================================
// Progress Display
// =============================================================================

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  const minutes = Math.floor(ms / 60000)
  const seconds = ((ms % 60000) / 1000).toFixed(0)
  return `${minutes}m ${seconds}s`
}

function createProgressReporter() {
  let lastLine = ''

  return (progress: ProgressInfo) => {
    const percent = progress.total > 0
      ? Math.round((progress.current / progress.total) * 100)
      : 0

    const bar = '='.repeat(Math.floor(percent / 2)).padEnd(50, ' ')
    const line = `[${bar}] ${percent}% | ${progress.phase}: ${progress.message}`

    // Clear previous line and write new one
    if (lastLine) {
      process.stdout.write('\r' + ' '.repeat(lastLine.length) + '\r')
    }
    process.stdout.write(line)
    lastLine = line
  }
}

function finishProgress() {
  process.stdout.write('\n')
}

// =============================================================================
// Download Utilities
// =============================================================================

const MAX_RETRIES = 3
const RETRY_DELAY_MS = 2000

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function downloadFile(
  url: string,
  destPath: string,
  onProgress: (downloaded: number, total: number) => void,
  options: { maxRetries?: number; retryDelay?: number } = {}
): Promise<void> {
  const maxRetries = options.maxRetries ?? MAX_RETRIES
  const retryDelay = options.retryDelay ?? RETRY_DELAY_MS

  // Check if file already exists (resumable downloads)
  if (existsSync(destPath)) {
    const stat = await fs.stat(destPath)
    console.log(`File already exists: ${destPath} (${formatBytes(stat.size)})`)
    return
  }

  // Check for partial download
  const partialPath = `${destPath}.partial`

  // Ensure directory exists
  await fs.mkdir(dirname(destPath), { recursive: true })

  let lastError: Error | null = null

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Downloading ${url}${attempt > 1 ? ` (attempt ${attempt}/${maxRetries})` : ''}...`)

      const response = await fetch(url, {
        headers: {
          'User-Agent': 'ParqueDB-DataLoader/1.0',
        },
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const contentLength = parseInt(response.headers.get('content-length') || '0', 10)
      let downloaded = 0

      const reader = response.body?.getReader()
      if (!reader) {
        throw new Error('No response body')
      }

      const chunks: Uint8Array[] = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        chunks.push(value)
        downloaded += value.length
        onProgress(downloaded, contentLength)
      }

      // Combine chunks and write to file
      const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
      const result = new Uint8Array(totalLength)
      let offset = 0
      for (const chunk of chunks) {
        result.set(chunk, offset)
        offset += chunk.length
      }

      // Write to partial file first, then rename (atomic)
      await fs.writeFile(partialPath, result)
      await fs.rename(partialPath, destPath)

      return // Success
    } catch (error) {
      lastError = error as Error
      console.error(`Download failed (attempt ${attempt}/${maxRetries}): ${lastError.message}`)

      // Clean up partial file on error
      try {
        await fs.unlink(partialPath)
      } catch {
        // Ignore
      }

      if (attempt < maxRetries) {
        console.log(`Retrying in ${retryDelay / 1000} seconds...`)
        await sleep(retryDelay * attempt) // Exponential backoff
      }
    }
  }

  throw new Error(`Download failed after ${maxRetries} attempts: ${lastError?.message}`)
}

// =============================================================================
// ZIP Extraction
// =============================================================================

interface ZipEntry {
  filename: string
  compressedSize: number
  uncompressedSize: number
  compressionMethod: number
  localHeaderOffset: number
}

async function extractZip(
  zipPath: string,
  extractDir: string,
  onProgress: (extracted: number, total: number, filename: string) => void
): Promise<string[]> {
  await fs.mkdir(extractDir, { recursive: true })

  const zipData = await fs.readFile(zipPath)
  const entries = parseZipEntries(zipData)
  const extractedFiles: string[] = []

  let extractedCount = 0
  for (const entry of entries) {
    if (entry.filename.endsWith('/')) {
      // Directory entry
      await fs.mkdir(join(extractDir, entry.filename), { recursive: true })
    } else {
      const data = extractZipEntry(zipData, entry)
      const outPath = join(extractDir, entry.filename)
      await fs.mkdir(dirname(outPath), { recursive: true })
      await fs.writeFile(outPath, data)
      extractedFiles.push(outPath)
    }
    extractedCount++
    onProgress(extractedCount, entries.length, entry.filename)
  }

  return extractedFiles
}

function parseZipEntries(data: Uint8Array): ZipEntry[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  const entries: ZipEntry[] = []

  // Find End of Central Directory
  let eocdOffset = -1
  for (let i = data.length - 22; i >= 0; i--) {
    if (view.getUint32(i, true) === 0x06054b50) {
      eocdOffset = i
      break
    }
  }

  if (eocdOffset === -1) {
    throw new Error('Invalid ZIP file: EOCD not found')
  }

  const centralDirOffset = view.getUint32(eocdOffset + 16, true)
  const numEntries = view.getUint16(eocdOffset + 10, true)

  let offset = centralDirOffset
  for (let i = 0; i < numEntries; i++) {
    if (view.getUint32(offset, true) !== 0x02014b50) {
      throw new Error('Invalid central directory entry')
    }

    const compressionMethod = view.getUint16(offset + 10, true)
    const compressedSize = view.getUint32(offset + 20, true)
    const uncompressedSize = view.getUint32(offset + 24, true)
    const filenameLength = view.getUint16(offset + 28, true)
    const extraLength = view.getUint16(offset + 30, true)
    const commentLength = view.getUint16(offset + 32, true)
    const localHeaderOffset = view.getUint32(offset + 42, true)

    const filename = new TextDecoder().decode(
      data.slice(offset + 46, offset + 46 + filenameLength)
    )

    entries.push({
      filename,
      compressedSize,
      uncompressedSize,
      compressionMethod,
      localHeaderOffset,
    })

    offset += 46 + filenameLength + extraLength + commentLength
  }

  return entries
}

function extractZipEntry(data: Uint8Array, entry: ZipEntry): Uint8Array {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  let offset = entry.localHeaderOffset

  if (view.getUint32(offset, true) !== 0x04034b50) {
    throw new Error('Invalid local file header')
  }

  const filenameLength = view.getUint16(offset + 26, true)
  const extraLength = view.getUint16(offset + 28, true)
  const dataOffset = offset + 30 + filenameLength + extraLength

  const compressedData = data.slice(dataOffset, dataOffset + entry.compressedSize)

  if (entry.compressionMethod === 0) {
    // Stored (no compression)
    return compressedData
  } else if (entry.compressionMethod === 8) {
    // Deflate
    return inflateSync(compressedData)
  } else {
    throw new Error(`Unsupported compression method: ${entry.compressionMethod}`)
  }
}

function inflateSync(data: Uint8Array): Uint8Array {
  // Use zlib's raw inflate (no header)
  return new Uint8Array(inflateRawSync(data))
}

// =============================================================================
// TSV/CSV Parsing
// =============================================================================

interface ParsedRow {
  [key: string]: string
}

async function* parseTsvFile(
  filePath: string
): AsyncGenerator<ParsedRow, void, unknown> {
  const fileStream = createReadStream(filePath)
  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  })

  let headers: string[] | null = null
  for await (const line of rl) {
    if (!headers) {
      headers = line.split('\t')
      continue
    }

    const values = line.split('\t')
    const row: ParsedRow = {}
    for (let i = 0; i < headers.length; i++) {
      row[headers[i] ?? `col_${i}`] = values[i] ?? ''
    }
    yield row
  }
}

// =============================================================================
// Parquet Writing
// =============================================================================

interface ColumnData {
  name: string
  data: unknown[]
}

async function writeParquetFile(
  outputPath: string,
  columns: ColumnData[],
  rowCount: number
): Promise<number> {
  await fs.mkdir(dirname(outputPath), { recursive: true })

  try {
    const { parquetWriteBuffer } = await import('hyparquet-writer')

    const buffer = parquetWriteBuffer({
      columnData: columns,
      compressed: true,
      statistics: true,
      rowGroupSize: Math.min(10000, rowCount),
    })

    await fs.writeFile(outputPath, new Uint8Array(buffer))
    return buffer.byteLength
  } catch (err) {
    // Fallback: write as JSON for now
    console.warn('hyparquet-writer not available, falling back to JSON')
    const jsonData = JSON.stringify({ columns, rowCount })
    const buffer = Buffer.from(jsonData)
    await fs.writeFile(outputPath.replace('.parquet', '.json'), buffer)
    return buffer.length
  }
}

// =============================================================================
// O*NET Dataset Loader
// =============================================================================

async function loadOnetDataset(ctx: LoaderContext): Promise<LoadResult> {
  const startTime = Date.now()
  const entityCounts: Record<string, number> = {}
  const files: string[] = []
  let totalRawSize = 0
  let totalParquetSize = 0

  // Download the ZIP file
  ctx.onProgress({
    phase: 'download',
    current: 0,
    total: 100,
    message: 'Starting download...',
  })

  const zipPath = join(ctx.downloadPath, 'onet_db.zip')
  await downloadFile(
    DATASETS.onet.url,
    zipPath,
    (downloaded, total) => {
      ctx.onProgress({
        phase: 'download',
        current: downloaded,
        total: total || DATASETS.onet.expectedSize,
        message: `${formatBytes(downloaded)} / ${formatBytes(total || DATASETS.onet.expectedSize)}`,
      })
    }
  )
  finishProgress()

  // Extract ZIP file
  ctx.onProgress({
    phase: 'extract',
    current: 0,
    total: 100,
    message: 'Extracting...',
  })

  const extractDir = join(ctx.downloadPath, 'onet_extracted')
  const extractedFiles = await extractZip(
    zipPath,
    extractDir,
    (extracted, total, filename) => {
      ctx.onProgress({
        phase: 'extract',
        current: extracted,
        total,
        message: basename(filename),
      })
    }
  )
  finishProgress()

  // Find TSV files
  const tsvFiles = extractedFiles.filter(f => f.endsWith('.txt'))
  console.log(`Found ${tsvFiles.length} TSV files`)

  // Process each TSV file
  for (let i = 0; i < tsvFiles.length; i++) {
    const tsvFile = tsvFiles[i]!
    const filename = basename(tsvFile, '.txt')

    ctx.onProgress({
      phase: 'process',
      current: i,
      total: tsvFiles.length,
      message: filename,
    })

    // Read and parse TSV
    const rows: ParsedRow[] = []
    for await (const row of parseTsvFile(tsvFile)) {
      rows.push(row)
    }

    if (rows.length === 0) continue

    // Get raw size
    const stat = await fs.stat(tsvFile)
    totalRawSize += stat.size

    // Convert to column format
    const columnNames = Object.keys(rows[0]!)
    const columns: ColumnData[] = columnNames.map(name => ({
      name,
      data: rows.map(row => row[name] ?? null),
    }))

    // Write Parquet file
    const outputPath = join(ctx.outputDir, `${filename}.parquet`)
    const parquetSize = await writeParquetFile(outputPath, columns, rows.length)
    totalParquetSize += parquetSize

    entityCounts[filename] = rows.length
    files.push(outputPath)

    console.log(
      `  ${filename}: ${rows.length.toLocaleString()} rows, ` +
        `${formatBytes(stat.size)} -> ${formatBytes(parquetSize)} ` +
        `(${((parquetSize / stat.size) * 100).toFixed(1)}%)`
    )
  }
  finishProgress()

  // Write manifest
  const manifest = {
    dataset: 'onet',
    version: '28.3',
    loadedAt: new Date().toISOString(),
    entityCounts,
    rawSize: totalRawSize,
    parquetSize: totalParquetSize,
    compressionRatio: totalParquetSize / totalRawSize,
    files: files.map(f => basename(f)),
  }
  await fs.writeFile(
    join(ctx.outputDir, 'manifest.json'),
    JSON.stringify(manifest, null, 2)
  )

  return {
    rawSize: totalRawSize,
    parquetSize: totalParquetSize,
    compressionRatio: totalParquetSize / totalRawSize,
    entityCounts,
    files,
    duration: Date.now() - startTime,
  }
}

// =============================================================================
// IMDB Dataset Loader
// =============================================================================

async function loadImdbDataset(ctx: LoaderContext): Promise<LoadResult> {
  const startTime = Date.now()
  const entityCounts: Record<string, number> = {}
  const files: string[] = []
  let totalRawSize = 0
  let totalParquetSize = 0

  const imdbFiles = [
    'name.basics.tsv.gz',
    'title.akas.tsv.gz',
    'title.basics.tsv.gz',
    'title.crew.tsv.gz',
    'title.episode.tsv.gz',
    'title.principals.tsv.gz',
    'title.ratings.tsv.gz',
  ]

  for (let i = 0; i < imdbFiles.length; i++) {
    const filename = imdbFiles[i]!
    const url = `https://datasets.imdbws.com/${filename}`
    const gzPath = join(ctx.downloadPath, filename)

    ctx.onProgress({
      phase: 'download',
      current: i,
      total: imdbFiles.length,
      message: `Downloading ${filename}...`,
    })

    // Download if not exists
    if (!existsSync(gzPath)) {
      await downloadFile(url, gzPath, (downloaded, total) => {
        ctx.onProgress({
          phase: 'download',
          current: downloaded,
          total: total || 500 * 1024 * 1024,
          message: `${filename}: ${formatBytes(downloaded)}`,
        })
      })
    }
    finishProgress()

    // Decompress and process
    ctx.onProgress({
      phase: 'process',
      current: i,
      total: imdbFiles.length,
      message: `Processing ${filename}...`,
    })

    const tsvPath = gzPath.replace('.gz', '')
    if (!existsSync(tsvPath)) {
      // Decompress
      const gunzip = createGunzip()
      const source = createReadStream(gzPath)
      const dest = createWriteStream(tsvPath)
      await pipeline(source, gunzip, dest)
    }

    // Parse TSV
    const rows: ParsedRow[] = []
    let rowCount = 0
    for await (const row of parseTsvFile(tsvPath)) {
      rows.push(row)
      rowCount++
      // Limit to 1 million rows for memory efficiency during development
      if (rowCount >= 1000000) break
    }

    if (rows.length === 0) continue

    const stat = await fs.stat(tsvPath)
    totalRawSize += stat.size

    // Convert to columns
    const columnNames = Object.keys(rows[0]!)
    const columns: ColumnData[] = columnNames.map(name => ({
      name,
      data: rows.map(row => row[name] ?? null),
    }))

    // Write Parquet
    const baseName = filename.replace('.tsv.gz', '')
    const outputPath = join(ctx.outputDir, `${baseName}.parquet`)
    const parquetSize = await writeParquetFile(outputPath, columns, rows.length)
    totalParquetSize += parquetSize

    entityCounts[baseName] = rows.length
    files.push(outputPath)

    console.log(
      `  ${baseName}: ${rows.length.toLocaleString()} rows, ` +
        `${formatBytes(stat.size)} -> ${formatBytes(parquetSize)}`
    )
  }
  finishProgress()

  // Write manifest
  const manifest = {
    dataset: 'imdb',
    loadedAt: new Date().toISOString(),
    entityCounts,
    rawSize: totalRawSize,
    parquetSize: totalParquetSize,
    compressionRatio: totalParquetSize / totalRawSize,
    files: files.map(f => basename(f)),
  }
  await fs.writeFile(
    join(ctx.outputDir, 'manifest.json'),
    JSON.stringify(manifest, null, 2)
  )

  return {
    rawSize: totalRawSize,
    parquetSize: totalParquetSize,
    compressionRatio: totalParquetSize / totalRawSize,
    entityCounts,
    files,
    duration: Date.now() - startTime,
  }
}

// =============================================================================
// Wiktionary Dataset Loader (Stub)
// =============================================================================

async function loadWiktionaryDataset(ctx: LoaderContext): Promise<LoadResult> {
  console.log('Wiktionary loader not yet implemented')
  return {
    rawSize: 0,
    parquetSize: 0,
    compressionRatio: 0,
    entityCounts: {},
    files: [],
    duration: 0,
  }
}

// =============================================================================
// UNSPSC Dataset Loader (Stub)
// =============================================================================

async function loadUnspscDataset(ctx: LoaderContext): Promise<LoadResult> {
  console.log('UNSPSC loader not yet implemented')
  console.log('UNSPSC data requires manual download from https://www.unspsc.org/')
  return {
    rawSize: 0,
    parquetSize: 0,
    compressionRatio: 0,
    entityCounts: {},
    files: [],
    duration: 0,
  }
}

// =============================================================================
// Wikidata Dataset Loader (Stub)
// =============================================================================

async function loadWikidataDataset(ctx: LoaderContext): Promise<LoadResult> {
  console.log('Wikidata loader not yet implemented')
  console.log('Note: Full Wikidata dump is ~100GB compressed')
  return {
    rawSize: 0,
    parquetSize: 0,
    compressionRatio: 0,
    entityCounts: {},
    files: [],
    duration: 0,
  }
}

// =============================================================================
// CommonCrawl Dataset Loader (Stub)
// =============================================================================

async function loadCommonCrawlDataset(ctx: LoaderContext): Promise<LoadResult> {
  console.log('CommonCrawl loader not yet implemented')
  console.log('Note: CommonCrawl data is very large (TBs)')
  return {
    rawSize: 0,
    parquetSize: 0,
    compressionRatio: 0,
    entityCounts: {},
    files: [],
    duration: 0,
  }
}

// =============================================================================
// Main CLI
// =============================================================================

async function main() {
  const { values } = parseArgs({
    options: {
      dataset: {
        type: 'string',
        short: 'd',
      },
      output: {
        type: 'string',
        short: 'o',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
      list: {
        type: 'boolean',
        short: 'l',
      },
    },
  })

  if (values.help) {
    console.log(`
Data Loader CLI

Usage:
  npx tsx scripts/load-data.ts --dataset <name> --output <dir>

Options:
  -d, --dataset   Dataset to load (onet, imdb, wiktionary, unspsc, wikidata, commoncrawl)
  -o, --output    Output directory for Parquet files
  -l, --list      List available datasets
  -h, --help      Show this help

Examples:
  npx tsx scripts/load-data.ts --dataset onet --output ./data/onet
  npx tsx scripts/load-data.ts --dataset imdb --output ./data/imdb
`)
    return
  }

  if (values.list) {
    console.log('\nAvailable Datasets:\n')
    for (const [key, config] of Object.entries(DATASETS)) {
      console.log(`  ${key.padEnd(12)} - ${config.name}`)
      console.log(`                  ${config.description}`)
      console.log(`                  Expected size: ~${formatBytes(config.expectedSize)}`)
      console.log('')
    }
    return
  }

  if (!values.dataset) {
    console.error('Error: --dataset is required')
    console.error('Use --list to see available datasets, or --help for usage')
    process.exit(1)
  }

  const datasetConfig = DATASETS[values.dataset]
  if (!datasetConfig) {
    console.error(`Error: Unknown dataset '${values.dataset}'`)
    console.error('Available datasets:', Object.keys(DATASETS).join(', '))
    process.exit(1)
  }

  const outputDir = values.output || `./data/${values.dataset}`
  const downloadDir = join(outputDir, '.cache')

  console.log(`
================================================================================
Loading ${datasetConfig.name} Dataset
================================================================================
Description: ${datasetConfig.description}
Source: ${datasetConfig.url}
Expected size: ~${formatBytes(datasetConfig.expectedSize)}
Output: ${outputDir}
================================================================================
`)

  await fs.mkdir(outputDir, { recursive: true })
  await fs.mkdir(downloadDir, { recursive: true })

  const progressReporter = createProgressReporter()

  try {
    const result = await datasetConfig.loader({
      downloadPath: downloadDir,
      outputDir,
      onProgress: progressReporter,
    })

    console.log(`
================================================================================
Load Complete
================================================================================
Duration:          ${formatDuration(result.duration)}
Raw size:          ${formatBytes(result.rawSize)}
Parquet size:      ${formatBytes(result.parquetSize)}
Compression ratio: ${(result.compressionRatio * 100).toFixed(1)}%
Files created:     ${result.files.length}

Entity Counts:
${Object.entries(result.entityCounts)
  .map(([type, count]) => `  ${type.padEnd(30)} ${count.toLocaleString()}`)
  .join('\n')}
================================================================================
`)
  } catch (error) {
    console.error('\nError loading dataset:', error)
    process.exit(1)
  }
}

main().catch(console.error)
