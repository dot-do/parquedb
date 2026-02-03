#!/usr/bin/env tsx
/**
 * Sync Data to R2 for Search Worker
 *
 * Automatically syncs JSON data files from data/snippets/ to the
 * parquedb-search-data R2 bucket using wrangler CLI.
 *
 * Prerequisites:
 *   1. wrangler CLI installed and authenticated
 *   2. Access to the parquedb-search-data R2 bucket
 *
 * Usage:
 *   pnpm sync:r2              # Sync all files
 *   pnpm sync:r2 --dry-run    # Show what would be synced
 *   pnpm sync:r2 --force      # Force sync even if unchanged
 */

import * as fs from 'node:fs'
import * as path from 'node:path'
import { execSync, spawn } from 'node:child_process'

// =============================================================================
// Configuration
// =============================================================================

const DATA_DIR = path.resolve(import.meta.dirname, '../../data/snippets')
const BUCKET_NAME = 'parquedb-search-data'
const ACCOUNT_ID = 'b6641681fe423910342b9ffa1364c76d'

// Files to sync (key = R2 object key, value = local file path relative to DATA_DIR)
const FILES_TO_SYNC: Record<string, string> = {
  'onet-occupations.json': 'onet-occupations.json',
  'unspsc.json': 'unspsc.json',
  'imdb-titles.json': 'imdb-titles.json',
}

// Cache file for tracking synced file hashes
const CACHE_FILE = path.resolve(import.meta.dirname, '../.r2-sync-cache.json')

// =============================================================================
// Helpers
// =============================================================================

interface SyncCache {
  [key: string]: {
    hash: string
    syncedAt: string
    size: number
  }
}

function loadCache(): SyncCache {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      return JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'))
    }
  } catch {
    // Ignore cache read errors
  }
  return {}
}

function saveCache(cache: SyncCache): void {
  fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2))
}

function computeFileHash(filePath: string): string {
  const content = fs.readFileSync(filePath)
  // Use a simple hash based on content length and sample bytes
  // For production, consider using crypto.createHash('sha256')
  const sample = content.slice(0, 1000).toString('hex')
  return `${content.length}-${sample.slice(0, 32)}`
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

async function runWrangler(args: string[]): Promise<{ success: boolean; output: string }> {
  return new Promise((resolve) => {
    const proc = spawn('npx', ['wrangler', ...args], {
      stdio: ['inherit', 'pipe', 'pipe'],
      env: { ...process.env },
    })

    let output = ''
    let error = ''

    proc.stdout?.on('data', (data) => {
      output += data.toString()
      process.stdout.write(data)
    })

    proc.stderr?.on('data', (data) => {
      error += data.toString()
      process.stderr.write(data)
    })

    proc.on('close', (code) => {
      resolve({
        success: code === 0,
        output: output + error,
      })
    })
  })
}

function checkWranglerAuth(): boolean {
  try {
    execSync('npx wrangler whoami', { stdio: 'pipe' })
    return true
  } catch {
    return false
  }
}

// =============================================================================
// Sync Logic
// =============================================================================

interface SyncOptions {
  dryRun: boolean
  force: boolean
  verbose: boolean
}

interface SyncResult {
  key: string
  status: 'synced' | 'skipped' | 'error'
  message: string
  size?: number
}

async function syncFile(
  key: string,
  localFile: string,
  cache: SyncCache,
  options: SyncOptions
): Promise<SyncResult> {
  const filePath = path.join(DATA_DIR, localFile)

  // Check if file exists
  if (!fs.existsSync(filePath)) {
    return {
      key,
      status: 'error',
      message: `File not found: ${filePath}`,
    }
  }

  const stats = fs.statSync(filePath)
  const hash = computeFileHash(filePath)

  // Check if file has changed
  if (!options.force && cache[key]?.hash === hash) {
    return {
      key,
      status: 'skipped',
      message: `Unchanged since ${cache[key].syncedAt}`,
      size: stats.size,
    }
  }

  if (options.dryRun) {
    return {
      key,
      status: 'synced',
      message: `Would sync ${formatBytes(stats.size)}`,
      size: stats.size,
    }
  }

  // Upload to R2 using wrangler
  console.log(`\nUploading ${key} (${formatBytes(stats.size)})...`)

  const result = await runWrangler([
    'r2',
    'object',
    'put',
    `${BUCKET_NAME}/${key}`,
    `--file=${filePath}`,
    '--content-type=application/json',
  ])

  if (result.success) {
    // Update cache
    cache[key] = {
      hash,
      syncedAt: new Date().toISOString(),
      size: stats.size,
    }
    return {
      key,
      status: 'synced',
      message: `Uploaded ${formatBytes(stats.size)}`,
      size: stats.size,
    }
  } else {
    return {
      key,
      status: 'error',
      message: `Upload failed`,
      size: stats.size,
    }
  }
}

async function syncAll(options: SyncOptions): Promise<void> {
  console.log('ParqueDB R2 Sync')
  console.log('================')
  console.log(`Bucket: ${BUCKET_NAME}`)
  console.log(`Source: ${DATA_DIR}`)
  console.log(`Mode: ${options.dryRun ? 'DRY RUN' : options.force ? 'FORCE' : 'INCREMENTAL'}`)
  console.log()

  // Check wrangler auth (skip in dry-run)
  if (!options.dryRun) {
    console.log('Checking wrangler authentication...')
    if (!checkWranglerAuth()) {
      console.error('Error: wrangler is not authenticated.')
      console.error('Run "npx wrangler login" first.')
      process.exit(1)
    }
    console.log('Authenticated.')
    console.log()
  }

  // Check data directory exists
  if (!fs.existsSync(DATA_DIR)) {
    console.error(`Error: Data directory not found: ${DATA_DIR}`)
    console.error('Run data generation scripts first.')
    process.exit(1)
  }

  const cache = options.force ? {} : loadCache()
  const results: SyncResult[] = []

  console.log('Syncing files:')
  console.log('-'.repeat(60))

  for (const [key, localFile] of Object.entries(FILES_TO_SYNC)) {
    const result = await syncFile(key, localFile, cache, options)
    results.push(result)

    const statusIcon = result.status === 'synced' ? '+' : result.status === 'skipped' ? '=' : 'x'
    const sizeStr = result.size ? ` (${formatBytes(result.size)})` : ''
    console.log(`  [${statusIcon}] ${key}${sizeStr}`)
    if (options.verbose || result.status === 'error') {
      console.log(`      ${result.message}`)
    }
  }

  // Save cache (unless dry-run)
  if (!options.dryRun) {
    saveCache(cache)
  }

  // Summary
  console.log()
  console.log('-'.repeat(60))
  const synced = results.filter((r) => r.status === 'synced').length
  const skipped = results.filter((r) => r.status === 'skipped').length
  const errors = results.filter((r) => r.status === 'error').length

  console.log(`Summary: ${synced} synced, ${skipped} skipped, ${errors} errors`)

  if (errors > 0) {
    process.exit(1)
  }
}

// =============================================================================
// CLI
// =============================================================================

function printUsage(): void {
  console.log(`
ParqueDB R2 Sync - Sync data files to R2 for search worker

Usage:
  pnpm sync:r2 [options]

Options:
  --dry-run    Show what would be synced without uploading
  --force      Force sync even if files haven't changed
  --verbose    Show detailed output
  --help       Show this help message

Files synced:
${Object.entries(FILES_TO_SYNC)
  .map(([key, file]) => `  ${key} <- ${file}`)
  .join('\n')}

Environment:
  CLOUDFLARE_API_TOKEN  Wrangler authentication (or use wrangler login)
`)
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)

  if (args.includes('--help') || args.includes('-h')) {
    printUsage()
    return
  }

  const options: SyncOptions = {
    dryRun: args.includes('--dry-run'),
    force: args.includes('--force'),
    verbose: args.includes('--verbose') || args.includes('-v'),
  }

  await syncAll(options)
}

main().catch((error) => {
  console.error('Fatal error:', error)
  process.exit(1)
})
