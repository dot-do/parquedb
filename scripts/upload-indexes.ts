#!/usr/bin/env bun
/**
 * Upload Secondary Indexes to R2
 *
 * Uploads built indexes from local filesystem to Cloudflare R2 bucket.
 *
 * Usage:
 *   bun scripts/upload-indexes.ts --dataset imdb-1m
 *   bun scripts/upload-indexes.ts --dataset imdb-1m --dry-run
 *   bun scripts/upload-indexes.ts --all
 *
 * Environment variables:
 *   R2_ACCOUNT_ID - Cloudflare account ID
 *   R2_ACCESS_KEY_ID - R2 access key
 *   R2_SECRET_ACCESS_KEY - R2 secret key
 *   R2_BUCKET - R2 bucket name (default: parquedb)
 */

import { readFileSync, existsSync, readdirSync, statSync } from 'fs'
import { join, relative } from 'path'
import { S3Client, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3'

// =============================================================================
// Configuration
// =============================================================================

const LOCAL_DIR = 'data-v3'
const DATASETS = ['imdb-1m', 'onet-full', 'unspsc-full']

// R2 configuration from environment
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY
const R2_BUCKET = process.env.R2_BUCKET || 'parquedb'

// =============================================================================
// Types
// =============================================================================

interface IndexFile {
  localPath: string
  r2Key: string
  size: number
}

interface UploadResult {
  skipped: boolean
  size: number
  error?: Error
}

interface DatasetUploadResult {
  uploaded: number
  skipped: number
  totalBytes: number
  errors: number
}

// =============================================================================
// R2 Client
// =============================================================================

function createR2Client(): S3Client {
  if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
    console.error('Missing R2 credentials. Set environment variables:')
    console.error('  R2_ACCOUNT_ID')
    console.error('  R2_ACCESS_KEY_ID')
    console.error('  R2_SECRET_ACCESS_KEY')
    process.exit(1)
  }

  return new S3Client({
    region: 'auto',
    endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: R2_ACCESS_KEY_ID,
      secretAccessKey: R2_SECRET_ACCESS_KEY,
    },
  })
}

// =============================================================================
// File Discovery
// =============================================================================

function findIndexFiles(dataset: string): IndexFile[] {
  const datasetDir = join(LOCAL_DIR, dataset)
  if (!existsSync(datasetDir)) {
    return []
  }

  const files: IndexFile[] = []

  function walkDir(dir: string, _baseR2Path: string): void {
    const entries = readdirSync(dir, { withFileTypes: true })
    for (const entry of entries) {
      const fullPath = join(dir, entry.name)
      if (entry.isDirectory()) {
        walkDir(fullPath, _baseR2Path)
      } else {
        const stat = statSync(fullPath)
        const r2Key = relative(LOCAL_DIR, fullPath)
        files.push({
          localPath: fullPath,
          r2Key,
          size: stat.size,
        })
      }
    }
  }

  // Walk through all collection directories looking for indexes subdirectory
  const entries = readdirSync(datasetDir, { withFileTypes: true })
  for (const entry of entries) {
    if (entry.isDirectory()) {
      const collectionDir = join(datasetDir, entry.name)
      const indexDir = join(collectionDir, 'indexes')
      if (existsSync(indexDir)) {
        walkDir(indexDir, `${dataset}/${entry.name}/indexes`)
      }
    }
  }

  return files
}

// =============================================================================
// Upload
// =============================================================================

async function uploadFile(client: S3Client, localPath: string, r2Key: string, dryRun: boolean): Promise<UploadResult> {
  const content = readFileSync(localPath)
  const size = content.length

  // Determine content type
  let contentType = 'application/octet-stream'
  if (r2Key.endsWith('.json')) {
    contentType = 'application/json'
  } else if (r2Key.endsWith('.idx')) {
    contentType = 'application/octet-stream'
  }

  if (dryRun) {
    console.log(`  [DRY RUN] Would upload: ${r2Key} (${formatSize(size)})`)
    return { skipped: false, size }
  }

  try {
    // Check if file already exists with same size
    const headCmd = new HeadObjectCommand({
      Bucket: R2_BUCKET,
      Key: r2Key,
    })

    try {
      const existing = await client.send(headCmd)
      if (existing.ContentLength === size) {
        console.log(`  [SKIP] Already exists with same size: ${r2Key}`)
        return { skipped: true, size }
      }
    } catch {
      // File doesn't exist, continue with upload
    }

    // Upload file
    const putCmd = new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: r2Key,
      Body: content,
      ContentType: contentType,
    })

    await client.send(putCmd)
    console.log(`  [OK] Uploaded: ${r2Key} (${formatSize(size)})`)
    return { skipped: false, size }
  } catch (err) {
    console.error(`  [ERROR] Failed to upload ${r2Key}: ${(err as Error).message}`)
    return { skipped: false, size: 0, error: err as Error }
  }
}

async function uploadDataset(client: S3Client, dataset: string, dryRun: boolean): Promise<DatasetUploadResult> {
  console.log(`\nUploading indexes for ${dataset}...`)

  const files = findIndexFiles(dataset)
  if (files.length === 0) {
    console.log(`  No index files found. Run build-indexes.mjs first.`)
    return { uploaded: 0, skipped: 0, totalBytes: 0, errors: 0 }
  }

  console.log(`  Found ${files.length} files to upload`)

  let uploaded = 0
  let skipped = 0
  let totalBytes = 0
  let errors = 0

  for (const file of files) {
    const result = await uploadFile(client, file.localPath, file.r2Key, dryRun)
    if (result.error) {
      errors++
    } else if (result.skipped) {
      skipped++
    } else {
      uploaded++
    }
    totalBytes += result.size
  }

  return { uploaded, skipped, totalBytes, errors }
}

// =============================================================================
// Utilities
// =============================================================================

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2)

  let dataset: string | null = null
  let uploadAll = false
  let dryRun = false

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--dataset' && args[i + 1]) {
      dataset = args[++i]
    } else if (args[i] === '--all') {
      uploadAll = true
    } else if (args[i] === '--dry-run') {
      dryRun = true
    }
  }

  if (!dataset && !uploadAll) {
    console.error('Usage: bun scripts/upload-indexes.ts --dataset <name> [--dry-run]')
    console.error('       bun scripts/upload-indexes.ts --all [--dry-run]')
    console.error('\nAvailable datasets:')
    for (const d of DATASETS) {
      console.error(`  ${d}`)
    }
    process.exit(1)
  }

  const datasetsToUpload = uploadAll ? DATASETS : [dataset!]
  const invalidDatasets = datasetsToUpload.filter(d => !DATASETS.includes(d))

  if (invalidDatasets.length > 0) {
    console.error(`Unknown dataset(s): ${invalidDatasets.join(', ')}`)
    process.exit(1)
  }

  // Create R2 client
  const client = createR2Client()

  console.log(`R2 Bucket: ${R2_BUCKET}`)
  if (dryRun) {
    console.log('Mode: DRY RUN (no actual uploads)')
  }

  // Upload each dataset
  let totalUploaded = 0
  let totalSkipped = 0
  let totalBytes = 0
  let totalErrors = 0

  for (const ds of datasetsToUpload) {
    const result = await uploadDataset(client, ds, dryRun)
    totalUploaded += result.uploaded
    totalSkipped += result.skipped
    totalBytes += result.totalBytes
    totalErrors += result.errors
  }

  // Summary
  console.log('\n' + '='.repeat(50))
  console.log('Upload Summary')
  console.log('='.repeat(50))
  console.log(`  Datasets:     ${datasetsToUpload.length}`)
  console.log(`  Uploaded:     ${totalUploaded} files`)
  console.log(`  Skipped:      ${totalSkipped} files (already exist)`)
  console.log(`  Total size:   ${formatSize(totalBytes)}`)
  if (totalErrors > 0) {
    console.log(`  Errors:       ${totalErrors}`)
  }

  if (dryRun) {
    console.log('\n[DRY RUN] No files were actually uploaded.')
  } else {
    console.log('\nDone!')
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
