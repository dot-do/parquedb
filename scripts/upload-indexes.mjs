#!/usr/bin/env node
/**
 * Upload Secondary Indexes to R2
 *
 * Uploads built indexes from local filesystem to Cloudflare R2 bucket.
 *
 * Usage:
 *   node scripts/upload-indexes.mjs --dataset imdb-1m
 *   node scripts/upload-indexes.mjs --dataset imdb-1m --dry-run
 *   node scripts/upload-indexes.mjs --all
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
// R2 Client
// =============================================================================

function createR2Client() {
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

function findIndexFiles(dataset) {
  const indexDir = join(LOCAL_DIR, dataset, 'indexes')
  if (!existsSync(indexDir)) {
    return []
  }

  const files = []

  function walkDir(dir) {
    const entries = readdirSync(dir, { withFileTypes: true })
    for (const entry of entries) {
      const fullPath = join(dir, entry.name)
      if (entry.isDirectory()) {
        walkDir(fullPath)
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

  walkDir(indexDir)
  return files
}

// =============================================================================
// Upload
// =============================================================================

async function uploadFile(client, localPath, r2Key, dryRun) {
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
    console.error(`  [ERROR] Failed to upload ${r2Key}: ${err.message}`)
    return { skipped: false, size: 0, error: err }
  }
}

async function uploadDataset(client, dataset, dryRun) {
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

function formatSize(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const args = process.argv.slice(2)

  let dataset = null
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
    console.error('Usage: node scripts/upload-indexes.mjs --dataset <name> [--dry-run]')
    console.error('       node scripts/upload-indexes.mjs --all [--dry-run]')
    console.error('\nAvailable datasets:')
    for (const d of DATASETS) {
      console.error(`  ${d}`)
    }
    process.exit(1)
  }

  const datasetsToUpload = uploadAll ? DATASETS : [dataset]
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
