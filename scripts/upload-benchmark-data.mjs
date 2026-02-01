#!/usr/bin/env node
/**
 * Upload benchmark data (data-v3) to R2 for real I/O performance testing
 *
 * Usage:
 *   CLOUDFLARE_ACCOUNT_ID=xxx node scripts/upload-benchmark-data.mjs
 *   CLOUDFLARE_ACCOUNT_ID=xxx node scripts/upload-benchmark-data.mjs --force
 *   CLOUDFLARE_ACCOUNT_ID=xxx node scripts/upload-benchmark-data.mjs --datasets imdb,onet
 *
 * This uploads all parquet files from data-v3/ to the parquedb R2 bucket
 * under the benchmark-data/ prefix.
 *
 * Available datasets:
 *   imdb        - 100K sample IMDB titles (~10MB)
 *   imdb-1m     - 1M IMDB titles (~88MB)
 *   onet        - Small O*NET sample (~92KB)
 *   onet-full   - Full O*NET with 73K occupation-skills (~8MB)
 *   unspsc      - Small UNSPSC sample (~24KB)
 *   unspsc-full - Full UNSPSC with 70K commodities (~2.2MB)
 */

import { execSync } from 'child_process'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'

const BUCKET = 'parquedb'
const DATA_DIR = './data-v3'
const R2_PREFIX = 'benchmark-data'

// Parse command line args
const args = process.argv.slice(2)
const forceUpload = args.includes('--force')
const datasetsArg = args.find(a => a.startsWith('--datasets='))
const selectedDatasets = datasetsArg ? datasetsArg.split('=')[1].split(',') : null

/**
 * Format bytes to human readable
 */
function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}

/**
 * Upload a file to R2 using wrangler CLI
 */
async function uploadFile(localPath, r2Path, size) {
  const sizeStr = formatBytes(size)
  console.log(`  Uploading: ${r2Path} (${sizeStr})`)
  try {
    execSync(
      `npx wrangler r2 object put ${BUCKET}/${r2Path} --file="${localPath}" --content-type="application/octet-stream"`,
      { stdio: 'pipe' }
    )
    return true
  } catch (error) {
    console.error(`    Failed: ${error.message}`)
    return false
  }
}

/**
 * Check if file exists in R2 and get its size
 */
async function getR2FileInfo(r2Path) {
  try {
    const output = execSync(`npx wrangler r2 object head ${BUCKET}/${r2Path}`, {
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    // Parse size from output (contentLength property)
    const sizeMatch = output.match(/contentLength[:\s]+(\d+)/i)
    return {
      exists: true,
      size: sizeMatch ? parseInt(sizeMatch[1]) : 0,
    }
  } catch {
    return { exists: false, size: 0 }
  }
}

/**
 * Upload dataset files to R2
 */
async function uploadDataset(datasetName, localDir) {
  console.log(`\n${'─'.repeat(50)}`)
  console.log(`Dataset: ${datasetName}`)
  console.log(`${'─'.repeat(50)}`)

  let files
  try {
    files = await fs.readdir(localDir)
  } catch {
    console.log(`  Skipping: directory not found`)
    return { uploaded: 0, skipped: 0, failed: 0, bytesUploaded: 0 }
  }

  let uploaded = 0
  let skipped = 0
  let failed = 0
  let bytesUploaded = 0

  for (const file of files) {
    if (!file.endsWith('.parquet')) continue

    const localPath = join(localDir, file)
    const stat = await fs.stat(localPath)
    if (!stat.isFile()) continue

    // R2 path: benchmark-data/{dataset}/{filename}
    const r2Path = `${R2_PREFIX}/${datasetName}/${file}`

    // Check if file already exists with same size
    if (!forceUpload) {
      const r2Info = await getR2FileInfo(r2Path)
      if (r2Info.exists && r2Info.size === stat.size) {
        console.log(`  Exists: ${r2Path} (${formatBytes(stat.size)})`)
        skipped++
        continue
      }
    }

    if (await uploadFile(localPath, r2Path, stat.size)) {
      uploaded++
      bytesUploaded += stat.size
    } else {
      failed++
    }
  }

  console.log(`  Summary: ${uploaded} uploaded, ${skipped} exists, ${failed} failed`)
  return { uploaded, skipped, failed, bytesUploaded }
}

/**
 * List all datasets in data-v3
 */
async function listDatasets() {
  const entries = await fs.readdir(DATA_DIR, { withFileTypes: true })
  return entries.filter((e) => e.isDirectory()).map((e) => e.name)
}

/**
 * Get total size of a dataset
 */
async function getDatasetSize(localDir) {
  let total = 0
  try {
    const files = await fs.readdir(localDir)
    for (const file of files) {
      if (!file.endsWith('.parquet')) continue
      const stat = await fs.stat(join(localDir, file))
      total += stat.size
    }
  } catch {
    // ignore
  }
  return total
}

/**
 * Main function
 */
async function main() {
  console.log('═'.repeat(60))
  console.log('Upload Benchmark Data to R2')
  console.log('═'.repeat(60))
  console.log()
  console.log(`Source: ${DATA_DIR}`)
  console.log(`Bucket: ${BUCKET}`)
  console.log(`Prefix: ${R2_PREFIX}`)
  if (forceUpload) console.log('Mode: Force re-upload')
  if (selectedDatasets) console.log(`Selected: ${selectedDatasets.join(', ')}`)

  // Check if data-v3 exists
  try {
    await fs.access(DATA_DIR)
  } catch {
    console.error(`\nError: ${DATA_DIR} directory not found`)
    console.error('Run "node scripts/reload-datasets.mjs" to generate benchmark data')
    process.exit(1)
  }

  // Get all datasets
  let datasets = await listDatasets()

  // Filter by selection
  if (selectedDatasets) {
    datasets = datasets.filter(d => selectedDatasets.includes(d))
  }

  // Show dataset sizes
  console.log('\nDatasets to upload:')
  let totalSize = 0
  for (const dataset of datasets) {
    const size = await getDatasetSize(join(DATA_DIR, dataset))
    totalSize += size
    console.log(`  ${dataset.padEnd(15)} ${formatBytes(size)}`)
  }
  console.log(`  ${'─'.repeat(25)}`)
  console.log(`  ${'Total'.padEnd(15)} ${formatBytes(totalSize)}`)

  let totalUploaded = 0
  let totalSkipped = 0
  let totalFailed = 0
  let totalBytesUploaded = 0

  for (const dataset of datasets) {
    const result = await uploadDataset(dataset, join(DATA_DIR, dataset))
    totalUploaded += result.uploaded
    totalSkipped += result.skipped
    totalFailed += result.failed
    totalBytesUploaded += result.bytesUploaded
  }

  // Summary
  console.log('\n' + '═'.repeat(60))
  console.log('Upload Complete')
  console.log('═'.repeat(60))
  console.log()
  console.log(`  Files uploaded:  ${totalUploaded}`)
  console.log(`  Files skipped:   ${totalSkipped}`)
  console.log(`  Files failed:    ${totalFailed}`)
  console.log(`  Bytes uploaded:  ${formatBytes(totalBytesUploaded)}`)

  // List all files in R2 bucket
  console.log('\n' + '─'.repeat(40))
  console.log('Files in R2 bucket:')
  try {
    const output = execSync(`npx wrangler r2 object list ${BUCKET} --prefix=${R2_PREFIX}/`, {
      encoding: 'utf-8',
    })
    const lines = output.trim().split('\n')
    for (const line of lines) {
      if (line.includes('.parquet')) {
        console.log(`  ${line.trim()}`)
      }
    }
  } catch {
    console.log('  (unable to list)')
  }

  console.log('\n' + '─'.repeat(40))
  console.log('Test the benchmark endpoint:')
  console.log('  curl https://parquedb.workers.do/benchmark-datasets')
  console.log('')
  console.log('With specific datasets:')
  console.log('  curl "https://parquedb.workers.do/benchmark-datasets?iterations=5"')
}

main().catch((error) => {
  console.error('Error:', error)
  process.exit(1)
})
