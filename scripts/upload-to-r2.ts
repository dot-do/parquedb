#!/usr/bin/env npx tsx
/**
 * Upload local Parquet files to R2 for testing
 */

import * as fs from 'fs'
import * as path from 'path'
import { execSync } from 'child_process'

const BUCKET = 'parquedb'

async function uploadFile(localPath: string, r2Path: string) {
  console.log(`  Uploading: ${r2Path}`)
  try {
    execSync(`npx wrangler r2 object put ${BUCKET}/${r2Path} --file="${localPath}" --content-type="application/octet-stream"`, {
      stdio: 'pipe',
    })
    return true
  } catch (error) {
    console.error(`    Failed: ${(error as Error).message}`)
    return false
  }
}

async function uploadDataset(datasetName: string, localDir: string) {
  console.log(`\nUploading ${datasetName}...`)

  const files = fs.readdirSync(localDir, { recursive: true }) as string[]
  let uploaded = 0
  let failed = 0

  for (const file of files) {
    const fullPath = path.join(localDir, file)
    if (!fs.statSync(fullPath).isFile()) continue
    if (!file.endsWith('.parquet')) continue

    // Convert to R2 path: data/{dataset}/{collection}/data.parquet
    // For O*NET: "Skills.parquet" -> "data/onet/skills/data.parquet"
    const baseName = path.basename(file, '.parquet')
    const collection = baseName.toLowerCase().replace(/\s+/g, '-')
    const r2Path = `data/${datasetName}/${collection}/data.parquet`

    if (await uploadFile(fullPath, r2Path)) {
      uploaded++
    } else {
      failed++
    }
  }

  console.log(`  Done: ${uploaded} uploaded, ${failed} failed`)
  return { uploaded, failed }
}

async function main() {
  console.log('Uploading data to R2 bucket: ' + BUCKET)

  // Upload O*NET (most useful for testing)
  await uploadDataset('onet', 'data/onet')

  // Upload wikidata sample
  const wikidataDataDir = 'data/wikidata/data'
  if (fs.existsSync(wikidataDataDir)) {
    const collections = fs.readdirSync(wikidataDataDir)
    for (const collection of collections) {
      const parquetPath = path.join(wikidataDataDir, collection, 'data.parquet')
      if (fs.existsSync(parquetPath)) {
        await uploadFile(parquetPath, `data/wikidata/${collection}/data.parquet`)
      }
    }
  }

  // Upload wiktionary sample
  const wiktionaryDataDir = 'data/wiktionary/data'
  if (fs.existsSync(wiktionaryDataDir)) {
    const collections = fs.readdirSync(wiktionaryDataDir)
    for (const collection of collections) {
      const parquetPath = path.join(wiktionaryDataDir, collection, 'data.parquet')
      if (fs.existsSync(parquetPath)) {
        await uploadFile(parquetPath, `data/wiktionary/${collection}/data.parquet`)
      }
    }
  }

  console.log('\nUpload complete!')
}

main().catch(console.error)
