#!/usr/bin/env bun
/**
 * Upload benchmark datasets (data + indexes) to R2
 */
import { readFileSync, existsSync, readdirSync, statSync } from 'fs'
import { join } from 'path'
import { S3Client, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3'

const LOCAL_DIR = 'data-v3'
const DATASETS = ['onet-full', 'unspsc-full', 'imdb-1m']

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY
const R2_BUCKET = process.env.R2_BUCKET || 'parquedb'

interface FileEntry {
  localPath: string
  r2Key: string
  size: number
}

const client = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID!,
    secretAccessKey: R2_SECRET_ACCESS_KEY!,
  },
})

function findAllFiles(dir: string, basePath: string = ''): FileEntry[] {
  const files: FileEntry[] = []
  const entries = readdirSync(dir, { withFileTypes: true })
  for (const entry of entries) {
    const fullPath = join(dir, entry.name)
    const r2Key = basePath ? `${basePath}/${entry.name}` : entry.name
    if (entry.isDirectory()) {
      files.push(...findAllFiles(fullPath, r2Key))
    } else {
      files.push({ localPath: fullPath, r2Key, size: statSync(fullPath).size })
    }
  }
  return files
}

async function uploadFile(file: FileEntry, dryRun: boolean): Promise<void> {
  const content = readFileSync(file.localPath)
  let contentType = 'application/octet-stream'
  if (file.r2Key.endsWith('.json')) contentType = 'application/json'
  else if (file.r2Key.endsWith('.parquet')) contentType = 'application/vnd.apache.parquet'

  if (dryRun) {
    console.log(`[DRY] ${file.r2Key} (${(file.size/1024/1024).toFixed(2)} MB)`)
    return
  }

  try {
    const head = await client.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: file.r2Key }))
    if (head.ContentLength === file.size) {
      console.log(`[SKIP] ${file.r2Key}`)
      return
    }
  } catch {
    // File doesn't exist, continue with upload
  }

  await client.send(new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: file.r2Key,
    Body: content,
    ContentType: contentType,
  }))
  console.log(`[OK] ${file.r2Key} (${(file.size/1024/1024).toFixed(2)} MB)`)
}

const dryRun = process.argv.includes('--dry-run')
const specificDataset = process.argv.find(a => !a.startsWith('-') && DATASETS.includes(a))
const datasets = specificDataset ? [specificDataset] : DATASETS

async function main(): Promise<void> {
  for (const dataset of datasets) {
    const dir = join(LOCAL_DIR, dataset)
    if (!existsSync(dir)) {
      console.log(`[SKIP] ${dataset} not found`)
      continue
    }
    console.log(`\nUploading ${dataset}...`)
    const files = findAllFiles(dir, dataset)
    for (const file of files) {
      await uploadFile(file, dryRun)
    }
  }
  console.log('\nDone!')
}

main().catch(console.error)
