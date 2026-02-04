#!/usr/bin/env npx tsx
/**
 * ETL: Transform IMDB title.basics.tsv to ParqueDB format
 *
 * Reads the real IMDB TSV file, transforms to ParqueDB format,
 * and uploads to R2.
 */

import { createReadStream, createWriteStream, existsSync } from 'node:fs'
import { promises as fs } from 'node:fs'
import { createGunzip } from 'node:zlib'
import { pipeline } from 'node:stream/promises'
import { createInterface } from 'node:readline'
import { join } from 'node:path'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'

const CACHE_DIR = 'data-v3/imdb-1m/.cache'
const OUTPUT_DIR = 'data-v3/imdb-real'
const LIMIT = 100000  // Limit rows for initial test

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || process.env.CLOUDFLARE_ACCOUNT_ID
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY
const R2_BUCKET = 'parquedb'

interface Title {
  $id: string
  $type: string
  name: string
  $index_titleType: string | null
  $index_startYear: number | null
  $index_endYear: number | null
  $index_runtimeMinutes: number | null
  $index_isAdult: boolean
  genres: string | null
  originalTitle: string | null
}

async function downloadIfNeeded() {
  const gzPath = join(CACHE_DIR, 'title.basics.tsv.gz')
  const tsvPath = join(CACHE_DIR, 'title.basics.tsv')

  if (!existsSync(gzPath)) {
    console.log('Downloading title.basics.tsv.gz...')
    const url = 'https://datasets.imdbws.com/title.basics.tsv.gz'
    const res = await fetch(url)
    if (!res.ok) throw new Error(`Download failed: ${res.status}`)

    await fs.mkdir(CACHE_DIR, { recursive: true })
    const data = await res.arrayBuffer()
    await fs.writeFile(gzPath, Buffer.from(data))
    console.log(`Downloaded ${(data.byteLength / 1024 / 1024).toFixed(1)} MB`)
  }

  if (!existsSync(tsvPath)) {
    console.log('Decompressing...')
    const gunzip = createGunzip()
    const source = createReadStream(gzPath)
    const dest = createWriteStream(tsvPath)
    await pipeline(source, gunzip, dest)
    console.log('Decompressed')
  }

  return tsvPath
}

async function* parseTsv(path: string): AsyncGenerator<Record<string, string>> {
  const stream = createReadStream(path)
  const rl = createInterface({ input: stream, crlfDelay: Infinity })

  let headers: string[] = []
  let isFirst = true

  for await (const line of rl) {
    if (isFirst) {
      headers = line.split('\t')
      isFirst = false
      continue
    }

    const values = line.split('\t')
    const row: Record<string, string> = {}
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]!] = values[i] ?? ''
    }
    yield row
  }
}

async function main() {
  console.log('=== IMDB Title ETL ===\n')

  const tsvPath = await downloadIfNeeded()
  console.log(`\nParsing ${tsvPath}...`)

  const titles: Title[] = []
  let count = 0

  for await (const row of parseTsv(tsvPath)) {
    if (count >= LIMIT) break

    // Skip if no valid tconst
    if (!row.tconst || row.tconst === '\\N') continue

    const title: Title = {
      $id: `title:${row.tconst}`,
      $type: 'Title',
      name: row.primaryTitle !== '\\N' ? row.primaryTitle : row.originalTitle !== '\\N' ? row.originalTitle : `Title ${row.tconst}`,
      $index_titleType: row.titleType !== '\\N' ? row.titleType : null,
      $index_startYear: row.startYear !== '\\N' ? parseInt(row.startYear) : null,
      $index_endYear: row.endYear !== '\\N' ? parseInt(row.endYear) : null,
      $index_runtimeMinutes: row.runtimeMinutes !== '\\N' ? parseInt(row.runtimeMinutes) : null,
      $index_isAdult: row.isAdult === '1',
      genres: row.genres !== '\\N' ? row.genres : null,
      originalTitle: row.originalTitle !== '\\N' ? row.originalTitle : null,
    }

    titles.push(title)
    count++

    if (count % 10000 === 0) {
      console.log(`  Processed ${count.toLocaleString()} titles...`)
    }
  }

  console.log(`\nTransformed ${titles.length.toLocaleString()} titles`)
  console.log('Sample:', JSON.stringify(titles[0], null, 2))

  // Write parquet in variant format:
  // - $id column (separate for pushdown filtering)
  // - $index_* columns (separate for pushdown filtering)
  // - data column (JSON blob with full entity)
  console.log('\nWriting parquet (variant format)...')
  const buffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: titles.map(t => t.$id) },
      // Indexed columns for predicate pushdown
      { name: '$index_titleType', type: 'STRING', data: titles.map(t => t.$index_titleType) },
      { name: '$index_startYear', type: 'INT32', data: titles.map(t => t.$index_startYear) },
      { name: '$index_endYear', type: 'INT32', data: titles.map(t => t.$index_endYear) },
      { name: '$index_runtimeMinutes', type: 'INT32', data: titles.map(t => t.$index_runtimeMinutes) },
      { name: '$index_isAdult', type: 'BOOLEAN', data: titles.map(t => t.$index_isAdult) },
      // Full entity as JSON blob
      { name: 'data', type: 'STRING', data: titles.map(t => JSON.stringify(t)) },
    ],
    rowGroupSize: 50000,
  })

  await fs.mkdir(OUTPUT_DIR, { recursive: true })
  await fs.writeFile(join(OUTPUT_DIR, 'titles.parquet'), Buffer.from(buffer))
  console.log(`Wrote ${(buffer.byteLength / 1024 / 1024).toFixed(2)} MB`)

  // Upload to R2
  if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
    console.log('\nR2 credentials not set. Using wrangler to upload...')
    const { execSync } = await import('node:child_process')
    try {
      execSync(`wrangler r2 object put parquedb/imdb/titles.parquet --file=${join(OUTPUT_DIR, 'titles.parquet')} --content-type=application/vnd.apache.parquet`, {
        stdio: 'inherit'
      })
      console.log('\nUploaded to R2!')
    } catch (e) {
      console.log('\nManual upload needed:')
      console.log(`  wrangler r2 object put parquedb/imdb/titles.parquet --file=${join(OUTPUT_DIR, 'titles.parquet')}`)
    }
    return
  }

  console.log('\nUploading to R2...')
  const client = new S3Client({
    region: 'auto',
    endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: R2_ACCESS_KEY_ID,
      secretAccessKey: R2_SECRET_ACCESS_KEY,
    },
  })

  await client.send(new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: 'imdb/titles.parquet',
    Body: Buffer.from(buffer),
    ContentType: 'application/vnd.apache.parquet',
  }))

  console.log('Uploaded to R2: imdb/titles.parquet')
  console.log('\nDone!')
}

main().catch(console.error)
