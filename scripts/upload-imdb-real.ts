#!/usr/bin/env npx tsx
/**
 * Transform and upload real IMDB data to R2
 *
 * Reads the title.basics.parquet file and transforms it to ParqueDB format,
 * then uploads to R2 bucket.
 */

import { parquetQuery } from 'hyparquet'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { promises as fs } from 'node:fs'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'

const INPUT_FILE = 'data-v3/imdb-1m/title.basics.parquet'
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || process.env.CLOUDFLARE_ACCOUNT_ID
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY
const R2_BUCKET = 'parquedb'

async function main() {
  console.log('Reading IMDB title.basics.parquet...')

  const buffer = await fs.readFile(INPUT_FILE)
  const file = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end: number) => buffer.slice(start, end).buffer
  }

  // Read all rows
  const rows = await parquetQuery({ file }) as Record<string, unknown>[]
  console.log(`Read ${rows.length.toLocaleString()} rows`)

  // Sample the first row to see structure
  console.log('Sample row:', JSON.stringify(rows[0], null, 2))

  // Transform to ParqueDB format
  console.log('Transforming to ParqueDB format...')
  const transformed = rows.map((row, i) => {
    const tconst = row.tconst as string
    return {
      $id: `title:${tconst}`,
      $type: 'Title',
      name: row.primaryTitle || row.originalTitle || `Title ${tconst}`,
      $index_titleType: row.titleType,
      $index_startYear: row.startYear ? parseInt(String(row.startYear)) : null,
      $index_endYear: row.endYear ? parseInt(String(row.endYear)) : null,
      $index_runtimeMinutes: row.runtimeMinutes ? parseInt(String(row.runtimeMinutes)) : null,
      $index_isAdult: row.isAdult === '1' || row.isAdult === 1,
      genres: row.genres,
      originalTitle: row.originalTitle,
    }
  })

  console.log('Sample transformed:', JSON.stringify(transformed[0], null, 2))

  // Write as parquet
  console.log('Writing transformed parquet...')
  const parquetBuffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', type: 'STRING', data: transformed.map(r => r.$id) },
      { name: '$type', type: 'STRING', data: transformed.map(r => r.$type) },
      { name: 'name', type: 'STRING', data: transformed.map(r => r.name) },
      { name: '$index_titleType', type: 'STRING', data: transformed.map(r => r.$index_titleType) },
      { name: '$index_startYear', type: 'INT32', data: transformed.map(r => r.$index_startYear) },
      { name: '$index_endYear', type: 'INT32', data: transformed.map(r => r.$index_endYear) },
      { name: '$index_runtimeMinutes', type: 'INT32', data: transformed.map(r => r.$index_runtimeMinutes) },
      { name: '$index_isAdult', type: 'BOOLEAN', data: transformed.map(r => r.$index_isAdult) },
      { name: 'genres', type: 'STRING', data: transformed.map(r => r.genres) },
      { name: 'originalTitle', type: 'STRING', data: transformed.map(r => r.originalTitle) },
    ],
    rowGroupSize: 50000,
  })

  console.log(`Parquet buffer size: ${(parquetBuffer.byteLength / 1024 / 1024).toFixed(2)} MB`)

  // Upload to R2
  if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
    console.log('R2 credentials not set. Saving locally instead.')
    await fs.writeFile('data-v3/imdb-1m/titles.parquet', Buffer.from(parquetBuffer))
    console.log('Saved to data-v3/imdb-1m/titles.parquet')
    return
  }

  console.log('Uploading to R2...')
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
    Body: Buffer.from(parquetBuffer),
    ContentType: 'application/vnd.apache.parquet',
  }))

  console.log('Uploaded to R2: imdb/titles.parquet')
  console.log('Done!')
}

main().catch(console.error)
