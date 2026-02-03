#!/usr/bin/env bun

import { parquetMetadataAsync } from 'hyparquet'
import { promises as fs } from 'node:fs'

interface ParquetFile {
  byteLength: number
  slice: (start: number, end: number) => ArrayBuffer
}

interface ParquetMetadata {
  num_rows: number | bigint
  row_groups: unknown[]
  schema: Array<{
    name: string
    type?: number
    converted_type?: string
  }>
}

async function showSchema(path: string): Promise<void> {
  try {
    const buffer = await fs.readFile(path)
    const file: ParquetFile = {
      byteLength: buffer.byteLength,
      slice: (s: number, e: number) => buffer.slice(s, e).buffer
    }
    const meta = await parquetMetadataAsync(file) as ParquetMetadata
    console.log('File:', path.split('/').pop())
    console.log('Rows:', Number(meta.num_rows).toLocaleString())
    console.log('Row Groups:', meta.row_groups.length)
    console.log('Columns:', meta.schema.slice(1).map(c => c.name).join(', '))
    console.log('---')
  } catch (e) {
    const error = e as Error
    console.log('Error reading', path, error.message)
  }
}

console.log('=== IMDB Schema ===')
await showSchema('./data/imdb/title.basics.parquet')
await showSchema('./data/imdb/name.basics.parquet')
await showSchema('./data/imdb/title.ratings.parquet')

console.log('\n=== O*NET Schema ===')
await showSchema('./data/onet/Occupation Data.parquet')
await showSchema('./data/onet/Skills.parquet')
await showSchema('./data/onet/Abilities.parquet')

console.log('\n=== O*NET Optimized ===')
await showSchema('./data/onet-optimized/data.parquet')
await showSchema('./data/onet-optimized/rels.parquet')
