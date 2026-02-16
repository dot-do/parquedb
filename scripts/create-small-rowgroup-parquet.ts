/**
 * Create a test parquet file with small row groups (10K rows)
 * to test 5ms CPU constraint feasibility
 */

import { parquetWriteBuffer } from 'hyparquet-writer'
import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { compressors, writeCompressors } from '../src/parquet/compression'
import { readFile, writeFile, mkdir } from 'node:fs/promises'

async function main() {
  console.log('Creating test file with small row groups...\n')

  // Load source data
  const buffer = await readFile('data/tpch/lineitem.parquet')
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  // Read first 50K rows
  const metadata = parquetMetadata(arrayBuffer)
  console.log(`Source: ${Number(metadata.num_rows).toLocaleString()} rows, ${metadata.row_groups.length} row groups`)

  const rows = await parquetReadObjects({
    file: arrayBuffer,
    compressors,
    columns: ['l_orderkey', 'l_partkey', 'l_quantity', 'l_extendedprice', 'l_discount'],
    rowEnd: 50000,
  }) as any[]

  console.log(`Loaded ${rows.length.toLocaleString()} rows`)

  // Sort by l_orderkey for good statistics (handle BigInt)
  rows.sort((a, b) => {
    const aKey = BigInt(a.l_orderkey)
    const bKey = BigInt(b.l_orderkey)
    if (aKey < bKey) return -1
    if (aKey > bKey) return 1
    return 0
  })

  // Convert to columnData format: array of { name, data }
  const columnData = [
    { name: 'l_orderkey', data: rows.map(r => Number(r.l_orderkey)) },
    { name: 'l_partkey', data: rows.map(r => Number(r.l_partkey)) },
    { name: 'l_quantity', data: rows.map(r => r.l_quantity) },
    { name: 'l_extendedprice', data: rows.map(r => r.l_extendedprice) },
    { name: 'l_discount', data: rows.map(r => r.l_discount) },
  ]

  await mkdir('data/test', { recursive: true })

  // Write with small row groups (10K rows each)
  const rowGroupSize = 10000

  const outputBuffer = parquetWriteBuffer({
    columnData,
    statistics: true,
    rowGroupSize,
    codec: 'SNAPPY',
    compressors: writeCompressors,
  })

  await writeFile('data/test/small-rowgroups.parquet', Buffer.from(outputBuffer))
  console.log(`\nWrote data/test/small-rowgroups.parquet`)

  // Verify
  const verifyBuffer = await readFile('data/test/small-rowgroups.parquet')
  const verifyArrayBuffer = verifyBuffer.buffer.slice(verifyBuffer.byteOffset, verifyBuffer.byteOffset + verifyBuffer.byteLength)
  const verifyMeta = parquetMetadata(verifyArrayBuffer)

  console.log(`\nResult:`)
  console.log(`  Rows: ${Number(verifyMeta.num_rows).toLocaleString()}`)
  console.log(`  Row groups: ${verifyMeta.row_groups.length}`)
  console.log(`  Rows per group: ~${Math.round(Number(verifyMeta.num_rows) / verifyMeta.row_groups.length).toLocaleString()}`)
  console.log(`  File size: ${(verifyBuffer.length / 1024).toFixed(1)}KB`)

  // Check statistics
  console.log(`\nRow group statistics (l_orderkey):`)
  for (let i = 0; i < Math.min(5, verifyMeta.row_groups.length); i++) {
    const rg = verifyMeta.row_groups[i]
    const col = rg.columns.find(c => c.meta_data?.path_in_schema?.includes('l_orderkey'))
    if (col?.meta_data?.statistics) {
      const stats = col.meta_data.statistics
      console.log(`  RG${i}: min=${stats.min_value}, max=${stats.max_value}, rows=${Number(rg.num_rows)}`)
    } else {
      console.log(`  RG${i}: no statistics, rows=${Number(rg.num_rows)}`)
    }
  }
}

main().catch(console.error)
