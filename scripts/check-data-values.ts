import { parquetReadObjects, parquetMetadataAsync } from 'hyparquet'
import { compressors } from '../src/parquet/compression'
import { readFile } from 'node:fs/promises'

async function main() {
  const buffer = await readFile('data/tpch/lineitem.parquet')
  const ab = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

  const metadata = await parquetMetadataAsync(ab)

  // Check statistics format
  console.log('=== Statistics Format ===')
  const rg0 = metadata.row_groups[0]
  const qtyCol = rg0.columns.find(c => c.meta_data?.path_in_schema?.includes('l_quantity'))
  console.log('l_quantity column stats:', qtyCol?.meta_data?.statistics)

  // Check actual data
  console.log('\n=== First 10 l_quantity values ===')
  const rows = await parquetReadObjects({
    file: ab,
    compressors,
    columns: ['l_quantity'],
    rowEnd: 10,
  }) as any[]

  rows.forEach((r, i) => console.log(i, r.l_quantity, typeof r.l_quantity))

  // Count distribution (avoid stack overflow)
  console.log('\n=== Distribution (first 10K rows) ===')
  const sampleRows = await parquetReadObjects({
    file: ab,
    compressors,
    columns: ['l_quantity'],
    rowEnd: 10000,
  }) as any[]

  let under5 = 0, under10 = 0, min = Infinity, max = -Infinity
  for (const r of sampleRows) {
    if (r.l_quantity < 5) under5++
    if (r.l_quantity < 10) under10++
    if (r.l_quantity < min) min = r.l_quantity
    if (r.l_quantity > max) max = r.l_quantity
  }

  console.log('Sample rows:', sampleRows.length)
  console.log('Actual min value:', min)
  console.log('Actual max value:', max)
  console.log('Rows < 5:', under5)
  console.log('Rows < 10:', under10)

  console.log('\n=== Scale mismatch ===')
  console.log('Stats min (100n) / 100 =', 100 / 100)
  console.log('Stats max (5000n) / 100 =', 5000 / 100)
  console.log('This suggests l_quantity is DECIMAL(15,2) stored as scaled integers')
}

main().catch(console.error)
