// Test metadata parsing on a real parquet file
import { parquetMetadata, parquetSchema } from 'hyparquet'
import { readFileSync } from 'fs'

// Read a small parquet file
const filePath = '../../data/onet-optimized/data.parquet'
const buffer = readFileSync(filePath)
console.log(`File size: ${buffer.byteLength} bytes`)

// Parse metadata
const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
const metadata = parquetMetadata(arrayBuffer)

console.log('\n=== Metadata ===')
console.log(`Version: ${metadata.version}`)
console.log(`Num rows: ${metadata.num_rows}`)
console.log(`Num row groups: ${metadata.row_groups.length}`)

// Get schema
const schema = parquetSchema(metadata)
console.log('\n=== Schema ===')
console.log(`Root columns: ${schema.children?.length || 0}`)
if (schema.children) {
  for (const col of schema.children.slice(0, 10)) {
    const name = col.element?.name || 'unknown'
    const type = col.element?.type ?? 'group'
    console.log(`  - ${name}: ${type}`)
  }
  if (schema.children.length > 10) {
    console.log(`  ... and ${schema.children.length - 10} more`)
  }
}

// Check row group info
if (metadata.row_groups.length > 0) {
  const rg = metadata.row_groups[0]
  console.log('\n=== Row Group 0 ===')
  console.log(`Num rows: ${rg.num_rows}`)
  console.log(`Total byte size: ${rg.total_byte_size}`)
  console.log(`Columns: ${rg.columns.length}`)

  // Show first column stats if available
  if (rg.columns.length > 0 && rg.columns[0].meta_data) {
    const col0 = rg.columns[0].meta_data
    console.log('\n=== First Column ===')
    console.log(`Path: ${col0.path_in_schema?.join('.') || 'root'}`)
    console.log(`Type: ${col0.type}`)
    console.log(`Codec: ${col0.codec}`)
    console.log(`Num values: ${col0.num_values}`)
    if (col0.statistics) {
      console.log(`Has statistics: yes`)
    }
  }
}

console.log('\n=== Memory Usage ===')
const used = process.memoryUsage()
console.log(`Heap used: ${Math.round(used.heapUsed / 1024)}KB`)
console.log(`RSS: ${Math.round(used.rss / 1024)}KB`)

console.log('\n✓ Real file parsing works!')
