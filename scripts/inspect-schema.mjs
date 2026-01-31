import { parquetMetadataAsync, parquetRead } from 'hyparquet'
import { readFileSync } from 'fs'
import { compressors } from 'hyparquet-compressors'

const filePath = process.argv[2]
if (!filePath) {
  console.error('Usage: node inspect-schema.mjs <parquet-file>')
  process.exit(1)
}

console.log(`Inspecting: ${filePath}\n`)

const buffer = readFileSync(filePath)
const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)

const metadata = await parquetMetadataAsync(arrayBuffer)

console.log('=== SCHEMA ===')
console.log(`Row count: ${metadata.num_rows}`)
console.log(`Row groups: ${metadata.row_groups.length}\n`)

console.log('Columns:')
for (const col of metadata.schema) {
  if (col.name === 'schema') continue // skip root
  const type = col.type !== undefined ? col.type : (col.converted_type || 'GROUP')
  const repetition = col.repetition_type !== undefined ? ['REQUIRED', 'OPTIONAL', 'REPEATED'][col.repetition_type] : ''
  console.log(`  - ${col.name}: ${type} ${repetition}`)
}

console.log('\n=== SAMPLE ROWS (first 2) ===')
await parquetRead({
  file: arrayBuffer,
  compressors,
  rowEnd: 2,
  onComplete: (rows) => {
    for (let i = 0; i < rows.length; i++) {
      console.log(`\nRow ${i}:`)
      console.log(JSON.stringify(rows[i], null, 2))
    }
  }
})
