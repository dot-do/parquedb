import { parquetMetadata } from 'hyparquet'
import * as fs from 'fs'

const file = process.argv[2] || 'data/onet-graph/data.parquet'
const buffer = fs.readFileSync(file)
const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
const metadata = parquetMetadata(arrayBuffer)

console.log(`File: ${file}`)
console.log(`Row groups: ${metadata.row_groups.length}`)
console.log()

for (let i = 0; i < metadata.row_groups.length; i++) {
  const rg = metadata.row_groups[i]
  console.log(`Row Group ${i}: ${rg.num_rows} rows`)

  // Find $id column stats
  const idCol = rg.columns.find(c => {
    const path = c.meta_data?.path_in_schema
    return path && path[path.length - 1] === '$id'
  })

  if (idCol?.meta_data?.statistics) {
    const stats = idCol.meta_data.statistics
    console.log(`  $id min: ${stats.min_value}`)
    console.log(`  $id max: ${stats.max_value}`)
  }
}
