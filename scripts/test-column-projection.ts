import { parquetQuery, parquetMetadata } from 'hyparquet'
import * as fs from 'fs'

async function test(file: string) {
  const buffer = fs.readFileSync(file)
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
  const metadata = parquetMetadata(arrayBuffer)

  console.log(`\n=== ${file} ===`)
  console.log('Schema:', metadata.schema.map(s => s.name))
  console.log('Row groups:', metadata.row_groups.length)

  // Test without column projection
  const allCols = await parquetQuery({
    file: arrayBuffer,
    metadata,
    filter: { $id: 'occupations/11-1011.00' },
  }) as Record<string, unknown>[]
  console.log('Without projection:', allCols.length, 'rows')
  if (allCols[0]) {
    console.log('  Keys:', Object.keys(allCols[0]))
    console.log('  $data type:', typeof allCols[0].$data)
  }

  // Test with column projection
  const projected = await parquetQuery({
    file: arrayBuffer,
    metadata,
    filter: { $id: 'occupations/11-1011.00' },
    columns: ['$id', '$data'],
  }) as Record<string, unknown>[]
  console.log('With projection [$id, $data]:', projected.length, 'rows')
  if (projected[0]) {
    console.log('  Keys:', Object.keys(projected[0]))
    console.log('  $data type:', typeof projected[0].$data)
    console.log('  $data value:', JSON.stringify(projected[0].$data)?.slice(0, 100))
  }
}

async function main() {
  await test('data/onet-graph/data.parquet')
  await test('/tmp/r2-data.parquet')
  await test('data/onet-graph/rels.parquet')
}

main().catch(console.error)
