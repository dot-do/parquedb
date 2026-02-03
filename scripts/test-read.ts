import { readFileSync } from 'fs'
import { parquetRead } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

interface Row {
  [key: string]: unknown
}

async function testRead(): Promise<void> {
  const path = 'data-v3/imdb-1m/titles.parquet'
  console.log('Reading', path)
  const nodeBuffer = readFileSync(path)
  const buffer = nodeBuffer.buffer.slice(nodeBuffer.byteOffset, nodeBuffer.byteOffset + nodeBuffer.byteLength)
  const rows: Row[] = []

  await parquetRead({
    rowFormat: 'object',
    file: buffer,
    compressors,
    onComplete: (data: Row[]) => {
      // Use concat instead of spread to avoid stack overflow
      for (let i = 0; i < data.length; i += 10000) {
        const chunk = data.slice(i, i + 10000)
        rows.push(...chunk)
      }
    },
  })

  console.log('Loaded', rows.length, 'rows')
  console.log('Sample row:', JSON.stringify(rows[0], null, 2).substring(0, 500))
}

testRead().catch(console.error)
