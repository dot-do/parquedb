import { readFileSync } from 'fs';
import { parquetRead } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

async function testRead() {
  const path = 'data-v3/imdb-1m/titles.parquet';
  console.log('Reading', path);
  const nodeBuffer = readFileSync(path);
  const buffer = nodeBuffer.buffer.slice(nodeBuffer.byteOffset, nodeBuffer.byteOffset + nodeBuffer.byteLength);
  const rows = [];

  await parquetRead({
    rowFormat: 'object',
    file: buffer,
    compressors,
    onComplete: (data) => {
      // Use concat instead of spread to avoid stack overflow
      for (let i = 0; i < data.length; i += 10000) {
        const chunk = data.slice(i, i + 10000);
        rows.push(...chunk);
      }
    },
  });

  console.log('Loaded', rows.length, 'rows');
  console.log('Sample row:', JSON.stringify(rows[0], null, 2).substring(0, 500));
}

testRead().catch(console.error);
