import { FsBackend } from '../src/storage/FsBackend';
import { ParquetReader } from '../src/parquet/reader';

async function main() {
  const storage = new FsBackend(process.cwd());
  const reader = new ParquetReader({ storage });
  const rows = await reader.read('data/onet-graph/data.parquet');
  console.log('Total rows:', rows.length);
  console.log('Columns:', Object.keys(rows[0]!));
  console.log('Sample:', JSON.stringify(rows[0], null, 2));
}
main().catch(console.error);
