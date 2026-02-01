import { parquetMetadataAsync } from 'hyparquet';
import { promises as fs } from 'node:fs';

async function showSchema(path) {
  try {
    const buffer = await fs.readFile(path);
    const file = {
      byteLength: buffer.byteLength,
      slice: (s, e) => buffer.slice(s, e).buffer
    };
    const meta = await parquetMetadataAsync(file);
    console.log('File:', path.split('/').pop());
    console.log('Rows:', meta.num_rows.toLocaleString());
    console.log('Row Groups:', meta.row_groups.length);
    console.log('Columns:', meta.schema.slice(1).map(c => c.name).join(', '));
    console.log('---');
  } catch (e) {
    console.log('Error reading', path, e.message);
  }
}

console.log('=== IMDB Schema ===');
await showSchema('./data/imdb/title.basics.parquet');
await showSchema('./data/imdb/name.basics.parquet');
await showSchema('./data/imdb/title.ratings.parquet');

console.log('\n=== O*NET Schema ===');
await showSchema('./data/onet/Occupation Data.parquet');
await showSchema('./data/onet/Skills.parquet');
await showSchema('./data/onet/Abilities.parquet');

console.log('\n=== O*NET Optimized ===');
await showSchema('./data/onet-optimized/data.parquet');
await showSchema('./data/onet-optimized/rels.parquet');
