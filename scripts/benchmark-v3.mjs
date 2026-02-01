/**
 * Benchmark Dual Variant Architecture (data-v3)
 *
 * Tests performance of $id | $index_* | $data format with:
 * - Row-group statistics pushdown
 * - Column projection
 * - Various query patterns
 */

import { parquetQuery, parquetMetadataAsync } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';

const DATA_DIR = './data-v3';

// =============================================================================
// Utilities
// =============================================================================

async function loadFile(path) {
  const buffer = await fs.readFile(join(DATA_DIR, path));
  // Convert Node.js Buffer to proper ArrayBuffer
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
  return {
    path,
    buffer,
    file: {
      byteLength: arrayBuffer.byteLength,
      slice: (s, e) => arrayBuffer.slice(s, e)
    }
  };
}

function median(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

async function benchmark(name, fn, iterations = 5) {
  const times = [];
  let result;
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    result = await fn();
    times.push(performance.now() - start);
  }
  const med = median(times);
  console.log(`  ${name.padEnd(45)} ${med.toFixed(1).padStart(8)}ms  ${(result?.length || 0).toLocaleString().padStart(8)} rows`);
  return { name, median: med, rows: result?.length || 0 };
}

async function getMetadata(path) {
  const { file } = await loadFile(path);
  return await parquetMetadataAsync(file);
}

// =============================================================================
// IMDB Benchmarks
// =============================================================================

async function benchmarkIMDB() {
  console.log('\n' + '═'.repeat(70));
  console.log('IMDB Benchmarks (100K titles, 50K people, 200K cast)');
  console.log('═'.repeat(70) + '\n');

  const titles = await loadFile('imdb/titles.parquet');
  const people = await loadFile('imdb/people.parquet');
  const cast = await loadFile('imdb/cast.parquet');

  const titlesMeta = await parquetMetadataAsync(titles.file);
  console.log(`Titles: ${titlesMeta.num_rows.toLocaleString()} rows, ${titlesMeta.row_groups.length} row groups`);
  console.log(`Columns: ${titlesMeta.schema.slice(1).map(c => c.name).join(', ')}\n`);

  const results = [];

  // Q1: Type filter (should use row-group stats since sorted by titleType)
  results.push(await benchmark('Filter by titleType="movie"', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: { '$index_titleType': 'movie' },
      compressors
    });
  }));

  // Q2: Year range
  results.push(await benchmark('Filter by startYear 2000-2020', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: { '$index_startYear': { $gte: 2000, $lte: 2020 } },
      compressors
    });
  }));

  // Q3: Compound filter
  results.push(await benchmark('Compound: movie + year >= 2010', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: { '$index_titleType': 'movie', '$index_startYear': { $gte: 2010 } },
      compressors
    });
  }));

  // Q4: High rating
  results.push(await benchmark('Filter rating >= 8.0', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: { '$index_averageRating': { $gte: 8.0 } },
      compressors
    });
  }));

  // Q5: Point lookup
  results.push(await benchmark('Point lookup by tconst', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: { '$index_tconst': 'tt0050000' },
      compressors
    });
  }));

  // Q6: Full scan (all columns)
  results.push(await benchmark('Full scan (all columns)', async () => {
    return await parquetQuery({
      file: titles.file,
      compressors
    });
  }));

  // Q7: Column projection (count only)
  results.push(await benchmark('Count only ($id, $index_titleType)', async () => {
    return await parquetQuery({
      file: titles.file,
      columns: ['$id', '$index_titleType'],
      filter: { '$index_titleType': 'movie' },
      compressors
    });
  }));

  // Q8: Cast lookup by title
  results.push(await benchmark('Cast for title (1-hop)', async () => {
    return await parquetQuery({
      file: cast.file,
      filter: { '$index_tconst': 'tt0050000' },
      compressors
    });
  }));

  // Q9: Person filmography
  results.push(await benchmark('Person filmography (1-hop)', async () => {
    return await parquetQuery({
      file: cast.file,
      filter: { '$index_nconst': 'nm0025000' },
      compressors
    });
  }));

  // Q10: Category filter on cast
  results.push(await benchmark('Cast by category="director"', async () => {
    return await parquetQuery({
      file: cast.file,
      filter: { '$index_category': 'director' },
      compressors
    });
  }));

  return results;
}

// =============================================================================
// O*NET Benchmarks
// =============================================================================

async function benchmarkONET() {
  console.log('\n' + '═'.repeat(70));
  console.log('O*NET Benchmarks (100 occupations, 10 skills, 1K occupation-skills)');
  console.log('═'.repeat(70) + '\n');

  const occupations = await loadFile('onet/occupations.parquet');
  const skills = await loadFile('onet/skills.parquet');
  const occSkills = await loadFile('onet/occupation-skills.parquet');

  const occMeta = await parquetMetadataAsync(occupations.file);
  console.log(`Occupations: ${occMeta.num_rows.toLocaleString()} rows, ${occMeta.row_groups.length} row groups`);

  const osMeta = await parquetMetadataAsync(occSkills.file);
  console.log(`Occupation-Skills: ${osMeta.num_rows.toLocaleString()} rows, ${osMeta.row_groups.length} row groups`);
  console.log(`Columns: ${osMeta.schema.slice(1).map(c => c.name).join(', ')}\n`);

  const results = [];

  // Q1: Job Zone filter
  results.push(await benchmark('Filter by jobZone=4', async () => {
    return await parquetQuery({
      file: occupations.file,
      filter: { '$index_jobZone': 4 },
      compressors
    });
  }));

  // Q2: SOC code prefix (since sorted)
  results.push(await benchmark('Filter by SOC prefix 15-', async () => {
    const rows = await parquetQuery({
      file: occupations.file,
      compressors
    });
    return rows.filter(r => r.$index_socCode?.startsWith('15-'));
  }));

  // Q3: Skills by importance threshold
  results.push(await benchmark('Filter skills importance >= 4.0', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_importance': { $gte: 4.0 } },
      compressors
    });
  }));

  // Q4: Skills for specific occupation
  results.push(await benchmark('Skills for occupation (1-hop)', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_socCode': '15-1252.00' },
      compressors
    });
  }));

  // Q5: Occupations requiring specific skill
  results.push(await benchmark('Occupations with skill (1-hop)', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_elementId': '2.B.2.i' },
      compressors
    });
  }));

  // Q6: Full scan occupations
  results.push(await benchmark('Full scan occupations', async () => {
    return await parquetQuery({
      file: occupations.file,
      compressors
    });
  }));

  // Q7: Full scan occupation-skills
  results.push(await benchmark('Full scan occupation-skills', async () => {
    return await parquetQuery({
      file: occSkills.file,
      compressors
    });
  }));

  return results;
}

// =============================================================================
// UNSPSC Benchmarks
// =============================================================================

async function benchmarkUNSPSC() {
  console.log('\n' + '═'.repeat(70));
  console.log('UNSPSC Benchmarks (6 segments, 4 families, 20 classes, 200 commodities)');
  console.log('═'.repeat(70) + '\n');

  const segments = await loadFile('unspsc/segments.parquet');
  const commodities = await loadFile('unspsc/commodities.parquet');

  const commMeta = await parquetMetadataAsync(commodities.file);
  console.log(`Commodities: ${commMeta.num_rows.toLocaleString()} rows, ${commMeta.row_groups.length} row groups`);
  console.log(`Columns: ${commMeta.schema.slice(1).map(c => c.name).join(', ')}\n`);

  const results = [];

  // Q1: Filter by segment code
  results.push(await benchmark('Filter by segmentCode="43"', async () => {
    return await parquetQuery({
      file: commodities.file,
      filter: { '$index_segmentCode': '43' },
      compressors
    });
  }));

  // Q2: Filter by family code
  results.push(await benchmark('Filter by familyCode="4310"', async () => {
    return await parquetQuery({
      file: commodities.file,
      filter: { '$index_familyCode': '4310' },
      compressors
    });
  }));

  // Q3: Code prefix search (hierarchy drill-down)
  results.push(await benchmark('Code prefix search (hierarchy)', async () => {
    const rows = await parquetQuery({
      file: commodities.file,
      compressors
    });
    return rows.filter(r => r.$index_code?.startsWith('4310'));
  }));

  // Q4: Full scan segments
  results.push(await benchmark('Full scan segments', async () => {
    return await parquetQuery({
      file: segments.file,
      compressors
    });
  }));

  // Q5: Full scan commodities
  results.push(await benchmark('Full scan commodities', async () => {
    return await parquetQuery({
      file: commodities.file,
      compressors
    });
  }));

  return results;
}

// =============================================================================
// Main
// =============================================================================

console.log('═'.repeat(70));
console.log('ParqueDB Dual Variant Architecture Benchmark');
console.log('═'.repeat(70));
console.log(`Data directory: ${DATA_DIR}`);

const imdbResults = await benchmarkIMDB();
const onetResults = await benchmarkONET();
const unspscResults = await benchmarkUNSPSC();

// Summary
console.log('\n' + '═'.repeat(70));
console.log('Summary');
console.log('═'.repeat(70));

console.log('\nFastest queries (< 5ms):');
[...imdbResults, ...onetResults, ...unspscResults]
  .filter(r => r.median < 5)
  .sort((a, b) => a.median - b.median)
  .forEach(r => console.log(`  ${r.median.toFixed(1)}ms - ${r.name}`));

console.log('\nSlowest queries (> 50ms):');
[...imdbResults, ...onetResults, ...unspscResults]
  .filter(r => r.median > 50)
  .sort((a, b) => b.median - a.median)
  .forEach(r => console.log(`  ${r.median.toFixed(1)}ms - ${r.name}`));

console.log('\n' + '═'.repeat(70));
