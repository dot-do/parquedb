/**
 * Benchmark current state of loaded datasets
 * Tests query performance on IMDB and O*NET data
 */

import { parquetQuery } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { promises as fs } from 'node:fs';

// =============================================================================
// Types
// =============================================================================

interface FileReader {
  byteLength: number;
  slice: (start: number, end: number) => ArrayBuffer;
}

interface BenchmarkResult {
  name: string;
  median: number;
  rows: number;
}

interface ParquetRow {
  [key: string]: unknown;
}

// =============================================================================
// Utilities
// =============================================================================

async function loadFile(path: string): Promise<FileReader> {
  const buffer = await fs.readFile(path);
  return {
    byteLength: buffer.byteLength,
    slice: (s: number, e: number) => buffer.slice(s, e).buffer
  };
}

function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

async function benchmark(name: string, fn: () => Promise<ParquetRow[]>, iterations = 5): Promise<BenchmarkResult> {
  const times: number[] = [];
  let result: ParquetRow[] | undefined;
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    result = await fn();
    times.push(performance.now() - start);
  }
  const med = median(times);
  console.log(`  ${name}: ${med.toFixed(1)}ms (${result?.length?.toLocaleString() || 0} rows)`);
  return { name, median: med, rows: result?.length || 0 };
}

// =============================================================================
// IMDB Benchmarks
// =============================================================================

async function benchmarkIMDB(): Promise<void> {
  console.log('\n=== IMDB Benchmarks (1M rows per table) ===\n');

  const titlesFile = await loadFile('./data/imdb/title.basics.parquet');
  const ratingsFile = await loadFile('./data/imdb/title.ratings.parquet');
  const namesFile = await loadFile('./data/imdb/name.basics.parquet');

  // Q1: Filter by titleType (string equality)
  await benchmark('Filter by titleType="movie"', async () => {
    return await parquetQuery({
      file: titlesFile,
      filter: { titleType: 'movie' },
      compressors
    });
  });

  // Q2: Filter by year range
  await benchmark('Filter by startYear 2000-2020', async () => {
    return await parquetQuery({
      file: titlesFile,
      filter: { startYear: { $gte: 2000, $lte: 2020 } },
      compressors
    });
  });

  // Q3: Compound filter
  await benchmark('Compound: movie + year >= 2010', async () => {
    return await parquetQuery({
      file: titlesFile,
      filter: { titleType: 'movie', startYear: { $gte: 2010 } },
      compressors
    });
  });

  // Q4: High rating filter
  await benchmark('Filter ratings >= 8.0', async () => {
    return await parquetQuery({
      file: ratingsFile,
      filter: { averageRating: { $gte: 8.0 } },
      compressors
    });
  });

  // Q5: Point lookup by ID
  await benchmark('Point lookup by tconst', async () => {
    return await parquetQuery({
      file: titlesFile,
      filter: { tconst: 'tt0000001' },
      compressors
    });
  });

  // Q6: Full scan (no filter)
  await benchmark('Full scan (all columns)', async () => {
    return await parquetQuery({
      file: titlesFile,
      compressors
    });
  });

  // Q7: Column projection only
  await benchmark('Column projection (2 cols)', async () => {
    return await parquetQuery({
      file: titlesFile,
      columns: ['tconst', 'primaryTitle'],
      compressors
    });
  });
}

// =============================================================================
// O*NET Benchmarks
// =============================================================================

async function benchmarkONET(): Promise<void> {
  console.log('\n=== O*NET Benchmarks ===\n');

  const occupationsFile = await loadFile('./data/onet/Occupation Data.parquet');
  const skillsFile = await loadFile('./data/onet/Skills.parquet');
  const abilitiesFile = await loadFile('./data/onet/Abilities.parquet');

  // Q1: Find occupation by SOC code prefix
  await benchmark('Filter occupations by SOC prefix 15-', async () => {
    const rows = await parquetQuery({
      file: occupationsFile,
      compressors
    }) as ParquetRow[];
    return rows.filter(r => (r['O*NET-SOC Code'] as string)?.startsWith('15-'));
  });

  // Q2: Filter skills by importance
  await benchmark('Filter skills by Data Value >= 4.0', async () => {
    return await parquetQuery({
      file: skillsFile,
      filter: { 'Data Value': { $gte: 4.0 } },
      compressors
    });
  });

  // Q3: Full scan occupations
  await benchmark('Full scan occupations (1K rows)', async () => {
    return await parquetQuery({
      file: occupationsFile,
      compressors
    });
  });

  // Q4: Full scan skills
  await benchmark('Full scan skills (61K rows)', async () => {
    return await parquetQuery({
      file: skillsFile,
      compressors
    });
  });

  // Q5: Full scan abilities
  await benchmark('Full scan abilities (91K rows)', async () => {
    return await parquetQuery({
      file: abilitiesFile,
      compressors
    });
  });
}

// =============================================================================
// O*NET Optimized Benchmarks (with $id/$type pattern)
// =============================================================================

async function benchmarkONETOptimized(): Promise<void> {
  console.log('\n=== O*NET Optimized (with $id/$type) ===\n');

  try {
    const dataFile = await loadFile('./data/onet-optimized/data.parquet');
    const relsFile = await loadFile('./data/onet-optimized/rels.parquet');

    // Q1: Filter by $type
    await benchmark('Filter by $type', async () => {
      return await parquetQuery({
        file: dataFile,
        filter: { '$type': 'occupation' },
        compressors
      });
    });

    // Q2: Full scan relationships
    await benchmark('Full scan relationships (209K)', async () => {
      return await parquetQuery({
        file: relsFile,
        compressors
      });
    });

    // Q3: Filter relationships by predicate
    await benchmark('Filter rels by pred="hasSkill"', async () => {
      return await parquetQuery({
        file: relsFile,
        filter: { pred: 'hasSkill' },
        compressors
      });
    });

  } catch (e) {
    console.log('  O*NET Optimized not available:', (e as Error).message);
  }
}

// =============================================================================
// Main
// =============================================================================

console.log('═'.repeat(60));
console.log('ParqueDB Current State Benchmark');
console.log('═'.repeat(60));

await benchmarkIMDB();
await benchmarkONET();
await benchmarkONETOptimized();

console.log('\n' + '═'.repeat(60));
console.log('Benchmark complete!');
console.log('═'.repeat(60));
