/**
 * Benchmark Secondary Indexes and Full-Text Search
 *
 * Tests the new index infrastructure:
 * - Hash indexes for equality lookups
 * - SST indexes for range queries
 * - FTS for text search
 */

import { HashIndex } from '../dist/indexes/secondary/hash.js';
import { SSTIndex } from '../dist/indexes/secondary/sst.js';
import { FTSIndex } from '../dist/indexes/fts/search.js';
import { MemoryBackend } from '../dist/storage/MemoryBackend.js';

// =============================================================================
// Types
// =============================================================================

interface BenchmarkResult {
  name: string;
  median: number;
  results: number;
}

interface MovieData {
  $id: string;
  titleType: string;
  name: string;
  description: string;
  startYear: number;
  rating: number;
  numVotes: number;
}

interface OccupationData {
  $id: string;
  title: string;
  description: string;
  jobZone: number;
  salary: number;
}

interface IndexEntry {
  doc: MovieData;
  docId: string;
  rowGroup: number;
  rowOffset: number;
}

interface FTSEntry {
  docId: string;
  doc: MovieData;
}

interface LookupResult {
  docIds?: string[];
  length?: number;
}

// =============================================================================
// Utilities
// =============================================================================

function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

async function benchmark(name: string, fn: () => unknown | Promise<unknown>, iterations = 10): Promise<BenchmarkResult> {
  const times: number[] = [];
  let result: unknown;
  // Warmup
  for (let i = 0; i < 3; i++) {
    result = await fn();
  }
  // Measure
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    result = await fn();
    times.push(performance.now() - start);
  }
  const med = median(times);
  const lookupResult = result as LookupResult | unknown[];
  const resultCount = Array.isArray(lookupResult) ? lookupResult.length :
                     (lookupResult as LookupResult)?.docIds?.length ?? (lookupResult as LookupResult)?.length ?? 0;
  console.log(`  ${name.padEnd(50)} ${med.toFixed(3).padStart(10)}ms  ${String(resultCount).padStart(8)} results`);
  return { name, median: med, results: resultCount };
}

// =============================================================================
// Data Generation
// =============================================================================

function generateMovieData(count: number): MovieData[] {
  const types = ['movie', 'tvSeries', 'short', 'tvEpisode', 'tvMovie'];
  const genres = ['Action', 'Drama', 'Comedy', 'Thriller', 'Horror', 'Sci-Fi'];

  return Array.from({ length: count }, (_, i) => ({
    $id: `tt${String(i).padStart(7, '0')}`,
    titleType: types[i % types.length],
    name: `Movie Title ${i} - ${genres[i % genres.length]} ${i % 100 === 0 ? 'masterpiece' : 'film'}`,
    description: `A ${genres[i % genres.length].toLowerCase()} ${types[i % types.length]} about ${
      i % 2 === 0 ? 'love and adventure' : 'mystery and suspense'
    }. ${i % 10 === 0 ? 'Features amazing special effects and incredible performances.' : ''}`,
    startYear: 1900 + (i % 125),
    rating: 5 + (i % 50) / 10,
    numVotes: 100 + i * 10,
  }));
}

function generateOccupationData(count: number): OccupationData[] {
  const jobZones = [1, 2, 3, 4, 5];
  const categories = ['Computer', 'Healthcare', 'Education', 'Engineering', 'Business'];

  return Array.from({ length: count }, (_, i) => ({
    $id: `${15 + (i % 5)}-${1000 + i}.00`,
    title: `${categories[i % categories.length]} ${i % 2 === 0 ? 'Developer' : 'Analyst'} ${i}`,
    description: `Responsible for ${categories[i % categories.length].toLowerCase()} tasks including analysis, development, and management.`,
    jobZone: jobZones[i % jobZones.length],
    salary: 50000 + (i * 100),
  }));
}

// =============================================================================
// Hash Index Benchmarks
// =============================================================================

async function benchmarkHashIndex(): Promise<BenchmarkResult[]> {
  console.log('\n' + '═'.repeat(75));
  console.log('Hash Index Benchmarks');
  console.log('═'.repeat(75) + '\n');

  const storage = new MemoryBackend();
  const results: BenchmarkResult[] = [];

  for (const size of [10_000, 100_000, 500_000]) {
    console.log(`\nDataset size: ${size.toLocaleString()}`);
    console.log('─'.repeat(75));

    const data = generateMovieData(size);

    const definition = {
      name: 'idx_titleType',
      type: 'hash' as const,
      fields: [{ path: 'titleType' }],
    };

    const index = new HashIndex(storage, 'movies', definition);

    // Build time
    const buildStart = performance.now();
    index.buildFromArray(data.map((doc, i): IndexEntry => ({
      doc,
      docId: doc.$id,
      rowGroup: Math.floor(i / 5000),
      rowOffset: i % 5000,
    })));
    const buildTime = performance.now() - buildStart;
    console.log(`  Build time: ${buildTime.toFixed(1)}ms\n`);

    // Lookup benchmarks
    results.push(await benchmark(`Lookup "movie" (${size.toLocaleString()} docs)`, () => {
      return index.lookup('movie');
    }));

    results.push(await benchmark(`Lookup "tvSeries" (${size.toLocaleString()} docs)`, () => {
      return index.lookup('tvSeries');
    }));

    results.push(await benchmark(`Lookup non-existent key`, () => {
      return index.lookup('nonexistent');
    }));
  }

  return results;
}

// =============================================================================
// SST Index Benchmarks
// =============================================================================

async function benchmarkSSTIndex(): Promise<BenchmarkResult[]> {
  console.log('\n' + '═'.repeat(75));
  console.log('SST Index Benchmarks (Range Queries)');
  console.log('═'.repeat(75) + '\n');

  const storage = new MemoryBackend();
  const results: BenchmarkResult[] = [];

  for (const size of [10_000, 100_000, 500_000]) {
    console.log(`\nDataset size: ${size.toLocaleString()}`);
    console.log('─'.repeat(75));

    const data = generateMovieData(size);

    const definition = {
      name: 'idx_startYear',
      type: 'sst' as const,
      fields: [{ path: 'startYear' }],
    };

    const index = new SSTIndex(storage, 'movies', definition);

    // Build time
    const buildStart = performance.now();
    index.buildFromArray(data.map((doc, i): IndexEntry => ({
      doc,
      docId: doc.$id,
      rowGroup: Math.floor(i / 5000),
      rowOffset: i % 5000,
    })));
    const buildTime = performance.now() - buildStart;
    console.log(`  Build time: ${buildTime.toFixed(1)}ms\n`);

    // Range query benchmarks
    results.push(await benchmark(`Range: year >= 2000 (${size.toLocaleString()} docs)`, () => {
      return index.range({ $gte: 2000 });
    }));

    results.push(await benchmark(`Range: 2010 <= year <= 2020`, () => {
      return index.range({ $gte: 2010, $lte: 2020 });
    }));

    results.push(await benchmark(`Range: year > 2020 (narrow)`, () => {
      return index.range({ $gt: 2020 });
    }));

    results.push(await benchmark(`Point lookup: year = 2015`, () => {
      return index.lookup(2015);
    }));
  }

  return results;
}

// =============================================================================
// FTS Index Benchmarks
// =============================================================================

async function benchmarkFTSIndex(): Promise<BenchmarkResult[]> {
  console.log('\n' + '═'.repeat(75));
  console.log('Full-Text Search Benchmarks');
  console.log('═'.repeat(75) + '\n');

  const storage = new MemoryBackend();
  const results: BenchmarkResult[] = [];

  for (const size of [10_000, 50_000, 100_000]) {
    console.log(`\nDataset size: ${size.toLocaleString()}`);
    console.log('─'.repeat(75));

    const data = generateMovieData(size);

    const definition = {
      name: 'idx_fts',
      type: 'fts' as const,
      fields: [{ path: 'name' }, { path: 'description' }],
    };

    const index = new FTSIndex(storage, 'movies', definition);

    // Build time
    const buildStart = performance.now();
    index.buildFromArray(data.map((doc): FTSEntry => ({
      docId: doc.$id,
      doc,
    })));
    const buildTime = performance.now() - buildStart;
    console.log(`  Build time: ${buildTime.toFixed(1)}ms`);
    console.log(`  Stats: ${index.documentCount} documents, vocabulary size: ${index.vocabularySize}\n`);

    // Search benchmarks
    results.push(await benchmark(`Search: "action" (${size.toLocaleString()} docs)`, () => {
      return index.search('action');
    }));

    results.push(await benchmark(`Search: "drama film"`, () => {
      return index.search('drama film');
    }));

    results.push(await benchmark(`Search: "masterpiece" (rare term)`, () => {
      return index.search('masterpiece');
    }));

    results.push(await benchmark(`Search: "amazing special effects"`, () => {
      return index.search('amazing special effects');
    }));

    results.push(await benchmark(`Search: "nonexistent term"`, () => {
      return index.search('xyznonexistentxyz');
    }));
  }

  return results;
}

// =============================================================================
// Comparison: Scan vs Index
// =============================================================================

async function benchmarkScanVsIndex(): Promise<void> {
  console.log('\n' + '═'.repeat(75));
  console.log('Full Scan vs Index Comparison');
  console.log('═'.repeat(75) + '\n');

  const size = 100_000;
  const data = generateMovieData(size);
  const storage = new MemoryBackend();

  console.log(`Dataset size: ${size.toLocaleString()}\n`);

  // Hash index for equality
  const hashIndex = new HashIndex(storage, 'movies', {
    name: 'idx_titleType',
    type: 'hash',
    fields: [{ path: 'titleType' }],
  });
  hashIndex.buildFromArray(data.map((doc, i): IndexEntry => ({
    doc, docId: doc.$id, rowGroup: 0, rowOffset: i,
  })));

  // SST index for range
  const sstIndex = new SSTIndex(storage, 'movies', {
    name: 'idx_rating',
    type: 'sst',
    fields: [{ path: 'rating' }],
  });
  sstIndex.buildFromArray(data.map((doc, i): IndexEntry => ({
    doc, docId: doc.$id, rowGroup: 0, rowOffset: i,
  })));

  // Equality comparison
  console.log('Equality: titleType = "movie"');
  console.log('─'.repeat(75));

  await benchmark('  Full scan (Array.filter)', () => {
    return data.filter(d => d.titleType === 'movie');
  });

  await benchmark('  Hash index lookup', () => {
    return hashIndex.lookup('movie');
  });

  // Range comparison
  console.log('\nRange: rating >= 8.0');
  console.log('─'.repeat(75));

  await benchmark('  Full scan (Array.filter)', () => {
    return data.filter(d => d.rating >= 8.0);
  });

  await benchmark('  SST index range', () => {
    return sstIndex.range({ $gte: 8.0 });
  });
}

// =============================================================================
// Main
// =============================================================================

console.log('═'.repeat(75));
console.log('ParqueDB Index Benchmarks');
console.log('═'.repeat(75));

await benchmarkHashIndex();
await benchmarkSSTIndex();
await benchmarkFTSIndex();
await benchmarkScanVsIndex();

console.log('\n' + '═'.repeat(75));
console.log('Benchmark Complete');
console.log('═'.repeat(75));
