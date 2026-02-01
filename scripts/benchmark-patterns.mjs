/**
 * Comprehensive Query Pattern Benchmarks for ParqueDB
 *
 * Based on QUERY_PATTERNS.md documentation for:
 * - IMDB dataset
 * - O*NET dataset
 * - UNSPSC dataset
 *
 * Tests each documented query pattern and measures:
 * - Query time (median of 5 runs)
 * - Rows returned
 * - Whether row-group statistics helped
 */

import { parquetQuery, parquetMetadataAsync } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';

const DATA_DIR = './data-v3';
const ITERATIONS = 5;

// =============================================================================
// Utilities
// =============================================================================

async function loadFile(path) {
  const buffer = await fs.readFile(join(DATA_DIR, path));
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

async function getRowGroupStats(file, columnName) {
  const meta = await parquetMetadataAsync(file);
  const colIndex = meta.schema.findIndex(c => c.name === columnName) - 1;
  if (colIndex < 0) return null;

  return meta.row_groups.map(rg => {
    const chunk = rg.columns[colIndex];
    return {
      num_values: chunk?.meta_data?.num_values || 0,
      min: chunk?.meta_data?.statistics?.min_value,
      max: chunk?.meta_data?.statistics?.max_value
    };
  });
}

async function countRowGroupsWithStats(file, columnName, predicate) {
  const stats = await getRowGroupStats(file, columnName);
  if (!stats) return { total: 0, matching: 0, skipped: 0 };

  let matching = 0;
  let skipped = 0;

  for (const stat of stats) {
    if (stat.min !== undefined && stat.max !== undefined) {
      if (predicate(stat.min, stat.max)) {
        matching++;
      } else {
        skipped++;
      }
    } else {
      matching++; // Must scan if no stats
    }
  }

  return {
    total: stats.length,
    matching,
    skipped,
    skipRate: ((skipped / stats.length) * 100).toFixed(0) + '%'
  };
}

// Track results for summary
const allResults = [];

async function benchmark(category, pattern, queryFn, options = {}) {
  const { statsColumn, statsPredicate, useCase, docRef, statsHelp } = options;

  const times = [];
  let result;
  let rowGroupInfo = null;

  for (let i = 0; i < ITERATIONS; i++) {
    const start = performance.now();
    result = await queryFn();
    times.push(performance.now() - start);
  }

  // Get row group stats info if specified
  if (statsColumn && statsPredicate && options.file) {
    rowGroupInfo = await countRowGroupsWithStats(options.file, statsColumn, statsPredicate);
  }

  const med = median(times);
  const rows = result?.length || 0;

  const record = {
    category,
    pattern,
    useCase: useCase || '',
    median: med,
    rows,
    rowGroupInfo,
    statsHelp: statsHelp || 'Unknown',
    docRef: docRef || ''
  };

  allResults.push(record);

  // Print result
  const statsInfo = rowGroupInfo
    ? `RG: ${rowGroupInfo.matching}/${rowGroupInfo.total} (skip ${rowGroupInfo.skipRate})`
    : '';
  console.log(`  ${pattern.padEnd(50)} ${med.toFixed(1).padStart(7)}ms ${rows.toLocaleString().padStart(8)} rows  ${statsInfo}`);

  return record;
}

// =============================================================================
// IMDB Query Pattern Benchmarks
// =============================================================================

async function benchmarkIMDBPatterns() {
  console.log('\n' + '='.repeat(100));
  console.log('IMDB QUERY PATTERNS (from examples/imdb/QUERY_PATTERNS.md)');
  console.log('='.repeat(100) + '\n');

  const titles = await loadFile('imdb/titles.parquet');
  const people = await loadFile('imdb/people.parquet');
  const cast = await loadFile('imdb/cast.parquet');

  const titlesMeta = await parquetMetadataAsync(titles.file);
  console.log(`Titles: ${titlesMeta.num_rows.toLocaleString()} rows, ${titlesMeta.row_groups.length} row groups`);
  console.log(`Columns: ${titlesMeta.schema.slice(1).map(c => c.name).join(', ')}\n`);

  // -------------------------------------------------------------------------
  // Streaming Service Patterns
  // -------------------------------------------------------------------------
  console.log('--- Streaming Service Patterns ---\n');

  // Pattern 1: Genre browsing with pagination (simulated - no genre column but we have type + rating)
  await benchmark('IMDB', 'P1: Movies by type + high rating (genre browse sim)', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: {
        '$index_titleType': 'movie',
        '$index_averageRating': { $gte: 7.5 }
      },
      compressors
    });
  }, {
    file: titles.file,
    statsColumn: '$index_averageRating',
    statsPredicate: (min, max) => max >= 7.5,
    useCase: 'Genre browsing pages with quality filters',
    statsHelp: 'Yes',
    docRef: 'Pattern 1'
  });

  // Pattern 2: New releases by year
  await benchmark('IMDB', 'P2: New releases (startYear=2024)', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: {
        '$index_titleType': 'movie',
        '$index_startYear': 2024
      },
      compressors
    });
  }, {
    file: titles.file,
    statsColumn: '$index_startYear',
    statsPredicate: (min, max) => min <= 2024 && max >= 2024,
    useCase: 'New This Year carousel',
    statsHelp: 'Yes',
    docRef: 'Pattern 2'
  });

  // Pattern 3: Decade collections
  await benchmark('IMDB', 'P3: Decade collection (1980s movies)', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: {
        '$index_titleType': 'movie',
        '$index_startYear': { $gte: 1980, $lt: 1990 }
      },
      compressors
    });
  }, {
    file: titles.file,
    statsColumn: '$index_startYear',
    statsPredicate: (min, max) => min < 1990 && max >= 1980,
    useCase: 'Decade-themed collections',
    statsHelp: 'Yes',
    docRef: 'Pattern 3'
  });

  // Pattern 4: Runtime filtering (simulated via numVotes since no runtime)
  await benchmark('IMDB', 'P4: Popular content (numVotes >= 10000)', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: {
        '$index_titleType': 'movie',
        '$index_numVotes': { $gte: 10000 }
      },
      compressors
    });
  }, {
    file: titles.file,
    statsColumn: '$index_numVotes',
    statsPredicate: (min, max) => max >= 10000,
    useCase: 'Filter by popularity/votes',
    statsHelp: 'Partial',
    docRef: 'Pattern 4'
  });

  // Pattern 6: Top-rated in category
  await benchmark('IMDB', 'P6: Top-rated (rating >= 8.5, votes >= 1000)', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: {
        '$index_averageRating': { $gte: 8.5 },
        '$index_numVotes': { $gte: 1000 }
      },
      compressors
    });
  }, {
    file: titles.file,
    statsColumn: '$index_averageRating',
    statsPredicate: (min, max) => max >= 8.5,
    useCase: 'Best of Genre curated lists',
    statsHelp: 'Yes',
    docRef: 'Pattern 6'
  });

  // -------------------------------------------------------------------------
  // Graph Traversal Patterns
  // -------------------------------------------------------------------------
  console.log('\n--- Graph Traversal Patterns ---\n');

  // Pattern 13: Six degrees (2-hop cast traversal simulation)
  await benchmark('IMDB', 'P13: Six degrees step 1 - person filmography', async () => {
    return await parquetQuery({
      file: cast.file,
      filter: { '$index_nconst': 'nm0000001' },
      compressors
    });
  }, {
    file: cast.file,
    statsColumn: '$index_nconst',
    statsPredicate: (min, max) => min <= 'nm0000001' && max >= 'nm0000001',
    useCase: 'Six degrees of Kevin Bacon',
    statsHelp: 'No',
    docRef: 'Pattern 13'
  });

  // Get movies for a person, then get other cast
  await benchmark('IMDB', 'P13: Six degrees step 2 - cast of a movie', async () => {
    return await parquetQuery({
      file: cast.file,
      filter: { '$index_tconst': 'tt0050000' },
      compressors
    });
  }, {
    file: cast.file,
    statsColumn: '$index_tconst',
    statsPredicate: (min, max) => min <= 'tt0050000' && max >= 'tt0050000',
    useCase: 'Six degrees - hop to co-stars',
    statsHelp: 'No',
    docRef: 'Pattern 13'
  });

  // Pattern 14: Director-actor pairs
  await benchmark('IMDB', 'P14: Director-actor pairs (filter directors)', async () => {
    return await parquetQuery({
      file: cast.file,
      filter: { '$index_category': 'director' },
      compressors
    });
  }, {
    file: cast.file,
    statsColumn: '$index_category',
    statsPredicate: (min, max) => min <= 'director' && max >= 'director',
    useCase: 'Collaboration analysis',
    statsHelp: 'Partial',
    docRef: 'Pattern 14'
  });

  // Pattern 18: Filmography queries
  await benchmark('IMDB', 'P18: Complete filmography lookup', async () => {
    const filmography = await parquetQuery({
      file: cast.file,
      filter: { '$index_nconst': 'nm0000001' },
      compressors
    });
    // Get title details for each
    const tconsts = [...new Set(filmography.map(r => r.$index_tconst))];
    // Simulated: in real system would batch lookup titles
    return filmography;
  }, {
    useCase: 'Actor profile pages',
    statsHelp: 'No',
    docRef: 'Pattern 18'
  });

  // -------------------------------------------------------------------------
  // Analytics Patterns
  // -------------------------------------------------------------------------
  console.log('\n--- Analytics Patterns ---\n');

  // Pattern 11: Distribution analysis (type)
  await benchmark('IMDB', 'P11: Type distribution (full scan)', async () => {
    const rows = await parquetQuery({
      file: titles.file,
      columns: ['$index_titleType'],
      compressors
    });
    // Group by type
    const counts = {};
    for (const r of rows) {
      counts[r.$index_titleType] = (counts[r.$index_titleType] || 0) + 1;
    }
    return Object.entries(counts);
  }, {
    useCase: 'Content library analysis',
    statsHelp: 'No',
    docRef: 'Pattern 11'
  });

  // Pattern 12: Year-over-year trends
  await benchmark('IMDB', 'P12: Year-over-year 2010s', async () => {
    const rows = await parquetQuery({
      file: titles.file,
      filter: {
        '$index_titleType': 'movie',
        '$index_startYear': { $gte: 2010, $lte: 2019 }
      },
      columns: ['$index_startYear'],
      compressors
    });
    // Group by year
    const counts = {};
    for (const r of rows) {
      counts[r.$index_startYear] = (counts[r.$index_startYear] || 0) + 1;
    }
    return Object.entries(counts);
  }, {
    file: titles.file,
    statsColumn: '$index_startYear',
    statsPredicate: (min, max) => min <= 2019 && max >= 2010,
    useCase: 'Industry trend analysis',
    statsHelp: 'Yes',
    docRef: 'Pattern 12'
  });

  // -------------------------------------------------------------------------
  // Baseline: Full Scans
  // -------------------------------------------------------------------------
  console.log('\n--- Baseline: Full Scans ---\n');

  await benchmark('IMDB', 'BASELINE: Full scan titles', async () => {
    return await parquetQuery({
      file: titles.file,
      compressors
    });
  }, {
    useCase: 'Reference baseline',
    statsHelp: 'N/A'
  });

  await benchmark('IMDB', 'BASELINE: Full scan cast', async () => {
    return await parquetQuery({
      file: cast.file,
      compressors
    });
  }, {
    useCase: 'Reference baseline',
    statsHelp: 'N/A'
  });

  await benchmark('IMDB', 'BASELINE: Full scan with projection', async () => {
    return await parquetQuery({
      file: titles.file,
      columns: ['$id', '$index_titleType'],
      compressors
    });
  }, {
    useCase: 'Projection optimization check',
    statsHelp: 'N/A'
  });
}

// =============================================================================
// O*NET Query Pattern Benchmarks
// =============================================================================

async function benchmarkONETPatterns() {
  console.log('\n' + '='.repeat(100));
  console.log('O*NET QUERY PATTERNS (from examples/onet/QUERY_PATTERNS.md)');
  console.log('='.repeat(100) + '\n');

  const occupations = await loadFile('onet/occupations.parquet');
  const skills = await loadFile('onet/skills.parquet');
  const occSkills = await loadFile('onet/occupation-skills.parquet');

  const occMeta = await parquetMetadataAsync(occupations.file);
  const osMeta = await parquetMetadataAsync(occSkills.file);
  console.log(`Occupations: ${occMeta.num_rows.toLocaleString()} rows, ${occMeta.row_groups.length} row groups`);
  console.log(`Occupation-Skills: ${osMeta.num_rows.toLocaleString()} rows, ${osMeta.row_groups.length} row groups`);
  console.log(`Columns (occ-skills): ${osMeta.schema.slice(1).map(c => c.name).join(', ')}\n`);

  // -------------------------------------------------------------------------
  // Career Exploration Patterns
  // -------------------------------------------------------------------------
  console.log('--- Career Exploration Patterns ---\n');

  // Q1: Job Zone filtering
  await benchmark('O*NET', 'Q1: Job Zone filter (jobZone=4)', async () => {
    return await parquetQuery({
      file: occupations.file,
      filter: { '$index_jobZone': 4 },
      compressors
    });
  }, {
    file: occupations.file,
    statsColumn: '$index_jobZone',
    statsPredicate: (min, max) => min <= 4 && max >= 4,
    useCase: 'Bachelor degree careers',
    statsHelp: 'Yes',
    docRef: 'Q1'
  });

  // Q4: SOC code prefix search (career pathway)
  await benchmark('O*NET', 'Q4: SOC prefix search (15- Computer)', async () => {
    const rows = await parquetQuery({
      file: occupations.file,
      compressors
    });
    return rows.filter(r => r.$index_socCode?.startsWith('15-'));
  }, {
    useCase: 'IT career pathway',
    statsHelp: 'Partial',
    docRef: 'Q4'
  });

  // Q3: Related occupations
  await benchmark('O*NET', 'Q3: Occupations by job zone range (1-2)', async () => {
    const rows = await parquetQuery({
      file: occupations.file,
      filter: { '$index_jobZone': { $lte: 2 } },
      compressors
    });
    return rows;
  }, {
    file: occupations.file,
    statsColumn: '$index_jobZone',
    statsPredicate: (min, max) => min <= 2,
    useCase: 'Entry-level careers',
    statsHelp: 'Yes',
    docRef: 'Q3'
  });

  // -------------------------------------------------------------------------
  // Skill Analysis Patterns
  // -------------------------------------------------------------------------
  console.log('\n--- Skill Analysis Patterns ---\n');

  // Q5: Skill importance threshold
  await benchmark('O*NET', 'Q5: Skills importance >= 4.0', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_importance': { $gte: 4.0 } },
      compressors
    });
  }, {
    file: occSkills.file,
    statsColumn: '$index_importance',
    statsPredicate: (min, max) => max >= 4.0,
    useCase: 'High-importance skills',
    statsHelp: 'Yes',
    docRef: 'Q5'
  });

  // Q8: Skills for specific occupation
  await benchmark('O*NET', 'Q8: Skills for occupation 15-1252.00', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_socCode': '15-1252.00' },
      compressors
    });
  }, {
    file: occSkills.file,
    statsColumn: '$index_socCode',
    statsPredicate: (min, max) => min <= '15-1252.00' && max >= '15-1252.00',
    useCase: 'Build job requirements',
    statsHelp: 'Yes',
    docRef: 'Q8'
  });

  // Q9: Career path queries (skills overlap) - simulate skill gap
  await benchmark('O*NET', 'Q9: Skill gap - source occupation skills', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_socCode': '15-1251.00' },
      compressors
    });
  }, {
    useCase: 'Career change skill gap',
    statsHelp: 'Yes',
    docRef: 'Q9'
  });

  await benchmark('O*NET', 'Q9: Skill gap - target occupation skills', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_socCode': '15-1252.00' },
      compressors
    });
  }, {
    useCase: 'Career change skill gap',
    statsHelp: 'Yes',
    docRef: 'Q9'
  });

  // Q11: Skill profile matching
  await benchmark('O*NET', 'Q11: Occupations requiring skill 2.B.2.i', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_elementId': '2.B.2.i' },
      compressors
    });
  }, {
    file: occSkills.file,
    statsColumn: '$index_elementId',
    statsPredicate: (min, max) => min <= '2.B.2.i' && max >= '2.B.2.i',
    useCase: 'Skill-based career matching',
    statsHelp: 'Yes',
    docRef: 'Q11'
  });

  // Combined: Skill + importance threshold
  await benchmark('O*NET', 'Q5+Q11: Skill + high importance', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: {
        '$index_elementId': '2.B.2.i',
        '$index_importance': { $gte: 3.5 }
      },
      compressors
    });
  }, {
    useCase: 'Qualified for skill threshold',
    statsHelp: 'Yes',
    docRef: 'Q5+Q11'
  });

  // -------------------------------------------------------------------------
  // Analytics Patterns
  // -------------------------------------------------------------------------
  console.log('\n--- Analytics Patterns ---\n');

  // Q12: Job Zone aggregation
  await benchmark('O*NET', 'Q12: Job Zone distribution', async () => {
    const rows = await parquetQuery({
      file: occupations.file,
      columns: ['$index_jobZone'],
      compressors
    });
    const counts = {};
    for (const r of rows) {
      counts[r.$index_jobZone] = (counts[r.$index_jobZone] || 0) + 1;
    }
    return Object.entries(counts);
  }, {
    useCase: 'Workforce analytics',
    statsHelp: 'Metadata',
    docRef: 'Q12'
  });

  // Q14: Skill demand analysis
  await benchmark('O*NET', 'Q14: Skill demand (all ratings for skill)', async () => {
    const rows = await parquetQuery({
      file: occSkills.file,
      filter: { '$index_elementId': '2.A.1.a' },
      compressors
    });
    // Compute average importance
    const total = rows.reduce((sum, r) => sum + (r.$index_importance || 0), 0);
    const avg = rows.length ? total / rows.length : 0;
    return [{ avgImportance: avg, count: rows.length }];
  }, {
    useCase: 'Skill demand across occupations',
    statsHelp: 'Yes',
    docRef: 'Q14'
  });

  // -------------------------------------------------------------------------
  // Baseline: Full Scans
  // -------------------------------------------------------------------------
  console.log('\n--- Baseline: Full Scans ---\n');

  await benchmark('O*NET', 'BASELINE: Full scan occupations', async () => {
    return await parquetQuery({
      file: occupations.file,
      compressors
    });
  }, {
    useCase: 'Reference baseline',
    statsHelp: 'N/A'
  });

  await benchmark('O*NET', 'BASELINE: Full scan occupation-skills', async () => {
    return await parquetQuery({
      file: occSkills.file,
      compressors
    });
  }, {
    useCase: 'Reference baseline',
    statsHelp: 'N/A'
  });
}

// =============================================================================
// UNSPSC Query Pattern Benchmarks
// =============================================================================

async function benchmarkUNSPSCPatterns() {
  console.log('\n' + '='.repeat(100));
  console.log('UNSPSC QUERY PATTERNS (from examples/unspsc/QUERY_PATTERNS.md)');
  console.log('='.repeat(100) + '\n');

  const segments = await loadFile('unspsc/segments.parquet');
  const families = await loadFile('unspsc/families.parquet');
  const classes = await loadFile('unspsc/classes.parquet');
  const commodities = await loadFile('unspsc/commodities.parquet');

  const commMeta = await parquetMetadataAsync(commodities.file);
  const classMeta = await parquetMetadataAsync(classes.file);
  console.log(`Commodities: ${commMeta.num_rows.toLocaleString()} rows, ${commMeta.row_groups.length} row groups`);
  console.log(`Classes: ${classMeta.num_rows.toLocaleString()} rows, ${classMeta.row_groups.length} row groups`);
  console.log(`Columns (commodities): ${commMeta.schema.slice(1).map(c => c.name).join(', ')}\n`);

  // -------------------------------------------------------------------------
  // Hierarchy Drill-down Patterns
  // -------------------------------------------------------------------------
  console.log('--- Hierarchy Drill-down Patterns ---\n');

  // Pattern 1: Segment filter
  await benchmark('UNSPSC', 'P1.3: Segment filter (segmentCode=43)', async () => {
    return await parquetQuery({
      file: commodities.file,
      filter: { '$index_segmentCode': '43' },
      compressors
    });
  }, {
    file: commodities.file,
    statsColumn: '$index_segmentCode',
    statsPredicate: (min, max) => min <= '43' && max >= '43',
    useCase: 'IT segment spend analysis',
    statsHelp: 'Yes',
    docRef: 'Pattern 1.3'
  });

  // Pattern 2.2: Family drill-down
  await benchmark('UNSPSC', 'P2.2: Family drill-down (familyCode=4310)', async () => {
    return await parquetQuery({
      file: commodities.file,
      filter: { '$index_familyCode': '4310' },
      compressors
    });
  }, {
    file: commodities.file,
    statsColumn: '$index_familyCode',
    statsPredicate: (min, max) => min <= '4310' && max >= '4310',
    useCase: 'Computer Equipment family',
    statsHelp: 'Yes',
    docRef: 'Pattern 2.2'
  });

  // Pattern: Class filter
  await benchmark('UNSPSC', 'P4.1: Class filter (classCode=431015)', async () => {
    return await parquetQuery({
      file: commodities.file,
      filter: { '$index_classCode': '431015' },
      compressors
    });
  }, {
    file: commodities.file,
    statsColumn: '$index_classCode',
    statsPredicate: (min, max) => min <= '431015' && max >= '431015',
    useCase: 'Supplier capability matching',
    statsHelp: 'Yes',
    docRef: 'Pattern 4.1'
  });

  // -------------------------------------------------------------------------
  // Code Prefix Search Patterns
  // -------------------------------------------------------------------------
  console.log('\n--- Code Prefix Search Patterns ---\n');

  // Pattern 2: Code prefix search
  await benchmark('UNSPSC', 'P2: Code prefix search (43%)', async () => {
    const rows = await parquetQuery({
      file: commodities.file,
      compressors
    });
    return rows.filter(r => r.$index_code?.startsWith('43'));
  }, {
    useCase: 'Code prefix lookup',
    statsHelp: 'Partial',
    docRef: 'Pattern 2'
  });

  // Pattern: Multi-segment query
  await benchmark('UNSPSC', 'P3.2: Multi-segment (43, 44)', async () => {
    const rows = await parquetQuery({
      file: commodities.file,
      compressors
    });
    return rows.filter(r =>
      r.$index_segmentCode === '43' || r.$index_segmentCode === '44'
    );
  }, {
    useCase: 'Set-aside category identification',
    statsHelp: 'Yes',
    docRef: 'Pattern 3.2'
  });

  // -------------------------------------------------------------------------
  // Breadcrumb Generation Patterns
  // -------------------------------------------------------------------------
  console.log('\n--- Breadcrumb Generation Patterns ---\n');

  // Pattern 2.4: Breadcrumb generation (parallel lookups)
  await benchmark('UNSPSC', 'P2.4: Breadcrumb - segment lookup', async () => {
    return await parquetQuery({
      file: segments.file,
      filter: { '$index_code': '43' },
      compressors
    });
  }, {
    useCase: 'Navigation breadcrumb',
    statsHelp: 'Yes',
    docRef: 'Pattern 2.4'
  });

  await benchmark('UNSPSC', 'P2.4: Breadcrumb - family lookup', async () => {
    return await parquetQuery({
      file: families.file,
      filter: { '$index_code': '4310' },
      compressors
    });
  }, {
    useCase: 'Navigation breadcrumb',
    statsHelp: 'Yes',
    docRef: 'Pattern 2.4'
  });

  await benchmark('UNSPSC', 'P2.4: Breadcrumb - class lookup', async () => {
    return await parquetQuery({
      file: classes.file,
      filter: { '$index_code': '431015' },
      compressors
    });
  }, {
    useCase: 'Navigation breadcrumb',
    statsHelp: 'Yes',
    docRef: 'Pattern 2.4'
  });

  await benchmark('UNSPSC', 'P2.4: Breadcrumb - commodity lookup', async () => {
    return await parquetQuery({
      file: commodities.file,
      filter: { '$index_code': '43101501' },
      compressors
    });
  }, {
    useCase: 'Navigation breadcrumb',
    statsHelp: 'Yes',
    docRef: 'Pattern 2.4'
  });

  // -------------------------------------------------------------------------
  // Cross-level Aggregation Patterns
  // -------------------------------------------------------------------------
  console.log('\n--- Cross-level Aggregation Patterns ---\n');

  // Pattern 5.1: Segment distribution
  await benchmark('UNSPSC', 'P5.1: Segment distribution count', async () => {
    const rows = await parquetQuery({
      file: commodities.file,
      columns: ['$index_segmentCode'],
      compressors
    });
    const counts = {};
    for (const r of rows) {
      counts[r.$index_segmentCode] = (counts[r.$index_segmentCode] || 0) + 1;
    }
    return Object.entries(counts);
  }, {
    useCase: 'Spend analytics dashboard',
    statsHelp: 'No',
    docRef: 'Pattern 5.1'
  });

  // Pattern 6.2: Category hierarchy export
  await benchmark('UNSPSC', 'P6.2: Hierarchy export - all families in segment', async () => {
    return await parquetQuery({
      file: families.file,
      filter: { '$index_segmentCode': '43' },
      compressors
    });
  }, {
    useCase: 'Category tree export',
    statsHelp: 'Yes',
    docRef: 'Pattern 6.2'
  });

  await benchmark('UNSPSC', 'P6.2: Hierarchy export - all classes in segment', async () => {
    return await parquetQuery({
      file: classes.file,
      filter: { '$index_segmentCode': '43' },
      compressors
    });
  }, {
    useCase: 'Category tree export',
    statsHelp: 'Yes',
    docRef: 'Pattern 6.2'
  });

  // -------------------------------------------------------------------------
  // Baseline: Full Scans
  // -------------------------------------------------------------------------
  console.log('\n--- Baseline: Full Scans ---\n');

  await benchmark('UNSPSC', 'BASELINE: Full scan segments', async () => {
    return await parquetQuery({
      file: segments.file,
      compressors
    });
  }, {
    useCase: 'Reference baseline',
    statsHelp: 'N/A'
  });

  await benchmark('UNSPSC', 'BASELINE: Full scan commodities', async () => {
    return await parquetQuery({
      file: commodities.file,
      compressors
    });
  }, {
    useCase: 'Reference baseline',
    statsHelp: 'N/A'
  });
}

// =============================================================================
// Row-Group Statistics Comparison Tests
// =============================================================================

async function benchmarkStatsComparison() {
  console.log('\n' + '='.repeat(100));
  console.log('ROW-GROUP STATISTICS COMPARISON: Filtered vs Full Scan');
  console.log('='.repeat(100) + '\n');

  const titles = await loadFile('imdb/titles.parquet');
  const cast = await loadFile('imdb/cast.parquet');
  const occSkills = await loadFile('onet/occupation-skills.parquet');
  const commodities = await loadFile('unspsc/commodities.parquet');

  console.log('Testing same queries with filter pushdown vs full scan + client filter:\n');

  // Test 1: Year range with stats pushdown
  console.log('--- IMDB Year Range Query ---\n');

  const yearFilteredResult = await benchmark('Compare', 'Year 2000-2010: WITH filter pushdown', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: { '$index_startYear': { $gte: 2000, $lte: 2010 } },
      compressors
    });
  }, { statsHelp: 'Yes (pushdown)' });

  const yearScanResult = await benchmark('Compare', 'Year 2000-2010: WITHOUT filter (full scan)', async () => {
    const rows = await parquetQuery({
      file: titles.file,
      compressors
    });
    return rows.filter(r => r.$index_startYear >= 2000 && r.$index_startYear <= 2010);
  }, { statsHelp: 'No (client)' });

  console.log(`  -> Speedup factor: ${(yearScanResult.median / yearFilteredResult.median).toFixed(1)}x\n`);

  // Test 2: High rating filter
  console.log('--- IMDB Rating Threshold Query ---\n');

  const ratingFilteredResult = await benchmark('Compare', 'Rating >= 8.0: WITH filter pushdown', async () => {
    return await parquetQuery({
      file: titles.file,
      filter: { '$index_averageRating': { $gte: 8.0 } },
      compressors
    });
  }, { statsHelp: 'Yes (pushdown)' });

  const ratingScanResult = await benchmark('Compare', 'Rating >= 8.0: WITHOUT filter (full scan)', async () => {
    const rows = await parquetQuery({
      file: titles.file,
      compressors
    });
    return rows.filter(r => r.$index_averageRating >= 8.0);
  }, { statsHelp: 'No (client)' });

  console.log(`  -> Speedup factor: ${(ratingScanResult.median / ratingFilteredResult.median).toFixed(1)}x\n`);

  // Test 3: Point lookup (ID)
  console.log('--- Cast Point Lookup Query ---\n');

  const idFilteredResult = await benchmark('Compare', 'tconst lookup: WITH filter pushdown', async () => {
    return await parquetQuery({
      file: cast.file,
      filter: { '$index_tconst': 'tt0050000' },
      compressors
    });
  }, { statsHelp: 'Yes (pushdown)' });

  const idScanResult = await benchmark('Compare', 'tconst lookup: WITHOUT filter (full scan)', async () => {
    const rows = await parquetQuery({
      file: cast.file,
      compressors
    });
    return rows.filter(r => r.$index_tconst === 'tt0050000');
  }, { statsHelp: 'No (client)' });

  console.log(`  -> Speedup factor: ${(idScanResult.median / idFilteredResult.median).toFixed(1)}x\n`);

  // Test 4: O*NET importance threshold
  console.log('--- O*NET Importance Threshold Query ---\n');

  const impFilteredResult = await benchmark('Compare', 'Importance >= 4.5: WITH filter pushdown', async () => {
    return await parquetQuery({
      file: occSkills.file,
      filter: { '$index_importance': { $gte: 4.5 } },
      compressors
    });
  }, { statsHelp: 'Yes (pushdown)' });

  const impScanResult = await benchmark('Compare', 'Importance >= 4.5: WITHOUT filter (full scan)', async () => {
    const rows = await parquetQuery({
      file: occSkills.file,
      compressors
    });
    return rows.filter(r => r.$index_importance >= 4.5);
  }, { statsHelp: 'No (client)' });

  console.log(`  -> Speedup factor: ${(impScanResult.median / impFilteredResult.median).toFixed(1)}x\n`);

  // Test 5: UNSPSC hierarchy filter
  console.log('--- UNSPSC Family Filter Query ---\n');

  const famFilteredResult = await benchmark('Compare', 'Family 4310: WITH filter pushdown', async () => {
    return await parquetQuery({
      file: commodities.file,
      filter: { '$index_familyCode': '4310' },
      compressors
    });
  }, { statsHelp: 'Yes (pushdown)' });

  const famScanResult = await benchmark('Compare', 'Family 4310: WITHOUT filter (full scan)', async () => {
    const rows = await parquetQuery({
      file: commodities.file,
      compressors
    });
    return rows.filter(r => r.$index_familyCode === '4310');
  }, { statsHelp: 'No (client)' });

  console.log(`  -> Speedup factor: ${(famScanResult.median / famFilteredResult.median).toFixed(1)}x\n`);

  // Summary of comparison results
  console.log('--- Comparison Summary ---\n');
  console.log('| Query Type | With Pushdown | Without | Speedup |');
  console.log('|------------|---------------|---------|---------|');
  console.log(`| Year range | ${yearFilteredResult.median.toFixed(1)}ms | ${yearScanResult.median.toFixed(1)}ms | ${(yearScanResult.median / yearFilteredResult.median).toFixed(1)}x |`);
  console.log(`| Rating threshold | ${ratingFilteredResult.median.toFixed(1)}ms | ${ratingScanResult.median.toFixed(1)}ms | ${(ratingScanResult.median / ratingFilteredResult.median).toFixed(1)}x |`);
  console.log(`| Point lookup | ${idFilteredResult.median.toFixed(1)}ms | ${idScanResult.median.toFixed(1)}ms | ${(idScanResult.median / idFilteredResult.median).toFixed(1)}x |`);
  console.log(`| Importance threshold | ${impFilteredResult.median.toFixed(1)}ms | ${impScanResult.median.toFixed(1)}ms | ${(impScanResult.median / impFilteredResult.median).toFixed(1)}x |`);
  console.log(`| Hierarchy filter | ${famFilteredResult.median.toFixed(1)}ms | ${famScanResult.median.toFixed(1)}ms | ${(famScanResult.median / famFilteredResult.median).toFixed(1)}x |`);
}

// =============================================================================
// Summary Report
// =============================================================================

function printSummaryReport() {
  console.log('\n' + '='.repeat(100));
  console.log('SUMMARY: Dual Variant Architecture Benefits');
  console.log('='.repeat(100) + '\n');

  // Group by statsHelp value
  const byStatsHelp = {
    'Yes': [],
    'Partial': [],
    'No': [],
    'N/A': [],
    'Metadata': [],
    'Unknown': []
  };

  for (const r of allResults) {
    const key = r.statsHelp || 'Unknown';
    if (!byStatsHelp[key]) byStatsHelp[key] = [];
    byStatsHelp[key].push(r);
  }

  // Print table header
  console.log('| Category | Pattern | Time (ms) | Rows | Stats Benefit |');
  console.log('|----------|---------|-----------|------|---------------|');

  // Sort by median time ascending
  const sorted = [...allResults].sort((a, b) => a.median - b.median);
  for (const r of sorted) {
    console.log(`| ${r.category.padEnd(8)} | ${r.pattern.slice(0, 45).padEnd(45)} | ${r.median.toFixed(1).padStart(9)} | ${r.rows.toString().padStart(4)} | ${r.statsHelp.padEnd(13)} |`);
  }

  // Print statistics by category
  console.log('\n--- Statistics by Row-Group Stats Benefit ---\n');

  for (const [benefit, patterns] of Object.entries(byStatsHelp)) {
    if (patterns.length === 0) continue;
    const medianTimes = patterns.map(p => p.median);
    const avgTime = medianTimes.reduce((a, b) => a + b, 0) / medianTimes.length;
    console.log(`${benefit.padEnd(10)}: ${patterns.length} patterns, avg ${avgTime.toFixed(1)}ms`);
  }

  // Patterns that benefit most from dual variant
  console.log('\n--- Patterns That Benefit Most from Dual Variant Architecture ---\n');

  const beneficial = allResults.filter(r => r.statsHelp === 'Yes' || r.statsHelp === 'Partial');
  const baselines = allResults.filter(r => r.pattern.includes('BASELINE'));

  if (baselines.length > 0 && beneficial.length > 0) {
    const avgBaseline = baselines.reduce((a, b) => a + b.median, 0) / baselines.length;
    const avgBeneficial = beneficial.reduce((a, b) => a + b.median, 0) / beneficial.length;

    console.log(`Average baseline (full scan): ${avgBaseline.toFixed(1)}ms`);
    console.log(`Average with stats pushdown:  ${avgBeneficial.toFixed(1)}ms`);
    console.log(`Improvement factor:           ${(avgBaseline / avgBeneficial).toFixed(1)}x`);
  }

  // Fast queries (< 5ms)
  console.log('\n--- Fast Queries (< 5ms) ---\n');
  const fast = sorted.filter(r => r.median < 5 && !r.pattern.includes('BASELINE'));
  for (const r of fast) {
    console.log(`  ${r.median.toFixed(1)}ms - ${r.category}: ${r.pattern}`);
  }

  // Patterns that could benefit from additional shredding
  console.log('\n--- Patterns Requiring Client-Side Filtering (potential shredding candidates) ---\n');
  const clientSide = allResults.filter(r => r.pattern.includes('prefix') || r.pattern.includes('Multi-segment'));
  for (const r of clientSide) {
    console.log(`  ${r.pattern} - currently requires full scan + filter`);
  }
}

// =============================================================================
// Main
// =============================================================================

console.log('='.repeat(100));
console.log('ParqueDB Comprehensive Query Pattern Benchmarks');
console.log('='.repeat(100));
console.log(`Data directory: ${DATA_DIR}`);
console.log(`Iterations per query: ${ITERATIONS}`);
console.log('');
console.log('Based on QUERY_PATTERNS.md documentation:');
console.log('  - examples/imdb/QUERY_PATTERNS.md');
console.log('  - examples/onet/QUERY_PATTERNS.md');
console.log('  - examples/unspsc/QUERY_PATTERNS.md');

await benchmarkIMDBPatterns();
await benchmarkONETPatterns();
await benchmarkUNSPSCPatterns();
await benchmarkStatsComparison();

printSummaryReport();

console.log('\n' + '='.repeat(100));
console.log('Benchmark complete');
console.log('='.repeat(100));
