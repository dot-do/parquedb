/**
 * Scale IMDB Dataset to 1M+ Rows
 *
 * Generates realistic IMDB-like data for benchmarking:
 * - 1,000,000 titles (sorted by titleType for row-group stats)
 * - 500,000 people (sorted by nconst)
 * - 2,000,000 cast entries (sorted by tconst)
 *
 * Uses dual Variant architecture: $id | $index_* | $data columns
 */

import { parquetWriteBuffer } from 'hyparquet-writer';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';

const OUTPUT_DIR = './data-v3/imdb-1m';
const ROW_GROUP_SIZE = 50000;

// =============================================================================
// Configuration
// =============================================================================

const TITLE_COUNT = 1_000_000;
const PEOPLE_COUNT = 500_000;
const CAST_COUNT = 2_000_000;

// Title type distribution: movie (35%), tvSeries (25%), tvEpisode (30%), other (10%)
const TITLE_TYPE_DISTRIBUTION = [
  { type: 'movie', weight: 0.35 },
  { type: 'tvSeries', weight: 0.25 },
  { type: 'tvEpisode', weight: 0.30 },
  { type: 'short', weight: 0.05 },
  { type: 'tvMovie', weight: 0.03 },
  { type: 'video', weight: 0.02 },
];

// Genre distribution (realistic mix)
const GENRES = [
  { name: 'Drama', weight: 0.25 },
  { name: 'Comedy', weight: 0.18 },
  { name: 'Action', weight: 0.12 },
  { name: 'Thriller', weight: 0.10 },
  { name: 'Horror', weight: 0.08 },
  { name: 'Romance', weight: 0.07 },
  { name: 'Documentary', weight: 0.06 },
  { name: 'Sci-Fi', weight: 0.05 },
  { name: 'Crime', weight: 0.04 },
  { name: 'Animation', weight: 0.03 },
  { name: 'Fantasy', weight: 0.02 },
];

// Profession distribution
const PROFESSIONS = [
  { name: 'actor', weight: 0.40 },
  { name: 'actress', weight: 0.30 },
  { name: 'director', weight: 0.10 },
  { name: 'writer', weight: 0.10 },
  { name: 'producer', weight: 0.05 },
  { name: 'composer', weight: 0.03 },
  { name: 'cinematographer', weight: 0.02 },
];

// Cast category distribution
const CAST_CATEGORIES = [
  { name: 'actor', weight: 0.35 },
  { name: 'actress', weight: 0.30 },
  { name: 'director', weight: 0.15 },
  { name: 'writer', weight: 0.10 },
  { name: 'producer', weight: 0.05 },
  { name: 'self', weight: 0.03 },
  { name: 'composer', weight: 0.02 },
];

// =============================================================================
// Utilities
// =============================================================================

// Seeded random for reproducibility
let seed = 42;
function random() {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff;
  return seed / 0x7fffffff;
}

function weightedRandom(items) {
  const r = random();
  let cumulative = 0;
  for (const item of items) {
    cumulative += item.weight;
    if (r < cumulative) {
      return item.name || item.type;
    }
  }
  return items[items.length - 1].name || items[items.length - 1].type;
}

// Normal distribution using Box-Muller transform
function normalRandom(mean, stdDev) {
  const u1 = random();
  const u2 = random();
  const z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
  return mean + z0 * stdDev;
}

// Year distribution: more recent years weighted higher
function generateYear() {
  // Use exponential-like distribution favoring recent years
  const base = random();
  const weighted = Math.pow(base, 0.5); // Square root favors higher values
  const year = Math.floor(1900 + weighted * 124); // 1900-2024
  return Math.max(1900, Math.min(2024, year));
}

// Generate realistic movie title
const TITLE_PREFIXES = ['The', 'A', '', '', '', '', ''];
const TITLE_WORDS = [
  'Dark', 'Last', 'First', 'Final', 'Lost', 'Secret', 'Hidden', 'Broken', 'Silent', 'Deadly',
  'Night', 'Day', 'Summer', 'Winter', 'Storm', 'Fire', 'Ice', 'Blood', 'Steel', 'Gold',
  'Road', 'Way', 'Path', 'Journey', 'Mission', 'Quest', 'Hunt', 'Chase', 'Search', 'Return',
  'Love', 'War', 'Peace', 'Truth', 'Lies', 'Dreams', 'Shadows', 'Light', 'Fear', 'Hope',
  'King', 'Queen', 'Prince', 'Warrior', 'Hunter', 'Ghost', 'Angel', 'Devil', 'Hero', 'Legend',
];
const TITLE_SUFFIXES = ['', '', '', '', 'II', 'III', 'Returns', 'Rising', 'Begins', 'Forever'];

function generateTitle(i) {
  const prefix = TITLE_PREFIXES[Math.floor(random() * TITLE_PREFIXES.length)];
  const word1 = TITLE_WORDS[Math.floor(random() * TITLE_WORDS.length)];
  const word2 = TITLE_WORDS[Math.floor(random() * TITLE_WORDS.length)];
  const suffix = TITLE_SUFFIXES[Math.floor(random() * TITLE_SUFFIXES.length)];

  let title = prefix ? `${prefix} ${word1}` : word1;
  if (random() > 0.5) title += ` ${word2}`;
  if (suffix) title += ` ${suffix}`;

  return title;
}

// Generate realistic person name
const FIRST_NAMES = [
  'James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Christopher',
  'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen',
  'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kenneth',
  'Emma', 'Olivia', 'Ava', 'Isabella', 'Sophia', 'Mia', 'Charlotte', 'Amelia', 'Harper', 'Evelyn',
];
const LAST_NAMES = [
  'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
  'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
  'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
  'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
];

function generatePersonName() {
  const first = FIRST_NAMES[Math.floor(random() * FIRST_NAMES.length)];
  const last = LAST_NAMES[Math.floor(random() * LAST_NAMES.length)];
  return `${first} ${last}`;
}

// Generate birth year with realistic distribution
function generateBirthYear() {
  // Most active professionals born 1940-2000
  const base = random();
  const weighted = Math.pow(base, 0.8);
  return Math.floor(1920 + weighted * 85); // 1920-2005
}

// Generate rating with normal distribution around 6.5
function generateRating() {
  const rating = normalRandom(6.5, 1.5);
  return Math.round(Math.max(1.0, Math.min(10.0, rating)) * 10) / 10;
}

// Generate vote count with exponential distribution
function generateVotes() {
  const base = random();
  const exp = Math.pow(base, 0.3); // Heavy tail towards lower values
  return Math.floor(exp * 1000000) + 10;
}

// Generate 1-3 genres
function generateGenres() {
  const count = random() < 0.3 ? 1 : random() < 0.7 ? 2 : 3;
  const selected = new Set();
  while (selected.size < count) {
    selected.add(weightedRandom(GENRES));
  }
  return Array.from(selected);
}

async function writeParquet(path, columnData, rowGroupSize = ROW_GROUP_SIZE) {
  const buffer = parquetWriteBuffer({ columnData, rowGroupSize });
  await fs.mkdir(join(OUTPUT_DIR, path.split('/').slice(0, -1).join('/')), { recursive: true });
  await fs.writeFile(join(OUTPUT_DIR, path), Buffer.from(buffer));
  const rowCount = columnData[0].data.length;
  const rowGroups = Math.ceil(rowCount / rowGroupSize);
  console.log(`  Wrote ${path}: ${buffer.byteLength.toLocaleString()} bytes (${rowCount.toLocaleString()} rows, ${rowGroups} row groups)`);
  return buffer.byteLength;
}

function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

// =============================================================================
// Data Generation
// =============================================================================

async function generateTitles() {
  console.log(`\nGenerating ${TITLE_COUNT.toLocaleString()} titles...`);
  const startTime = Date.now();

  const titles = [];

  for (let i = 0; i < TITLE_COUNT; i++) {
    const titleType = weightedRandom(TITLE_TYPE_DISTRIBUTION);
    const year = generateYear();
    const rating = generateRating();
    const votes = generateVotes();
    const genres = generateGenres();

    titles.push({
      tconst: `tt${String(i).padStart(7, '0')}`,
      titleType,
      primaryTitle: generateTitle(i),
      startYear: year,
      endYear: titleType === 'tvSeries' ? (random() > 0.3 ? year + Math.floor(random() * 10) : null) : null,
      runtimeMinutes: titleType === 'movie' ? 80 + Math.floor(random() * 80) :
                     titleType === 'tvEpisode' ? 20 + Math.floor(random() * 40) :
                     30 + Math.floor(random() * 60),
      genres,
      averageRating: rating,
      numVotes: votes,
      isAdult: random() < 0.05,
    });

    if ((i + 1) % 100000 === 0) {
      console.log(`    Generated ${(i + 1).toLocaleString()} titles...`);
    }
  }

  console.log(`  Generation took ${formatDuration(Date.now() - startTime)}`);

  // Sort by titleType for row-group statistics pushdown
  console.log('  Sorting by titleType...');
  titles.sort((a, b) => a.titleType.localeCompare(b.titleType));

  // Log distribution
  const typeCounts = {};
  for (const t of titles) {
    typeCounts[t.titleType] = (typeCounts[t.titleType] || 0) + 1;
  }
  console.log('  Title type distribution:');
  for (const [type, count] of Object.entries(typeCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${type}: ${count.toLocaleString()} (${(count / TITLE_COUNT * 100).toFixed(1)}%)`);
  }

  // Write parquet
  console.log('  Writing parquet...');
  await writeParquet('titles.parquet', [
    { name: '$id', type: 'STRING', data: titles.map(t => `title:${t.tconst}`) },
    { name: '$index_tconst', type: 'STRING', data: titles.map(t => t.tconst) },
    { name: '$index_titleType', type: 'STRING', data: titles.map(t => t.titleType) },
    { name: '$index_startYear', type: 'INT32', data: titles.map(t => t.startYear) },
    { name: '$index_averageRating', type: 'DOUBLE', data: titles.map(t => t.averageRating) },
    { name: '$index_numVotes', type: 'INT32', data: titles.map(t => t.numVotes) },
    { name: 'name', type: 'STRING', data: titles.map(t => t.primaryTitle) },
    { name: '$data', type: 'STRING', data: titles.map(t => JSON.stringify(t)) },
  ]);

  console.log(`  Titles complete in ${formatDuration(Date.now() - startTime)}`);
  return titles;
}

async function generatePeople() {
  console.log(`\nGenerating ${PEOPLE_COUNT.toLocaleString()} people...`);
  const startTime = Date.now();

  const people = [];

  for (let i = 0; i < PEOPLE_COUNT; i++) {
    const birthYear = generateBirthYear();
    const profession = weightedRandom(PROFESSIONS);
    const deathYear = birthYear < 1950 && random() < 0.5 ? birthYear + 50 + Math.floor(random() * 40) : null;

    people.push({
      nconst: `nm${String(i).padStart(7, '0')}`,
      primaryName: generatePersonName(),
      birthYear,
      deathYear,
      primaryProfession: profession,
      knownForTitles: [], // Will be populated later if needed
    });

    if ((i + 1) % 100000 === 0) {
      console.log(`    Generated ${(i + 1).toLocaleString()} people...`);
    }
  }

  console.log(`  Generation took ${formatDuration(Date.now() - startTime)}`);

  // Sort by nconst
  console.log('  Sorting by nconst...');
  people.sort((a, b) => a.nconst.localeCompare(b.nconst));

  // Log distribution
  const profCounts = {};
  for (const p of people) {
    profCounts[p.primaryProfession] = (profCounts[p.primaryProfession] || 0) + 1;
  }
  console.log('  Profession distribution:');
  for (const [prof, count] of Object.entries(profCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${prof}: ${count.toLocaleString()} (${(count / PEOPLE_COUNT * 100).toFixed(1)}%)`);
  }

  // Write parquet
  console.log('  Writing parquet...');
  await writeParquet('people.parquet', [
    { name: '$id', type: 'STRING', data: people.map(p => `person:${p.nconst}`) },
    { name: '$index_nconst', type: 'STRING', data: people.map(p => p.nconst) },
    { name: '$index_birthYear', type: 'INT32', data: people.map(p => p.birthYear) },
    { name: '$index_primaryProfession', type: 'STRING', data: people.map(p => p.primaryProfession) },
    { name: 'name', type: 'STRING', data: people.map(p => p.primaryName) },
    { name: '$data', type: 'STRING', data: people.map(p => JSON.stringify(p)) },
  ]);

  console.log(`  People complete in ${formatDuration(Date.now() - startTime)}`);
  return people;
}

async function generateCast(titleCount, peopleCount) {
  console.log(`\nGenerating ${CAST_COUNT.toLocaleString()} cast entries...`);
  const startTime = Date.now();

  const cast = [];

  // Track which titles have how many cast members for realistic ordering
  const titleCastCounts = new Map();

  for (let i = 0; i < CAST_COUNT; i++) {
    // Slightly favor popular titles (lower IDs get more cast)
    const titleIdx = Math.floor(Math.pow(random(), 0.8) * titleCount);
    const personIdx = Math.floor(random() * peopleCount);

    const tconst = `tt${String(titleIdx).padStart(7, '0')}`;
    const nconst = `nm${String(personIdx).padStart(7, '0')}`;

    // Increment ordering for this title
    const currentCount = titleCastCounts.get(tconst) || 0;
    titleCastCounts.set(tconst, currentCount + 1);

    const category = weightedRandom(CAST_CATEGORIES);
    const characters = category === 'actor' || category === 'actress'
      ? [`Character ${Math.floor(random() * 1000)}`]
      : null;

    cast.push({
      tconst,
      nconst,
      ordering: currentCount + 1,
      category,
      job: category === 'director' ? 'director' :
           category === 'writer' ? 'screenplay' : null,
      characters,
    });

    if ((i + 1) % 500000 === 0) {
      console.log(`    Generated ${(i + 1).toLocaleString()} cast entries...`);
    }
  }

  console.log(`  Generation took ${formatDuration(Date.now() - startTime)}`);

  // Sort by tconst for row-group statistics pushdown
  console.log('  Sorting by tconst...');
  cast.sort((a, b) => a.tconst.localeCompare(b.tconst));

  // Log distribution
  const catCounts = {};
  for (const c of cast) {
    catCounts[c.category] = (catCounts[c.category] || 0) + 1;
  }
  console.log('  Category distribution:');
  for (const [cat, count] of Object.entries(catCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${cat}: ${count.toLocaleString()} (${(count / CAST_COUNT * 100).toFixed(1)}%)`);
  }

  // Write parquet
  console.log('  Writing parquet...');
  await writeParquet('cast.parquet', [
    { name: '$id', type: 'STRING', data: cast.map((c, i) => `cast:${i}`) },
    { name: '$index_tconst', type: 'STRING', data: cast.map(c => c.tconst) },
    { name: '$index_nconst', type: 'STRING', data: cast.map(c => c.nconst) },
    { name: '$index_category', type: 'STRING', data: cast.map(c => c.category) },
    { name: '$index_ordering', type: 'INT32', data: cast.map(c => c.ordering) },
    { name: '$data', type: 'STRING', data: cast.map(c => JSON.stringify(c)) },
  ]);

  console.log(`  Cast complete in ${formatDuration(Date.now() - startTime)}`);
  return cast;
}

// =============================================================================
// Main
// =============================================================================

const totalStart = Date.now();

console.log('='.repeat(70));
console.log('Scale IMDB Dataset to 1M+ Rows');
console.log('='.repeat(70));
console.log(`Output: ${OUTPUT_DIR}`);
console.log(`Row group size: ${ROW_GROUP_SIZE.toLocaleString()}`);
console.log(`Targets: ${TITLE_COUNT.toLocaleString()} titles, ${PEOPLE_COUNT.toLocaleString()} people, ${CAST_COUNT.toLocaleString()} cast`);
console.log('='.repeat(70));

await fs.mkdir(OUTPUT_DIR, { recursive: true });

await generateTitles();
await generatePeople();
await generateCast(TITLE_COUNT, PEOPLE_COUNT);

// Summary
console.log('\n' + '='.repeat(70));
console.log('Summary');
console.log('='.repeat(70));

const files = await fs.readdir(OUTPUT_DIR);
let totalSize = 0;
for (const f of files.filter(f => f.endsWith('.parquet'))) {
  const stat = await fs.stat(join(OUTPUT_DIR, f));
  console.log(`  ${f}: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);
  totalSize += stat.size;
}

console.log('='.repeat(70));
console.log(`Total size: ${(totalSize / 1024 / 1024).toFixed(2)} MB`);
console.log(`Total time: ${formatDuration(Date.now() - totalStart)}`);
console.log('='.repeat(70));
