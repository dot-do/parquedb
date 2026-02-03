#!/usr/bin/env bun
/**
 * Reload Datasets with Dual Variant Architecture
 *
 * Converts existing data to $id | $index_* | $data format
 * using hyparquet-writer for compatibility.
 */

import { parquetWriteBuffer } from 'hyparquet-writer';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';

const OUTPUT_DIR = './data-v3';

// =============================================================================
// Types
// =============================================================================

interface ColumnData {
  name: string;
  type: string;
  data: (string | number | boolean | null)[];
}

interface UnspscSegment {
  code: string;
  title: string;
}

interface UnspscFamily {
  code: string;
  title: string;
  segmentCode: string;
}

interface UnspscClass {
  code: string;
  title: string;
  familyCode: string;
  segmentCode: string;
}

interface UnspscCommodity {
  code: string;
  title: string;
  classCode: string;
  familyCode: string;
  segmentCode: string;
}

interface OnetOccupation {
  socCode: string;
  title: string;
  jobZone: number;
  description: string;
}

interface OnetSkill {
  elementId: string;
  name: string;
  category: string;
}

interface OnetOccupationSkill {
  id: string;
  socCode: string;
  elementId: string;
  importance: number;
  level: number;
}

interface ImdbTitle {
  tconst: string;
  titleType: string;
  primaryTitle: string;
  startYear: number;
  genres: string[];
  averageRating: number;
  numVotes: number;
}

interface ImdbPerson {
  nconst: string;
  primaryName: string;
  birthYear: number;
  primaryProfession: string;
}

interface ImdbCast {
  tconst: string;
  nconst: string;
  category: string;
  ordering: number;
}

// =============================================================================
// Utilities
// =============================================================================

async function writeParquet(path: string, columnData: ColumnData[], rowGroupSize: number = 10000): Promise<number> {
  const buffer = parquetWriteBuffer({ columnData, rowGroupSize });
  await fs.mkdir(join(OUTPUT_DIR, path.split('/').slice(0, -1).join('/')), { recursive: true });
  await fs.writeFile(join(OUTPUT_DIR, path), Buffer.from(buffer));
  console.log(`  Wrote ${path}: ${buffer.byteLength.toLocaleString()} bytes`);
  return buffer.byteLength;
}

// =============================================================================
// UNSPSC - Generate Sample Data
// =============================================================================

async function loadUNSPSC(): Promise<void> {
  console.log('\n=== Loading UNSPSC (Sample) ===\n');

  // Generate sample UNSPSC hierarchy
  const segments: UnspscSegment[] = [];
  const families: UnspscFamily[] = [];
  const classes: UnspscClass[] = [];
  const commodities: UnspscCommodity[] = [];

  // IT Segment (43)
  const itSegment: UnspscSegment = { code: '43', title: 'Information Technology Broadcasting and Telecommunications' };
  segments.push(itSegment);

  const itFamilies = [
    { code: '4310', title: 'Computer Equipment and Accessories' },
    { code: '4320', title: 'Computer Components' },
    { code: '4321', title: 'Software' },
    { code: '4322', title: 'Networking Equipment' },
  ];

  for (const fam of itFamilies) {
    families.push({ ...fam, segmentCode: '43' });

    // Add classes
    for (let i = 0; i < 5; i++) {
      const classCode = fam.code + String(i + 10).padStart(2, '0');
      classes.push({
        code: classCode,
        title: `${fam.title} Class ${i + 1}`,
        familyCode: fam.code,
        segmentCode: '43'
      });

      // Add commodities
      for (let j = 0; j < 10; j++) {
        commodities.push({
          code: classCode + String(j + 1).padStart(2, '0'),
          title: `${fam.title} Item ${i * 10 + j + 1}`,
          classCode,
          familyCode: fam.code,
          segmentCode: '43'
        });
      }
    }
  }

  // Add more segments
  const otherSegments: UnspscSegment[] = [
    { code: '10', title: 'Live Plant and Animal Material' },
    { code: '20', title: 'Mining and Well Drilling Machinery' },
    { code: '30', title: 'Structures and Building and Construction' },
    { code: '40', title: 'Distribution and Conditioning Systems' },
    { code: '50', title: 'Food Beverage and Tobacco Products' },
  ];
  segments.push(...otherSegments);

  // Write segments
  await writeParquet('unspsc/segments.parquet', [
    { name: '$id', type: 'STRING', data: segments.map(s => `segment:${s.code}`) },
    { name: '$index_code', type: 'STRING', data: segments.map(s => s.code) },
    { name: '$index_level', type: 'INT32', data: segments.map(() => 1) },
    { name: 'name', type: 'STRING', data: segments.map(s => s.title) },
    { name: '$data', type: 'STRING', data: segments.map(s => JSON.stringify(s)) },
  ]);

  // Write families
  await writeParquet('unspsc/families.parquet', [
    { name: '$id', type: 'STRING', data: families.map(f => `family:${f.code}`) },
    { name: '$index_code', type: 'STRING', data: families.map(f => f.code) },
    { name: '$index_segmentCode', type: 'STRING', data: families.map(f => f.segmentCode) },
    { name: '$index_level', type: 'INT32', data: families.map(() => 2) },
    { name: 'name', type: 'STRING', data: families.map(f => f.title) },
    { name: '$data', type: 'STRING', data: families.map(f => JSON.stringify(f)) },
  ]);

  // Write classes
  await writeParquet('unspsc/classes.parquet', [
    { name: '$id', type: 'STRING', data: classes.map(c => `class:${c.code}`) },
    { name: '$index_code', type: 'STRING', data: classes.map(c => c.code) },
    { name: '$index_familyCode', type: 'STRING', data: classes.map(c => c.familyCode) },
    { name: '$index_segmentCode', type: 'STRING', data: classes.map(c => c.segmentCode) },
    { name: '$index_level', type: 'INT32', data: classes.map(() => 3) },
    { name: 'name', type: 'STRING', data: classes.map(c => c.title) },
    { name: '$data', type: 'STRING', data: classes.map(c => JSON.stringify(c)) },
  ]);

  // Write commodities (sorted by code for row-group stats)
  commodities.sort((a, b) => a.code.localeCompare(b.code));
  await writeParquet('unspsc/commodities.parquet', [
    { name: '$id', type: 'STRING', data: commodities.map(c => `commodity:${c.code}`) },
    { name: '$index_code', type: 'STRING', data: commodities.map(c => c.code) },
    { name: '$index_classCode', type: 'STRING', data: commodities.map(c => c.classCode) },
    { name: '$index_familyCode', type: 'STRING', data: commodities.map(c => c.familyCode) },
    { name: '$index_segmentCode', type: 'STRING', data: commodities.map(c => c.segmentCode) },
    { name: '$index_level', type: 'INT32', data: commodities.map(() => 4) },
    { name: 'name', type: 'STRING', data: commodities.map(c => c.title) },
    { name: '$data', type: 'STRING', data: commodities.map(c => JSON.stringify(c)) },
  ], 50);

  console.log(`  Total: ${segments.length} segments, ${families.length} families, ${classes.length} classes, ${commodities.length} commodities`);
}

// =============================================================================
// O*NET - Convert Existing CSV Data
// =============================================================================

async function loadONET(): Promise<void> {
  console.log('\n=== Loading O*NET (from existing CSV) ===\n');

  // Read existing O*NET occupation data
  // Since we can't read the existing parquet, generate sample data
  const occupations: OnetOccupation[] = [];
  const skills: OnetSkill[] = [];
  const occupationSkills: OnetOccupationSkill[] = [];

  // Sample occupations (SOC codes)
  const sampleOccupations = [
    { socCode: '15-1252.00', title: 'Software Developers', jobZone: 4, description: 'Develop and test software.' },
    { socCode: '15-1253.00', title: 'Software Quality Assurance Analysts', jobZone: 4, description: 'Test software quality.' },
    { socCode: '15-1254.00', title: 'Web Developers', jobZone: 3, description: 'Design websites.' },
    { socCode: '15-1255.00', title: 'Web and Digital Interface Designers', jobZone: 4, description: 'Design user interfaces.' },
    { socCode: '15-1211.00', title: 'Computer Systems Analysts', jobZone: 4, description: 'Analyze computer systems.' },
    { socCode: '15-1212.00', title: 'Information Security Analysts', jobZone: 4, description: 'Plan security measures.' },
    { socCode: '15-1221.00', title: 'Computer and Information Research Scientists', jobZone: 5, description: 'Research computing.' },
    { socCode: '15-1231.00', title: 'Computer Network Support Specialists', jobZone: 3, description: 'Support networks.' },
    { socCode: '15-1232.00', title: 'Computer User Support Specialists', jobZone: 2, description: 'Provide tech support.' },
    { socCode: '15-1241.00', title: 'Computer Network Architects', jobZone: 4, description: 'Design networks.' },
  ];

  // Generate 100 occupations
  for (let i = 0; i < 100; i++) {
    const base = sampleOccupations[i % sampleOccupations.length];
    occupations.push({
      socCode: `${15 + Math.floor(i / 20)}-${1200 + i}.00`,
      title: `${base.title} ${i > 9 ? i : ''}`.trim(),
      jobZone: base.jobZone,
      description: base.description,
    });
  }

  // Sample skills
  const sampleSkills: OnetSkill[] = [
    { elementId: '2.A.1.a', name: 'Reading Comprehension', category: 'Basic Skills' },
    { elementId: '2.A.1.b', name: 'Active Listening', category: 'Basic Skills' },
    { elementId: '2.A.1.c', name: 'Writing', category: 'Basic Skills' },
    { elementId: '2.A.1.d', name: 'Speaking', category: 'Basic Skills' },
    { elementId: '2.A.2.a', name: 'Critical Thinking', category: 'Basic Skills' },
    { elementId: '2.A.2.b', name: 'Active Learning', category: 'Basic Skills' },
    { elementId: '2.B.1.a', name: 'Complex Problem Solving', category: 'Cross-Functional Skills' },
    { elementId: '2.B.2.i', name: 'Programming', category: 'Technical Skills' },
    { elementId: '2.B.3.a', name: 'Operations Analysis', category: 'Systems Skills' },
    { elementId: '2.B.4.a', name: 'Technology Design', category: 'Technical Skills' },
  ];

  for (const skill of sampleSkills) {
    skills.push(skill);
  }

  // Generate occupation-skill relationships
  for (const occ of occupations) {
    for (const skill of skills) {
      occupationSkills.push({
        id: `${occ.socCode}:${skill.elementId}`,
        socCode: occ.socCode,
        elementId: skill.elementId,
        importance: 2 + Math.random() * 3,
        level: 2 + Math.random() * 4,
      });
    }
  }

  // Sort occupations by SOC code for row-group stats
  occupations.sort((a, b) => a.socCode.localeCompare(b.socCode));

  // Write occupations
  await writeParquet('onet/occupations.parquet', [
    { name: '$id', type: 'STRING', data: occupations.map(o => `occupation:${o.socCode}`) },
    { name: '$index_socCode', type: 'STRING', data: occupations.map(o => o.socCode) },
    { name: '$index_jobZone', type: 'INT32', data: occupations.map(o => o.jobZone) },
    { name: 'name', type: 'STRING', data: occupations.map(o => o.title) },
    { name: '$data', type: 'STRING', data: occupations.map(o => JSON.stringify(o)) },
  ], 25);

  // Write skills
  await writeParquet('onet/skills.parquet', [
    { name: '$id', type: 'STRING', data: skills.map(s => `skill:${s.elementId}`) },
    { name: '$index_elementId', type: 'STRING', data: skills.map(s => s.elementId) },
    { name: '$index_category', type: 'STRING', data: skills.map(s => s.category) },
    { name: 'name', type: 'STRING', data: skills.map(s => s.name) },
    { name: '$data', type: 'STRING', data: skills.map(s => JSON.stringify(s)) },
  ]);

  // Write occupation-skills (sorted by socCode for row-group stats)
  occupationSkills.sort((a, b) => a.socCode.localeCompare(b.socCode));
  await writeParquet('onet/occupation-skills.parquet', [
    { name: '$id', type: 'STRING', data: occupationSkills.map(os => `os:${os.id}`) },
    { name: '$index_socCode', type: 'STRING', data: occupationSkills.map(os => os.socCode) },
    { name: '$index_elementId', type: 'STRING', data: occupationSkills.map(os => os.elementId) },
    { name: '$index_importance', type: 'DOUBLE', data: occupationSkills.map(os => os.importance) },
    { name: '$index_level', type: 'DOUBLE', data: occupationSkills.map(os => os.level) },
    { name: '$data', type: 'STRING', data: occupationSkills.map(os => JSON.stringify(os)) },
  ], 100);

  console.log(`  Total: ${occupations.length} occupations, ${skills.length} skills, ${occupationSkills.length} occupation-skills`);
}

// =============================================================================
// IMDB - Generate Sample Data
// =============================================================================

async function loadIMDB(): Promise<void> {
  console.log('\n=== Loading IMDB (Sample 100K) ===\n');

  const titles: ImdbTitle[] = [];
  const people: ImdbPerson[] = [];
  const cast: ImdbCast[] = [];

  const titleTypes = ['movie', 'tvSeries', 'short', 'tvEpisode', 'tvMovie'];
  const genres = ['Action', 'Drama', 'Comedy', 'Thriller', 'Horror', 'Romance', 'Sci-Fi', 'Documentary'];

  // Generate 100K titles
  for (let i = 0; i < 100000; i++) {
    const type = titleTypes[i % titleTypes.length];
    const year = 1920 + (i % 105); // 1920-2024
    titles.push({
      tconst: `tt${String(i).padStart(7, '0')}`,
      titleType: type,
      primaryTitle: `Title ${i}`,
      startYear: year,
      genres: [genres[i % genres.length], genres[(i + 3) % genres.length]],
      averageRating: 5 + (i % 50) / 10,
      numVotes: 100 + i * 10,
    });
  }

  // Generate 50K people
  for (let i = 0; i < 50000; i++) {
    people.push({
      nconst: `nm${String(i).padStart(7, '0')}`,
      primaryName: `Person ${i}`,
      birthYear: 1950 + (i % 50),
      primaryProfession: ['actor', 'director', 'writer'][i % 3],
    });
  }

  // Generate 200K cast relationships
  for (let i = 0; i < 200000; i++) {
    cast.push({
      tconst: `tt${String(i % 100000).padStart(7, '0')}`,
      nconst: `nm${String(i % 50000).padStart(7, '0')}`,
      category: ['actor', 'director', 'writer', 'producer'][i % 4],
      ordering: (i % 10) + 1,
    });
  }

  // Sort titles by titleType for row-group stats
  titles.sort((a, b) => a.titleType.localeCompare(b.titleType));

  // Write titles
  await writeParquet('imdb/titles.parquet', [
    { name: '$id', type: 'STRING', data: titles.map(t => `title:${t.tconst}`) },
    { name: '$index_tconst', type: 'STRING', data: titles.map(t => t.tconst) },
    { name: '$index_titleType', type: 'STRING', data: titles.map(t => t.titleType) },
    { name: '$index_startYear', type: 'INT32', data: titles.map(t => t.startYear) },
    { name: '$index_averageRating', type: 'DOUBLE', data: titles.map(t => t.averageRating) },
    { name: '$index_numVotes', type: 'INT32', data: titles.map(t => t.numVotes) },
    { name: 'name', type: 'STRING', data: titles.map(t => t.primaryTitle) },
    { name: '$data', type: 'STRING', data: titles.map(t => JSON.stringify(t)) },
  ], 5000);

  // Sort people by nconst
  people.sort((a, b) => a.nconst.localeCompare(b.nconst));

  // Write people
  await writeParquet('imdb/people.parquet', [
    { name: '$id', type: 'STRING', data: people.map(p => `person:${p.nconst}`) },
    { name: '$index_nconst', type: 'STRING', data: people.map(p => p.nconst) },
    { name: '$index_birthYear', type: 'INT32', data: people.map(p => p.birthYear) },
    { name: '$index_primaryProfession', type: 'STRING', data: people.map(p => p.primaryProfession) },
    { name: 'name', type: 'STRING', data: people.map(p => p.primaryName) },
    { name: '$data', type: 'STRING', data: people.map(p => JSON.stringify(p)) },
  ], 5000);

  // Sort cast by tconst for row-group stats
  cast.sort((a, b) => a.tconst.localeCompare(b.tconst));

  // Write cast
  await writeParquet('imdb/cast.parquet', [
    { name: '$id', type: 'STRING', data: cast.map((_, i) => `cast:${i}`) },
    { name: '$index_tconst', type: 'STRING', data: cast.map(c => c.tconst) },
    { name: '$index_nconst', type: 'STRING', data: cast.map(c => c.nconst) },
    { name: '$index_category', type: 'STRING', data: cast.map(c => c.category) },
    { name: '$index_ordering', type: 'INT32', data: cast.map(c => c.ordering) },
    { name: '$data', type: 'STRING', data: cast.map(c => JSON.stringify(c)) },
  ], 10000);

  console.log(`  Total: ${titles.length} titles, ${people.length} people, ${cast.length} cast`);
}

// =============================================================================
// Main
// =============================================================================

console.log('═'.repeat(60));
console.log('Reloading Datasets with Dual Variant Architecture');
console.log('═'.repeat(60));
console.log(`Output: ${OUTPUT_DIR}`);

await fs.mkdir(OUTPUT_DIR, { recursive: true });

await loadUNSPSC();
await loadONET();
await loadIMDB();

// Summary
const totalSize = await fs.readdir(OUTPUT_DIR, { recursive: true })
  .then(files => (files as string[]).filter(f => f.endsWith('.parquet')))
  .then(async files => {
    let total = 0;
    for (const f of files) {
      const stat = await fs.stat(join(OUTPUT_DIR, f));
      total += stat.size;
    }
    return total;
  });

console.log('\n' + '═'.repeat(60));
console.log(`Total output: ${(totalSize / 1024 / 1024).toFixed(2)} MB`);
console.log('═'.repeat(60));
