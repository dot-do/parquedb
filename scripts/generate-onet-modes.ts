#!/usr/bin/env npx tsx
/**
 * Generate O*NET Dataset in All Storage Modes
 *
 * Creates 4 variants for benchmarking:
 * - columnar-only: Native columns, no row store
 * - columnar-row:  Native columns + data JSON blob
 * - row-only:      Just $id, $type, name, data
 * - row-index:     data + $index_* shredded columns
 *
 * Usage:
 *   npx tsx scripts/generate-onet-modes.ts                    # All modes
 *   npx tsx scripts/generate-onet-modes.ts --mode=columnar-only
 *   npx tsx scripts/generate-onet-modes.ts --real             # Use real O*NET data
 */

import { existsSync, createReadStream } from 'node:fs'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { ParquetReader } from '../src/parquet/reader'
import { FsBackend } from '../src/storage/FsBackend'
import {
  type StorageMode,
  type EntitySchema,
  generateColumns,
  writeParquetFile,
  getOutputDir,
  formatBytes,
  formatNumber,
} from './lib/storage-modes'

// =============================================================================
// Configuration
// =============================================================================

const BASE_DIR = 'data-v3'
const ONET_DIR = 'data/onet'
const ALL_MODES: StorageMode[] = ['columnar-only', 'columnar-row', 'row-only', 'row-index']

// Occupation schema
const OCCUPATION_SCHEMA: EntitySchema = {
  columns: [
    { name: 'name', type: 'STRING' },
    { name: 'code', type: 'STRING', indexed: true },
    { name: 'description', type: 'STRING' },
    { name: 'jobZone', type: 'INT32', indexed: true },
  ],
}

// Skill/Ability/Knowledge schema
const ELEMENT_SCHEMA: EntitySchema = {
  columns: [
    { name: 'name', type: 'STRING' },
    { name: 'elementId', type: 'STRING', indexed: true },
    { name: 'description', type: 'STRING' },
    { name: 'category', type: 'STRING', indexed: true },
  ],
}

// =============================================================================
// Types
// =============================================================================

interface Occupation {
  code: string
  name: string
  description: string
  jobZone: number | null
}

interface Element {
  elementId: string
  name: string
  description: string
  category: string
}

interface GenerationStats {
  mode: StorageMode
  collection: string
  file: string
  rows: number
  size: number
  bytesPerRow: number
}

// =============================================================================
// Data Loading
// =============================================================================

async function loadParquet<T>(filePath: string): Promise<T[]> {
  const storage = new FsBackend(process.cwd())
  const reader = new ParquetReader({ storage })
  return reader.read<T>(filePath)
}

interface OccupationRow {
  'O*NET-SOC Code': string
  'Title': string
  'Description': string
}

interface JobZoneRow {
  'O*NET-SOC Code': string
  'Job Zone': string
}

interface ContentModelRow {
  'Element ID': string
  'Element Name': string
  'Description': string
}

async function loadRealOnetData(): Promise<{ occupations: Occupation[]; skills: Element[]; abilities: Element[]; knowledge: Element[] }> {
  console.log('\nLoading real O*NET data...')

  // Check if data exists
  const occupationPath = join(ONET_DIR, 'Occupation Data.parquet')
  const contentModelPath = join(ONET_DIR, 'Content Model Reference.parquet')
  const jobZonePath = join(ONET_DIR, 'Job Zones.parquet')

  if (!existsSync(occupationPath)) {
    throw new Error(`O*NET data not found at ${occupationPath}. Run ETL first or use synthetic data.`)
  }

  // Load occupations
  const occRows = await loadParquet<OccupationRow>(occupationPath)
  console.log(`  Loaded ${occRows.length} occupations`)

  // Load job zones
  const jobZoneMap = new Map<string, number>()
  if (existsSync(jobZonePath)) {
    const jzRows = await loadParquet<JobZoneRow>(jobZonePath)
    for (const row of jzRows) {
      jobZoneMap.set(row['O*NET-SOC Code'], parseInt(row['Job Zone']))
    }
    console.log(`  Loaded ${jzRows.length} job zone mappings`)
  }

  const occupations: Occupation[] = occRows.map(row => ({
    code: row['O*NET-SOC Code'],
    name: row['Title'],
    description: row['Description'],
    jobZone: jobZoneMap.get(row['O*NET-SOC Code']) ?? null,
  }))

  // Load content model (skills, abilities, knowledge)
  const contentModel = await loadParquet<ContentModelRow>(contentModelPath)
  console.log(`  Loaded ${contentModel.length} content model elements`)

  const skills: Element[] = []
  const abilities: Element[] = []
  const knowledge: Element[] = []

  for (const row of contentModel) {
    const id = row['Element ID']
    const element: Element = {
      elementId: id,
      name: row['Element Name'],
      description: row['Description'],
      category: id.split('.')[0] || 'unknown',
    }

    if (id.startsWith('2.A.') || id.startsWith('2.B.')) {
      element.category = id.startsWith('2.A.') ? 'Basic' : 'Cross-Functional'
      skills.push(element)
    } else if (id.startsWith('1.A.')) {
      element.category = 'Cognitive'
      abilities.push(element)
    } else if (id.startsWith('2.C.')) {
      element.category = 'Knowledge'
      knowledge.push(element)
    }
  }

  console.log(`  Skills: ${skills.length}, Abilities: ${abilities.length}, Knowledge: ${knowledge.length}`)

  return { occupations, skills, abilities, knowledge }
}

function generateSyntheticData(): { occupations: Occupation[]; skills: Element[]; abilities: Element[]; knowledge: Element[] } {
  console.log('\nGenerating synthetic O*NET data...')

  const jobFamilies = ['Software', 'Healthcare', 'Finance', 'Engineering', 'Education', 'Marketing', 'Legal', 'Operations']
  const levels = ['Entry', 'Junior', 'Mid', 'Senior', 'Lead', 'Manager', 'Director']

  const occupations: Occupation[] = []
  for (let i = 0; i < 1000; i++) {
    const family = jobFamilies[i % jobFamilies.length]
    const level = levels[i % levels.length]
    occupations.push({
      code: `${15 + (i % 20)}-${String(1000 + i).slice(1)}.00`,
      name: `${level} ${family} Specialist`,
      description: `Performs ${family.toLowerCase()}-related tasks at the ${level.toLowerCase()} level.`,
      jobZone: 1 + (i % 5),
    })
  }

  const skillCategories = ['Basic', 'Cross-Functional']
  const skills: Element[] = []
  for (let i = 0; i < 35; i++) {
    const cat = skillCategories[i % 2]
    skills.push({
      elementId: `2.${cat === 'Basic' ? 'A' : 'B'}.${i + 1}`,
      name: `${cat} Skill ${i + 1}`,
      description: `Description for ${cat.toLowerCase()} skill ${i + 1}`,
      category: cat,
    })
  }

  const abilityTypes = ['Cognitive', 'Psychomotor', 'Physical', 'Sensory']
  const abilities: Element[] = []
  for (let i = 0; i < 52; i++) {
    const type = abilityTypes[i % 4]
    abilities.push({
      elementId: `1.A.${i + 1}`,
      name: `${type} Ability ${i + 1}`,
      description: `Description for ${type.toLowerCase()} ability ${i + 1}`,
      category: type,
    })
  }

  const knowledgeAreas = ['Business', 'Technical', 'Social', 'Scientific']
  const knowledge: Element[] = []
  for (let i = 0; i < 33; i++) {
    const area = knowledgeAreas[i % 4]
    knowledge.push({
      elementId: `2.C.${i + 1}`,
      name: `${area} Knowledge ${i + 1}`,
      description: `Description for ${area.toLowerCase()} knowledge area ${i + 1}`,
      category: area,
    })
  }

  console.log(`  Generated: ${occupations.length} occupations, ${skills.length} skills, ${abilities.length} abilities, ${knowledge.length} knowledge`)

  return { occupations, skills, abilities, knowledge }
}

// =============================================================================
// Generation
// =============================================================================

async function generateModeForCollection<T extends Record<string, unknown>>(
  items: T[],
  schema: EntitySchema,
  mode: StorageMode,
  collection: string,
  getEntityId: (e: T) => string,
  getEntityType: (e: T) => string,
  getEntityName: (e: T) => string
): Promise<GenerationStats> {
  const outputDir = getOutputDir(BASE_DIR, 'onet', mode)
  const outputPath = join(outputDir, `${collection}.parquet`)

  const columns = generateColumns(items, schema, mode, getEntityId, getEntityType, getEntityName)

  const result = await writeParquetFile(outputPath, columns, 5000)

  return {
    mode,
    collection,
    file: outputPath,
    rows: result.rows,
    size: result.size,
    bytesPerRow: result.rows > 0 ? Math.round(result.size / result.rows) : 0,
  }
}

async function generateMode(
  data: { occupations: Occupation[]; skills: Element[]; abilities: Element[]; knowledge: Element[] },
  mode: StorageMode
): Promise<GenerationStats[]> {
  console.log(`\nGenerating ${mode}...`)

  const stats: GenerationStats[] = []

  // Generate occupations
  const occStats = await generateModeForCollection(
    data.occupations,
    OCCUPATION_SCHEMA,
    mode,
    'occupations',
    (o) => `occupation:${o.code}`,
    () => 'Occupation',
    (o) => o.name
  )
  stats.push(occStats)
  console.log(`  occupations: ${formatBytes(occStats.size)} (${occStats.bytesPerRow} bytes/row)`)

  // Generate skills
  const skillStats = await generateModeForCollection(
    data.skills,
    ELEMENT_SCHEMA,
    mode,
    'skills',
    (s) => `skill:${s.elementId}`,
    () => 'Skill',
    (s) => s.name
  )
  stats.push(skillStats)
  console.log(`  skills: ${formatBytes(skillStats.size)} (${skillStats.bytesPerRow} bytes/row)`)

  // Generate abilities
  const abilityStats = await generateModeForCollection(
    data.abilities,
    ELEMENT_SCHEMA,
    mode,
    'abilities',
    (a) => `ability:${a.elementId}`,
    () => 'Ability',
    (a) => a.name
  )
  stats.push(abilityStats)
  console.log(`  abilities: ${formatBytes(abilityStats.size)} (${abilityStats.bytesPerRow} bytes/row)`)

  // Generate knowledge
  const knowledgeStats = await generateModeForCollection(
    data.knowledge,
    ELEMENT_SCHEMA,
    mode,
    'knowledge',
    (k) => `knowledge:${k.elementId}`,
    () => 'Knowledge',
    (k) => k.name
  )
  stats.push(knowledgeStats)
  console.log(`  knowledge: ${formatBytes(knowledgeStats.size)} (${knowledgeStats.bytesPerRow} bytes/row)`)

  return stats
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const args = process.argv.slice(2)
  const useReal = args.includes('--real')
  const modeArg = args.find(a => a.startsWith('--mode='))

  const modes: StorageMode[] = modeArg
    ? [modeArg.split('=')[1] as StorageMode]
    : ALL_MODES

  console.log('=== O*NET Storage Mode Generator ===')
  console.log(`Modes: ${modes.join(', ')}`)
  console.log(`Source: ${useReal ? 'Real O*NET data' : 'Synthetic'}`)

  // Load or generate data
  const data = useReal
    ? await loadRealOnetData()
    : generateSyntheticData()

  // Generate each mode
  const allStats: GenerationStats[] = []
  for (const mode of modes) {
    const stats = await generateMode(data, mode)
    allStats.push(...stats)
  }

  // Print summary by mode
  console.log('\n=== Summary by Mode ===')
  for (const mode of modes) {
    const modeStats = allStats.filter(s => s.mode === mode)
    const totalSize = modeStats.reduce((sum, s) => sum + s.size, 0)
    const totalRows = modeStats.reduce((sum, s) => sum + s.rows, 0)
    console.log(`\n${mode}:`)
    console.log(`  Total: ${formatBytes(totalSize)} (${formatNumber(totalRows)} rows)`)
    for (const s of modeStats) {
      console.log(`    ${s.collection}: ${formatBytes(s.size)} (${s.bytesPerRow} bytes/row)`)
    }
  }

  // Calculate overhead
  const columnarOnlyStats = allStats.filter(s => s.mode === 'columnar-only')
  if (columnarOnlyStats.length > 0) {
    const baselineSize = columnarOnlyStats.reduce((sum, s) => sum + s.size, 0)
    console.log('\nStorage overhead vs columnar-only:')
    for (const mode of modes) {
      if (mode === 'columnar-only') continue
      const modeSize = allStats.filter(s => s.mode === mode).reduce((sum, s) => sum + s.size, 0)
      const overhead = ((modeSize / baselineSize - 1) * 100).toFixed(1)
      console.log(`  ${mode}: +${overhead}%`)
    }
  }

  console.log('\nDone!')
}

main().catch(console.error)
