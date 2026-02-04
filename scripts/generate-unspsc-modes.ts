#!/usr/bin/env npx tsx
/**
 * Generate UNSPSC Dataset in All Storage Modes
 *
 * Creates 4 variants for benchmarking:
 * - columnar-only: Native columns, no row store
 * - columnar-row:  Native columns + data JSON blob
 * - row-only:      Just $id, $type, name, data
 * - row-index:     data + $index_* shredded columns
 *
 * Usage:
 *   npx tsx scripts/generate-unspsc-modes.ts                    # All modes, ~70K items
 *   npx tsx scripts/generate-unspsc-modes.ts --mode=columnar-only
 *   npx tsx scripts/generate-unspsc-modes.ts --scale=small      # ~7K items
 *   npx tsx scripts/generate-unspsc-modes.ts --scale=large      # ~200K items
 */

import { promises as fs } from 'node:fs'
import { join } from 'node:path'
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
const ALL_MODES: StorageMode[] = ['columnar-only', 'columnar-row', 'row-only', 'row-index']

// UNSPSC schemas for each level
const SEGMENT_SCHEMA: EntitySchema = {
  columns: [
    { name: 'name', type: 'STRING' },
    { name: 'code', type: 'STRING', indexed: true },
    { name: 'level', type: 'INT32', indexed: true },
  ],
}

const FAMILY_SCHEMA: EntitySchema = {
  columns: [
    { name: 'name', type: 'STRING' },
    { name: 'code', type: 'STRING', indexed: true },
    { name: 'segmentCode', type: 'STRING', indexed: true },
    { name: 'level', type: 'INT32', indexed: true },
  ],
}

const CLASS_SCHEMA: EntitySchema = {
  columns: [
    { name: 'name', type: 'STRING' },
    { name: 'code', type: 'STRING', indexed: true },
    { name: 'familyCode', type: 'STRING', indexed: true },
    { name: 'segmentCode', type: 'STRING', indexed: true },
    { name: 'level', type: 'INT32', indexed: true },
  ],
}

const COMMODITY_SCHEMA: EntitySchema = {
  columns: [
    { name: 'name', type: 'STRING' },
    { name: 'code', type: 'STRING', indexed: true },
    { name: 'classCode', type: 'STRING', indexed: true },
    { name: 'familyCode', type: 'STRING', indexed: true },
    { name: 'segmentCode', type: 'STRING', indexed: true },
    { name: 'level', type: 'INT32', indexed: true },
  ],
}

// =============================================================================
// Types
// =============================================================================

interface Segment {
  code: string
  title: string
  level: number
}

interface Family {
  code: string
  title: string
  segmentCode: string
  level: number
}

interface Class {
  code: string
  title: string
  familyCode: string
  segmentCode: string
  level: number
}

interface Commodity {
  code: string
  title: string
  classCode: string
  familyCode: string
  segmentCode: string
  level: number
}

interface GenerationStats {
  mode: StorageMode
  collection: string
  file: string
  rows: number
  size: number
  bytesPerRow: number
}

interface ScaleConfig {
  families: number
  classes: number
  commodities: number
}

// =============================================================================
// Segment Names - Based on real UNSPSC taxonomy
// =============================================================================

const SEGMENT_NAMES: Record<string, string> = {
  '10': 'Live Plant and Animal Material and Accessories and Supplies',
  '11': 'Mineral and Textile and Inedible Plant and Animal Materials',
  '12': 'Chemicals including Bio Chemicals and Gas Materials',
  '13': 'Resin and Rosin and Rubber and Foam and Film and Elastomeric Materials',
  '14': 'Paper Materials and Products',
  '15': 'Fuels and Fuel Additives and Lubricants and Anti corrosive Materials',
  '20': 'Mining and Well Drilling Machinery and Accessories',
  '21': 'Farming and Fishing and Forestry and Wildlife Machinery and Accessories',
  '22': 'Building and Construction Machinery and Accessories',
  '23': 'Industrial Manufacturing and Processing Machinery and Accessories',
  '24': 'Material Handling and Conditioning and Storage Machinery and Accessories',
  '25': 'Commercial and Military and Private Vehicles and their Accessories and Components',
  '26': 'Power Generation and Distribution Machinery and Accessories',
  '27': 'Tools and General Machinery',
  '30': 'Structures and Building and Construction and Manufacturing Components and Supplies',
  '31': 'Manufacturing Components and Supplies',
  '32': 'Electronic Components and Supplies',
  '39': 'Lighting and Electrical Accessories and Supplies',
  '40': 'Distribution and Conditioning Systems and Equipment and Components',
  '41': 'Laboratory and Measuring and Observing and Testing Equipment',
  '42': 'Medical Equipment and Accessories and Supplies',
  '43': 'Information Technology Broadcasting and Telecommunications',
  '44': 'Office Equipment and Accessories and Supplies',
  '45': 'Printing and Photographic and Audio and Visual Equipment and Supplies',
  '46': 'Defense and Law Enforcement and Security and Safety Equipment and Supplies',
  '47': 'Cleaning Equipment and Supplies',
  '48': 'Service Industry Machinery and Equipment and Supplies',
  '49': 'Sports and Recreational Equipment and Supplies and Accessories',
  '50': 'Food Beverage and Tobacco Products',
  '51': 'Drugs and Pharmaceutical Products',
  '52': 'Domestic Appliances and Supplies and Consumer Electronic Products',
  '53': 'Apparel and Luggage and Personal Care Products',
  '54': 'Timepieces and Jewelry and Gemstone Products',
  '55': 'Published Products',
  '56': 'Furniture and Furnishings',
  '60': 'Musical Instruments and Games and Toys and Arts and Crafts',
  '70': 'Farming and Fishing and Forestry and Wildlife Contracting Services',
  '71': 'Mining and oil and gas services',
  '72': 'Building and Facility Construction and Maintenance Services',
  '73': 'Industrial Production and Manufacturing Services',
  '76': 'Industrial Cleaning Services',
  '77': 'Environmental Services',
  '78': 'Transportation and Storage and Mail Services',
  '80': 'Management and Business Professionals and Administrative Services',
  '81': 'Engineering and Research and Technology Based Services',
  '82': 'Editorial and Design and Graphic and Fine Art Services',
  '83': 'Public Utilities and Public Sector Related Services',
  '84': 'Financial and Insurance Services',
  '85': 'Healthcare Services',
  '86': 'Education and Training Services',
  '90': 'Travel and Food and Lodging and Entertainment Services',
  '91': 'Personal and Domestic Services',
  '92': 'National Defense and Public Order and Security and Safety Services',
  '93': 'Politics and Civic Affairs Services',
  '94': 'Organizations and Clubs',
  '95': 'Land and Buildings and Structures and Thoroughfares',
}

// Name templates
const FAMILY_TEMPLATES = ['Equipment and Supplies', 'Systems and Components', 'Services and Consulting', 'Materials and Products', 'Machinery and Tools', 'Accessories and Parts', 'Processing and Manufacturing', 'Distribution and Storage']
const CLASS_TEMPLATES = ['Standard', 'Industrial', 'Commercial', 'Professional', 'Specialized', 'General Purpose', 'High Performance', 'Heavy Duty', 'Precision', 'Custom']
const COMMODITY_ADJECTIVES = ['Standard', 'Premium', 'Economy', 'Industrial', 'Commercial', 'Professional', 'Heavy Duty', 'Light Duty', 'Portable', 'Stationary', 'Electric', 'Manual', 'Automatic', 'Semi-automatic', 'Digital', 'Analog', 'Wireless', 'Wired', 'Compact', 'Full Size']

// =============================================================================
// Data Generation
// =============================================================================

function generateFamilyName(segmentTitle: string, index: number): string {
  const template = FAMILY_TEMPLATES[index % FAMILY_TEMPLATES.length]
  const words = segmentTitle.split(' and ').slice(0, 2).join(' ')
  return `${words} ${template}`
}

function generateClassName(familyTitle: string, index: number): string {
  const template = CLASS_TEMPLATES[index % CLASS_TEMPLATES.length]
  const words = familyTitle.split(' ').slice(0, 3).join(' ')
  return `${template} ${words}`
}

function generateCommodityName(classTitle: string, index: number): string {
  const adj = COMMODITY_ADJECTIVES[index % COMMODITY_ADJECTIVES.length]
  const words = classTitle.split(' ').slice(0, 4).join(' ')
  return `${adj} ${words} Item ${(index % 100) + 1}`
}

function generateUnspscData(scale: ScaleConfig): { segments: Segment[]; families: Family[]; classes: Class[]; commodities: Commodity[] } {
  console.log('\nGenerating UNSPSC taxonomy...')

  const segments: Segment[] = []
  const families: Family[] = []
  const classes: Class[] = []
  const commodities: Commodity[] = []

  const segmentCodes = Object.keys(SEGMENT_NAMES).sort()

  // Calculate distribution
  const familiesPerSegment = Math.ceil(scale.families / segmentCodes.length)
  const classesPerFamily = Math.ceil(scale.classes / scale.families)
  const commoditiesPerClass = Math.ceil(scale.commodities / scale.classes)

  console.log(`  Target: ${scale.families} families, ${scale.classes} classes, ${scale.commodities} commodities`)
  console.log(`  Distribution: ~${familiesPerSegment} families/segment, ~${classesPerFamily} classes/family, ~${commoditiesPerClass} commodities/class`)

  let familyCount = 0
  let classCount = 0
  let commodityCount = 0

  for (const segmentCode of segmentCodes) {
    const segmentTitle = SEGMENT_NAMES[segmentCode]!
    segments.push({
      code: segmentCode,
      title: segmentTitle,
      level: 1,
    })

    // Generate families
    for (let f = 0; f < familiesPerSegment && familyCount < scale.families; f++) {
      const familyCode = segmentCode + String(10 + f).padStart(2, '0')
      const familyTitle = generateFamilyName(segmentTitle, f)

      families.push({
        code: familyCode,
        title: familyTitle,
        segmentCode,
        level: 2,
      })
      familyCount++

      // Generate classes
      for (let c = 0; c < classesPerFamily && classCount < scale.classes; c++) {
        const classCode = familyCode + String(10 + c).padStart(2, '0')
        const classTitle = generateClassName(familyTitle, c)

        classes.push({
          code: classCode,
          title: classTitle,
          familyCode,
          segmentCode,
          level: 3,
        })
        classCount++

        // Generate commodities
        for (let m = 0; m < commoditiesPerClass && commodityCount < scale.commodities; m++) {
          const commodityCode = classCode + String(1 + m).padStart(2, '0')
          const commodityTitle = generateCommodityName(classTitle, m)

          commodities.push({
            code: commodityCode,
            title: commodityTitle,
            classCode,
            familyCode,
            segmentCode,
            level: 4,
          })
          commodityCount++
        }
      }
    }
  }

  // Sort by code for row-group statistics
  families.sort((a, b) => a.code.localeCompare(b.code))
  classes.sort((a, b) => a.code.localeCompare(b.code))
  commodities.sort((a, b) => a.code.localeCompare(b.code))

  console.log(`  Generated: ${segments.length} segments, ${families.length} families, ${classes.length} classes, ${commodities.length} commodities`)

  return { segments, families, classes, commodities }
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
  const outputDir = getOutputDir(BASE_DIR, 'unspsc', mode)
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
  data: { segments: Segment[]; families: Family[]; classes: Class[]; commodities: Commodity[] },
  mode: StorageMode
): Promise<GenerationStats[]> {
  console.log(`\nGenerating ${mode}...`)

  const stats: GenerationStats[] = []

  // Generate segments
  const segStats = await generateModeForCollection(
    data.segments,
    SEGMENT_SCHEMA,
    mode,
    'segments',
    (s) => `segment:${s.code}`,
    () => 'Segment',
    (s) => s.title
  )
  stats.push(segStats)
  console.log(`  segments: ${formatBytes(segStats.size)} (${segStats.rows} rows)`)

  // Generate families
  const famStats = await generateModeForCollection(
    data.families,
    FAMILY_SCHEMA,
    mode,
    'families',
    (f) => `family:${f.code}`,
    () => 'Family',
    (f) => f.title
  )
  stats.push(famStats)
  console.log(`  families: ${formatBytes(famStats.size)} (${famStats.rows} rows)`)

  // Generate classes
  const classStats = await generateModeForCollection(
    data.classes,
    CLASS_SCHEMA,
    mode,
    'classes',
    (c) => `class:${c.code}`,
    () => 'Class',
    (c) => c.title
  )
  stats.push(classStats)
  console.log(`  classes: ${formatBytes(classStats.size)} (${classStats.rows} rows)`)

  // Generate commodities
  const commStats = await generateModeForCollection(
    data.commodities,
    COMMODITY_SCHEMA,
    mode,
    'commodities',
    (c) => `commodity:${c.code}`,
    () => 'Commodity',
    (c) => c.title
  )
  stats.push(commStats)
  console.log(`  commodities: ${formatBytes(commStats.size)} (${commStats.rows} rows)`)

  return stats
}

// =============================================================================
// Main
// =============================================================================

const SCALE_CONFIGS: Record<string, ScaleConfig> = {
  small: { families: 40, classes: 400, commodities: 7000 },
  medium: { families: 400, classes: 4000, commodities: 70000 },
  large: { families: 800, classes: 8000, commodities: 200000 },
}

async function main() {
  const args = process.argv.slice(2)
  const modeArg = args.find(a => a.startsWith('--mode='))
  const scaleArg = args.find(a => a.startsWith('--scale='))

  const modes: StorageMode[] = modeArg
    ? [modeArg.split('=')[1] as StorageMode]
    : ALL_MODES

  const scaleName = scaleArg ? scaleArg.split('=')[1]! : 'medium'
  const scale = SCALE_CONFIGS[scaleName] ?? SCALE_CONFIGS.medium

  console.log('=== UNSPSC Storage Mode Generator ===')
  console.log(`Modes: ${modes.join(', ')}`)
  console.log(`Scale: ${scaleName}`)

  // Generate data
  const data = generateUnspscData(scale)

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
      console.log(`    ${s.collection}: ${formatBytes(s.size)} (${s.rows} rows, ${s.bytesPerRow} bytes/row)`)
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
