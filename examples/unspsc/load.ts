/**
 * UNSPSC Data Loader for ParqueDB
 *
 * Loads UNSPSC taxonomy data from CSV/TSV files into ParqueDB using the
 * proper database API with entity creation and relationship linking.
 *
 * UNSPSC Hierarchy:
 * - Segments (2-digit): ~55 segments
 * - Families (4-digit): ~400 families
 * - Classes (6-digit): ~4000 classes
 * - Commodities (8-digit): ~70000 commodities
 *
 * Data sources:
 * - Official: https://www.unspsc.org/ (requires registration)
 * - Sample/open versions available from various government procurement sites
 *
 * Usage:
 *   npx tsx examples/unspsc/load.ts --input data/unspsc.csv --output ./output
 *   npx tsx examples/unspsc/load.ts --generate-sample --output ./sample.csv
 *   npx tsx examples/unspsc/load.ts --generate-it-sample --output ./it-sample.csv
 *
 * Expected CSV format:
 *   Segment,Segment Title,Family,Family Title,Class,Class Title,Commodity,Commodity Title
 *   10,Live Plant and Animal...,1010,Live animals,101015,Livestock,10101501,Cats
 */

import { parse } from 'csv-parse/sync'
import * as fs from 'fs'
import * as path from 'path'
import { ParqueDB, FsBackend } from '../../src'
import type { EntityId } from '../../src/types'
import {
  UNSPSCSchema,
  parseCode,
  entityId,
  type Segment,
  type Family,
  type Class,
  type Commodity,
  type UNSPSCCSVRow,
  type UNSPSCFlatRow,
} from './schema'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for loading UNSPSC data
 */
export interface LoadUnspscOptions {
  /** Batch size for writes (default: 1000) */
  batchSize?: number
  /** Whether to log progress */
  verbose?: boolean
  /** Actor ID for audit fields */
  actorId?: string
}

/**
 * Result of loading UNSPSC data
 */
export interface LoadUnspscResult {
  segments: number
  families: number
  classes: number
  commodities: number
  totalEntities: number
  totalRelationships: number
  durationMs: number
  outputPath: string
}

/**
 * Statistics about loaded data
 */
export interface UnspscStats {
  segments: { count: number; examples: string[] }
  families: { count: number; examples: string[] }
  classes: { count: number; examples: string[] }
  commodities: { count: number; examples: string[] }
}

// =============================================================================
// Deduplication Maps
// =============================================================================

interface EntityMaps {
  segments: Map<string, Segment>
  families: Map<string, Family>
  classes: Map<string, Class>
  commodities: Map<string, Commodity>
}

// =============================================================================
// CSV Parsing
// =============================================================================

/**
 * Detect CSV format and parse accordingly
 */
function parseCSV(content: string): UNSPSCCSVRow[] | UNSPSCFlatRow[] {
  // Detect delimiter (comma or tab)
  const firstLine = content.split('\n')[0]
  const delimiter = firstLine.includes('\t') ? '\t' : ','

  const records = parse(content, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true, // Handle BOM in UTF-8 files
    delimiter,
    relaxColumnCount: true, // Handle inconsistent column counts
  })

  if (records.length === 0) {
    throw new Error('CSV file is empty')
  }

  // Detect format based on column headers
  const firstRow = records[0] as Record<string, unknown>

  if ('Segment' in firstRow && 'Family' in firstRow) {
    // Standard UNSPSC format with all levels in one row
    return records as UNSPSCCSVRow[]
  } else if ('Code' in firstRow && 'Level' in firstRow) {
    // Flat format with one entity per row
    return records as UNSPSCFlatRow[]
  } else if ('SEGMENT' in firstRow || 'segment' in firstRow) {
    // Handle uppercase column names
    return records.map((row: Record<string, unknown>) => ({
      Segment: (row['SEGMENT'] || row['segment']) as string,
      'Segment Title': (row['SEGMENT TITLE'] || row['Segment Title'] || row['segment_title']) as string,
      Family: (row['FAMILY'] || row['family']) as string,
      'Family Title': (row['FAMILY TITLE'] || row['Family Title'] || row['family_title']) as string,
      Class: (row['CLASS'] || row['class']) as string,
      'Class Title': (row['CLASS TITLE'] || row['Class Title'] || row['class_title']) as string,
      Commodity: (row['COMMODITY'] || row['commodity']) as string,
      'Commodity Title': (row['COMMODITY TITLE'] || row['Commodity Title'] || row['commodity_title']) as string,
    })) as UNSPSCCSVRow[]
  }

  throw new Error(
    'Unrecognized CSV format. Expected either UNSPSC standard format (Segment,Segment Title,...) or flat format (Code,Level,Title).'
  )
}

/**
 * Transform standard UNSPSC CSV rows into entity maps
 */
function transformStandardFormat(rows: UNSPSCCSVRow[]): EntityMaps {
  const segments = new Map<string, Segment>()
  const families = new Map<string, Family>()
  const classes = new Map<string, Class>()
  const commodities = new Map<string, Commodity>()

  for (const row of rows) {
    // Extract segment (2-digit)
    if (row.Segment && row['Segment Title']) {
      const code = row.Segment.toString().padStart(2, '0')
      if (!segments.has(code)) {
        segments.set(code, {
          $type: 'Segment',
          code,
          title: row['Segment Title'],
          isActive: true,
        })
      }
    }

    // Extract family (4-digit)
    if (row.Family && row['Family Title']) {
      const code = row.Family.toString().padStart(4, '0')
      if (!families.has(code)) {
        families.set(code, {
          $type: 'Family',
          code,
          title: row['Family Title'],
          segmentCode: code.slice(0, 2),
          isActive: true,
        })
      }
    }

    // Extract class (6-digit)
    if (row.Class && row['Class Title']) {
      const code = row.Class.toString().padStart(6, '0')
      if (!classes.has(code)) {
        classes.set(code, {
          $type: 'Class',
          code,
          title: row['Class Title'],
          familyCode: code.slice(0, 4),
          segmentCode: code.slice(0, 2),
          isActive: true,
        })
      }
    }

    // Extract commodity (8-digit)
    if (row.Commodity && row['Commodity Title']) {
      const code = row.Commodity.toString().padStart(8, '0')
      if (!commodities.has(code)) {
        commodities.set(code, {
          $type: 'Commodity',
          code,
          title: row['Commodity Title'],
          classCode: code.slice(0, 6),
          familyCode: code.slice(0, 4),
          segmentCode: code.slice(0, 2),
          isActive: true,
        })
      }
    }
  }

  return { segments, families, classes, commodities }
}

/**
 * Transform flat format CSV rows into entity maps
 */
function transformFlatFormat(rows: UNSPSCFlatRow[]): EntityMaps {
  const segments = new Map<string, Segment>()
  const families = new Map<string, Family>()
  const classes = new Map<string, Class>()
  const commodities = new Map<string, Commodity>()

  for (const row of rows) {
    const code = row.Code.replace(/[^0-9]/g, '')
    const parsed = parseCode(code)

    switch (row.Level) {
      case 'Segment':
        segments.set(code, {
          $type: 'Segment',
          code: parsed.segment,
          title: row.Title,
          description: row.Description,
          isActive: true,
        })
        break

      case 'Family':
        families.set(code, {
          $type: 'Family',
          code,
          title: row.Title,
          description: row.Description,
          segmentCode: parsed.segment,
          isActive: true,
        })
        break

      case 'Class':
        classes.set(code, {
          $type: 'Class',
          code,
          title: row.Title,
          description: row.Description,
          familyCode: parsed.family!,
          segmentCode: parsed.segment,
          isActive: true,
        })
        break

      case 'Commodity':
        commodities.set(code, {
          $type: 'Commodity',
          code,
          title: row.Title,
          description: row.Description,
          classCode: parsed.class!,
          familyCode: parsed.family!,
          segmentCode: parsed.segment,
          isActive: true,
        })
        break
    }
  }

  return { segments, families, classes, commodities }
}

// =============================================================================
// Main Load Function
// =============================================================================

/**
 * Load UNSPSC data from CSV/TSV into ParqueDB
 *
 * Uses the ParqueDB API to create entities and establish relationships:
 * - Creates entities via db.collection('codes').create()
 * - Links parent-child relationships via $link operator
 *
 * Data flows to:
 * - data/codes/data.parquet for entity storage
 * - rels/ directory for bidirectional relationship indexes
 *
 * @param inputFile - Path to UNSPSC CSV/TSV file
 * @param outputPath - Output directory for ParqueDB data
 * @param options - Loading options
 * @returns Load result with statistics
 *
 * @example
 * ```typescript
 * import { loadUnspsc } from './load'
 *
 * const result = await loadUnspsc(
 *   './data/unspsc.csv',
 *   './output',
 *   { verbose: true }
 * )
 *
 * console.log(`Loaded ${result.totalEntities} entities`)
 * ```
 */
export async function loadUnspsc(
  inputFile: string,
  outputPath: string,
  options: LoadUnspscOptions = {}
): Promise<LoadUnspscResult> {
  const startTime = Date.now()
  const {
    batchSize = 1000,
    verbose = false,
    actorId = 'system/unspsc-loader',
  } = options

  if (verbose) {
    console.log(`Loading UNSPSC data from: ${inputFile}`)
  }

  // Resolve paths
  const resolvedInputPath = path.resolve(inputFile)
  const resolvedOutputPath = path.resolve(outputPath)

  // Create output directory
  if (!fs.existsSync(resolvedOutputPath)) {
    fs.mkdirSync(resolvedOutputPath, { recursive: true })
  }

  // Initialize ParqueDB with FsBackend
  const storage = new FsBackend(resolvedOutputPath)
  const db = new ParqueDB({
    storage,
    schema: UNSPSCSchema as any,
  })

  // Read and parse CSV
  const content = fs.readFileSync(resolvedInputPath, 'utf-8')
  const rows = parseCSV(content)

  if (verbose) {
    console.log(`  Parsed ${rows.length} CSV rows`)
  }

  // Transform to entity maps
  const entities =
    'Segment' in rows[0]
      ? transformStandardFormat(rows as UNSPSCCSVRow[])
      : transformFlatFormat(rows as UNSPSCFlatRow[])

  if (verbose) {
    console.log(`  Found ${entities.segments.size} segments`)
    console.log(`  Found ${entities.families.size} families`)
    console.log(`  Found ${entities.classes.size} classes`)
    console.log(`  Found ${entities.commodities.size} commodities`)
  }

  // Get the codes collection
  const codes = db.collection('codes')
  const actor = actorId as EntityId

  // Track created entity IDs for relationship linking
  const segmentIds = new Map<string, EntityId>()
  const familyIds = new Map<string, EntityId>()
  const classIds = new Map<string, EntityId>()

  let totalRelationships = 0

  // ==========================================================================
  // Step 1: Create Segments (no parent relationships)
  // ==========================================================================
  if (verbose) {
    console.log('\nCreating segments...')
  }

  for (const [code, segment] of entities.segments) {
    const entity = await codes.create(
      {
        $type: 'Segment',
        name: segment.title,
        code: segment.code,
        title: segment.title,
        description: segment.description,
        isActive: segment.isActive,
      },
      { actor }
    )
    segmentIds.set(code, entity.$id)
  }

  if (verbose) {
    console.log(`  Created ${entities.segments.size} segments`)
  }

  // ==========================================================================
  // Step 2: Create Families and link to parent Segments
  // ==========================================================================
  if (verbose) {
    console.log('\nCreating families...')
  }

  let familyBatch = 0
  for (const [code, family] of entities.families) {
    // Create the family entity
    const entity = await codes.create(
      {
        $type: 'Family',
        name: family.title,
        code: family.code,
        title: family.title,
        description: family.description,
        segmentCode: family.segmentCode,
        isActive: family.isActive,
      },
      { actor }
    )
    familyIds.set(code, entity.$id)

    // Link to parent segment using $link operator
    const parentSegmentId = segmentIds.get(family.segmentCode)
    if (parentSegmentId) {
      await codes.update(entity.$id, {
        $link: {
          segment: parentSegmentId,
        },
      })
      totalRelationships++
    }

    familyBatch++
    if (verbose && familyBatch % batchSize === 0) {
      console.log(`    Processed ${familyBatch} families...`)
    }
  }

  if (verbose) {
    console.log(`  Created ${entities.families.size} families`)
  }

  // ==========================================================================
  // Step 3: Create Classes and link to parent Families
  // ==========================================================================
  if (verbose) {
    console.log('\nCreating classes...')
  }

  let classBatch = 0
  for (const [code, cls] of entities.classes) {
    // Create the class entity
    const entity = await codes.create(
      {
        $type: 'Class',
        name: cls.title,
        code: cls.code,
        title: cls.title,
        description: cls.description,
        familyCode: cls.familyCode,
        segmentCode: cls.segmentCode,
        isActive: cls.isActive,
      },
      { actor }
    )
    classIds.set(code, entity.$id)

    // Link to parent family using $link operator
    const parentFamilyId = familyIds.get(cls.familyCode)
    if (parentFamilyId) {
      await codes.update(entity.$id, {
        $link: {
          family: parentFamilyId,
        },
      })
      totalRelationships++
    }

    classBatch++
    if (verbose && classBatch % batchSize === 0) {
      console.log(`    Processed ${classBatch} classes...`)
    }
  }

  if (verbose) {
    console.log(`  Created ${entities.classes.size} classes`)
  }

  // ==========================================================================
  // Step 4: Create Commodities and link to parent Classes
  // ==========================================================================
  if (verbose) {
    console.log('\nCreating commodities...')
  }

  let commodityBatch = 0
  for (const [code, commodity] of entities.commodities) {
    // Create the commodity entity
    const entity = await codes.create(
      {
        $type: 'Commodity',
        name: commodity.title,
        code: commodity.code,
        title: commodity.title,
        description: commodity.description,
        classCode: commodity.classCode,
        familyCode: commodity.familyCode,
        segmentCode: commodity.segmentCode,
        isActive: commodity.isActive,
      },
      { actor }
    )

    // Link to parent class using $link operator
    const parentClassId = classIds.get(commodity.classCode)
    if (parentClassId) {
      await codes.update(entity.$id, {
        $link: {
          class: parentClassId,
        },
      })
      totalRelationships++
    }

    commodityBatch++
    if (verbose && commodityBatch % batchSize === 0) {
      console.log(`    Processed ${commodityBatch} commodities...`)
    }
  }

  if (verbose) {
    console.log(`  Created ${entities.commodities.size} commodities`)
  }

  // ==========================================================================
  // Write metadata
  // ==========================================================================
  const metaDir = path.join(resolvedOutputPath, '_meta')
  if (!fs.existsSync(metaDir)) {
    fs.mkdirSync(metaDir, { recursive: true })
  }

  // Write schema
  const schemaJson = JSON.stringify(UNSPSCSchema, null, 2)
  fs.writeFileSync(path.join(metaDir, 'schema.json'), schemaJson)

  // Write manifest
  const totalEntities =
    entities.segments.size +
    entities.families.size +
    entities.classes.size +
    entities.commodities.size

  const manifest = {
    name: 'UNSPSC',
    version: '1.0.0',
    description: 'United Nations Standard Products and Services Code taxonomy',
    createdAt: new Date().toISOString(),
    namespaces: ['codes'],
    entityCount: totalEntities,
    relationshipCount: totalRelationships,
    stats: {
      segments: entities.segments.size,
      families: entities.families.size,
      classes: entities.classes.size,
      commodities: entities.commodities.size,
    },
  }
  fs.writeFileSync(path.join(metaDir, 'manifest.json'), JSON.stringify(manifest, null, 2))

  const durationMs = Date.now() - startTime

  if (verbose) {
    console.log(`\nLoad complete in ${durationMs}ms`)
  }

  return {
    segments: entities.segments.size,
    families: entities.families.size,
    classes: entities.classes.size,
    commodities: entities.commodities.size,
    totalEntities,
    totalRelationships,
    durationMs,
    outputPath: resolvedOutputPath,
  }
}

// =============================================================================
// Sample Data Generator
// =============================================================================

/**
 * Generate basic sample UNSPSC data for testing
 */
export function generateSampleData(): string {
  const rows = [
    'Segment,Segment Title,Family,Family Title,Class,Class Title,Commodity,Commodity Title',
    '10,Live Plant and Animal Material and Accessories and Supplies,1010,Live animals,101015,Livestock,10101501,Cats',
    '10,Live Plant and Animal Material and Accessories and Supplies,1010,Live animals,101015,Livestock,10101502,Dogs',
    '10,Live Plant and Animal Material and Accessories and Supplies,1010,Live animals,101015,Livestock,10101503,Mice',
    '10,Live Plant and Animal Material and Accessories and Supplies,1010,Live animals,101016,Birds,10101601,Parrots',
    '10,Live Plant and Animal Material and Accessories and Supplies,1010,Live animals,101016,Birds,10101602,Canaries',
    '10,Live Plant and Animal Material and Accessories and Supplies,1011,Domestic pet products,101110,Pet food,10111001,Dog food',
    '10,Live Plant and Animal Material and Accessories and Supplies,1011,Domestic pet products,101110,Pet food,10111002,Cat food',
    '10,Live Plant and Animal Material and Accessories and Supplies,1011,Domestic pet products,101111,Pet toys,10111101,Chew toys',
    '11,Mineral and Textile and Inedible Plant and Animal Materials,1110,Minerals and ores and metals,111010,Ore,11101001,Iron ore',
    '11,Mineral and Textile and Inedible Plant and Animal Materials,1110,Minerals and ores and metals,111010,Ore,11101002,Copper ore',
    '11,Mineral and Textile and Inedible Plant and Animal Materials,1110,Minerals and ores and metals,111011,Precious metals,11101101,Gold',
    '11,Mineral and Textile and Inedible Plant and Animal Materials,1110,Minerals and ores and metals,111011,Precious metals,11101102,Silver',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101501,Notebook computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101502,Desktop computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101503,Tablet computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101601,LCD monitors',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101602,LED monitors',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111001,Windows',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111002,Linux',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111003,macOS',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111101,MySQL',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111102,PostgreSQL',
  ]

  return rows.join('\n')
}

/**
 * Generate comprehensive IT-focused sample UNSPSC data (Segment 43)
 *
 * This generates a realistic subset of UNSPSC codes for the
 * "Information Technology Broadcasting and Telecommunications" segment,
 * useful for testing and development.
 */
export function generateITSampleData(): string {
  const rows = [
    'Segment,Segment Title,Family,Family Title,Class,Class Title,Commodity,Commodity Title',

    // Segment 43: Information Technology Broadcasting and Telecommunications

    // 4310: Computer Equipment and Accessories
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101501,Notebook computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101502,Desktop computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101503,Tablet computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101504,Servers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101505,Mainframe computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101506,Thin client computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101507,Workstation computers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101508,High performance computing HPC systems',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101509,Blade servers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431015,Computers,43101510,Rack mount servers',

    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101601,LCD monitors',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101602,LED monitors',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101603,CRT monitors',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101604,Touch screen monitors',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101605,OLED monitors',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431016,Computer displays,43101606,Gaming monitors',

    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101701,Keyboards',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101702,Computer mice',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101703,Scanners',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101704,Graphics tablets',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101705,Barcode readers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101706,Webcams',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101707,Microphones',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431017,Computer data input devices,43101708,Touchpads',

    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431018,Printers,43101801,Laser printers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431018,Printers,43101802,Inkjet printers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431018,Printers,43101803,Dot matrix printers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431018,Printers,43101804,Thermal printers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431018,Printers,43101805,3D printers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431018,Printers,43101806,Label printers',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431018,Printers,43101807,Photo printers',

    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101901,Hard disk drives HDD',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101902,Solid state drives SSD',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101903,USB flash drives',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101904,Memory cards',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101905,Optical drives',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101906,Tape drives',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101907,Network attached storage NAS',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101908,Storage area networks SAN',
    '43,Information Technology Broadcasting and Telecommunications,4310,Computer Equipment and Accessories,431019,Data storage,43101909,NVMe drives',

    // 4311: Software
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111001,Microsoft Windows operating systems',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111002,Linux operating systems',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111003,Apple macOS operating systems',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111004,Unix operating systems',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111005,Mobile operating systems',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111006,Real time operating systems',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431110,Operating systems,43111007,Embedded operating systems',

    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111101,Relational database management systems',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111102,MySQL database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111103,PostgreSQL database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111104,Oracle database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111105,Microsoft SQL Server database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111106,NoSQL database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111107,MongoDB database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111108,Redis database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111109,Elasticsearch database software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431111,Database software,43111110,SQLite database software',

    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111201,Integrated development environments IDE',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111202,Programming language compilers',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111203,Version control software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111204,Software testing tools',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111205,Debugging software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111206,Application programming interface API development tools',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111207,Low code development platforms',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111208,Container orchestration software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431112,Development software,43111209,Continuous integration continuous deployment CI CD software',

    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111301,Enterprise resource planning ERP software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111302,Customer relationship management CRM software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111303,Human resource management software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111304,Accounting software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111305,Project management software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111306,Business intelligence software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111307,Document management software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431113,Business applications,43111308,Workflow automation software',

    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111401,Antivirus software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111402,Firewall software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111403,Encryption software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111404,Intrusion detection software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111405,Identity and access management software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111406,Security information and event management SIEM software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111407,Virtual private network VPN software',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431114,Security software,43111408,Endpoint protection software',

    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431115,Cloud computing software,43111501,Infrastructure as a service IaaS platforms',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431115,Cloud computing software,43111502,Platform as a service PaaS platforms',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431115,Cloud computing software,43111503,Software as a service SaaS platforms',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431115,Cloud computing software,43111504,Serverless computing platforms',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431115,Cloud computing software,43111505,Cloud storage services',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431115,Cloud computing software,43111506,Cloud container services',
    '43,Information Technology Broadcasting and Telecommunications,4311,Software,431115,Cloud computing software,43111507,Cloud database services',

    // 4312: Networking
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121001,Network routers',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121002,Network switches',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121003,Network hubs',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121004,Network firewalls',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121005,Load balancers',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121006,Network access points',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121007,VPN appliances',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431210,Network equipment,43121008,Software defined networking SDN equipment',

    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431211,Network cables,43121101,Ethernet cables Cat5e',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431211,Network cables,43121102,Ethernet cables Cat6',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431211,Network cables,43121103,Fiber optic cables',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431211,Network cables,43121104,Coaxial cables',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431211,Network cables,43121105,Patch cables',

    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431212,Wireless networking,43121201,WiFi routers',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431212,Wireless networking,43121202,WiFi access points',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431212,Wireless networking,43121203,WiFi range extenders',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431212,Wireless networking,43121204,WiFi mesh systems',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431212,Wireless networking,43121205,WiFi network adapters',
    '43,Information Technology Broadcasting and Telecommunications,4312,Networking,431212,Wireless networking,43121206,5G network equipment',

    // 4313: IT Services (conceptual - may vary in actual UNSPSC)
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431310,IT consulting services,43131001,IT strategy consulting',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431310,IT consulting services,43131002,Systems integration consulting',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431310,IT consulting services,43131003,IT architecture consulting',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431310,IT consulting services,43131004,Digital transformation consulting',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431310,IT consulting services,43131005,Cloud migration consulting',

    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431311,Software development services,43131101,Custom software development',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431311,Software development services,43131102,Mobile application development',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431311,Software development services,43131103,Web application development',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431311,Software development services,43131104,API development services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431311,Software development services,43131105,Software maintenance services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431311,Software development services,43131106,DevOps services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431311,Software development services,43131107,Quality assurance testing services',

    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431312,IT support services,43131201,Help desk services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431312,IT support services,43131202,Technical support services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431312,IT support services,43131203,IT maintenance services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431312,IT support services,43131204,Remote IT support',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431312,IT support services,43131205,On site IT support',

    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431313,Managed IT services,43131301,Managed hosting services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431313,Managed IT services,43131302,Managed security services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431313,Managed IT services,43131303,Managed network services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431313,Managed IT services,43131304,Managed backup services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431313,Managed IT services,43131305,Disaster recovery services',

    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431314,Data services,43131401,Data analytics services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431314,Data services,43131402,Data migration services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431314,Data services,43131403,Data warehousing services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431314,Data services,43131404,Machine learning services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431314,Data services,43131405,Artificial intelligence services',
    '43,Information Technology Broadcasting and Telecommunications,4313,IT Services,431314,Data services,43131406,Big data services',

    // 4314: Telecommunications Equipment
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431410,Telephone equipment,43141001,VoIP phones',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431410,Telephone equipment,43141002,Desktop telephones',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431410,Telephone equipment,43141003,Conference phones',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431410,Telephone equipment,43141004,Telephone headsets',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431410,Telephone equipment,43141005,PBX systems',

    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431411,Video conferencing,43141101,Video conferencing systems',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431411,Video conferencing,43141102,Video conferencing cameras',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431411,Video conferencing,43141103,Video conferencing software',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431411,Video conferencing,43141104,Webinar platforms',
    '43,Information Technology Broadcasting and Telecommunications,4314,Telecommunications Equipment,431411,Video conferencing,43141105,Room video systems',
  ]

  return rows.join('\n')
}

/**
 * Write sample data to a file
 *
 * @param outputPath - Output file path
 * @param itOnly - If true, generate only IT-related codes (segment 43)
 */
export function writeSampleData(outputPath: string, itOnly: boolean = false): void {
  const data = itOnly ? generateITSampleData() : generateSampleData()
  const resolvedPath = path.resolve(outputPath)
  const dir = path.dirname(resolvedPath)

  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true })
  }

  fs.writeFileSync(resolvedPath, data)
  console.log(`Sample data written to: ${resolvedPath}`)
}

/**
 * Get statistics about UNSPSC data from a file
 *
 * @param inputFile - Path to UNSPSC CSV/TSV file
 * @returns Statistics about the data
 */
export function getUnspscStats(inputFile: string): UnspscStats {
  const content = fs.readFileSync(inputFile, 'utf-8')
  const rows = parseCSV(content)

  const entities =
    'Segment' in rows[0]
      ? transformStandardFormat(rows as UNSPSCCSVRow[])
      : transformFlatFormat(rows as UNSPSCFlatRow[])

  const getExamples = (map: Map<string, { code: string; title: string }>, count: number = 3) =>
    Array.from(map.values())
      .slice(0, count)
      .map((e) => `${e.code}: ${e.title}`)

  return {
    segments: {
      count: entities.segments.size,
      examples: getExamples(entities.segments),
    },
    families: {
      count: entities.families.size,
      examples: getExamples(entities.families),
    },
    classes: {
      count: entities.classes.size,
      examples: getExamples(entities.classes),
    },
    commodities: {
      count: entities.commodities.size,
      examples: getExamples(entities.commodities),
    },
  }
}

// =============================================================================
// CLI Entry Point
// =============================================================================

async function main() {
  const args = process.argv.slice(2)

  // Parse arguments
  let inputPath: string | null = null
  let outputDir = './unspsc-output'
  let verbose = false
  let generateSample = false
  let generateITSample = false
  let showStats = false

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--input':
      case '-i':
        inputPath = args[++i]
        break
      case '--output':
      case '-o':
        outputDir = args[++i]
        break
      case '--verbose':
      case '-v':
        verbose = true
        break
      case '--generate-sample':
        generateSample = true
        break
      case '--generate-it-sample':
        generateITSample = true
        break
      case '--stats':
        showStats = true
        break
      case '--help':
      case '-h':
        console.log(`
UNSPSC Data Loader for ParqueDB

Usage:
  npx tsx examples/unspsc/load.ts [options]

Options:
  -i, --input <path>      Path to UNSPSC CSV/TSV file
  -o, --output <dir>      Output directory (default: ./unspsc-output)
  -v, --verbose           Show detailed progress
  --generate-sample       Generate basic sample data file instead of loading
  --generate-it-sample    Generate IT-focused sample data (segment 43)
  --stats                 Show statistics about the input file
  -h, --help              Show this help message

Examples:
  # Load from CSV
  npx tsx examples/unspsc/load.ts -i data/unspsc.csv -v

  # Generate basic sample data
  npx tsx examples/unspsc/load.ts --generate-sample -o ./sample.csv

  # Generate IT-focused sample data
  npx tsx examples/unspsc/load.ts --generate-it-sample -o ./it-sample.csv

  # Show stats about a file
  npx tsx examples/unspsc/load.ts -i data/unspsc.csv --stats
        `)
        process.exit(0)
    }
  }

  // Generate sample data if requested
  if (generateSample || generateITSample) {
    const samplePath = outputDir.endsWith('.csv') ? outputDir : path.join(outputDir, 'unspsc-sample.csv')
    writeSampleData(samplePath, generateITSample)
    process.exit(0)
  }

  // Show stats if requested
  if (showStats && inputPath) {
    const stats = getUnspscStats(inputPath)
    console.log('\nUNSPSC Data Statistics:')
    console.log('========================')
    console.log(`\nSegments: ${stats.segments.count}`)
    console.log('  Examples:')
    stats.segments.examples.forEach((e) => console.log(`    - ${e}`))
    console.log(`\nFamilies: ${stats.families.count}`)
    console.log('  Examples:')
    stats.families.examples.forEach((e) => console.log(`    - ${e}`))
    console.log(`\nClasses: ${stats.classes.count}`)
    console.log('  Examples:')
    stats.classes.examples.forEach((e) => console.log(`    - ${e}`))
    console.log(`\nCommodities: ${stats.commodities.count}`)
    console.log('  Examples:')
    stats.commodities.examples.forEach((e) => console.log(`    - ${e}`))
    process.exit(0)
  }

  // Load data
  if (!inputPath) {
    console.error('Error: --input <path> is required')
    console.error('Use --help for usage information')
    process.exit(1)
  }

  // Run loader
  try {
    const result = await loadUnspsc(inputPath, outputDir, {
      verbose,
    })

    console.log('\nLoad Summary:')
    console.log(`  Segments: ${result.segments}`)
    console.log(`  Families: ${result.families}`)
    console.log(`  Classes: ${result.classes}`)
    console.log(`  Commodities: ${result.commodities}`)
    console.log(`  Total Entities: ${result.totalEntities}`)
    console.log(`  Total Relationships: ${result.totalRelationships}`)
    console.log(`  Duration: ${result.durationMs}ms`)
    console.log(`\nOutput written to: ${result.outputPath}`)
  } catch (error) {
    console.error('Error:', error instanceof Error ? error.message : String(error))
    process.exit(1)
  }
}

// Run if executed directly
const isMainModule = (() => {
  try {
    // Works with ESM
    if (typeof import.meta !== 'undefined' && import.meta.url) {
      const modulePath = import.meta.url.replace('file://', '')
      const argPath = process.argv[1]?.replace(/\\/g, '/')
      return modulePath.endsWith(argPath) || argPath?.endsWith('load.ts') || argPath?.endsWith('load.js')
    }
    return false
  } catch {
    // Fallback for CJS or other environments
    return process.argv[1]?.endsWith('load.ts') || process.argv[1]?.endsWith('load.js')
  }
})()

if (isMainModule) {
  main().catch(console.error)
}

// =============================================================================
// Legacy exports for backward compatibility
// =============================================================================

/**
 * @deprecated Use loadUnspsc instead
 */
export async function loadUNSPSC(options: {
  inputPath: string
  bucket?: unknown
  batchSize?: number
  verbose?: boolean
  actorId?: string
}): Promise<LoadUnspscResult> {
  // For backward compatibility with bucket-based API
  const outputPath = './unspsc-output'
  return loadUnspsc(options.inputPath, outputPath, {
    batchSize: options.batchSize,
    verbose: options.verbose,
    actorId: options.actorId,
  })
}
