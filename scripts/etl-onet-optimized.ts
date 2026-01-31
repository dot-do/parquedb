#!/usr/bin/env npx tsx
/**
 * ETL: Transform O*NET data into optimized ParqueDB graph structure
 *
 * Output format optimized for fast lookups:
 * - data.parquet: Single `data` column (JSON), sorted by $id
 * - rels.parquet: Single `data` column (JSON), sorted by from_id, edges stored 2x
 *
 * Target: <50ms entity lookups with relationships
 */

import { ParquetReader } from '../src/parquet/reader'
import { FsBackend } from '../src/storage/FsBackend'
import { parquetWriteBuffer } from 'hyparquet-writer'
import * as fs from 'fs'
import * as path from 'path'

const INPUT_DIR = 'data/onet'
const OUTPUT_DIR = 'data/onet-optimized'

// =============================================================================
// Types
// =============================================================================

interface OccupationRow {
  'O*NET-SOC Code': string
  'Title': string
  'Description': string
}

interface RelationshipRow {
  'O*NET-SOC Code': string
  'Element ID': string
  'Element Name': string
  'Scale ID': string
  'Data Value': string
}

interface ContentModelRow {
  'Element ID': string
  'Element Name': string
  'Description': string
}

// Entity stored in data.parquet (single JSON column)
interface Entity {
  $id: string
  $type: string
  name: string
  description?: string
  [key: string]: unknown
}

// Edge stored in rels.parquet (single JSON column)
interface Edge {
  to: string        // Target $id (e.g., "skills/2.B.4.e")
  ns: string        // Target namespace (e.g., "skills")
  name: string      // Target name for display
  pred: string      // Predicate (e.g., "skills", "requiredBy")
  rev: string       // Reverse predicate
  // Edge data (properties on the relationship)
  importance?: number
  level?: number
  [key: string]: unknown
}

// =============================================================================
// Helpers
// =============================================================================

async function loadParquet<T>(filePath: string): Promise<T[]> {
  const storage = new FsBackend(process.cwd())
  const reader = new ParquetReader({ storage })
  return reader.read<T>(filePath)
}

// =============================================================================
// Main ETL
// =============================================================================

async function main() {
  console.log('=== O*NET Optimized ETL ===\n')
  console.log('Target format:')
  console.log('  data.parquet: $id (sorted) | data (JSON)')
  console.log('  rels.parquet: from_id (sorted) | data (JSON)')
  console.log('')

  fs.mkdirSync(OUTPUT_DIR, { recursive: true })

  // 1. Load reference data
  console.log('Loading Content Model Reference...')
  const contentModel = await loadParquet<ContentModelRow>(
    path.join(INPUT_DIR, 'Content Model Reference.parquet')
  )

  const elementMap = new Map<string, ContentModelRow>()
  for (const row of contentModel) {
    elementMap.set(row['Element ID'], row)
  }

  // Categorize elements
  const skills: ContentModelRow[] = []
  const abilities: ContentModelRow[] = []
  const knowledge: ContentModelRow[] = []

  for (const row of contentModel) {
    const id = row['Element ID']
    if (id.startsWith('2.A.') || id.startsWith('2.B.')) skills.push(row)
    else if (id.startsWith('1.A.')) abilities.push(row)
    else if (id.startsWith('2.C.')) knowledge.push(row)
  }

  console.log(`  Skills: ${skills.length}, Abilities: ${abilities.length}, Knowledge: ${knowledge.length}`)

  // 2. Load occupations
  console.log('\nLoading Occupations...')
  const occupations = await loadParquet<OccupationRow>(
    path.join(INPUT_DIR, 'Occupation Data.parquet')
  )
  console.log(`  Loaded ${occupations.length} occupations`)

  // 3. Load relationship data
  console.log('\nLoading relationship data...')
  const skillsData = await loadParquet<RelationshipRow>(path.join(INPUT_DIR, 'Skills.parquet'))
  const abilitiesData = await loadParquet<RelationshipRow>(path.join(INPUT_DIR, 'Abilities.parquet'))
  const knowledgeData = await loadParquet<RelationshipRow>(path.join(INPUT_DIR, 'Knowledge.parquet'))

  console.log(`  Skills: ${skillsData.length}, Abilities: ${abilitiesData.length}, Knowledge: ${knowledgeData.length}`)

  // 4. Build entities (nodes)
  console.log('\nBuilding entities...')
  const entities: Entity[] = []

  // Occupation entities
  for (const occ of occupations) {
    entities.push({
      $id: `occupations/${occ['O*NET-SOC Code']}`,
      $type: 'Occupation',
      name: occ['Title'],
      code: occ['O*NET-SOC Code'],
      description: occ['Description'],
    })
  }

  // Skill entities
  for (const skill of skills) {
    entities.push({
      $id: `skills/${skill['Element ID']}`,
      $type: 'Skill',
      name: skill['Element Name'],
      elementId: skill['Element ID'],
      description: skill['Description'],
    })
  }

  // Ability entities
  for (const ability of abilities) {
    entities.push({
      $id: `abilities/${ability['Element ID']}`,
      $type: 'Ability',
      name: ability['Element Name'],
      elementId: ability['Element ID'],
      description: ability['Description'],
    })
  }

  // Knowledge entities
  for (const k of knowledge) {
    entities.push({
      $id: `knowledge/${k['Element ID']}`,
      $type: 'Knowledge',
      name: k['Element Name'],
      elementId: k['Element ID'],
      description: k['Description'],
    })
  }

  console.log(`  Total entities: ${entities.length}`)

  // 5. Build edges (relationships stored 2x)
  console.log('\nBuilding edges (stored 2x for bidirectional lookup)...')
  const edges: { from_id: string; data: Edge }[] = []

  // Helper to process relationship data
  function processRelationships(
    data: RelationshipRow[],
    targetNs: string,
    pred: string,
    rev: string
  ) {
    // Group by occupation to combine importance/level
    const byOccupation = new Map<string, Map<string, { importance?: number; level?: number }>>()

    for (const row of data) {
      const occCode = row['O*NET-SOC Code']
      const elementId = row['Element ID']
      const scaleId = row['Scale ID']
      const value = parseFloat(row['Data Value'])

      if (!byOccupation.has(occCode)) byOccupation.set(occCode, new Map())
      const occMap = byOccupation.get(occCode)!

      if (!occMap.has(elementId)) occMap.set(elementId, {})
      const scores = occMap.get(elementId)!

      if (scaleId === 'IM') scores.importance = value
      if (scaleId === 'LV') scores.level = value
    }

    // Create edges (forward and reverse)
    for (const [occCode, elementScores] of byOccupation) {
      for (const [elementId, scores] of elementScores) {
        const element = elementMap.get(elementId)
        if (!element) continue

        const occEntity = occupations.find(o => o['O*NET-SOC Code'] === occCode)
        if (!occEntity) continue

        // Forward edge: occupation -> element
        edges.push({
          from_id: occCode,
          data: {
            to: `${targetNs}/${elementId}`,
            ns: targetNs,
            name: element['Element Name'],
            pred,
            rev,
            ...scores,
          },
        })

        // Reverse edge: element -> occupation
        edges.push({
          from_id: elementId,
          data: {
            to: `occupations/${occCode}`,
            ns: 'occupations',
            name: occEntity['Title'],
            pred: rev,
            rev: pred,
            ...scores,
          },
        })
      }
    }
  }

  processRelationships(skillsData, 'skills', 'skills', 'requiredBy')
  processRelationships(abilitiesData, 'abilities', 'abilities', 'requiredBy')
  processRelationships(knowledgeData, 'knowledge', 'knowledge', 'requiredBy')

  console.log(`  Total edges: ${edges.length} (${edges.length / 2} unique, stored 2x)`)

  // 6. Sort and write data.parquet
  console.log('\nWriting data.parquet (sorted by $id)...')
  entities.sort((a, b) => a.$id.localeCompare(b.$id))

  const dataRows = entities.map(e => ({
    $id: e.$id,
    data: JSON.stringify(e),
  }))

  const dataBuffer = parquetWriteBuffer({
    columnData: [
      { name: '$id', data: dataRows.map(r => r.$id), type: 'STRING' },
      { name: 'data', data: dataRows.map(r => r.data), type: 'JSON' },
    ],
    // Use SNAPPY for good compression (UNCOMPRESSED for debugging only)
    codec: 'SNAPPY',
  })

  fs.writeFileSync(path.join(OUTPUT_DIR, 'data.parquet'), Buffer.from(dataBuffer))
  console.log(`  Written ${entities.length} entities (${(dataBuffer.byteLength / 1024).toFixed(1)} KB)`)

  // 7. Write sharded rels files by source type
  console.log('\nWriting sharded rels files...')

  // Determine source namespace from from_id format
  const getSourceNs = (fromId: string): string => {
    // SOC codes like "11-1011.00" are occupations
    if (/^\d{2}-\d{4}\.\d{2}$/.test(fromId)) return 'occupations'
    // Element IDs: 1.A.x = abilities, 2.A/2.B = skills, 2.C = knowledge
    if (fromId.startsWith('1.A')) return 'abilities'
    if (fromId.startsWith('2.A') || fromId.startsWith('2.B')) return 'skills'
    if (fromId.startsWith('2.C')) return 'knowledge'
    return 'other'
  }

  // Group edges by source namespace
  const edgesByNs: Record<string, typeof edges> = {}
  for (const edge of edges) {
    const ns = getSourceNs(edge.from_id)
    if (!edgesByNs[ns]) edgesByNs[ns] = []
    edgesByNs[ns].push(edge)
  }

  // Write each shard
  fs.mkdirSync(path.join(OUTPUT_DIR, 'rels'), { recursive: true })
  let totalRelsBytes = 0

  for (const [ns, nsEdges] of Object.entries(edgesByNs)) {
    nsEdges.sort((a, b) => a.from_id.localeCompare(b.from_id))

    const relsRows = nsEdges.map(e => ({
      from_id: e.from_id,
      data: JSON.stringify(e.data),
    }))

    const relsBuffer = parquetWriteBuffer({
      columnData: [
        { name: 'from_id', data: relsRows.map(r => r.from_id), type: 'STRING' },
        { name: 'data', data: relsRows.map(r => r.data), type: 'JSON' },
      ],
      codec: 'SNAPPY',
    })

    fs.writeFileSync(path.join(OUTPUT_DIR, 'rels', `${ns}.parquet`), Buffer.from(relsBuffer))
    console.log(`  rels/${ns}.parquet: ${nsEdges.length} edges (${(relsBuffer.byteLength / 1024).toFixed(1)} KB)`)
    totalRelsBytes += relsBuffer.byteLength
  }

  console.log(`  Total rels: ${edges.length} edges (${(totalRelsBytes / 1024).toFixed(1)} KB)`)

  // 8. Summary
  console.log('\n=== Summary ===')
  console.log(`Entities: ${entities.length}`)
  console.log(`  - Occupations: ${occupations.length}`)
  console.log(`  - Skills: ${skills.length}`)
  console.log(`  - Abilities: ${abilities.length}`)
  console.log(`  - Knowledge: ${knowledge.length}`)
  console.log(`Edges: ${edges.length} (${edges.length / 2} unique × 2)`)
  console.log(`\nOutput:`)
  console.log(`  ${OUTPUT_DIR}/data.parquet - ${(dataBuffer.byteLength / 1024).toFixed(1)} KB`)
  console.log(`  ${OUTPUT_DIR}/rels/ - ${(totalRelsBytes / 1024).toFixed(1)} KB total`)
  for (const [ns, nsEdges] of Object.entries(edgesByNs)) {
    console.log(`    ${ns}: ${nsEdges.length} edges`)
  }
  console.log(`\nOptimized for:`)
  console.log(`  - Single column reads (no columnar assembly)`)
  console.log(`  - Sharded rels by source namespace (smaller files)`)
  console.log(`  - Sorted by ID (row group stats enable skipping)`)
}

main().catch(console.error)
