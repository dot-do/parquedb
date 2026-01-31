#!/usr/bin/env npx tsx
/**
 * ETL: Transform O*NET relational data into ParqueDB graph structure
 *
 * Source: 41 relational Parquet files
 * Target: Graph with occupations, skills, abilities, knowledge as entities
 *         and rich relationships with importance/level scores on edges
 */

import { ParquetReader } from '../src/parquet/reader'
import { FsBackend } from '../src/storage/FsBackend'
import { parquetWriteBuffer } from 'hyparquet-writer'
import * as fs from 'fs'
import * as path from 'path'

// Helper to convert objects to columnData format for hyparquet-writer
function objectsToColumns(objects: Record<string, unknown>[]): { name: string; data: unknown[] }[] {
  if (objects.length === 0) return []
  const keys = Object.keys(objects[0])
  return keys.map(name => ({
    name,
    data: objects.map(obj => obj[name]),
  }))
}

const INPUT_DIR = 'data/onet'
const OUTPUT_DIR = 'data/onet-graph'

interface OccupationRow {
  'O*NET-SOC Code': string
  'Title': string
  'Description': string
}

interface SkillRow {
  'O*NET-SOC Code': string
  'Element ID': string
  'Element Name': string
  'Scale ID': string
  'Data Value': string
  'N': string
  'Standard Error': string
  'Lower CI Bound': string
  'Upper CI Bound': string
  'Recommend Suppress': string
  'Not Relevant': string
  'Date': string
  'Domain Source': string
}

interface ContentModelRow {
  'Element ID': string
  'Element Name': string
  'Description': string
}

async function loadParquet<T>(filePath: string): Promise<T[]> {
  const storage = new FsBackend(process.cwd())
  const reader = new ParquetReader({ storage })
  return reader.read<T>(filePath)
}

async function main() {
  console.log('=== O*NET Graph ETL ===\n')

  // Create output directory (simple: just data.parquet + rels.parquet)
  fs.mkdirSync(OUTPUT_DIR, { recursive: true })

  // 1. Load reference data (skills, abilities, knowledge definitions)
  console.log('Loading Content Model Reference...')
  const contentModel = await loadParquet<ContentModelRow>(
    path.join(INPUT_DIR, 'Content Model Reference.parquet')
  )
  console.log(`  Loaded ${contentModel.length} element definitions`)

  // Build lookup maps
  const elementMap = new Map<string, ContentModelRow>()
  for (const row of contentModel) {
    elementMap.set(row['Element ID'], row)
  }

  // Categorize elements by type (based on Element ID prefix)
  // O*NET Content Model structure:
  // 1.A.* = Abilities (Cognitive, Psychomotor, Physical, Sensory)
  // 2.A.* = Basic Skills (Content, Process)
  // 2.B.* = Cross-Functional Skills (Social, Technical, Systems, Resource Management, Complex Problem Solving)
  // 2.C.* = Knowledge
  const skills: ContentModelRow[] = []
  const abilities: ContentModelRow[] = []
  const knowledge: ContentModelRow[] = []

  for (const row of contentModel) {
    const id = row['Element ID']
    if (id.startsWith('2.A.') || id.startsWith('2.B.')) skills.push(row)  // Basic + Cross-Functional Skills
    else if (id.startsWith('1.A.')) abilities.push(row)  // Abilities
    else if (id.startsWith('2.C.')) knowledge.push(row)  // Knowledge
  }

  console.log(`  Skills: ${skills.length}, Abilities: ${abilities.length}, Knowledge: ${knowledge.length}`)

  // 2. Load occupations
  console.log('\nLoading Occupations...')
  const occupations = await loadParquet<OccupationRow>(
    path.join(INPUT_DIR, 'Occupation Data.parquet')
  )
  console.log(`  Loaded ${occupations.length} occupations`)

  // 3. Load relationship data (skills, abilities, knowledge scores)
  console.log('\nLoading relationship data...')

  const skillsData = await loadParquet<SkillRow>(
    path.join(INPUT_DIR, 'Skills.parquet')
  )
  console.log(`  Skills relationships: ${skillsData.length}`)

  const abilitiesData = await loadParquet<SkillRow>(
    path.join(INPUT_DIR, 'Abilities.parquet')
  )
  console.log(`  Abilities relationships: ${abilitiesData.length}`)

  const knowledgeData = await loadParquet<SkillRow>(
    path.join(INPUT_DIR, 'Knowledge.parquet')
  )
  console.log(`  Knowledge relationships: ${knowledgeData.length}`)

  // 4. Build occupation entities with embedded relationship summaries
  console.log('\nBuilding occupation entities...')

  // Group relationship data by occupation
  const occSkills = new Map<string, SkillRow[]>()
  const occAbilities = new Map<string, SkillRow[]>()
  const occKnowledge = new Map<string, SkillRow[]>()

  for (const row of skillsData) {
    const code = row['O*NET-SOC Code']
    if (!occSkills.has(code)) occSkills.set(code, [])
    occSkills.get(code)!.push(row)
  }

  for (const row of abilitiesData) {
    const code = row['O*NET-SOC Code']
    if (!occAbilities.has(code)) occAbilities.set(code, [])
    occAbilities.get(code)!.push(row)
  }

  for (const row of knowledgeData) {
    const code = row['O*NET-SOC Code']
    if (!occKnowledge.has(code)) occKnowledge.set(code, [])
    occKnowledge.get(code)!.push(row)
  }

  // Build occupation entities
  const occupationEntities = occupations.map(occ => {
    const code = occ['O*NET-SOC Code']
    const occSkillRows = occSkills.get(code) || []
    const occAbilityRows = occAbilities.get(code) || []
    const occKnowledgeRows = occKnowledge.get(code) || []

    // Build skill relationships with scores
    const skillRels: Record<string, string> = {}
    const skillScores: Record<string, { importance?: number, level?: number }> = {}

    for (const row of occSkillRows) {
      const elementId = row['Element ID']
      const elementName = row['Element Name']
      const scaleId = row['Scale ID']
      const value = parseFloat(row['Data Value'])

      // Use element name as relationship key, element ID as target
      skillRels[elementName] = `skills/${elementId}`

      if (!skillScores[elementName]) skillScores[elementName] = {}
      if (scaleId === 'IM') skillScores[elementName].importance = value
      if (scaleId === 'LV') skillScores[elementName].level = value
    }

    // Build ability relationships
    const abilityRels: Record<string, string> = {}
    const abilityScores: Record<string, { importance?: number, level?: number }> = {}

    for (const row of occAbilityRows) {
      const elementId = row['Element ID']
      const elementName = row['Element Name']
      const scaleId = row['Scale ID']
      const value = parseFloat(row['Data Value'])

      abilityRels[elementName] = `abilities/${elementId}`

      if (!abilityScores[elementName]) abilityScores[elementName] = {}
      if (scaleId === 'IM') abilityScores[elementName].importance = value
      if (scaleId === 'LV') abilityScores[elementName].level = value
    }

    // Build knowledge relationships
    const knowledgeRels: Record<string, string> = {}
    const knowledgeScores: Record<string, { importance?: number, level?: number }> = {}

    for (const row of occKnowledgeRows) {
      const elementId = row['Element ID']
      const elementName = row['Element Name']
      const scaleId = row['Scale ID']
      const value = parseFloat(row['Data Value'])

      knowledgeRels[elementName] = `knowledge/${elementId}`

      if (!knowledgeScores[elementName]) knowledgeScores[elementName] = {}
      if (scaleId === 'IM') knowledgeScores[elementName].importance = value
      if (scaleId === 'LV') knowledgeScores[elementName].level = value
    }

    return {
      $id: `occupations/${code}`,
      $type: 'Occupation',
      name: occ['Title'],
      code,
      description: occ['Description'],
      // Relationships with counts
      skills: {
        $count: Object.keys(skillRels).length,
        ...skillRels,
      },
      skillScores,
      abilities: {
        $count: Object.keys(abilityRels).length,
        ...abilityRels,
      },
      abilityScores,
      knowledge: {
        $count: Object.keys(knowledgeRels).length,
        ...knowledgeRels,
      },
      knowledgeScores,
    }
  })

  console.log(`  Built ${occupationEntities.length} occupation entities`)

  // 5. Build skill entities
  const skillEntities = skills.map(skill => ({
    $id: `skills/${skill['Element ID']}`,
    $type: 'Skill',
    name: skill['Element Name'],
    elementId: skill['Element ID'],
    description: skill['Description'],
    // Reverse relationship: which occupations require this skill
    requiredBy: {
      $count: 0, // Will be computed
      $reverse: 'skills',
    },
  }))

  // 6. Build ability entities
  const abilityEntities = abilities.map(ability => ({
    $id: `abilities/${ability['Element ID']}`,
    $type: 'Ability',
    name: ability['Element Name'],
    elementId: ability['Element ID'],
    description: ability['Description'],
    requiredBy: {
      $count: 0,
      $reverse: 'abilities',
    },
  }))

  // 7. Build knowledge entities
  const knowledgeEntities = knowledge.map(k => ({
    $id: `knowledge/${k['Element ID']}`,
    $type: 'Knowledge',
    name: k['Element Name'],
    elementId: k['Element ID'],
    description: k['Description'],
    requiredBy: {
      $count: 0,
      $reverse: 'knowledge',
    },
  }))

  // 8. Build relationship edges with full data
  console.log('\nBuilding relationship edges...')

  const forwardRels: Array<{
    from_ns: string
    from_id: string
    from_name: string
    from_type: string
    predicate: string
    reverse: string
    to_ns: string
    to_id: string
    to_name: string
    to_type: string
    importance: number | null
    level: number | null
    standardError: number | null
    date: string | null
  }> = []

  // Process skills relationships
  for (const row of skillsData) {
    if (row['Scale ID'] !== 'IM') continue // Only create one edge per pair

    const occCode = row['O*NET-SOC Code']
    const occ = occupations.find(o => o['O*NET-SOC Code'] === occCode)
    if (!occ) continue

    const levelRow = skillsData.find(
      r => r['O*NET-SOC Code'] === occCode &&
           r['Element ID'] === row['Element ID'] &&
           r['Scale ID'] === 'LV'
    )

    forwardRels.push({
      from_ns: 'occupations',
      from_id: occCode,
      from_name: occ['Title'],
      from_type: 'Occupation',
      predicate: 'skills',
      reverse: 'requiredBy',
      to_ns: 'skills',
      to_id: row['Element ID'],
      to_name: row['Element Name'],
      to_type: 'Skill',
      importance: parseFloat(row['Data Value']),
      level: levelRow ? parseFloat(levelRow['Data Value']) : null,
      standardError: parseFloat(row['Standard Error']),
      date: row['Date'],
    })
  }

  // Process abilities relationships
  for (const row of abilitiesData) {
    if (row['Scale ID'] !== 'IM') continue

    const occCode = row['O*NET-SOC Code']
    const occ = occupations.find(o => o['O*NET-SOC Code'] === occCode)
    if (!occ) continue

    const levelRow = abilitiesData.find(
      r => r['O*NET-SOC Code'] === occCode &&
           r['Element ID'] === row['Element ID'] &&
           r['Scale ID'] === 'LV'
    )

    forwardRels.push({
      from_ns: 'occupations',
      from_id: occCode,
      from_name: occ['Title'],
      from_type: 'Occupation',
      predicate: 'abilities',
      reverse: 'requiredBy',
      to_ns: 'abilities',
      to_id: row['Element ID'],
      to_name: row['Element Name'],
      to_type: 'Ability',
      importance: parseFloat(row['Data Value']),
      level: levelRow ? parseFloat(levelRow['Data Value']) : null,
      standardError: parseFloat(row['Standard Error']),
      date: row['Date'],
    })
  }

  // Process knowledge relationships
  for (const row of knowledgeData) {
    if (row['Scale ID'] !== 'IM') continue

    const occCode = row['O*NET-SOC Code']
    const occ = occupations.find(o => o['O*NET-SOC Code'] === occCode)
    if (!occ) continue

    const levelRow = knowledgeData.find(
      r => r['O*NET-SOC Code'] === occCode &&
           r['Element ID'] === row['Element ID'] &&
           r['Scale ID'] === 'LV'
    )

    forwardRels.push({
      from_ns: 'occupations',
      from_id: occCode,
      from_name: occ['Title'],
      from_type: 'Occupation',
      predicate: 'knowledge',
      reverse: 'requiredBy',
      to_ns: 'knowledge',
      to_id: row['Element ID'],
      to_name: row['Element Name'],
      to_type: 'Knowledge',
      importance: parseFloat(row['Data Value']),
      level: levelRow ? parseFloat(levelRow['Data Value']) : null,
      standardError: parseFloat(row['Standard Error']),
      date: row['Date'],
    })
  }

  console.log(`  Built ${forwardRels.length} relationship edges`)

  // 9. Write output files: data.parquet (all nodes) + rels.parquet (all edges both directions)
  console.log('\nWriting output files...')

  // Combine ALL entities into single data.parquet
  const allNodes = [
    // Occupations
    ...occupationEntities.map(occ => ({
      $id: occ.$id,
      $type: occ.$type,
      name: occ.name,
      code: occ.code,
      elementId: null as string | null,
      description: occ.description,
    })),
    // Skills
    ...skillEntities.map(s => ({
      $id: s.$id,
      $type: s.$type,
      name: s.name,
      code: null as string | null,
      elementId: s.elementId,
      description: s.description,
    })),
    // Abilities
    ...abilityEntities.map(a => ({
      $id: a.$id,
      $type: a.$type,
      name: a.name,
      code: null as string | null,
      elementId: a.elementId,
      description: a.description,
    })),
    // Knowledge
    ...knowledgeEntities.map(k => ({
      $id: k.$id,
      $type: k.$type,
      name: k.name,
      code: null as string | null,
      elementId: k.elementId,
      description: k.description,
    })),
  ]

  // Sort by $id for fast lookups
  allNodes.sort((a, b) => a.$id.localeCompare(b.$id))

  const dataBuffer = parquetWriteBuffer({ columnData: objectsToColumns(allNodes) })
  fs.writeFileSync(path.join(OUTPUT_DIR, 'data.parquet'), Buffer.from(dataBuffer))
  console.log(`  Wrote ${allNodes.length} nodes to data.parquet`)

  // Build ALL edges in BOTH directions into single rels.parquet
  const allEdges: typeof forwardRels = []

  // Add forward edges (occupation -> skill/ability/knowledge)
  for (const rel of forwardRels) {
    allEdges.push(rel)
  }

  // Add reverse edges (skill/ability/knowledge -> occupation)
  for (const rel of forwardRels) {
    allEdges.push({
      from_ns: rel.to_ns,
      from_id: rel.to_id,
      from_name: rel.to_name,
      from_type: rel.to_type,
      predicate: rel.reverse,  // 'requiredBy'
      reverse: rel.predicate,  // 'skills', 'abilities', 'knowledge'
      to_ns: rel.from_ns,
      to_id: rel.from_id,
      to_name: rel.from_name,
      to_type: rel.from_type,
      importance: rel.importance,
      level: rel.level,
      standardError: rel.standardError,
      date: rel.date,
    })
  }

  // Sort by (from_ns, from_id, predicate) for fast lookups in either direction
  allEdges.sort((a, b) => {
    const nsCompare = a.from_ns.localeCompare(b.from_ns)
    if (nsCompare !== 0) return nsCompare
    const idCompare = a.from_id.localeCompare(b.from_id)
    if (idCompare !== 0) return idCompare
    return a.predicate.localeCompare(b.predicate)
  })

  const relsBuffer = parquetWriteBuffer({ columnData: objectsToColumns(allEdges) })
  fs.writeFileSync(path.join(OUTPUT_DIR, 'rels.parquet'), Buffer.from(relsBuffer))
  console.log(`  Wrote ${allEdges.length} edges to rels.parquet (${forwardRels.length} forward + ${forwardRels.length} reverse)`)

  // Summary
  console.log('\n=== ETL Complete ===')
  console.log(`Output: ${OUTPUT_DIR}`)
  console.log(`Entities: ${occupationEntities.length + skillEntities.length + abilityEntities.length + knowledgeEntities.length}`)
  console.log(`Relationships: ${forwardRels.length}`)
}

main().catch(console.error)
