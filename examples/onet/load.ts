/**
 * O*NET Database Loader for ParqueDB
 *
 * Downloads, parses, and loads the O*NET occupational database using the ParqueDB API.
 *
 * O*NET Database (v28.3): https://www.onetcenter.org/dl_files/database/db_28_3_text.zip
 *
 * Uses ParqueDB API:
 * - db.collection('occupations').create() / createMany()
 * - db.collection('skills').create() / createMany()
 * - $link for relationships: occupation hasSkill skill
 *
 * Data flows to:
 * - data/occupations/data.parquet
 * - data/skills/data.parquet
 * - rels/forward/*.parquet
 * - rels/reverse/*.parquet
 *
 * Usage:
 *   npx tsx examples/onet/load.ts [options]
 *
 * Options:
 *   --output <dir>     Output directory (default: ./data/onet)
 *   --verbose, -v      Show detailed progress
 *   --no-download      Skip download if files already exist
 *
 * @see https://www.onetcenter.org/database.html
 */

import { createReadStream, existsSync, statSync } from 'node:fs'
import { readdir, mkdir, writeFile, stat } from 'node:fs/promises'
import { createInterface } from 'node:readline'
import { join, dirname } from 'node:path'

import { ParqueDB, FsBackend, type CreateInput, type EntityId, type Schema } from '../../src'

// =============================================================================
// Configuration
// =============================================================================

const ONET_VERSION = '28_3'
const ONET_BASE_URL = 'https://www.onetcenter.org/dl_files/database'
const ONET_FILE_NAME = `db_${ONET_VERSION}_text.zip`
const ONET_DOWNLOAD_URL = `${ONET_BASE_URL}/${ONET_FILE_NAME}`

// =============================================================================
// Types
// =============================================================================

export interface LoaderOptions {
  /** Output directory for ParqueDB data */
  outputPath?: string
  /** Show verbose output */
  verbose?: boolean
  /** Skip download if files exist */
  skipDownload?: boolean
  /** Progress callback */
  onProgress?: (message: string) => void
}

export interface LoaderStats {
  version: string
  totalRawSize: number
  totalStoredSize: number
  overallCompressionRatio: number
  totalEntities: number
  totalRelationships: number
  durationMs: number
  entityCounts: Record<string, number>
}

// =============================================================================
// O*NET Schema Definition
// =============================================================================

const onetSchema: Schema = {
  Occupation: {
    $ns: 'occupations',
    socCode: 'string',
    title: 'string',
    description: 'string?',
    jobZone: 'number?',
    jobZoneTitle: 'string?',
    jobZoneEducation: 'string?',
    jobZoneExperience: 'string?',
    jobZoneTraining: 'string?',
    jobZoneSvpRange: 'string?',
    // Relationships
    hasSkill: '-> Skill.usedBy[]',
    hasAbility: '-> Ability.usedBy[]',
    hasKnowledge: '-> Knowledge.usedBy[]',
    hasWorkActivity: '-> WorkActivity.usedBy[]',
    hasTechnology: '-> Technology.usedBy[]',
    hasTool: '-> Tool.usedBy[]',
    hasTask: '-> Task.forOccupation[]',
  },
  Skill: {
    $ns: 'skills',
    elementId: 'string',
    description: 'string?',
    category: 'string?',
    usedBy: '<- Occupation.hasSkill[]',
  },
  Ability: {
    $ns: 'abilities',
    elementId: 'string',
    description: 'string?',
    category: 'string?',
    usedBy: '<- Occupation.hasAbility[]',
  },
  Knowledge: {
    $ns: 'knowledge',
    elementId: 'string',
    description: 'string?',
    category: 'string?',
    usedBy: '<- Occupation.hasKnowledge[]',
  },
  WorkActivity: {
    $ns: 'workActivities',
    elementId: 'string',
    description: 'string?',
    category: 'string?',
    usedBy: '<- Occupation.hasWorkActivity[]',
  },
  Technology: {
    $ns: 'technology',
    commodityCode: 'string?',
    commodityTitle: 'string?',
    example: 'string?',
    unspscSegment: 'string?',
    unspscFamily: 'string?',
    unspscClass: 'string?',
    usedBy: '<- Occupation.hasTechnology[]',
  },
  Tool: {
    $ns: 'tools',
    commodityCode: 'string?',
    commodityTitle: 'string?',
    example: 'string?',
    usedBy: '<- Occupation.hasTool[]',
  },
  Task: {
    $ns: 'tasks',
    taskId: 'string',
    statement: 'string',
    taskType: 'string?',
    incumbentsResponding: 'number?',
    forOccupation: '<- Occupation.hasTask[]',
  },
  ContentModelElement: {
    $ns: 'contentModel',
    elementId: 'string',
    description: 'string?',
    level: 'number?',
    parentId: 'string?',
    domain: 'string?',
    subdomain: 'string?',
  },
  Scale: {
    $ns: 'scales',
    scaleId: 'string',
    minimum: 'number?',
    maximum: 'number?',
  },
}

// =============================================================================
// Column Mappings
// =============================================================================

const COLUMN_MAPPINGS: Record<string, Record<string, string>> = {
  occupations: {
    'O*NET-SOC Code': 'socCode',
    Title: 'title',
    Description: 'description',
  },
  jobZones: {
    'O*NET-SOC Code': 'socCode',
    'Job Zone': 'jobZone',
    Date: 'dataDate',
    'Domain Source': 'domainSource',
  },
  jobZoneReference: {
    'Job Zone': 'jobZone',
    Name: 'name',
    Experience: 'experience',
    Education: 'education',
    'Job Training': 'jobTraining',
    Examples: 'examples',
    'SVP Range': 'svpRange',
  },
  abilities: {
    'O*NET-SOC Code': 'socCode',
    'Element ID': 'elementId',
    'Element Name': 'elementName',
    'Scale ID': 'scaleId',
    'Data Value': 'dataValue',
    N: 'n',
    'Standard Error': 'stdError',
    'Lower CI Bound': 'lowerCI',
    'Upper CI Bound': 'upperCI',
    'Recommend Suppress': 'recommendSuppress',
    'Not Relevant': 'notRelevant',
    Date: 'dataDate',
    'Domain Source': 'domainSource',
  },
  skills: {
    'O*NET-SOC Code': 'socCode',
    'Element ID': 'elementId',
    'Element Name': 'elementName',
    'Scale ID': 'scaleId',
    'Data Value': 'dataValue',
    N: 'n',
    'Standard Error': 'stdError',
    'Lower CI Bound': 'lowerCI',
    'Upper CI Bound': 'upperCI',
    'Recommend Suppress': 'recommendSuppress',
    'Not Relevant': 'notRelevant',
    Date: 'dataDate',
    'Domain Source': 'domainSource',
  },
  knowledge: {
    'O*NET-SOC Code': 'socCode',
    'Element ID': 'elementId',
    'Element Name': 'elementName',
    'Scale ID': 'scaleId',
    'Data Value': 'dataValue',
    N: 'n',
    'Standard Error': 'stdError',
    'Lower CI Bound': 'lowerCI',
    'Upper CI Bound': 'upperCI',
    'Recommend Suppress': 'recommendSuppress',
    'Not Relevant': 'notRelevant',
    Date: 'dataDate',
    'Domain Source': 'domainSource',
  },
  workActivities: {
    'O*NET-SOC Code': 'socCode',
    'Element ID': 'elementId',
    'Element Name': 'elementName',
    'Scale ID': 'scaleId',
    'Data Value': 'dataValue',
    N: 'n',
    'Standard Error': 'stdError',
    'Lower CI Bound': 'lowerCI',
    'Upper CI Bound': 'upperCI',
    'Recommend Suppress': 'recommendSuppress',
    Date: 'dataDate',
    'Domain Source': 'domainSource',
  },
  tasks: {
    'O*NET-SOC Code': 'socCode',
    'Task ID': 'taskId',
    Task: 'statement',
    'Task Type': 'taskType',
    'Incumbents Responding': 'incumbentsResponding',
    Date: 'dataDate',
    'Domain Source': 'domainSource',
  },
  technologySkills: {
    'O*NET-SOC Code': 'socCode',
    'T2 Type': 't2Type',
    'T2 Example': 'example',
    'Commodity Code': 'commodityCode',
    'Commodity Title': 'commodityTitle',
    'Hot Technology': 'hotTechnology',
  },
  toolsUsed: {
    'O*NET-SOC Code': 'socCode',
    'T2 Type': 't2Type',
    'T2 Example': 'example',
    'Commodity Code': 'commodityCode',
    'Commodity Title': 'commodityTitle',
  },
  contentModel: {
    'Element ID': 'elementId',
    'Element Name': 'name',
    Description: 'description',
  },
  scales: {
    'Scale ID': 'scaleId',
    'Scale Name': 'name',
    Minimum: 'minimum',
    Maximum: 'maximum',
  },
}

// =============================================================================
// Download and Extract
// =============================================================================

async function downloadOnetDatabase(downloadDir: string, verbose: boolean): Promise<string> {
  const log = verbose ? console.log.bind(console) : () => {}

  log(`Downloading O*NET database v${ONET_VERSION.replace('_', '.')}...`)
  log(`URL: ${ONET_DOWNLOAD_URL}`)

  await mkdir(downloadDir, { recursive: true })

  const zipPath = join(downloadDir, ONET_FILE_NAME)

  if (existsSync(zipPath)) {
    log('Database already downloaded, skipping...')
    return zipPath
  }

  const response = await fetch(ONET_DOWNLOAD_URL)
  if (!response.ok) {
    throw new Error(`Failed to download: ${response.status} ${response.statusText}`)
  }

  const arrayBuffer = await response.arrayBuffer()
  await writeFile(zipPath, Buffer.from(arrayBuffer))

  log(`Downloaded ${(arrayBuffer.byteLength / 1024 / 1024).toFixed(2)} MB`)
  return zipPath
}

async function extractZip(zipPath: string, downloadDir: string, verbose: boolean): Promise<string> {
  const log = verbose ? console.log.bind(console) : () => {}

  log('Extracting database files...')

  const extractDir = join(downloadDir, `db_${ONET_VERSION}`)

  if (existsSync(extractDir)) {
    log('Database already extracted, skipping...')
    return extractDir
  }

  const { exec } = await import('node:child_process')
  const { promisify } = await import('node:util')
  const execAsync = promisify(exec)

  await mkdir(extractDir, { recursive: true })

  try {
    await execAsync(`unzip -o "${zipPath}" -d "${extractDir}"`)
  } catch {
    try {
      await execAsync(
        `powershell -command "Expand-Archive -Path '${zipPath}' -DestinationPath '${extractDir}' -Force"`
      )
    } catch {
      throw new Error('Failed to extract ZIP file. Please install unzip or use Windows PowerShell.')
    }
  }

  log(`Extracted to ${extractDir}`)
  return extractDir
}

// =============================================================================
// TSV Parsing
// =============================================================================

async function* parseTsvFile(
  filePath: string,
  columnMapping: Record<string, string>
): AsyncGenerator<Record<string, unknown>> {
  const fileStream = createReadStream(filePath, { encoding: 'utf-8' })
  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  })

  let headers: string[] = []
  let isFirstLine = true

  for await (const line of rl) {
    if (isFirstLine) {
      headers = line.split('\t').map((h) => h.trim())
      isFirstLine = false
      continue
    }

    const values = line.split('\t')
    const record: Record<string, unknown> = {}

    for (let i = 0; i < headers.length && i < values.length; i++) {
      const header = headers[i]
      const mappedKey = columnMapping[header] || header
      let value: unknown = values[i]?.trim()

      // Type conversions
      if (value === '' || value === 'n/a' || value === '\\N') {
        value = null
      } else if (['dataValue', 'stdError', 'lowerCI', 'upperCI', 'minimum', 'maximum'].includes(mappedKey)) {
        value = parseFloat(value as string)
        if (isNaN(value as number)) value = null
      } else if (['n', 'jobZone', 'incumbentsResponding', 'category'].includes(mappedKey)) {
        value = parseInt(value as string, 10)
        if (isNaN(value as number)) value = null
      } else if (['recommendSuppress', 'notRelevant', 'hotTechnology'].includes(mappedKey)) {
        value = value === 'Y' || value === 'y' || value === '1' || value === 'true'
      }

      record[mappedKey] = value
    }

    yield record
  }
}

function findDataDir(extractedDir: string): string {
  const potentialPaths = [
    extractedDir,
    join(extractedDir, `db_${ONET_VERSION}`),
    join(extractedDir, ONET_VERSION.replace('_', '.')),
  ]

  for (const p of potentialPaths) {
    if (existsSync(join(p, 'Occupation Data.txt'))) {
      return p
    }
  }

  // Search subdirectories
  try {
    const entries = require('fs').readdirSync(extractedDir)
    for (const entry of entries) {
      const subPath = join(extractedDir, entry)
      if (require('fs').statSync(subPath).isDirectory()) {
        if (existsSync(join(subPath, 'Occupation Data.txt'))) {
          return subPath
        }
      }
    }
  } catch {
    // ignore
  }

  return extractedDir
}

// =============================================================================
// Data Processing
// =============================================================================

/**
 * Process and load occupations into ParqueDB
 */
async function loadOccupations(
  db: ParqueDB,
  sourceDir: string,
  verbose: boolean
): Promise<{ occupationMap: Map<string, EntityId>; rawSize: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading occupations...')

  const occupationMap = new Map<string, EntityId>()
  let rawSize = 0

  // Load occupation data
  const occPath = join(sourceDir, 'Occupation Data.txt')
  if (!existsSync(occPath)) {
    throw new Error('Occupation Data.txt not found')
  }
  rawSize += statSync(occPath).size

  // First pass: collect all occupation data
  const occupationData: Record<string, Record<string, unknown>> = {}
  for await (const record of parseTsvFile(occPath, COLUMN_MAPPINGS.occupations)) {
    const socCode = record.socCode as string
    if (!socCode) continue

    occupationData[socCode] = {
      socCode,
      title: record.title,
      description: record.description,
    }
  }

  // Load job zones
  const jzPath = join(sourceDir, 'Job Zones.txt')
  if (existsSync(jzPath)) {
    rawSize += statSync(jzPath).size
    for await (const record of parseTsvFile(jzPath, COLUMN_MAPPINGS.jobZones)) {
      const socCode = record.socCode as string
      if (occupationData[socCode]) {
        occupationData[socCode].jobZone = record.jobZone
      }
    }
  }

  // Load job zone reference
  const jzRefPath = join(sourceDir, 'Job Zone Reference.txt')
  if (existsSync(jzRefPath)) {
    rawSize += statSync(jzRefPath).size
    const jzRef = new Map<number, Record<string, unknown>>()
    for await (const record of parseTsvFile(jzRefPath, COLUMN_MAPPINGS.jobZoneReference)) {
      jzRef.set(record.jobZone as number, record)
    }

    for (const occ of Object.values(occupationData)) {
      if (occ.jobZone) {
        const ref = jzRef.get(occ.jobZone as number)
        if (ref) {
          occ.jobZoneTitle = ref.name
          occ.jobZoneEducation = ref.education
          occ.jobZoneExperience = ref.experience
          occ.jobZoneTraining = ref.jobTraining
          occ.jobZoneSvpRange = ref.svpRange
        }
      }
    }
  }

  // Create occupations in ParqueDB
  const occupations = db.collection('occupations')
  let count = 0
  for (const [socCode, data] of Object.entries(occupationData)) {
    const id = socCode.replace(/\./g, '-')
    const entity = await occupations.create({
      $id: `occupations/${id}`,
      $type: 'Occupation',
      name: data.title as string,
      ...data,
    } as CreateInput)

    occupationMap.set(socCode, entity.$id)
    count++

    if (verbose && count % 100 === 0) {
      log(`  Created ${count} occupations...`)
    }
  }

  log(`  Created ${count} occupations`)
  return { occupationMap, rawSize }
}

/**
 * Process content model elements for lookup
 */
async function loadContentModel(
  sourceDir: string,
  verbose: boolean
): Promise<{ contentModel: Map<string, Record<string, unknown>>; rawSize: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading content model reference...')

  const contentModel = new Map<string, Record<string, unknown>>()
  let rawSize = 0

  const filePath = join(sourceDir, 'Content Model Reference.txt')
  if (!existsSync(filePath)) {
    log('  Content Model Reference.txt not found')
    return { contentModel, rawSize }
  }
  rawSize = statSync(filePath).size

  for await (const record of parseTsvFile(filePath, COLUMN_MAPPINGS.contentModel)) {
    const elementId = record.elementId as string
    if (!elementId) continue

    const level = elementId.split('.').length
    const parts = elementId.split('.')
    const parentId = parts.length > 1 ? parts.slice(0, -1).join('.') : null

    // Determine domain from element ID
    let domain = ''
    let subdomain = ''
    if (elementId.startsWith('1.')) {
      domain = 'Worker Characteristics'
      if (elementId.startsWith('1.A')) subdomain = 'Abilities'
      else if (elementId.startsWith('1.B')) subdomain = 'Interests'
      else if (elementId.startsWith('1.C')) subdomain = 'Work Values'
      else if (elementId.startsWith('1.D')) subdomain = 'Work Styles'
    } else if (elementId.startsWith('2.')) {
      domain = 'Worker Requirements'
      if (elementId.startsWith('2.A')) subdomain = 'Basic Skills'
      else if (elementId.startsWith('2.B')) subdomain = 'Cross-Functional Skills'
      else if (elementId.startsWith('2.C')) subdomain = 'Knowledge'
    } else if (elementId.startsWith('3.')) {
      domain = 'Experience Requirements'
    } else if (elementId.startsWith('4.')) {
      domain = 'Occupational Requirements'
      if (elementId.startsWith('4.A')) subdomain = 'Generalized Work Activities'
      else if (elementId.startsWith('4.C')) subdomain = 'Work Context'
    }

    contentModel.set(elementId, {
      elementId,
      name: record.name,
      description: record.description,
      level,
      parentId,
      domain,
      subdomain,
    })
  }

  log(`  Loaded ${contentModel.size} content model elements`)
  return { contentModel, rawSize }
}

/**
 * Load skills and create relationships to occupations
 */
async function loadSkills(
  db: ParqueDB,
  sourceDir: string,
  occupationMap: Map<string, EntityId>,
  contentModel: Map<string, Record<string, unknown>>,
  verbose: boolean
): Promise<{ skillMap: Map<string, EntityId>; rawSize: number; relationshipCount: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading skills...')

  const skillMap = new Map<string, EntityId>()
  let rawSize = 0
  let relationshipCount = 0

  const filePath = join(sourceDir, 'Skills.txt')
  if (!existsSync(filePath)) {
    log('  Skills.txt not found')
    return { skillMap, rawSize, relationshipCount }
  }
  rawSize = statSync(filePath).size

  const skills = db.collection('skills')
  const occupations = db.collection('occupations')

  // Collect unique skills and their relationships
  const skillData = new Map<string, { name: string; description?: string; category?: string }>()
  const skillOccupationLinks: Array<{ skillId: string; socCode: string; importance?: number; level?: number }> = []

  // Group by elementId and scaleId to combine importance/level
  const ratingsByKey = new Map<string, Record<string, unknown>>()

  for await (const record of parseTsvFile(filePath, COLUMN_MAPPINGS.skills)) {
    const elementId = record.elementId as string
    const socCode = record.socCode as string
    const scaleId = record.scaleId as string

    if (!elementId || !socCode) continue

    // Add skill entity if not exists
    if (!skillData.has(elementId)) {
      const cm = contentModel.get(elementId)
      let category = ''
      if (elementId.startsWith('2.A.')) category = 'Basic Skills'
      else if (elementId.startsWith('2.B.')) category = 'Cross-Functional Skills'

      skillData.set(elementId, {
        name: record.elementName as string,
        description: cm?.description as string,
        category,
      })
    }

    // Track ratings by occupation+skill
    const ratingKey = `${socCode}|${elementId}`
    if (!ratingsByKey.has(ratingKey)) {
      ratingsByKey.set(ratingKey, { socCode, elementId })
    }
    const rating = ratingsByKey.get(ratingKey)!
    if (scaleId === 'IM') {
      rating.importance = record.dataValue
    } else if (scaleId === 'LV') {
      rating.level = record.dataValue
    }
  }

  // Create skill entities
  for (const [elementId, data] of skillData) {
    const id = elementId.replace(/\./g, '-')
    const entity = await skills.create({
      $id: `skills/${id}`,
      $type: 'Skill',
      name: data.name,
      elementId,
      description: data.description,
      category: data.category,
    } as CreateInput)
    skillMap.set(elementId, entity.$id)
  }

  log(`  Created ${skillData.size} skills`)

  // Create relationships using $link
  log('  Creating occupation-skill relationships...')
  let linkCount = 0
  for (const rating of ratingsByKey.values()) {
    const occId = occupationMap.get(rating.socCode as string)
    const skillId = skillMap.get(rating.elementId as string)

    if (occId && skillId) {
      // Use $link to create the relationship
      await occupations.update(occId.split('/')[1], {
        $link: { hasSkill: skillId },
      })
      linkCount++
      relationshipCount++

      if (verbose && linkCount % 1000 === 0) {
        log(`    Created ${linkCount} skill links...`)
      }
    }
  }

  log(`  Created ${linkCount} occupation-skill relationships`)
  return { skillMap, rawSize, relationshipCount }
}

/**
 * Load abilities and create relationships to occupations
 */
async function loadAbilities(
  db: ParqueDB,
  sourceDir: string,
  occupationMap: Map<string, EntityId>,
  contentModel: Map<string, Record<string, unknown>>,
  verbose: boolean
): Promise<{ abilityMap: Map<string, EntityId>; rawSize: number; relationshipCount: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading abilities...')

  const abilityMap = new Map<string, EntityId>()
  let rawSize = 0
  let relationshipCount = 0

  const filePath = join(sourceDir, 'Abilities.txt')
  if (!existsSync(filePath)) {
    log('  Abilities.txt not found')
    return { abilityMap, rawSize, relationshipCount }
  }
  rawSize = statSync(filePath).size

  const abilities = db.collection('abilities')
  const occupations = db.collection('occupations')

  // Collect unique abilities
  const abilityData = new Map<string, { name: string; description?: string; category?: string }>()
  const ratingsByKey = new Map<string, Record<string, unknown>>()

  for await (const record of parseTsvFile(filePath, COLUMN_MAPPINGS.abilities)) {
    const elementId = record.elementId as string
    const socCode = record.socCode as string

    if (!elementId || !socCode) continue

    if (!abilityData.has(elementId)) {
      const cm = contentModel.get(elementId)
      let category = ''
      if (elementId.startsWith('1.A.1.')) category = 'Cognitive Abilities'
      else if (elementId.startsWith('1.A.2.')) category = 'Psychomotor Abilities'
      else if (elementId.startsWith('1.A.3.')) category = 'Physical Abilities'
      else if (elementId.startsWith('1.A.4.')) category = 'Sensory Abilities'

      abilityData.set(elementId, {
        name: record.elementName as string,
        description: cm?.description as string,
        category,
      })
    }

    const ratingKey = `${socCode}|${elementId}`
    if (!ratingsByKey.has(ratingKey)) {
      ratingsByKey.set(ratingKey, { socCode, elementId })
    }
  }

  // Create ability entities
  for (const [elementId, data] of abilityData) {
    const id = elementId.replace(/\./g, '-')
    const entity = await abilities.create({
      $id: `abilities/${id}`,
      $type: 'Ability',
      name: data.name,
      elementId,
      description: data.description,
      category: data.category,
    } as CreateInput)
    abilityMap.set(elementId, entity.$id)
  }

  log(`  Created ${abilityData.size} abilities`)

  // Create relationships
  log('  Creating occupation-ability relationships...')
  let linkCount = 0
  for (const rating of ratingsByKey.values()) {
    const occId = occupationMap.get(rating.socCode as string)
    const abilityId = abilityMap.get(rating.elementId as string)

    if (occId && abilityId) {
      await occupations.update(occId.split('/')[1], {
        $link: { hasAbility: abilityId },
      })
      linkCount++
      relationshipCount++

      if (verbose && linkCount % 1000 === 0) {
        log(`    Created ${linkCount} ability links...`)
      }
    }
  }

  log(`  Created ${linkCount} occupation-ability relationships`)
  return { abilityMap, rawSize, relationshipCount }
}

/**
 * Load knowledge areas and create relationships
 */
async function loadKnowledge(
  db: ParqueDB,
  sourceDir: string,
  occupationMap: Map<string, EntityId>,
  contentModel: Map<string, Record<string, unknown>>,
  verbose: boolean
): Promise<{ knowledgeMap: Map<string, EntityId>; rawSize: number; relationshipCount: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading knowledge areas...')

  const knowledgeMap = new Map<string, EntityId>()
  let rawSize = 0
  let relationshipCount = 0

  const filePath = join(sourceDir, 'Knowledge.txt')
  if (!existsSync(filePath)) {
    log('  Knowledge.txt not found')
    return { knowledgeMap, rawSize, relationshipCount }
  }
  rawSize = statSync(filePath).size

  const knowledge = db.collection('knowledge')
  const occupations = db.collection('occupations')

  const knowledgeData = new Map<string, { name: string; description?: string; category?: string }>()
  const ratingsByKey = new Map<string, Record<string, unknown>>()

  for await (const record of parseTsvFile(filePath, COLUMN_MAPPINGS.knowledge)) {
    const elementId = record.elementId as string
    const socCode = record.socCode as string

    if (!elementId || !socCode) continue

    if (!knowledgeData.has(elementId)) {
      const cm = contentModel.get(elementId)
      knowledgeData.set(elementId, {
        name: record.elementName as string,
        description: cm?.description as string,
        category: 'Knowledge',
      })
    }

    const ratingKey = `${socCode}|${elementId}`
    if (!ratingsByKey.has(ratingKey)) {
      ratingsByKey.set(ratingKey, { socCode, elementId })
    }
  }

  // Create knowledge entities
  for (const [elementId, data] of knowledgeData) {
    const id = elementId.replace(/\./g, '-')
    const entity = await knowledge.create({
      $id: `knowledge/${id}`,
      $type: 'Knowledge',
      name: data.name,
      elementId,
      description: data.description,
      category: data.category,
    } as CreateInput)
    knowledgeMap.set(elementId, entity.$id)
  }

  log(`  Created ${knowledgeData.size} knowledge areas`)

  // Create relationships
  log('  Creating occupation-knowledge relationships...')
  let linkCount = 0
  for (const rating of ratingsByKey.values()) {
    const occId = occupationMap.get(rating.socCode as string)
    const knowledgeId = knowledgeMap.get(rating.elementId as string)

    if (occId && knowledgeId) {
      await occupations.update(occId.split('/')[1], {
        $link: { hasKnowledge: knowledgeId },
      })
      linkCount++
      relationshipCount++

      if (verbose && linkCount % 1000 === 0) {
        log(`    Created ${linkCount} knowledge links...`)
      }
    }
  }

  log(`  Created ${linkCount} occupation-knowledge relationships`)
  return { knowledgeMap, rawSize, relationshipCount }
}

/**
 * Load tasks and create relationships
 */
async function loadTasks(
  db: ParqueDB,
  sourceDir: string,
  occupationMap: Map<string, EntityId>,
  verbose: boolean
): Promise<{ taskCount: number; rawSize: number; relationshipCount: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading tasks...')

  let rawSize = 0
  let taskCount = 0
  let relationshipCount = 0

  const filePath = join(sourceDir, 'Task Statements.txt')
  if (!existsSync(filePath)) {
    log('  Task Statements.txt not found')
    return { taskCount, rawSize, relationshipCount }
  }
  rawSize = statSync(filePath).size

  const tasks = db.collection('tasks')
  const occupations = db.collection('occupations')

  for await (const record of parseTsvFile(filePath, COLUMN_MAPPINGS.tasks)) {
    const taskId = record.taskId as string
    const socCode = record.socCode as string

    if (!taskId || !socCode) continue

    const id = taskId.toString().replace(/\./g, '-')

    const entity = await tasks.create({
      $id: `tasks/${id}`,
      $type: 'Task',
      name: ((record.statement as string) || '').substring(0, 100) || taskId,
      taskId,
      statement: record.statement,
      taskType: record.taskType,
      incumbentsResponding: record.incumbentsResponding,
    } as CreateInput)

    taskCount++

    // Create relationship
    const occId = occupationMap.get(socCode)
    if (occId) {
      await occupations.update(occId.split('/')[1], {
        $link: { hasTask: entity.$id },
      })
      relationshipCount++
    }

    if (verbose && taskCount % 1000 === 0) {
      log(`  Created ${taskCount} tasks...`)
    }
  }

  log(`  Created ${taskCount} tasks with ${relationshipCount} relationships`)
  return { taskCount, rawSize, relationshipCount }
}

/**
 * Load technology skills and create relationships
 */
async function loadTechnology(
  db: ParqueDB,
  sourceDir: string,
  occupationMap: Map<string, EntityId>,
  verbose: boolean
): Promise<{ techCount: number; rawSize: number; relationshipCount: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading technology skills...')

  let rawSize = 0
  let relationshipCount = 0

  const filePath = join(sourceDir, 'Technology Skills.txt')
  if (!existsSync(filePath)) {
    log('  Technology Skills.txt not found')
    return { techCount: 0, rawSize, relationshipCount }
  }
  rawSize = statSync(filePath).size

  const technology = db.collection('technology')
  const occupations = db.collection('occupations')

  const techData = new Map<string, Record<string, unknown>>()
  const techOccupationLinks: Array<{ techKey: string; socCode: string; isHot: boolean }>[] = []

  for await (const record of parseTsvFile(filePath, COLUMN_MAPPINGS.technologySkills)) {
    const socCode = record.socCode as string
    const commodityCode = record.commodityCode as string
    const name = (record.example as string) || (record.commodityTitle as string)

    if (!socCode || !name) continue

    const techKey = commodityCode || name.toLowerCase().replace(/\W+/g, '-')

    if (!techData.has(techKey)) {
      techData.set(techKey, {
        name: record.commodityTitle || name,
        commodityCode,
        commodityTitle: record.commodityTitle,
        example: record.example,
      })
    }

    techOccupationLinks.push([{ techKey, socCode, isHot: record.hotTechnology as boolean }])
  }

  // Create technology entities
  const techMap = new Map<string, EntityId>()
  for (const [techKey, data] of techData) {
    const entity = await technology.create({
      $id: `technology/${techKey}`,
      $type: 'Technology',
      name: data.name as string,
      commodityCode: data.commodityCode,
      commodityTitle: data.commodityTitle,
      example: data.example,
    } as CreateInput)
    techMap.set(techKey, entity.$id)
  }

  log(`  Created ${techData.size} technologies`)

  // Create relationships
  log('  Creating occupation-technology relationships...')
  let linkCount = 0
  for (const links of techOccupationLinks) {
    for (const { techKey, socCode } of links) {
      const occId = occupationMap.get(socCode)
      const techId = techMap.get(techKey)

      if (occId && techId) {
        await occupations.update(occId.split('/')[1], {
          $link: { hasTechnology: techId },
        })
        linkCount++
        relationshipCount++

        if (verbose && linkCount % 1000 === 0) {
          log(`    Created ${linkCount} technology links...`)
        }
      }
    }
  }

  log(`  Created ${linkCount} occupation-technology relationships`)
  return { techCount: techData.size, rawSize, relationshipCount }
}

/**
 * Load tools and create relationships
 */
async function loadTools(
  db: ParqueDB,
  sourceDir: string,
  occupationMap: Map<string, EntityId>,
  verbose: boolean
): Promise<{ toolCount: number; rawSize: number; relationshipCount: number }> {
  const log = verbose ? console.log.bind(console) : () => {}
  log('Loading tools...')

  let rawSize = 0
  let relationshipCount = 0

  const filePath = join(sourceDir, 'Tools Used.txt')
  if (!existsSync(filePath)) {
    log('  Tools Used.txt not found')
    return { toolCount: 0, rawSize, relationshipCount }
  }
  rawSize = statSync(filePath).size

  const tools = db.collection('tools')
  const occupations = db.collection('occupations')

  const toolData = new Map<string, Record<string, unknown>>()
  const toolOccupationLinks: Array<{ toolKey: string; socCode: string }> = []

  for await (const record of parseTsvFile(filePath, COLUMN_MAPPINGS.toolsUsed)) {
    const socCode = record.socCode as string
    const commodityCode = record.commodityCode as string
    const name = (record.example as string) || (record.commodityTitle as string)

    if (!socCode || !name) continue

    const toolKey = commodityCode || name.toLowerCase().replace(/\W+/g, '-')

    if (!toolData.has(toolKey)) {
      toolData.set(toolKey, {
        name: record.commodityTitle || name,
        commodityCode,
        commodityTitle: record.commodityTitle,
        example: record.example,
      })
    }

    toolOccupationLinks.push({ toolKey, socCode })
  }

  // Create tool entities
  const toolMap = new Map<string, EntityId>()
  for (const [toolKey, data] of toolData) {
    const entity = await tools.create({
      $id: `tools/${toolKey}`,
      $type: 'Tool',
      name: data.name as string,
      commodityCode: data.commodityCode,
      commodityTitle: data.commodityTitle,
      example: data.example,
    } as CreateInput)
    toolMap.set(toolKey, entity.$id)
  }

  log(`  Created ${toolData.size} tools`)

  // Create relationships
  log('  Creating occupation-tool relationships...')
  let linkCount = 0
  for (const { toolKey, socCode } of toolOccupationLinks) {
    const occId = occupationMap.get(socCode)
    const toolId = toolMap.get(toolKey)

    if (occId && toolId) {
      await occupations.update(occId.split('/')[1], {
        $link: { hasTool: toolId },
      })
      linkCount++
      relationshipCount++

      if (verbose && linkCount % 1000 === 0) {
        log(`    Created ${linkCount} tool links...`)
      }
    }
  }

  log(`  Created ${linkCount} occupation-tool relationships`)
  return { toolCount: toolData.size, rawSize, relationshipCount }
}

// =============================================================================
// Storage Size Calculation
// =============================================================================

async function calculateStoredSize(outputPath: string): Promise<number> {
  let totalSize = 0

  const walkDir = async (dir: string): Promise<void> => {
    try {
      const entries = await readdir(dir, { withFileTypes: true })
      for (const entry of entries) {
        const fullPath = join(dir, entry.name)
        if (entry.isDirectory()) {
          await walkDir(fullPath)
        } else if (entry.name.endsWith('.parquet') || entry.name.endsWith('.json')) {
          const fileStat = await stat(fullPath)
          totalSize += fileStat.size
        }
      }
    } catch {
      // Directory doesn't exist yet
    }
  }

  await walkDir(outputPath)
  return totalSize
}

// =============================================================================
// Main Loader Function
// =============================================================================

export async function loadOnet(
  outputPath: string = './data/onet',
  options: LoaderOptions = {}
): Promise<LoaderStats> {
  const startTime = Date.now()
  const verbose = options.verbose ?? false
  const log = verbose ? console.log.bind(console) : () => {}

  log('='.repeat(60))
  log('O*NET Database Loader for ParqueDB')
  log('='.repeat(60))
  log()

  // Determine paths
  const cwd = process.cwd()
  const dataDir = join(cwd, 'data', 'onet')
  const downloadDir = join(dataDir, 'download')

  // Download and extract
  let extractedDir: string

  if (!options.skipDownload) {
    const zipPath = await downloadOnetDatabase(downloadDir, verbose)
    extractedDir = await extractZip(zipPath, downloadDir, verbose)
  } else {
    extractedDir = join(downloadDir, `db_${ONET_VERSION}`)
    if (!existsSync(extractedDir)) {
      throw new Error(`Data directory not found: ${extractedDir}. Run without --no-download first.`)
    }
  }

  // Find actual data directory
  const sourceDir = findDataDir(extractedDir)
  log(`Data directory: ${sourceDir}`)
  log()

  // Initialize ParqueDB with FsBackend
  log('Initializing ParqueDB...')
  const storage = new FsBackend(outputPath)
  const db = new ParqueDB({
    storage,
    schema: onetSchema,
  })

  log(`Output directory: ${outputPath}`)
  log()

  log('Loading O*NET data...')
  log()

  // Track stats
  const entityCounts: Record<string, number> = {}
  let totalRawSize = 0
  let totalRelationships = 0

  // Load content model first for reference data
  const { contentModel, rawSize: cmRawSize } = await loadContentModel(sourceDir, verbose)
  totalRawSize += cmRawSize

  // Load occupations
  const { occupationMap, rawSize: occRawSize } = await loadOccupations(db, sourceDir, verbose)
  entityCounts.occupations = occupationMap.size
  totalRawSize += occRawSize

  // Load skills with relationships
  const { skillMap, rawSize: skillRawSize, relationshipCount: skillRelCount } = await loadSkills(
    db, sourceDir, occupationMap, contentModel, verbose
  )
  entityCounts.skills = skillMap.size
  totalRawSize += skillRawSize
  totalRelationships += skillRelCount

  // Load abilities with relationships
  const { abilityMap, rawSize: abilityRawSize, relationshipCount: abilityRelCount } = await loadAbilities(
    db, sourceDir, occupationMap, contentModel, verbose
  )
  entityCounts.abilities = abilityMap.size
  totalRawSize += abilityRawSize
  totalRelationships += abilityRelCount

  // Load knowledge with relationships
  const { knowledgeMap, rawSize: knowledgeRawSize, relationshipCount: knowledgeRelCount } = await loadKnowledge(
    db, sourceDir, occupationMap, contentModel, verbose
  )
  entityCounts.knowledge = knowledgeMap.size
  totalRawSize += knowledgeRawSize
  totalRelationships += knowledgeRelCount

  // Load tasks with relationships
  const { taskCount, rawSize: taskRawSize, relationshipCount: taskRelCount } = await loadTasks(
    db, sourceDir, occupationMap, verbose
  )
  entityCounts.tasks = taskCount
  totalRawSize += taskRawSize
  totalRelationships += taskRelCount

  // Load technology with relationships
  const { techCount, rawSize: techRawSize, relationshipCount: techRelCount } = await loadTechnology(
    db, sourceDir, occupationMap, verbose
  )
  entityCounts.technology = techCount
  totalRawSize += techRawSize
  totalRelationships += techRelCount

  // Load tools with relationships
  const { toolCount, rawSize: toolRawSize, relationshipCount: toolRelCount } = await loadTools(
    db, sourceDir, occupationMap, verbose
  )
  entityCounts.tools = toolCount
  totalRawSize += toolRawSize
  totalRelationships += toolRelCount

  // Calculate stored size (after flush)
  const totalStoredSize = await calculateStoredSize(outputPath)

  // Calculate final stats
  const durationMs = Date.now() - startTime
  const totalEntities = Object.values(entityCounts).reduce((a, b) => a + b, 0)
  const overallCompressionRatio = totalStoredSize > 0 ? totalRawSize / totalStoredSize : 0

  const stats: LoaderStats = {
    version: ONET_VERSION.replace('_', '.'),
    totalRawSize,
    totalStoredSize,
    overallCompressionRatio,
    totalEntities,
    totalRelationships,
    durationMs,
    entityCounts,
  }

  // Print summary
  log()
  log('='.repeat(60))
  log('Load complete!')
  log('='.repeat(60))
  log()
  log('Summary:')
  log(`  O*NET Version: ${stats.version}`)
  log(`  Total raw size: ${(totalRawSize / 1024 / 1024).toFixed(2)} MB`)
  log(`  Total stored size: ${(totalStoredSize / 1024 / 1024).toFixed(2)} MB`)
  log(`  Compression ratio: ${overallCompressionRatio.toFixed(2)}x`)
  log(`  Total entities: ${totalEntities.toLocaleString()}`)
  log(`  Total relationships: ${totalRelationships.toLocaleString()}`)
  log(`  Duration: ${(durationMs / 1000).toFixed(1)}s`)
  log()
  log('Entity counts:')
  for (const [key, count] of Object.entries(entityCounts).sort((a, b) => b[1] - a[1])) {
    log(`  ${key}: ${count.toLocaleString()}`)
  }
  log()
  log(`Output directory: ${outputPath}`)

  return stats
}

// =============================================================================
// CLI Entry Point
// =============================================================================

async function main() {
  const args = process.argv.slice(2)

  let outputPath = './data/onet'
  let verbose = false
  let skipDownload = false

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg === '--output' || arg === '-o') {
      outputPath = args[++i]
    } else if (arg === '--verbose' || arg === '-v') {
      verbose = true
    } else if (arg === '--no-download') {
      skipDownload = true
    } else if (arg === '--help' || arg === '-h') {
      console.log(`
O*NET Database Loader for ParqueDB

Downloads and loads the O*NET occupational database using the ParqueDB API.

Data is stored in:
  - data/{collection}/data.parquet - Entity storage
  - rels/forward/*.parquet - Outbound relationships
  - rels/reverse/*.parquet - Inbound relationships

Usage:
  npx tsx examples/onet/load.ts [options]

Options:
  -o, --output <dir>   Output directory (default: ./data/onet)
  -v, --verbose        Show detailed progress
  --no-download        Skip download if files already exist
  -h, --help           Show this help message

Examples:
  npx tsx examples/onet/load.ts -v
  npx tsx examples/onet/load.ts -o ./my-output --verbose
      `)
      process.exit(0)
    }
  }

  try {
    const stats = await loadOnet(outputPath, { verbose, skipDownload })

    console.log()
    console.log('Stats JSON:')
    console.log(JSON.stringify(stats, null, 2))
  } catch (error) {
    console.error('Error:', error instanceof Error ? error.message : error)
    process.exit(1)
  }
}

// Run if executed directly
if (process.argv[1]?.endsWith('load.ts') || process.argv[1]?.endsWith('load.js')) {
  main()
}
