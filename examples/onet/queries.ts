/**
 * O*NET Database Query Examples for ParqueDB - COMPLETE CONTENT MODEL
 *
 * This file demonstrates various queries against the FULL O*NET occupational database
 * loaded into ParqueDB. These examples cover all 40+ entity types:
 *
 * WORKER CHARACTERISTICS (1.x):
 * - Abilities, Interests, Work Values, Work Styles
 *
 * WORKER REQUIREMENTS (2.x):
 * - Skills, Knowledge, Education
 *
 * OCCUPATIONAL REQUIREMENTS (4.x):
 * - Generalized/Detailed Work Activities, Work Context
 *
 * OCCUPATION-SPECIFIC (6.x):
 * - Tasks, Tools & Technology, Alternate Titles
 *
 * WORKFORCE (5.x):
 * - Labor Market Info, Occupational Outlook
 *
 * @see ./schema.ts for entity definitions
 * @see ./load.ts for data loading
 */

// import { ParqueDB, R2Backend, FsBackend, MemoryBackend } from 'parquedb'
// import { onetSchema } from './schema'

/**
 * Initialize ParqueDB with FULL O*NET schema
 */
export async function initializeOnetDB() {
  // Example with local filesystem (Node.js)
  // const db = new ParqueDB(new FsBackend('./data/onet/parquet'), onetSchema)

  // Example with Cloudflare R2
  // const db = new ParqueDB(new R2Backend(env.R2_BUCKET), onetSchema)

  console.log('ParqueDB would be initialized with FULL O*NET schema (40+ entity types)')

  return {
    // Mock DB methods for demonstration - ALL entity types
    // Core
    Occupations: mockCollection('Occupation'),

    // Worker Characteristics (1.x)
    Abilities: mockCollection('Ability'),
    OccupationAbilities: mockCollection('OccupationAbility'),
    Interests: mockCollection('Interest'),
    OccupationInterests: mockCollection('OccupationInterest'),
    WorkValues: mockCollection('WorkValue'),
    OccupationWorkValues: mockCollection('OccupationWorkValue'),
    WorkStyles: mockCollection('WorkStyle'),
    OccupationWorkStyles: mockCollection('OccupationWorkStyle'),

    // Worker Requirements (2.x)
    Skills: mockCollection('Skill'),
    OccupationSkills: mockCollection('OccupationSkill'),
    Knowledge: mockCollection('Knowledge'),
    OccupationKnowledge: mockCollection('OccupationKnowledge'),
    Education: mockCollection('Education'),
    OccupationEducation: mockCollection('OccupationEducation'),

    // Experience Requirements (3.x)
    OccupationExperience: mockCollection('OccupationExperience'),
    Licenses: mockCollection('License'),
    OccupationLicenses: mockCollection('OccupationLicense'),

    // Occupational Requirements (4.x)
    WorkActivities: mockCollection('WorkActivity'),
    OccupationWorkActivities: mockCollection('OccupationWorkActivity'),
    IntermediateWorkActivities: mockCollection('IntermediateWorkActivity'),
    DetailedWorkActivities: mockCollection('DetailedWorkActivity'),
    OccupationDWAs: mockCollection('OccupationDWA'),
    WorkContexts: mockCollection('WorkContext'),
    OccupationWorkContexts: mockCollection('OccupationWorkContext'),

    // Occupation-Specific (6.x)
    Tasks: mockCollection('Task'),
    TaskRatings: mockCollection('TaskRating'),
    TaskDWAs: mockCollection('TaskDWA'),
    Technologies: mockCollection('Technology'),
    OccupationTechnologies: mockCollection('OccupationTechnology'),
    Tools: mockCollection('Tool'),
    OccupationTools: mockCollection('OccupationTool'),
    AlternativeTitles: mockCollection('AlternativeTitle'),

    // Reference
    UNSPSC: mockCollection('UNSPSC'),
    ContentModel: mockCollection('ContentModelElement'),
    Scales: mockCollection('Scale'),
    ScaleAnchors: mockCollection('ScaleAnchor'),

    // Crosswalks
    RelatedOccupations: mockCollection('RelatedOccupation'),
    CareerChangers: mockCollection('CareerChangerMatrix'),
    CareerStarters: mockCollection('CareerStarterMatrix'),
    SOCCrosswalks: mockCollection('SOCCrosswalk'),

    // Workforce
    LaborMarketInfo: mockCollection('LaborMarketInfo'),
    OccupationalOutlook: mockCollection('OccupationalOutlook'),
  }
}

type OnetDB = Awaited<ReturnType<typeof initializeOnetDB>>

// Mock collection for demonstration
function mockCollection(type: string) {
  return {
    find: async (filter: object, options?: object) => {
      console.log(`db.${type}.find(${JSON.stringify(filter)}, ${JSON.stringify(options)})`)
      return []
    },
    get: async (id: string, options?: object) => {
      console.log(`db.${type}.get('${id}', ${JSON.stringify(options)})`)
      return null
    },
    count: async (filter: object) => {
      console.log(`db.${type}.count(${JSON.stringify(filter)})`)
      return 0
    },
    aggregate: async (pipeline: object[]) => {
      console.log(`db.${type}.aggregate(${JSON.stringify(pipeline)})`)
      return []
    },
    referencedBy: async (id: string, field: string, options?: object) => {
      console.log(`db.${type}.referencedBy('${id}', '${field}', ${JSON.stringify(options)})`)
      return []
    },
  }
}

// =============================================================================
// BASIC QUERIES
// =============================================================================

/**
 * Find occupations by title search
 */
export async function findSoftwareOccupations(db: OnetDB) {
  console.log('\n=== Find Software Occupations ===\n')

  const results = await db.Occupations.find(
    { title: { $contains: 'Software' } },
    { limit: 10, sort: { title: 1 } }
  )

  return results
}

/**
 * Find occupations by SOC code pattern
 *
 * O*NET-SOC codes follow a hierarchical structure:
 * - 15-*: Computer and Mathematical Occupations
 * - 29-*: Healthcare Practitioners
 * - 11-*: Management Occupations
 */
export async function findOccupationsBySOCCode(db: OnetDB) {
  console.log('\n=== Find Occupations by SOC Code ===\n')

  // All computer occupations (15-*)
  const computerOccupations = await db.Occupations.find(
    { socCode: { $startsWith: '15-' } },
    { limit: 50, sort: { socCode: 1 } }
  )

  return computerOccupations
}

/**
 * Find occupations by job zone (education/training level)
 *
 * Job Zones:
 * 1 - Little or No Preparation Needed
 * 2 - Some Preparation Needed
 * 3 - Medium Preparation Needed
 * 4 - Considerable Preparation Needed
 * 5 - Extensive Preparation Needed
 */
export async function findOccupationsByJobZone(db: OnetDB) {
  console.log('\n=== Find Occupations by Job Zone ===\n')

  // High-level occupations (Job Zone 5) - typically require doctoral/professional degree
  const extensivePrep = await db.Occupations.find(
    { jobZone: 5 },
    { limit: 20, sort: { title: 1 } }
  )

  return extensivePrep
}

// =============================================================================
// WORKER CHARACTERISTICS (1.x) QUERIES
// =============================================================================

/**
 * Find abilities by category
 *
 * 52 abilities across 4 categories:
 * - Cognitive Abilities (1.A.1): Verbal, Quantitative, Reasoning, etc.
 * - Psychomotor Abilities (1.A.2): Fine Motor, Control Movement, Reaction Time
 * - Physical Abilities (1.A.3): Strength, Flexibility, Endurance
 * - Sensory Abilities (1.A.4): Visual, Auditory, Speech
 */
export async function findAbilitiesByCategory(db: OnetDB) {
  console.log('\n=== Find Abilities by Category ===\n')

  const cognitiveAbilities = await db.Abilities.find(
    { category: 'Cognitive Abilities' },
    { sort: { name: 1 } }
  )

  const physicalAbilities = await db.Abilities.find(
    { category: 'Physical Abilities' },
    { sort: { name: 1 } }
  )

  return { cognitiveAbilities, physicalAbilities }
}

/**
 * Query Holland codes (RIASEC) for an occupation
 *
 * Holland codes represent occupational interests:
 * R - Realistic: Practical, physical, hands-on
 * I - Investigative: Analytical, intellectual, scientific
 * A - Artistic: Creative, original, independent
 * S - Social: Helping, teaching, counseling
 * E - Enterprising: Persuading, leading, managing
 * C - Conventional: Detail-oriented, organizing, clerical
 */
export async function getOccupationHollandCodes(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Holland Codes ===\n')

  const id = socCode.replace(/\./g, '-')

  // Get interest ratings for the occupation
  const interestRatings = await db.OccupationInterests.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    {
      sort: { dataValue: -1 }, // Highest interest scores first
      populate: ['interest'],
    }
  )

  return interestRatings
}

/**
 * Find occupations matching work values
 *
 * 6 work values:
 * - Achievement: Using abilities, getting results
 * - Working Conditions: Job security, variety, compensation
 * - Recognition: Advancement, authority, status
 * - Relationships: Co-workers, moral values, social service
 * - Support: Company policies, supervision
 * - Independence: Creativity, responsibility, autonomy
 */
export async function findOccupationsByWorkValue(db: OnetDB, workValueName: string) {
  console.log('\n=== Find Occupations by Work Value ===\n')

  // Find the work value first
  const workValues = await db.WorkValues.find({ name: workValueName })

  // Find occupations with high ratings on this work value
  const ratings = await db.OccupationWorkValues.find(
    {
      'workValue.name': workValueName,
      extent: { $gte: 5.0 }, // High extent rating
    },
    {
      sort: { extent: -1 },
      limit: 50,
      populate: ['occupation'],
    }
  )

  return ratings
}

/**
 * Query work styles for an occupation
 *
 * 16 work styles representing personal characteristics
 */
export async function getOccupationWorkStyles(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Work Styles ===\n')

  const id = socCode.replace(/\./g, '-')

  const workStyleRatings = await db.OccupationWorkStyles.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    {
      sort: { importance: -1 },
      populate: ['workStyle'],
    }
  )

  return workStyleRatings
}

// =============================================================================
// WORKER REQUIREMENTS (2.x) QUERIES
// =============================================================================

/**
 * Find skills by category
 *
 * 35 skills in two main categories:
 * - Basic Skills (2.A): Content skills, Process skills
 * - Cross-Functional Skills (2.B): Social, Problem Solving, Technical, Systems, Resource Management
 */
export async function findSkillsByCategory(db: OnetDB) {
  console.log('\n=== Find Skills by Category ===\n')

  const basicSkills = await db.Skills.find(
    { category: 'Basic Skills' },
    { sort: { name: 1 } }
  )

  const crossFunctionalSkills = await db.Skills.find(
    { category: 'Cross-Functional Skills' },
    { sort: { name: 1 } }
  )

  return { basicSkills, crossFunctionalSkills }
}

/**
 * Get complete skill profile for an occupation
 */
export async function getOccupationSkillProfile(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Skill Profile ===\n')

  const id = socCode.replace(/\./g, '-')

  // Get skill ratings sorted by importance
  const skillRatings = await db.OccupationSkills.find(
    {
      'occupation.$id': `onet/occupations/${id}`,
      notRelevant: { $ne: true },
    },
    {
      sort: { importance: -1 },
      populate: ['skill'],
    }
  )

  return skillRatings
}

/**
 * Find occupations requiring specific knowledge
 *
 * 33 knowledge areas organized by domain
 */
export async function findOccupationsRequiringKnowledge(
  db: OnetDB,
  knowledgeName: string,
  minImportance: number = 3.5
) {
  console.log('\n=== Find Occupations Requiring Knowledge ===\n')

  const ratings = await db.OccupationKnowledge.find(
    {
      'knowledge.name': knowledgeName,
      importance: { $gte: minImportance },
    },
    {
      sort: { importance: -1 },
      limit: 50,
      populate: ['occupation'],
    }
  )

  return ratings
}

// =============================================================================
// OCCUPATIONAL REQUIREMENTS (4.x) QUERIES
// =============================================================================

/**
 * Get Generalized Work Activities (GWAs) for an occupation
 *
 * 41 GWAs organized into 4 categories:
 * - Information Input: Getting and receiving information
 * - Mental Processes: Processing, planning, decision-making
 * - Work Output: Performing physical activities
 * - Interacting with Others: Communication and interaction
 */
export async function getOccupationWorkActivities(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Work Activities ===\n')

  const id = socCode.replace(/\./g, '-')

  const workActivityRatings = await db.OccupationWorkActivities.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    {
      sort: { importance: -1 },
      populate: ['workActivity'],
    }
  )

  return workActivityRatings
}

/**
 * Query Detailed Work Activities (DWAs)
 *
 * Over 2,000 specific work activities linked to occupations and tasks
 */
export async function findDetailedWorkActivities(db: OnetDB, searchTerm: string) {
  console.log('\n=== Find Detailed Work Activities ===\n')

  const dwas = await db.DetailedWorkActivities.find(
    { name: { $contains: searchTerm } },
    { limit: 50, sort: { name: 1 } }
  )

  return dwas
}

/**
 * Get DWAs for an occupation
 */
export async function getOccupationDWAs(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation DWAs ===\n')

  const id = socCode.replace(/\./g, '-')

  const occupationDWAs = await db.OccupationDWAs.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    { populate: ['dwa'] }
  )

  return occupationDWAs
}

/**
 * Get work context for an occupation
 *
 * 57 work context descriptors covering:
 * - Interpersonal Relationships
 * - Physical Work Conditions
 * - Structural Job Characteristics
 */
export async function getOccupationWorkContext(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Work Context ===\n')

  const id = socCode.replace(/\./g, '-')

  const workContextRatings = await db.OccupationWorkContexts.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    {
      sort: { dataValue: -1 },
      populate: ['workContext'],
    }
  )

  return workContextRatings
}

/**
 * Find occupations by work context (e.g., outdoor work, hazardous conditions)
 */
export async function findOccupationsByWorkContext(db: OnetDB, contextName: string, minValue: number = 3) {
  console.log('\n=== Find Occupations by Work Context ===\n')

  const ratings = await db.OccupationWorkContexts.find(
    {
      'workContext.name': contextName,
      dataValue: { $gte: minValue },
    },
    {
      sort: { dataValue: -1 },
      limit: 50,
      populate: ['occupation'],
    }
  )

  return ratings
}

// =============================================================================
// OCCUPATION-SPECIFIC (6.x) QUERIES
// =============================================================================

/**
 * Get tasks for an occupation
 *
 * Over 20,000 task statements across all occupations
 */
export async function getOccupationTasks(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Tasks ===\n')

  const id = socCode.replace(/\./g, '-')

  const tasks = await db.Tasks.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    { sort: { isCore: -1 } } // Core tasks first
  )

  return tasks
}

/**
 * Get task ratings (importance, frequency, relevance)
 */
export async function getTaskRatings(db: OnetDB, taskId: string) {
  console.log('\n=== Get Task Ratings ===\n')

  const ratings = await db.TaskRatings.find(
    { 'task.taskId': taskId },
    { populate: ['task'] }
  )

  return ratings
}

/**
 * Get technologies used in an occupation (with UNSPSC codes!)
 *
 * Technologies include software, applications, and programming languages
 * Each is linked to UNSPSC commodity codes for cross-referencing
 */
export async function getOccupationTechnologies(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Technologies ===\n')

  const id = socCode.replace(/\./g, '-')

  const techUsage = await db.OccupationTechnologies.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    {
      populate: ['technology'],
    }
  )

  return techUsage
}

/**
 * Find hot technologies (frequently appearing in job postings)
 */
export async function findHotTechnologies(db: OnetDB) {
  console.log('\n=== Find Hot Technologies ===\n')

  const hotTech = await db.OccupationTechnologies.find(
    { isHotTechnology: true },
    {
      populate: ['technology', 'occupation'],
      limit: 100,
    }
  )

  return hotTech
}

/**
 * Get tools used in an occupation (with UNSPSC codes!)
 *
 * Tools are physical equipment linked to UNSPSC classification
 */
export async function getOccupationTools(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Tools ===\n')

  const id = socCode.replace(/\./g, '-')

  const toolUsage = await db.OccupationTools.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    {
      populate: ['tool'],
    }
  )

  return toolUsage
}

/**
 * Query UNSPSC codes for tools and technology
 *
 * UNSPSC hierarchy:
 * - Segment (2 digits): Broad category
 * - Family (4 digits): Category
 * - Class (6 digits): Product type
 * - Commodity (8 digits): Specific product
 */
export async function queryUNSPSCHierarchy(db: OnetDB, segmentCode: string) {
  console.log('\n=== Query UNSPSC Hierarchy ===\n')

  // Get segment
  const segment = await db.UNSPSC.find({
    code: segmentCode,
    level: 'Segment',
  })

  // Get families in segment
  const families = await db.UNSPSC.find({
    code: { $startsWith: segmentCode },
    level: 'Family',
  })

  return { segment, families }
}

/**
 * Find technologies by UNSPSC code
 */
export async function findTechnologiesByUNSPSC(db: OnetDB, unspscCode: string) {
  console.log('\n=== Find Technologies by UNSPSC ===\n')

  const technologies = await db.Technologies.find({
    commodityCode: { $startsWith: unspscCode },
  })

  return technologies
}

/**
 * Get alternate titles for an occupation
 */
export async function getOccupationAlternateTitles(db: OnetDB, socCode: string) {
  console.log('\n=== Get Occupation Alternate Titles ===\n')

  const id = socCode.replace(/\./g, '-')

  const titles = await db.AlternativeTitles.find(
    { 'occupation.$id': `onet/occupations/${id}` },
    { sort: { title: 1 } }
  )

  return titles
}

// =============================================================================
// CAREER EXPLORATION QUERIES
// =============================================================================

/**
 * Find related occupations for career exploration
 */
export async function findRelatedOccupations(db: OnetDB, socCode: string) {
  console.log('\n=== Find Related Occupations ===\n')

  const id = socCode.replace(/\./g, '-')

  const related = await db.RelatedOccupations.find(
    { 'fromOccupation.$id': `onet/occupations/${id}` },
    {
      sort: { similarityScore: -1 },
      populate: ['toOccupation'],
    }
  )

  return related
}

/**
 * Get career changer paths (for experienced workers)
 *
 * Career Changers Matrix shows transition paths for experienced workers
 */
export async function getCareerChangerPaths(db: OnetDB, fromSocCode: string) {
  console.log('\n=== Get Career Changer Paths ===\n')

  const id = fromSocCode.replace(/\./g, '-')

  const paths = await db.CareerChangers.find(
    { 'fromOccupation.$id': `onet/occupations/${id}` },
    {
      sort: { transitionEase: -1 },
      populate: ['toOccupation'],
    }
  )

  return paths
}

/**
 * Get career starter paths (for new workers)
 *
 * Career Starters Matrix shows entry pathways for new workers
 */
export async function getCareerStarterPaths(db: OnetDB, fromSocCode: string) {
  console.log('\n=== Get Career Starter Paths ===\n')

  const id = fromSocCode.replace(/\./g, '-')

  const paths = await db.CareerStarters.find(
    { 'fromOccupation.$id': `onet/occupations/${id}` },
    {
      sort: { entryEase: -1 },
      populate: ['toOccupation'],
    }
  )

  return paths
}

// =============================================================================
// AGGREGATION AND ANALYSIS QUERIES
// =============================================================================

/**
 * Compare skill profiles between two occupations
 */
export async function compareOccupationSkills(db: OnetDB, socCode1: string, socCode2: string) {
  console.log('\n=== Compare Occupation Skills ===\n')

  const id1 = socCode1.replace(/\./g, '-')
  const id2 = socCode2.replace(/\./g, '-')

  const [skills1, skills2] = await Promise.all([
    db.OccupationSkills.find(
      { 'occupation.$id': `onet/occupations/${id1}`, importance: { $gte: 3.0 } },
      { populate: ['skill'] }
    ),
    db.OccupationSkills.find(
      { 'occupation.$id': `onet/occupations/${id2}`, importance: { $gte: 3.0 } },
      { populate: ['skill'] }
    ),
  ])

  return { skills1, skills2 }
}

/**
 * Aggregate skill importance across occupation groups
 */
export async function aggregateSkillImportance(db: OnetDB) {
  console.log('\n=== Aggregate Skill Importance ===\n')

  const avgImportanceBySkill = await db.OccupationSkills.aggregate([
    { $match: { notRelevant: { $ne: true } } },
    {
      $group: {
        _id: '$skill.$id',
        avgImportance: { $avg: '$importance' },
        avgLevel: { $avg: '$level' },
        count: { $sum: 1 },
      },
    },
    { $sort: { avgImportance: -1 } },
    { $limit: 20 },
  ])

  return avgImportanceBySkill
}

/**
 * Get complete occupation profile (all characteristics)
 */
export async function getCompleteOccupationProfile(db: OnetDB, socCode: string) {
  console.log('\n=== Get Complete Occupation Profile ===\n')

  const id = socCode.replace(/\./g, '-')
  const occId = `onet/occupations/${id}`

  // Fetch all related data in parallel
  const [
    occupation,
    abilities,
    interests,
    workValues,
    workStyles,
    skills,
    knowledge,
    workActivities,
    workContexts,
    tasks,
    technologies,
    tools,
    alternateTitles,
  ] = await Promise.all([
    db.Occupations.get(occId),
    db.OccupationAbilities.find({ 'occupation.$id': occId }, { populate: ['ability'], sort: { importance: -1 }, limit: 20 }),
    db.OccupationInterests.find({ 'occupation.$id': occId }, { populate: ['interest'], sort: { dataValue: -1 } }),
    db.OccupationWorkValues.find({ 'occupation.$id': occId }, { populate: ['workValue'], sort: { extent: -1 } }),
    db.OccupationWorkStyles.find({ 'occupation.$id': occId }, { populate: ['workStyle'], sort: { importance: -1 } }),
    db.OccupationSkills.find({ 'occupation.$id': occId }, { populate: ['skill'], sort: { importance: -1 }, limit: 20 }),
    db.OccupationKnowledge.find({ 'occupation.$id': occId }, { populate: ['knowledge'], sort: { importance: -1 }, limit: 20 }),
    db.OccupationWorkActivities.find({ 'occupation.$id': occId }, { populate: ['workActivity'], sort: { importance: -1 }, limit: 20 }),
    db.OccupationWorkContexts.find({ 'occupation.$id': occId }, { populate: ['workContext'], sort: { dataValue: -1 }, limit: 20 }),
    db.Tasks.find({ 'occupation.$id': occId }, { limit: 20 }),
    db.OccupationTechnologies.find({ 'occupation.$id': occId }, { populate: ['technology'] }),
    db.OccupationTools.find({ 'occupation.$id': occId }, { populate: ['tool'] }),
    db.AlternativeTitles.find({ 'occupation.$id': occId }),
  ])

  return {
    occupation,
    workerCharacteristics: { abilities, interests, workValues, workStyles },
    workerRequirements: { skills, knowledge },
    occupationalRequirements: { workActivities, workContexts },
    occupationSpecific: { tasks, technologies, tools, alternateTitles },
  }
}

// =============================================================================
// CROSS-REFERENCE QUERIES
// =============================================================================

/**
 * Search across all content types
 */
export async function searchAllContent(db: OnetDB, query: string) {
  console.log('\n=== Search All Content ===\n')

  const [occupations, skills, knowledge, abilities, tasks, technologies] = await Promise.all([
    db.Occupations.find({ $text: { $search: query } }, { limit: 10 }),
    db.Skills.find({ $text: { $search: query } }, { limit: 10 }),
    db.Knowledge.find({ $text: { $search: query } }, { limit: 10 }),
    db.Abilities.find({ $text: { $search: query } }, { limit: 10 }),
    db.Tasks.find({ $text: { $search: query } }, { limit: 10 }),
    db.Technologies.find({ $text: { $search: query } }, { limit: 10 }),
  ])

  return { occupations, skills, knowledge, abilities, tasks, technologies }
}

/**
 * Traverse from task to DWA to IWA to GWA hierarchy
 */
export async function getTaskWorkActivityHierarchy(db: OnetDB, taskId: string) {
  console.log('\n=== Get Task Work Activity Hierarchy ===\n')

  // Get task's DWAs
  const taskDWAs = await db.TaskDWAs.find(
    { 'task.taskId': taskId },
    { populate: ['dwa'] }
  )

  // For each DWA, get IWA and GWA
  const hierarchies = []
  for (const td of taskDWAs as any[]) {
    if (td.dwa) {
      const iwa = td.dwa.iwaId
        ? await db.IntermediateWorkActivities.find({ iwaId: td.dwa.iwaId })
        : null

      const gwa = td.dwa.gwaId
        ? await db.WorkActivities.find({ elementId: td.dwa.gwaId })
        : null

      hierarchies.push({
        dwa: td.dwa,
        iwa,
        gwa,
      })
    }
  }

  return hierarchies
}

// =============================================================================
// RUN ALL EXAMPLES
// =============================================================================

export async function runAllExamples() {
  console.log('='.repeat(60))
  console.log('O*NET Database Query Examples - COMPLETE CONTENT MODEL')
  console.log('='.repeat(60))

  const db = await initializeOnetDB()

  // Basic queries
  await findSoftwareOccupations(db)
  await findOccupationsBySOCCode(db)
  await findOccupationsByJobZone(db)

  // Worker Characteristics (1.x)
  await findAbilitiesByCategory(db)
  await getOccupationHollandCodes(db, '15-1252.00')
  await findOccupationsByWorkValue(db, 'Achievement')
  await getOccupationWorkStyles(db, '15-1252.00')

  // Worker Requirements (2.x)
  await findSkillsByCategory(db)
  await getOccupationSkillProfile(db, '15-1252.00')
  await findOccupationsRequiringKnowledge(db, 'Computers and Electronics', 4.0)

  // Occupational Requirements (4.x)
  await getOccupationWorkActivities(db, '15-1252.00')
  await findDetailedWorkActivities(db, 'software')
  await getOccupationDWAs(db, '15-1252.00')
  await getOccupationWorkContext(db, '15-1252.00')
  await findOccupationsByWorkContext(db, 'Spend Time Sitting', 4)

  // Occupation-Specific (6.x)
  await getOccupationTasks(db, '15-1252.00')
  await getOccupationTechnologies(db, '15-1252.00')
  await findHotTechnologies(db)
  await getOccupationTools(db, '15-1252.00')
  await queryUNSPSCHierarchy(db, '43')
  await findTechnologiesByUNSPSC(db, '43231')
  await getOccupationAlternateTitles(db, '15-1252.00')

  // Career exploration
  await findRelatedOccupations(db, '15-1252.00')
  await getCareerChangerPaths(db, '15-1252.00')
  await getCareerStarterPaths(db, '15-1252.00')

  // Aggregation and analysis
  await compareOccupationSkills(db, '15-1252.00', '15-1253.00')
  await aggregateSkillImportance(db)
  await getCompleteOccupationProfile(db, '15-1252.00')

  // Cross-reference
  await searchAllContent(db, 'software programming')

  console.log('\n' + '='.repeat(60))
  console.log('All examples completed!')
  console.log('='.repeat(60))
}

// CLI entry point
if (process.argv[1]?.endsWith('queries.ts') || process.argv[1]?.endsWith('queries.js')) {
  runAllExamples().catch(console.error)
}
