/**
 * O*NET Database Schema for ParqueDB - COMPLETE CONTENT MODEL
 *
 * This schema defines the GraphDL types for the FULL O*NET occupational database,
 * implementing all 40+ entity types from the O*NET Content Model including:
 *
 * WORKER CHARACTERISTICS (1.x):
 * - Abilities (52 types: cognitive, psychomotor, physical, sensory)
 * - Interests (6 Holland codes: RIASEC)
 * - Work Values (6 types)
 * - Work Styles (16 types)
 *
 * WORKER REQUIREMENTS (2.x):
 * - Skills (35 types: basic, cross-functional, complex problem solving)
 * - Knowledge (33 areas)
 * - Education (levels, fields of study)
 *
 * EXPERIENCE REQUIREMENTS (3.x):
 * - Experience and Training
 * - Basic Skills Entry Requirements
 * - Cross-Functional Skills Entry Requirements
 * - Licensing
 *
 * OCCUPATIONAL REQUIREMENTS (4.x):
 * - Generalized Work Activities (41 categories)
 * - Detailed Work Activities (2000+ activities)
 * - Organizational Context
 * - Work Context (57 descriptors)
 *
 * OCCUPATION-SPECIFIC INFORMATION (6.x):
 * - Tasks (20,000+ task statements)
 * - Tools and Technology (with UNSPSC codes)
 * - Alternate Titles
 *
 * WORKFORCE CHARACTERISTICS (5.x):
 * - Labor Market Information
 * - Occupational Outlook
 *
 * @see https://www.onetcenter.org/database.html
 * @see https://www.onetcenter.org/content.html
 */

import type { Schema } from '../../src/types/schema'

// =============================================================================
// O*NET CONTENT MODEL ELEMENT ID PREFIXES
// =============================================================================
//
// 1.A - Abilities
// 1.B - Interests (Holland Codes)
// 1.C - Work Values
// 1.D - Work Styles
//
// 2.A - Basic Skills
// 2.B - Cross-Functional Skills
// 2.C - Knowledge
//
// 3.A - Experience & Training
// 3.B - Entry Requirements (Education, Skills)
//
// 4.A - Generalized Work Activities
// 4.B - Detailed Work Activities (mapped from IWAs)
// 4.C - Organizational Context
//
// 5.A - Workforce Characteristics (Labor Market)
//
// 6.A - Occupation-Specific Tasks
// 6.B - Tools & Technology
// =============================================================================

export const onetSchema: Schema = {
  // ===========================================================================
  // CORE: OCCUPATIONS
  // ===========================================================================

  /**
   * Occupation - Core occupational classification
   *
   * Based on O*NET-SOC codes which extend standard SOC codes
   * with additional specializations (e.g., "11-1011.00", "15-1252.00")
   */
  Occupation: {
    $type: 'schema:Occupation',
    $ns: 'onet/occupations',
    $description: 'O*NET-SOC occupation classification',

    // Core fields from Occupation Data file
    socCode: 'string!', // O*NET-SOC Code (10 chars, e.g., "15-1252.00")
    title: 'string!', // O*NET-SOC Title (up to 150 chars)
    description: 'text', // O*NET-SOC Description (up to 1000 chars)

    // Job Zone information (linked from Job Zones file)
    jobZone: 'int', // Job Zone (1-5)
    jobZoneTitle: 'string', // e.g., "Job Zone Four: Considerable Preparation"
    jobZoneEducation: 'text', // Typical education level description
    jobZoneExperience: 'text', // Typical work experience description
    jobZoneTraining: 'text', // Typical on-the-job training
    jobZoneExamples: 'text', // Example occupations for this zone
    jobZoneSvpRange: 'string', // SVP Range (Specific Vocational Preparation)

    // Metadata
    lastUpdated: 'date',

    // --- WORKER CHARACTERISTICS (1.x) ---
    abilityRatings: '<- OccupationAbility.occupation[]',
    interestRatings: '<- OccupationInterest.occupation[]',
    workValueRatings: '<- OccupationWorkValue.occupation[]',
    workStyleRatings: '<- OccupationWorkStyle.occupation[]',

    // --- WORKER REQUIREMENTS (2.x) ---
    skillRatings: '<- OccupationSkill.occupation[]',
    knowledgeRatings: '<- OccupationKnowledge.occupation[]',
    educationRequirements: '<- OccupationEducation.occupation[]',

    // --- EXPERIENCE REQUIREMENTS (3.x) ---
    experienceRequirements: '<- OccupationExperience.occupation[]',
    licensingRequirements: '<- OccupationLicense.occupation[]',

    // --- OCCUPATIONAL REQUIREMENTS (4.x) ---
    workActivityRatings: '<- OccupationWorkActivity.occupation[]',
    detailedWorkActivities: '<- OccupationDWA.occupation[]',
    workContextRatings: '<- OccupationWorkContext.occupation[]',

    // --- OCCUPATION-SPECIFIC (6.x) ---
    taskStatements: '<- Task.occupation[]',
    taskRatings: '<- TaskRating.occupation[]',
    technologySkills: '<- OccupationTechnology.occupation[]',
    toolsUsed: '<- OccupationTool.occupation[]',
    alternativeTitles: '<- AlternativeTitle.occupation[]',

    // --- RELATED DATA ---
    relatedOccupations: '<- RelatedOccupation.fromOccupation[]',
    careerChangers: '<- CareerChangerMatrix.fromOccupation[]',
    careerStarters: '<- CareerStarterMatrix.fromOccupation[]',
  },

  // ===========================================================================
  // WORKER CHARACTERISTICS (1.x)
  // ===========================================================================

  /**
   * Ability - O*NET ability (1.A)
   *
   * Abilities are enduring attributes that influence performance.
   * 52 abilities across 4 categories:
   * - Cognitive Abilities (1.A.1): Verbal, Idea Generation, Quantitative, etc.
   * - Psychomotor Abilities (1.A.2): Fine/Gross Motor, Control Movement, Reaction Time
   * - Physical Abilities (1.A.3): Strength, Flexibility, Endurance, Balance
   * - Sensory Abilities (1.A.4): Visual, Auditory, Speech
   */
  Ability: {
    $type: 'schema:Ability',
    $ns: 'onet/abilities',
    $description: 'O*NET ability classification (52 types)',

    elementId: 'string!', // Content Model position (e.g., "1.A.1.a.1")
    name: 'string!', // Ability name
    description: 'text', // Ability description

    // Hierarchy
    category: 'string', // Cognitive, Psychomotor, Physical, or Sensory
    subcategory: 'string', // More specific grouping (e.g., "Verbal Abilities")
    parentId: 'string', // Parent element ID

    // Relationships
    occupationRatings: '<- OccupationAbility.ability[]',
    workActivities: '~> WorkActivity[]', // Fuzzy link to related activities
    workContexts: '~> WorkContext[]', // Fuzzy link to work contexts
  },

  /**
   * OccupationAbility - Rating linking occupation to ability
   *
   * Each rating includes importance (1-5) and level (0-7) scores.
   */
  OccupationAbility: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-abilities',
    $description: 'Occupation to ability rating',

    occupation: '-> Occupation.abilityRatings',
    ability: '-> Ability.occupationRatings',

    importance: 'float', // Scale IM: 1-5
    importanceN: 'int',
    importanceStdErr: 'float',
    importanceLowerCI: 'float',
    importanceUpperCI: 'float',

    level: 'float', // Scale LV: 0-7
    levelN: 'int',
    levelStdErr: 'float',
    levelLowerCI: 'float',
    levelUpperCI: 'float',

    recommendSuppress: 'boolean',
    notRelevant: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * Interest - O*NET occupational interest (1.B) - Holland Codes RIASEC
   *
   * Based on Holland's theory of vocational personalities:
   * - Realistic (R): Practical, physical, hands-on
   * - Investigative (I): Analytical, intellectual, scientific
   * - Artistic (A): Creative, original, independent
   * - Social (S): Helping, teaching, counseling
   * - Enterprising (E): Persuading, leading, managing
   * - Conventional (C): Detail-oriented, organizing, clerical
   */
  Interest: {
    $type: 'schema:Interest',
    $ns: 'onet/interests',
    $description: 'O*NET occupational interest (Holland RIASEC)',

    elementId: 'string!', // e.g., "1.B.1.a" for Realistic
    name: 'string!', // e.g., "Realistic"
    description: 'text',
    hollandCode: 'string', // R, I, A, S, E, or C

    occupationRatings: '<- OccupationInterest.interest[]',
  },

  /**
   * OccupationInterest - Rating linking occupation to interest
   *
   * Uses Occupational Interest Profile (OIP) scale.
   */
  OccupationInterest: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-interests',
    $description: 'Occupation to interest rating',

    occupation: '-> Occupation.interestRatings',
    interest: '-> Interest.occupationRatings',

    // Interest ratings use different scale (OIP: 1-7)
    dataValue: 'float', // Primary value
    n: 'int',
    stdError: 'float',
    lowerCI: 'float',
    upperCI: 'float',

    // High-point code assignment
    highPoint: 'boolean', // Is this a high-point interest?

    recommendSuppress: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * WorkValue - O*NET work value (1.C)
   *
   * 6 work values derived from Theory of Work Adjustment:
   * - Achievement: Occupations that satisfy this need offer results, use abilities
   * - Working Conditions: Job security, variety, compensation
   * - Recognition: Advancement, authority, social status
   * - Relationships: Co-workers, moral values, social service
   * - Support: Company policies, human relations supervision, technical supervision
   * - Independence: Creativity, responsibility, autonomy
   */
  WorkValue: {
    $type: 'schema:WorkValue',
    $ns: 'onet/work-values',
    $description: 'O*NET work value (6 types)',

    elementId: 'string!', // e.g., "1.C.1.a"
    name: 'string!',
    description: 'text',

    // Work value components (needs within values)
    components: 'string[]', // Sub-elements like Ability Utilization, Achievement, etc.

    occupationRatings: '<- OccupationWorkValue.workValue[]',
  },

  /**
   * OccupationWorkValue - Rating linking occupation to work value
   *
   * Uses Occupational Reinforcer Pattern (ORP) scale.
   */
  OccupationWorkValue: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-work-values',
    $description: 'Occupation to work value rating',

    occupation: '-> Occupation.workValueRatings',
    workValue: '-> WorkValue.occupationRatings',

    // Extent scale
    extent: 'float', // Scale EX: 1-7
    extentN: 'int',
    extentStdErr: 'float',
    extentLowerCI: 'float',
    extentUpperCI: 'float',

    recommendSuppress: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * WorkStyle - O*NET work style (1.D)
   *
   * 16 work styles representing personal characteristics:
   * - Achievement Orientation: Achievement/Effort, Persistence, Initiative
   * - Social Influence: Leadership, Social Orientation
   * - Interpersonal Orientation: Cooperative, Concern for Others
   * - Adjustment: Self-Control, Stress Tolerance, Adaptability/Flexibility
   * - Conscientiousness: Dependability, Attention to Detail, Integrity
   * - Independence: Independence, Innovation
   * - Practical Intelligence: Analytical Thinking
   */
  WorkStyle: {
    $type: 'schema:WorkStyle',
    $ns: 'onet/work-styles',
    $description: 'O*NET work style (16 types)',

    elementId: 'string!', // e.g., "1.D.1.a"
    name: 'string!',
    description: 'text',

    category: 'string', // Parent category
    parentId: 'string',

    occupationRatings: '<- OccupationWorkStyle.workStyle[]',
  },

  /**
   * OccupationWorkStyle - Rating linking occupation to work style
   */
  OccupationWorkStyle: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-work-styles',
    $description: 'Occupation to work style rating',

    occupation: '-> Occupation.workStyleRatings',
    workStyle: '-> WorkStyle.occupationRatings',

    importance: 'float', // Scale IM: 1-5
    importanceN: 'int',
    importanceStdErr: 'float',
    importanceLowerCI: 'float',
    importanceUpperCI: 'float',

    recommendSuppress: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  // ===========================================================================
  // WORKER REQUIREMENTS (2.x)
  // ===========================================================================

  /**
   * Skill - O*NET skill element (2.A, 2.B)
   *
   * 35 skills organized into:
   * - Basic Skills (2.A): Content (reading, writing, math, science) and
   *   Process (critical thinking, learning strategies, monitoring)
   * - Cross-Functional Skills (2.B): Social, Complex Problem Solving,
   *   Technical, Systems, Resource Management
   */
  Skill: {
    $type: 'schema:Skill',
    $ns: 'onet/skills',
    $description: 'O*NET skill classification (35 types)',

    elementId: 'string!', // Content Model position (e.g., "2.A.1.a")
    name: 'string!',
    description: 'text',

    // Hierarchy
    category: 'string', // "Basic Skills" or "Cross-Functional Skills"
    subcategory: 'string', // e.g., "Content Skills", "Social Skills"
    parentId: 'string',

    // Relationships
    occupationRatings: '<- OccupationSkill.skill[]',
    workActivities: '~> WorkActivity[]',
    relatedKnowledge: '~> Knowledge[]',
  },

  /**
   * OccupationSkill - Rating linking occupation to skill
   */
  OccupationSkill: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-skills',
    $description: 'Occupation to skill rating',

    occupation: '-> Occupation.skillRatings',
    skill: '-> Skill.occupationRatings',

    importance: 'float', // Scale IM: 1-5
    importanceN: 'int',
    importanceStdErr: 'float',
    importanceLowerCI: 'float',
    importanceUpperCI: 'float',

    level: 'float', // Scale LV: 0-7
    levelN: 'int',
    levelStdErr: 'float',
    levelLowerCI: 'float',
    levelUpperCI: 'float',

    recommendSuppress: 'boolean',
    notRelevant: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * Knowledge - O*NET knowledge area (2.C)
   *
   * 33 knowledge areas representing organized sets of principles and facts:
   * - Business and Management
   * - Manufacturing and Production
   * - Engineering and Technology
   * - Mathematics and Science
   * - Health Services
   * - Education and Training
   * - Arts and Humanities
   * - Law and Public Safety
   * - Communications
   * - Transportation
   */
  Knowledge: {
    $type: 'schema:KnowledgeArea',
    $ns: 'onet/knowledge',
    $description: 'O*NET knowledge area classification (33 areas)',

    elementId: 'string!', // e.g., "2.C.1.a"
    name: 'string!',
    description: 'text',

    category: 'string', // Parent category
    parentId: 'string',

    occupationRatings: '<- OccupationKnowledge.knowledge[]',
    relatedSkills: '<~ Skill.relatedKnowledge[]',
  },

  /**
   * OccupationKnowledge - Rating linking occupation to knowledge
   */
  OccupationKnowledge: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-knowledge',
    $description: 'Occupation to knowledge area rating',

    occupation: '-> Occupation.knowledgeRatings',
    knowledge: '-> Knowledge.occupationRatings',

    importance: 'float',
    importanceN: 'int',
    importanceStdErr: 'float',
    importanceLowerCI: 'float',
    importanceUpperCI: 'float',

    level: 'float',
    levelN: 'int',
    levelStdErr: 'float',
    levelLowerCI: 'float',
    levelUpperCI: 'float',

    recommendSuppress: 'boolean',
    notRelevant: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * Education - Education level requirements (2.D)
   *
   * Education levels from O*NET Education, Training, and Experience categories:
   * 1 - Less than High School
   * 2 - High School Diploma or GED
   * 3 - Post-Secondary Certificate
   * 4 - Some College
   * 5 - Associate's Degree
   * 6 - Bachelor's Degree
   * 7 - Post-Baccalaureate Certificate
   * 8 - Master's Degree
   * 9 - Post-Master's Certificate
   * 10 - First Professional Degree
   * 11 - Doctoral Degree
   * 12 - Post-Doctoral Training
   */
  Education: {
    $type: 'schema:EducationalOccupationalCredential',
    $ns: 'onet/education',
    $description: 'Education level definition',

    categoryId: 'int!', // 1-12
    name: 'string!', // e.g., "Bachelor's Degree"
    description: 'text',
    typicalYears: 'int', // Typical years of education

    occupationRequirements: '<- OccupationEducation.education[]',
  },

  /**
   * OccupationEducation - Education requirements for occupation
   */
  OccupationEducation: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-education',
    $description: 'Occupation education requirements',

    occupation: '-> Occupation.educationRequirements',
    education: '-> Education.occupationRequirements',

    // Percentage of respondents reporting this level
    percentRequired: 'float', // Category scale
    n: 'int',

    // Required vs preferred
    isRequired: 'boolean',
    isPreferred: 'boolean',

    dataDate: 'date',
    domainSource: 'string',
  },

  // ===========================================================================
  // EXPERIENCE REQUIREMENTS (3.x)
  // ===========================================================================

  /**
   * ExperienceLevel - Experience level definitions (3.A)
   *
   * Related work experience requirements
   */
  ExperienceLevel: {
    $type: 'schema:ExperienceRequirement',
    $ns: 'onet/experience-levels',
    $description: 'Experience level definition',

    categoryId: 'string!',
    name: 'string!',
    description: 'text',
    monthsMin: 'int', // Minimum months
    monthsMax: 'int', // Maximum months
  },

  /**
   * OccupationExperience - Experience requirements linking
   */
  OccupationExperience: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-experience',
    $description: 'Occupation experience requirements',

    occupation: '-> Occupation.experienceRequirements',

    // Related experience required
    relatedExperience: 'string', // Category
    relatedExperienceMonths: 'int',

    // On-site/in-plant training
    onsiteTraining: 'string',
    onsiteTrainingMonths: 'int',

    // On-the-job training
    onJobTraining: 'string',
    onJobTrainingMonths: 'int',

    // Apprenticeship
    apprenticeshipRequired: 'boolean',

    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * License - Licensing and certification information
   */
  License: {
    $type: 'schema:EducationalOccupationalCredential',
    $ns: 'onet/licenses',
    $description: 'Professional license or certification',

    name: 'string!',
    description: 'text',
    certificationOrLicense: 'string', // "Certification", "License", or "Both"
    issuingOrganization: 'string',
    stateRequired: 'string[]', // State abbreviations where required
    url: 'url',

    occupations: '<- OccupationLicense.license[]',
  },

  /**
   * OccupationLicense - License requirements for occupation
   */
  OccupationLicense: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-licenses',
    $description: 'Occupation license requirements',

    occupation: '-> Occupation.licensingRequirements',
    license: '-> License.occupations',

    isRequired: 'boolean',
    isPreferred: 'boolean',
    stateSpecific: 'boolean',

    dataDate: 'date',
    domainSource: 'string',
  },

  // ===========================================================================
  // OCCUPATIONAL REQUIREMENTS (4.x)
  // ===========================================================================

  /**
   * WorkActivity - Generalized Work Activity (4.A)
   *
   * 41 Generalized Work Activities organized into categories:
   * - Information Input: Looking for and receiving job-related information
   * - Mental Processes: Processing, planning, and decision-making
   * - Work Output: Performing physical, manipulative, or technical activities
   * - Interacting with Others: Communicating and interacting
   */
  WorkActivity: {
    $type: 'schema:WorkActivity',
    $ns: 'onet/work-activities',
    $description: 'O*NET Generalized Work Activity (41 GWAs)',

    elementId: 'string!', // e.g., "4.A.1.a.1"
    name: 'string!',
    description: 'text',

    category: 'string', // Parent category
    subcategory: 'string', // More specific grouping
    parentId: 'string',

    // Relationships
    occupationRatings: '<- OccupationWorkActivity.workActivity[]',
    detailedActivities: '<- DetailedWorkActivity.generalizedActivity[]',
    skills: '<~ Skill.workActivities[]',
    abilities: '<~ Ability.workActivities[]',
  },

  /**
   * OccupationWorkActivity - Rating linking occupation to GWA
   */
  OccupationWorkActivity: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-work-activities',
    $description: 'Occupation to generalized work activity rating',

    occupation: '-> Occupation.workActivityRatings',
    workActivity: '-> WorkActivity.occupationRatings',

    importance: 'float', // Scale IM: 1-5
    importanceN: 'int',
    importanceStdErr: 'float',
    importanceLowerCI: 'float',
    importanceUpperCI: 'float',

    level: 'float', // Scale LV: 0-7
    levelN: 'int',
    levelStdErr: 'float',
    levelLowerCI: 'float',
    levelUpperCI: 'float',

    recommendSuppress: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * IntermediateWorkActivity - Intermediate Work Activity (4.A/4.B bridge)
   *
   * IWAs sit between GWAs and DWAs in the hierarchy:
   * GWA -> IWA -> DWA
   */
  IntermediateWorkActivity: {
    $type: 'schema:WorkActivity',
    $ns: 'onet/intermediate-work-activities',
    $description: 'O*NET Intermediate Work Activity (IWA)',

    iwaId: 'string!', // IWA ID
    name: 'string!',
    description: 'text',

    // Link to parent GWA
    gwaId: 'string', // Parent GWA element ID
    generalizedActivity: '~> WorkActivity',

    // Relationships
    detailedActivities: '<- DetailedWorkActivity.intermediateActivity[]',
  },

  /**
   * DetailedWorkActivity - Detailed Work Activity (4.B)
   *
   * Over 2,000 DWAs providing specific work activity descriptions.
   * DWAs are linked to occupations and roll up to IWAs/GWAs.
   */
  DetailedWorkActivity: {
    $type: 'schema:WorkActivity',
    $ns: 'onet/detailed-work-activities',
    $description: 'O*NET Detailed Work Activity (2000+ DWAs)',

    dwaId: 'string!', // DWA ID (e.g., "4.B.2.a.1")
    name: 'string!',
    description: 'text',

    // Hierarchy links
    iwaId: 'string', // Parent IWA ID
    gwaId: 'string', // Grandparent GWA element ID
    intermediateActivity: '-> IntermediateWorkActivity.detailedActivities',
    generalizedActivity: '-> WorkActivity.detailedActivities',

    // Relationships
    occupations: '<- OccupationDWA.dwa[]',
    tasks: '<- TaskDWA.dwa[]',
  },

  /**
   * OccupationDWA - Links occupation to detailed work activity
   */
  OccupationDWA: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-dwas',
    $description: 'Occupation to detailed work activity link',

    occupation: '-> Occupation.detailedWorkActivities',
    dwa: '-> DetailedWorkActivity.occupations',

    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * WorkContext - Work context element (4.C)
   *
   * 57 work context descriptors across categories:
   * - Interpersonal Relationships
   * - Physical Work Conditions
   * - Structural Job Characteristics
   */
  WorkContext: {
    $type: 'schema:WorkContext',
    $ns: 'onet/work-contexts',
    $description: 'O*NET work context element (57 descriptors)',

    elementId: 'string!', // e.g., "4.C.1.a.1"
    name: 'string!',
    description: 'text',

    category: 'string', // Interpersonal, Physical, or Structural
    subcategory: 'string',
    parentId: 'string',

    // Scale information
    scaleId: 'string', // Context scale ID (CT, CX, etc.)
    scaleName: 'string',

    // Response categories for this context element
    responseCategories: 'json', // Array of {value, label} for categorical responses

    // Relationships
    occupationRatings: '<- OccupationWorkContext.workContext[]',
    abilities: '<~ Ability.workContexts[]',
  },

  /**
   * OccupationWorkContext - Rating linking occupation to work context
   *
   * Work context responses vary by element type:
   * - Frequency (how often)
   * - Importance
   * - Contact (extent of interpersonal contact)
   * - Categorical responses
   */
  OccupationWorkContext: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-work-contexts',
    $description: 'Occupation to work context rating',

    occupation: '-> Occupation.workContextRatings',
    workContext: '-> WorkContext.occupationRatings',

    // Generic value fields (interpretation depends on scale)
    dataValue: 'float', // Primary response value
    n: 'int',
    stdError: 'float',
    lowerCI: 'float',
    upperCI: 'float',

    // Category distribution (for categorical contexts)
    categoryData: 'json', // Array of {category, percent}

    recommendSuppress: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  // ===========================================================================
  // OCCUPATION-SPECIFIC INFORMATION (6.x)
  // ===========================================================================

  /**
   * Task - Occupation-specific task statement (6.A)
   *
   * Over 20,000 task statements describing specific work activities.
   */
  Task: {
    $type: 'schema:Task',
    $ns: 'onet/tasks',
    $description: 'Occupation-specific task statement (20,000+)',

    taskId: 'string!', // Task identifier
    statement: 'text!', // Task description
    occupation: '-> Occupation.taskStatements',

    // Task type
    taskType: 'string', // "Core" or "Supplemental"

    // Task metadata
    incumbentsResponding: 'int',
    isCore: 'boolean', // Core task for the occupation

    // Relationships
    ratings: '<- TaskRating.task[]',
    dwas: '<- TaskDWA.task[]',

    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * TaskRating - Ratings for task statements
   *
   * Tasks are rated on multiple dimensions.
   */
  TaskRating: {
    $type: 'schema:Rating',
    $ns: 'onet/task-ratings',
    $description: 'Task statement ratings',

    task: '-> Task.ratings',
    occupation: '-> Occupation.taskRatings',

    scaleId: 'string!', // Scale identifier (FT, IM, RT)
    scaleName: 'string',

    dataValue: 'float',
    n: 'int',
    stdError: 'float',
    lowerCI: 'float',
    upperCI: 'float',

    // Specific rating dimensions (populated based on scale)
    importance: 'float', // Scale IM
    relevance: 'float', // Scale RT
    frequency: 'float', // Scale FT

    recommendSuppress: 'boolean',
    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * TaskDWA - Links tasks to detailed work activities
   */
  TaskDWA: {
    $type: 'schema:Link',
    $ns: 'onet/task-dwas',
    $description: 'Task to DWA mapping',

    task: '-> Task.dwas',
    dwa: '-> DetailedWorkActivity.tasks',

    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * Technology - Technology skill (6.B) - Software
   *
   * Technology skills representing software, applications, and
   * programming languages used in occupations.
   * Linked to UNSPSC commodity codes.
   */
  Technology: {
    $type: 'schema:SoftwareApplication',
    $ns: 'onet/technology',
    $description: 'Technology skill (software, applications) with UNSPSC classification',

    commodityCode: 'string', // UNSPSC commodity code (8 digits)
    commodityTitle: 'string', // UNSPSC commodity title

    // UNSPSC hierarchy
    unspscSegment: 'string', // 2-digit segment
    unspscFamily: 'string', // 4-digit family
    unspscClass: 'string', // 6-digit class
    unspscCommodity: 'string', // 8-digit commodity

    name: 'string!', // Technology category name
    example: 'string', // Example product/tool (e.g., "Microsoft Excel")

    // Cross-dataset relationship to UNSPSC
    unspsc: '-> UNSPSC.technologies',

    occupationUsage: '<- OccupationTechnology.technology[]',
  },

  /**
   * OccupationTechnology - Links occupation to technology
   */
  OccupationTechnology: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-technology',
    $description: 'Occupation to technology link',

    occupation: '-> Occupation.technologySkills',
    technology: '-> Technology.occupationUsage',

    // Example product for this occupation
    exampleProduct: 'string',

    // Hot technology indicator
    isHotTechnology: 'boolean', // Frequently mentioned in job postings

    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * Tool - Physical tool/equipment (6.B)
   *
   * Tools and equipment used in occupations.
   * Also linked to UNSPSC codes.
   */
  Tool: {
    $type: 'schema:Product',
    $ns: 'onet/tools',
    $description: 'Physical tool or equipment with UNSPSC classification',

    commodityCode: 'string', // UNSPSC commodity code
    commodityTitle: 'string', // UNSPSC commodity title

    // UNSPSC hierarchy
    unspscSegment: 'string',
    unspscFamily: 'string',
    unspscClass: 'string',
    unspscCommodity: 'string',

    name: 'string!', // Tool category name
    example: 'string', // Example tool

    // Cross-dataset relationship to UNSPSC
    unspsc: '-> UNSPSC.tools',

    occupationUsage: '<- OccupationTool.tool[]',
  },

  /**
   * OccupationTool - Links occupation to tool
   */
  OccupationTool: {
    $type: 'schema:Rating',
    $ns: 'onet/occupation-tools',
    $description: 'Occupation to tool link',

    occupation: '-> Occupation.toolsUsed',
    tool: '-> Tool.occupationUsage',

    exampleTool: 'string',

    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * UNSPSC - United Nations Standard Products and Services Code reference
   *
   * Hierarchical classification:
   * - Segment (2 digits): Broad category
   * - Family (4 digits): Category
   * - Class (6 digits): Product type
   * - Commodity (8 digits): Specific product
   */
  UNSPSC: {
    $type: 'schema:CategoryCode',
    $ns: 'onet/unspsc',
    $description: 'UNSPSC product classification code',

    code: 'string!', // Full UNSPSC code
    title: 'string!',
    level: 'string', // "Segment", "Family", "Class", "Commodity"

    parentCode: 'string',
    parent: '-> UNSPSC.children',
    children: '<- UNSPSC.parent[]',

    technologies: '<- Technology.unspsc[]',
    tools: '<- Tool.unspsc[]',
  },

  /**
   * AlternativeTitle - Alternative job titles for occupations
   *
   * Multiple alternative titles per occupation from various sources.
   */
  AlternativeTitle: {
    $type: 'schema:JobTitle',
    $ns: 'onet/alternative-titles',
    $description: 'Alternative job title for occupation',

    title: 'string!',
    shortTitle: 'string',
    occupation: '-> Occupation.alternativeTitles',

    source: 'string', // Source of the title
    greenOccupation: 'boolean', // Green economy title

    dataDate: 'date',
  },

  // ===========================================================================
  // WORKFORCE CHARACTERISTICS (5.x)
  // ===========================================================================

  /**
   * LaborMarketInfo - Labor market information for occupations
   *
   * Employment statistics and projections.
   */
  LaborMarketInfo: {
    $type: 'schema:StatisticalPopulation',
    $ns: 'onet/labor-market',
    $description: 'Labor market information',

    occupation: '~> Occupation',
    socCode: 'string!',

    // Employment statistics
    employment: 'int', // Current employment
    employmentYear: 'int', // Year of employment data

    // Projections
    projectedEmployment: 'int', // Projected employment
    projectedYear: 'int', // Projection target year
    percentChange: 'float', // Percent change
    annualOpenings: 'int', // Annual job openings

    // Wages
    medianWage: 'float', // Median annual wage
    meanWage: 'float', // Mean annual wage
    wageYear: 'int',
    hourlyWage10: 'float', // 10th percentile hourly
    hourlyWage25: 'float',
    hourlyWage50: 'float', // Median hourly
    hourlyWage75: 'float',
    hourlyWage90: 'float', // 90th percentile hourly

    // Geographic data
    stateData: 'json', // State-level employment data
    metroData: 'json', // Metro area data

    dataDate: 'date',
    domainSource: 'string',
  },

  /**
   * OccupationalOutlook - Career outlook information
   */
  OccupationalOutlook: {
    $type: 'schema:Article',
    $ns: 'onet/occupational-outlook',
    $description: 'Occupational outlook information',

    occupation: '~> Occupation',
    socCode: 'string!',

    brightOutlook: 'boolean', // Bright Outlook designation
    brightOutlookCategory: 'string', // Reason for bright outlook

    greenOccupation: 'boolean', // Green economy occupation
    greenCategory: 'string', // Green economy category

    growthRate: 'string', // "Much faster", "Faster", "Average", etc.
    projectedGrowth: 'float', // Percentage growth

    dataDate: 'date',
    domainSource: 'string',
  },

  // ===========================================================================
  // CROSSWALKS AND RELATIONSHIPS
  // ===========================================================================

  /**
   * RelatedOccupation - Occupation relationships
   *
   * Links between related occupations for career exploration.
   */
  RelatedOccupation: {
    $type: 'schema:Link',
    $ns: 'onet/related-occupations',
    $description: 'Related occupation link',

    fromOccupation: '-> Occupation.relatedOccupations',
    toOccupation: '-> Occupation',

    relationshipType: 'string', // Type of relationship
    similarityScore: 'float', // Similarity metric

    dataDate: 'date',
  },

  /**
   * CareerChangerMatrix - Career changer transition paths
   *
   * For experienced workers transitioning between occupations.
   */
  CareerChangerMatrix: {
    $type: 'schema:Link',
    $ns: 'onet/career-changers',
    $description: 'Career changer transition paths',

    fromOccupation: '-> Occupation.careerChangers',
    toOccupation: '-> Occupation',

    transitionEase: 'float', // Ease of transition score
    skillTransferability: 'float',
    additionalTrainingNeeded: 'text',

    dataDate: 'date',
  },

  /**
   * CareerStarterMatrix - Career starter pathways
   *
   * For new workers entering occupations.
   */
  CareerStarterMatrix: {
    $type: 'schema:Link',
    $ns: 'onet/career-starters',
    $description: 'Career starter pathways',

    fromOccupation: '-> Occupation.careerStarters',
    toOccupation: '-> Occupation',

    entryEase: 'float',
    typicalEntryPath: 'text',

    dataDate: 'date',
  },

  /**
   * SOCCrosswalk - SOC code crosswalk
   *
   * Maps O*NET-SOC codes to other classification systems.
   */
  SOCCrosswalk: {
    $type: 'schema:CategoryCode',
    $ns: 'onet/soc-crosswalk',
    $description: 'SOC code crosswalk',

    onetSocCode: 'string!', // O*NET-SOC code
    socCode: 'string', // Standard SOC code
    socTitle: 'string',

    // Previous version mappings
    onetSoc2019: 'string', // O*NET-SOC 2019
    onetSoc2010: 'string', // O*NET-SOC 2010

    // Other classification systems
    censusCode: 'string', // Census occupation code
    cipCode: 'string', // CIP (education programs)

    occupation: '~> Occupation',
  },

  // ===========================================================================
  // CONTENT MODEL TAXONOMY
  // ===========================================================================

  /**
   * ContentModelElement - Base taxonomy element
   *
   * Represents the hierarchical structure of the O*NET Content Model.
   * Used for reference and navigation.
   */
  ContentModelElement: {
    $type: 'schema:TaxonomyElement',
    $ns: 'onet/content-model',
    $description: 'O*NET Content Model taxonomy element',

    elementId: 'string!', // Hierarchical ID (e.g., "1.A.1.a")
    name: 'string!',
    description: 'text',

    level: 'int', // Depth in hierarchy (1-5)
    parentId: 'string', // Parent element ID

    // Domain classification
    domain: 'string', // Worker Characteristics, Worker Requirements, etc.
    subdomain: 'string', // Abilities, Skills, etc.

    // Self-referential hierarchy
    parent: '-> ContentModelElement.children',
    children: '<- ContentModelElement.parent[]',
  },

  /**
   * Scale - Rating scale reference
   *
   * Defines all rating scales used in O*NET:
   * - IM: Importance (1-5)
   * - LV: Level (0-7)
   * - EX: Extent (1-7)
   * - OIP: Occupational Interest Profile (1-7)
   * - CT: Context (varies)
   * - FT: Frequency (1-7)
   * - RT: Relevance (1-3)
   */
  Scale: {
    $type: 'schema:Scale',
    $ns: 'onet/scales',
    $description: 'O*NET rating scale definition',

    scaleId: 'string!', // Scale identifier (IM, LV, etc.)
    name: 'string!', // Scale name
    minimum: 'float!', // Minimum value
    maximum: 'float!', // Maximum value
    description: 'text',

    // Scale anchors
    anchors: 'json', // Array of {value, label} for scale points
  },

  /**
   * ScaleAnchor - Scale anchor definitions
   *
   * Defines anchor points for level scales (what each level means).
   */
  ScaleAnchor: {
    $type: 'schema:DefinedTerm',
    $ns: 'onet/scale-anchors',
    $description: 'Scale anchor definition',

    scaleId: 'string!',
    elementId: 'string!', // Which element this anchor is for
    anchorValue: 'int!', // The level/value
    anchorDescription: 'text!', // What this level means

    scale: '~> Scale',
    element: '~> ContentModelElement',
  },
}

// =============================================================================
// TYPE EXPORTS - TypeScript interfaces for application code
// =============================================================================

export interface Occupation {
  $id: string
  $type: 'Occupation'
  name: string
  socCode: string
  title: string
  description?: string
  jobZone?: number
  jobZoneTitle?: string
  jobZoneEducation?: string
  jobZoneExperience?: string
  jobZoneTraining?: string
  jobZoneSvpRange?: string
  lastUpdated?: Date
}

export interface Ability {
  $id: string
  $type: 'Ability'
  name: string
  elementId: string
  description?: string
  category?: string
  subcategory?: string
}

export interface Interest {
  $id: string
  $type: 'Interest'
  name: string
  elementId: string
  hollandCode?: string
  description?: string
}

export interface WorkValue {
  $id: string
  $type: 'WorkValue'
  name: string
  elementId: string
  description?: string
  components?: string[]
}

export interface WorkStyle {
  $id: string
  $type: 'WorkStyle'
  name: string
  elementId: string
  category?: string
  description?: string
}

export interface Skill {
  $id: string
  $type: 'Skill'
  name: string
  elementId: string
  description?: string
  category?: string
  subcategory?: string
}

export interface Knowledge {
  $id: string
  $type: 'Knowledge'
  name: string
  elementId: string
  description?: string
  category?: string
}

export interface Education {
  $id: string
  $type: 'Education'
  name: string
  categoryId: number
  description?: string
  typicalYears?: number
}

export interface WorkActivity {
  $id: string
  $type: 'WorkActivity'
  name: string
  elementId: string
  description?: string
  category?: string
  subcategory?: string
}

export interface IntermediateWorkActivity {
  $id: string
  $type: 'IntermediateWorkActivity'
  name: string
  iwaId: string
  description?: string
  gwaId?: string
}

export interface DetailedWorkActivity {
  $id: string
  $type: 'DetailedWorkActivity'
  name: string
  dwaId: string
  description?: string
  iwaId?: string
  gwaId?: string
}

export interface WorkContext {
  $id: string
  $type: 'WorkContext'
  name: string
  elementId: string
  description?: string
  category?: string
}

export interface Task {
  $id: string
  $type: 'Task'
  name: string
  taskId: string
  statement: string
  taskType?: string
  isCore?: boolean
  dataDate?: Date
}

export interface Technology {
  $id: string
  $type: 'Technology'
  name: string
  commodityCode?: string
  commodityTitle?: string
  example?: string
  unspscSegment?: string
  unspscFamily?: string
  unspscClass?: string
  unspscCommodity?: string
  // Aliases for cross-dataset integration
  unspscCode?: string // Alias for commodityCode
  hotTechnology?: boolean // Frequently mentioned in job postings
  category?: string // Tool category for grouping
}

export interface Tool {
  $id: string
  $type: 'Tool'
  name: string
  commodityCode?: string
  commodityTitle?: string
  example?: string
}

export interface AlternativeTitle {
  $id: string
  $type: 'AlternativeTitle'
  name: string
  title: string
  shortTitle?: string
  source?: string
  greenOccupation?: boolean
}

export interface OccupationRating {
  $id: string
  importance?: number
  importanceN?: number
  importanceStdErr?: number
  importanceLowerCI?: number
  importanceUpperCI?: number
  level?: number
  levelN?: number
  levelStdErr?: number
  levelLowerCI?: number
  levelUpperCI?: number
  recommendSuppress?: boolean
  notRelevant?: boolean
  dataDate?: Date
  domainSource?: string
}

export interface ContentModelElement {
  $id: string
  $type: 'ContentModelElement'
  name: string
  elementId: string
  description?: string
  level?: number
  parentId?: string
  domain?: string
  subdomain?: string
}

export interface Scale {
  $id: string
  $type: 'Scale'
  name: string
  scaleId: string
  minimum: number
  maximum: number
  description?: string
  anchors?: Array<{ value: number; label: string }>
}

export interface LaborMarketInfo {
  $id: string
  $type: 'LaborMarketInfo'
  socCode: string
  employment?: number
  employmentYear?: number
  projectedEmployment?: number
  projectedYear?: number
  percentChange?: number
  medianWage?: number
  meanWage?: number
}

export interface UNSPSC {
  $id: string
  $type: 'UNSPSC'
  code: string
  title: string
  level?: string
  parentCode?: string
}

export interface License {
  $id: string
  $type: 'License'
  name: string
  description?: string
  certificationOrLicense?: string
  issuingOrganization?: string
  stateRequired?: string[]
  url?: string
}

export interface OccupationTechnology {
  $id: string
  $type: 'OccupationTechnology'
  occupation: string
  technology: string
  exampleProduct?: string
  isHotTechnology?: boolean
  dataDate?: Date
  domainSource?: string
}

export interface OccupationalOutlook {
  $id: string
  $type: 'OccupationalOutlook'
  socCode: string
  brightOutlook?: boolean
  brightOutlookCategory?: string
  greenOccupation?: boolean
  greenCategory?: string
  growthRate?: string
  projectedGrowth?: number
}

export default onetSchema
