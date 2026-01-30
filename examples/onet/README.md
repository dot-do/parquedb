# O*NET Occupational Database Example - COMPLETE CONTENT MODEL

This example demonstrates loading and querying the **FULL** O\*NET occupational database using ParqueDB, including all 40+ entity types from the O\*NET Content Model.

## Overview

[O\*NET](https://www.onetcenter.org/) (Occupational Information Network) is the US Department of Labor's comprehensive database of occupational information. It contains standardized descriptors for nearly 1,000 occupations.

This implementation covers the **COMPLETE** O\*NET Content Model:

### Worker Characteristics (1.x)
- **Abilities** (52 types): Cognitive, Psychomotor, Physical, Sensory
- **Interests** (6 types): Holland RIASEC codes
- **Work Values** (6 types): Achievement, Working Conditions, Recognition, etc.
- **Work Styles** (16 types): Achievement Orientation, Social Influence, etc.

### Worker Requirements (2.x)
- **Skills** (35 types): Basic Skills, Cross-Functional Skills
- **Knowledge** (33 areas): Business, Engineering, Health Services, etc.
- **Education**: 12 levels from High School to Post-Doctoral

### Experience Requirements (3.x)
- **Experience and Training**: Related experience, on-job training
- **Entry Requirements**: Skill entry levels
- **Licensing**: Professional certifications and licenses

### Occupational Requirements (4.x)
- **Generalized Work Activities** (41 GWAs): Information Input, Mental Processes, Work Output, Interacting
- **Intermediate Work Activities** (IWAs): Bridge between GWAs and DWAs
- **Detailed Work Activities** (2000+ DWAs): Specific work activities
- **Work Context** (57 descriptors): Interpersonal, Physical, Structural

### Occupation-Specific Information (6.x)
- **Tasks** (20,000+ statements): Job-specific activities
- **Tools and Technology**: With **UNSPSC commodity codes**!
- **Alternate Titles**: Multiple job titles per occupation

### Workforce Characteristics (5.x)
- **Labor Market Information**: Employment, wages, projections
- **Occupational Outlook**: Growth rates, bright outlook designations

## Data Model

### Complete Entity Hierarchy

```
Occupation (SOC Code based)
    |
    |-- WORKER CHARACTERISTICS (1.x)
    |   |-- abilityRatings --> OccupationAbility --> Ability
    |   |-- interestRatings --> OccupationInterest --> Interest (Holland RIASEC)
    |   |-- workValueRatings --> OccupationWorkValue --> WorkValue
    |   |-- workStyleRatings --> OccupationWorkStyle --> WorkStyle
    |
    |-- WORKER REQUIREMENTS (2.x)
    |   |-- skillRatings --> OccupationSkill --> Skill
    |   |-- knowledgeRatings --> OccupationKnowledge --> Knowledge
    |   |-- educationRequirements --> OccupationEducation --> Education
    |
    |-- EXPERIENCE REQUIREMENTS (3.x)
    |   |-- experienceRequirements --> OccupationExperience
    |   |-- licensingRequirements --> OccupationLicense --> License
    |
    |-- OCCUPATIONAL REQUIREMENTS (4.x)
    |   |-- workActivityRatings --> OccupationWorkActivity --> WorkActivity (GWA)
    |   |-- detailedWorkActivities --> OccupationDWA --> DetailedWorkActivity --> IntermediateWorkActivity
    |   |-- workContextRatings --> OccupationWorkContext --> WorkContext
    |
    |-- OCCUPATION-SPECIFIC (6.x)
    |   |-- taskStatements --> Task --> TaskRating
    |   |                   --> TaskDWA --> DetailedWorkActivity
    |   |-- technologySkills --> OccupationTechnology --> Technology --> UNSPSC
    |   |-- toolsUsed --> OccupationTool --> Tool --> UNSPSC
    |   |-- alternativeTitles --> AlternativeTitle
    |
    |-- RELATED DATA
        |-- relatedOccupations --> RelatedOccupation
        |-- careerChangers --> CareerChangerMatrix
        |-- careerStarters --> CareerStarterMatrix
```

### Entity Counts

| Category | Entity | Description | Count (approx) |
|----------|--------|-------------|----------------|
| **Core** | Occupation | Job classifications | ~1,000 |
| | ContentModelElement | Taxonomy hierarchy | ~300 |
| **1.x** | Ability | Enduring attributes | 52 |
| | Interest | Holland RIASEC codes | 6 |
| | WorkValue | Work satisfaction factors | 6 |
| | WorkStyle | Personal characteristics | 16 |
| **2.x** | Skill | Learned competencies | 35 |
| | Knowledge | Subject matter expertise | 33 |
| | Education | Education levels | 12 |
| **4.x** | WorkActivity | Generalized Work Activities | 41 |
| | IntermediateWorkActivity | IWAs | ~300 |
| | DetailedWorkActivity | Specific activities | ~2,000 |
| | WorkContext | Work environment descriptors | 57 |
| **6.x** | Task | Job-specific activities | ~20,000 |
| | Technology | Software/applications | ~10,000 |
| | Tool | Physical equipment | ~8,000 |
| | AlternativeTitle | Job title variants | ~50,000 |
| **Ref** | UNSPSC | Product classification | ~15,000 |
| | Scale | Rating scale definitions | ~20 |

### Rating Entities

Ratings link occupations to characteristics with importance/level scores:

| Entity | Links | Count (approx) |
|--------|-------|----------------|
| OccupationAbility | Occupation -> Ability | ~45,000 |
| OccupationInterest | Occupation -> Interest | ~6,000 |
| OccupationWorkValue | Occupation -> WorkValue | ~6,000 |
| OccupationWorkStyle | Occupation -> WorkStyle | ~16,000 |
| OccupationSkill | Occupation -> Skill | ~30,000 |
| OccupationKnowledge | Occupation -> Knowledge | ~33,000 |
| OccupationWorkActivity | Occupation -> GWA | ~40,000 |
| OccupationWorkContext | Occupation -> WorkContext | ~55,000 |
| TaskRating | Task ratings (IM/FT/RT) | ~60,000 |
| OccupationTechnology | Occupation -> Technology | ~100,000 |
| OccupationTool | Occupation -> Tool | ~80,000 |

## Files

```
examples/onet/
├── README.md          # This file
├── schema.ts          # GraphDL schema (40+ entity types)
├── load.ts            # Complete data loading script
└── queries.ts         # Comprehensive query examples
```

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Download and Load Data

```bash
# Download O*NET database and load ALL entity types
npx tsx examples/onet/load.ts

# Skip download if already downloaded
npx tsx examples/onet/load.ts --no-download
```

This will:
- Download the O\*NET database (~100MB compressed)
- Extract and parse ALL data files
- Transform into 40+ entity types
- Write to `data/onet/parquet/`

Output summary:
```
Core:
  occupations: 923
  contentModel: 277

Worker Characteristics (1.x):
  abilities: 52 (45,196 ratings)
  interests: 6 (5,538 ratings)
  workValues: 6 (5,538 ratings)
  workStyles: 16 (14,768 ratings)

Worker Requirements (2.x):
  skills: 35 (32,305 ratings)
  knowledge: 33 (30,459 ratings)

Occupational Requirements (4.x):
  workActivities: 41 (37,843 ratings)
  workContexts: 57 (52,611 ratings)
  detailedWorkActivities: 2,069
  intermediateWorkActivities: 332

Occupation-Specific (6.x):
  tasks: 19,633 (58,899 ratings)
  technologies: 11,467 (102,581 links)
  tools: 8,112 (83,456 links)
  alternateTitles: 54,732

Reference:
  scales: 20
  unspsc: 14,789

Total: ~600,000+ entities
```

### 3. Run Example Queries

```bash
npx tsx examples/onet/queries.ts
```

## Schema

The schema is defined in `schema.ts` using ParqueDB's GraphDL format. Key features:

### UNSPSC Cross-References

Tools and Technology entities link to UNSPSC codes:

```typescript
Technology: {
  $type: 'schema:SoftwareApplication',
  $ns: 'onet/technology',

  commodityCode: 'string',     // UNSPSC 8-digit code
  commodityTitle: 'string',

  // UNSPSC hierarchy
  unspscSegment: 'string',     // 2-digit segment
  unspscFamily: 'string',      // 4-digit family
  unspscClass: 'string',       // 6-digit class
  unspscCommodity: 'string',   // 8-digit commodity

  name: 'string!',
  example: 'string',

  // Cross-dataset relationship
  unspsc: '-> UNSPSC.technologies',
  occupationUsage: '<- OccupationTechnology.technology[]',
}
```

### Work Activity Hierarchy

DWAs link up to IWAs and GWAs:

```typescript
DetailedWorkActivity: {
  dwaId: 'string!',
  name: 'string!',

  // Hierarchy links
  iwaId: 'string',
  gwaId: 'string',
  intermediateActivity: '-> IntermediateWorkActivity.detailedActivities',
  generalizedActivity: '-> WorkActivity.detailedActivities',

  // Relationships
  occupations: '<- OccupationDWA.dwa[]',
  tasks: '<- TaskDWA.dwa[]',
}
```

## Example Queries

### Worker Characteristics Queries

```typescript
// Get Holland codes (RIASEC) for an occupation
const interestProfile = await db.OccupationInterests.find(
  { 'occupation.$id': 'onet/occupations/15-1252-00' },
  { sort: { dataValue: -1 }, populate: ['interest'] }
)

// Find occupations valuing Independence
const independentJobs = await db.OccupationWorkValues.find(
  { 'workValue.name': 'Independence', extent: { $gte: 5.0 } },
  { populate: ['occupation'] }
)
```

### Work Activity Queries

```typescript
// Get GWAs for an occupation
const workActivities = await db.OccupationWorkActivities.find(
  { 'occupation.$id': 'onet/occupations/15-1252-00' },
  { sort: { importance: -1 }, populate: ['workActivity'] }
)

// Find DWAs related to a task
const taskDWAs = await db.TaskDWAs.find(
  { 'task.taskId': '12345' },
  { populate: ['dwa'] }
)
```

### Technology & Tools with UNSPSC

```typescript
// Get technologies with UNSPSC codes
const technologies = await db.OccupationTechnologies.find(
  { 'occupation.$id': 'onet/occupations/15-1252-00' },
  { populate: ['technology'] }
)

// Find technologies by UNSPSC segment (43 = IT)
const itTech = await db.Technologies.find({
  unspscSegment: '43'
})

// Query UNSPSC hierarchy
const softwareFamily = await db.UNSPSC.find({
  code: { $startsWith: '4323' },
  level: 'Class'
})
```

### Career Exploration

```typescript
// Get career changer paths
const careerPaths = await db.CareerChangers.find(
  { 'fromOccupation.$id': 'onet/occupations/15-1252-00' },
  { sort: { transitionEase: -1 }, populate: ['toOccupation'] }
)

// Find related occupations
const related = await db.RelatedOccupations.find(
  { 'fromOccupation.$id': 'onet/occupations/15-1252-00' },
  { sort: { similarityScore: -1 }, populate: ['toOccupation'] }
)
```

### Complete Occupation Profile

```typescript
// Get ALL characteristics for an occupation
const profile = await Promise.all([
  db.OccupationAbilities.find({ 'occupation.$id': occId }, { populate: ['ability'] }),
  db.OccupationInterests.find({ 'occupation.$id': occId }, { populate: ['interest'] }),
  db.OccupationWorkValues.find({ 'occupation.$id': occId }, { populate: ['workValue'] }),
  db.OccupationWorkStyles.find({ 'occupation.$id': occId }, { populate: ['workStyle'] }),
  db.OccupationSkills.find({ 'occupation.$id': occId }, { populate: ['skill'] }),
  db.OccupationKnowledge.find({ 'occupation.$id': occId }, { populate: ['knowledge'] }),
  db.OccupationWorkActivities.find({ 'occupation.$id': occId }, { populate: ['workActivity'] }),
  db.OccupationWorkContexts.find({ 'occupation.$id': occId }, { populate: ['workContext'] }),
  db.Tasks.find({ 'occupation.$id': occId }),
  db.OccupationTechnologies.find({ 'occupation.$id': occId }, { populate: ['technology'] }),
  db.OccupationTools.find({ 'occupation.$id': occId }, { populate: ['tool'] }),
  db.AlternativeTitles.find({ 'occupation.$id': occId }),
])
```

## Content Model Structure

The complete O\*NET Content Model hierarchy:

```
1. WORKER CHARACTERISTICS
   1.A. Abilities (52)
        1.A.1. Cognitive Abilities
               - Verbal Abilities (4)
               - Idea Generation and Reasoning (6)
               - Quantitative Abilities (3)
               - Memory (2)
               - Perceptual Abilities (6)
               - Spatial Abilities (3)
               - Attentiveness (2)
        1.A.2. Psychomotor Abilities
               - Fine Manipulative (4)
               - Control Movement (4)
               - Reaction Time and Speed (4)
        1.A.3. Physical Abilities
               - Physical Strength (4)
               - Endurance (1)
               - Flexibility, Balance, Coordination (5)
        1.A.4. Sensory Abilities
               - Visual (4)
               - Auditory and Speech (4)

   1.B. Occupational Interests (6 - RIASEC)
        - Realistic (R)
        - Investigative (I)
        - Artistic (A)
        - Social (S)
        - Enterprising (E)
        - Conventional (C)

   1.C. Work Values (6)
        - Achievement
        - Working Conditions
        - Recognition
        - Relationships
        - Support
        - Independence

   1.D. Work Styles (16)
        - Achievement/Effort, Persistence, Initiative
        - Leadership, Social Orientation
        - Cooperation, Concern for Others
        - Self-Control, Stress Tolerance, Adaptability
        - Dependability, Attention to Detail, Integrity
        - Independence, Innovation
        - Analytical Thinking

2. WORKER REQUIREMENTS
   2.A. Basic Skills
        - Content (Reading, Writing, Math, Science)
        - Process (Critical Thinking, Learning, Monitoring)

   2.B. Cross-Functional Skills
        - Social Skills
        - Complex Problem Solving Skills
        - Technical Skills
        - Systems Skills
        - Resource Management Skills

   2.C. Knowledge (33 areas across 10 domains)

3. EXPERIENCE REQUIREMENTS
   3.A. Experience and Training
   3.B. Entry Skill Requirements
   3.C. Licensing

4. OCCUPATIONAL REQUIREMENTS
   4.A. Generalized Work Activities (41 GWAs)
        - Information Input
        - Mental Processes
        - Work Output
        - Interacting with Others

   4.B. Detailed Work Activities (2000+ DWAs)
        - Linked via Intermediate Work Activities (IWAs)

   4.C. Work Context (57 descriptors)
        - Interpersonal Relationships
        - Physical Work Conditions
        - Structural Job Characteristics

5. WORKFORCE CHARACTERISTICS
   5.A. Labor Market Information
   5.B. Occupational Outlook

6. OCCUPATION-SPECIFIC INFORMATION
   6.A. Tasks (20,000+)
   6.B. Tools and Technology (with UNSPSC codes)
        - Software/Applications
        - Physical Tools/Equipment
```

## UNSPSC Integration

The O\*NET Tools and Technology data includes UNSPSC (United Nations Standard Products and Services Code) commodity codes, enabling cross-referencing with product databases.

UNSPSC hierarchy:
- **Segment** (2 digits): Broad category (e.g., 43 = IT/Broadcasting)
- **Family** (4 digits): Category (e.g., 4323 = Software)
- **Class** (6 digits): Product type (e.g., 432315 = Application Software)
- **Commodity** (8 digits): Specific product (e.g., 43231513 = Spreadsheet Software)

Example:
```typescript
// Microsoft Excel would have:
commodityCode: "43231513"
unspscSegment: "43"        // IT/Broadcasting
unspscFamily: "4323"       // Software
unspscClass: "432315"      // Application Software
unspscCommodity: "43231513" // Spreadsheet Software
```

## Data Sources

The O\*NET database is available from the National Center for O\*NET Development:

- **Download**: https://www.onetcenter.org/database.html
- **Data Dictionary**: https://www.onetcenter.org/dictionary/
- **Content Model**: https://www.onetcenter.org/content.html
- **License**: Creative Commons Attribution 4.0 International

## Use Cases

This comprehensive O\*NET + ParqueDB integration enables:

1. **Career Exploration**: Match interests, values, and abilities to occupations
2. **Skills Gap Analysis**: Compare current vs. target occupation requirements
3. **Workforce Planning**: Analyze skill requirements across industries
4. **Training Program Design**: Identify skills needed for career transitions
5. **Job Matching**: Match candidates to occupations by complete profiles
6. **Labor Market Analysis**: Track skill and technology trends
7. **Product/Tool Analysis**: Cross-reference occupations with UNSPSC product codes
8. **Work Activity Mapping**: Link tasks to standardized work activities

## References

- [O\*NET Resource Center](https://www.onetcenter.org/)
- [O\*NET OnLine](https://www.onetonline.org/)
- [O\*NET Content Model](https://www.onetcenter.org/content.html)
- [O\*NET Data Dictionary](https://www.onetcenter.org/dictionary.html)
- [SOC Classification System](https://www.bls.gov/soc/)
- [UNSPSC](https://www.unspsc.org/)
- [Holland Codes (RIASEC)](https://www.onetcenter.org/content.html#cm_ih)
