# O*NET Query Patterns for ParqueDB

This document outlines real-world query patterns for the O*NET occupational database when stored in ParqueDB's dual Variant architecture (`$id | $index_* | $data`).

## Data Model Overview

The O*NET database contains:
- **~1,000 occupations** with O*NET-SOC codes (e.g., "15-1252.00")
- **35 skills** (basic + cross-functional)
- **52 abilities** (cognitive, psychomotor, physical, sensory)
- **33 knowledge areas**
- **41 generalized work activities** (GWAs)
- **2,000+ detailed work activities** (DWAs)
- **20,000+ task statements**
- **Thousands of technology/tool entries**

All linked through rating relationships with importance (1-5) and level (0-7) scales.

## Storage Architecture

### Recommended Index Columns by Entity Type

| Entity | $index_* Columns | Rationale |
|--------|------------------|-----------|
| Occupation | `$index_socCode`, `$index_jobZone`, `$index_title` | High-selectivity filters, frequent queries |
| Skill/Ability/Knowledge | `$index_elementId`, `$index_category`, `$index_name` | Content model hierarchy, category-based access |
| Task | `$index_taskType`, `$index_occupation` | Core vs supplemental filtering |
| Technology | `$index_commodityCode`, `$index_isHotTechnology` | UNSPSC classification, trending tech |
| OccupationSkill (rating) | `$index_importance`, `$index_level`, `$index_occupation`, `$index_skill` | Threshold queries, relationship traversal |

---

## Query Patterns by Use Case

### 1. Career Exploration Tools

Applications that help users discover careers based on interests, skills, or current occupation.

#### Q1: Find occupations by Job Zone (education level)

**Business Question**: "What careers require a Bachelor's degree (Job Zone 4)?"

```typescript
// Filter
{ jobZone: 4 }

// Index column benefit
$index_jobZone: 4  // INT32 - excellent for equality
```

| Metric | Value |
|--------|-------|
| Selectivity | ~20% (5 zones, roughly even distribution) |
| Index Column | `$index_jobZone` (INT32) |
| Stats Pushdown | Yes - min/max enables row group skipping |
| Estimated Rows | ~200 of 1,000 occupations |

---

#### Q2: Find occupations by Holland code (RIASEC interests)

**Business Question**: "What careers match Investigative (I) interest profile?"

```typescript
// Filter on OccupationInterest ratings
{
  interest: { $ref: 'interests/1-B-1-b' }, // Investigative
  highPoint: true
}

// Or for top-N by interest score
{ interest: { $ref: 'interests/1-B-1-b' } }
// Sort by: dataValue DESC
```

| Metric | Value |
|--------|-------|
| Selectivity | ~17% (6 Holland codes) |
| Index Column | `$index_interest`, `$index_highPoint` |
| Stats Pushdown | Yes for boolean highPoint |
| Join Required | Interest ratings -> Occupations |

---

#### Q3: Find related occupations for career transitions

**Business Question**: "What occupations are similar to Software Developer?"

```typescript
// Filter on RelatedOccupation
{ fromOccupation: { $ref: 'occupations/15-1252-00' } }
// Sort by: similarityScore DESC
```

| Metric | Value |
|--------|-------|
| Selectivity | Low per occupation (~20-50 related) |
| Index Column | `$index_fromOccupation` |
| Stats Pushdown | Yes for string prefix matching |
| Graph Traversal | Single hop from source occupation |

---

#### Q4: Career pathway query (multi-hop)

**Business Question**: "Show career progression from Entry-Level to Senior roles in IT"

```typescript
// Multi-step query:
// 1. Find Job Zone 1-2 IT occupations
{ jobZone: { $lte: 2 }, socCode: { $regex: '^15-' } }

// 2. Traverse CareerStarterMatrix to find paths
{ fromOccupation: { $in: [...entryLevelIds] } }
```

| Metric | Value |
|--------|-------|
| Selectivity | Low (filtered by SOC prefix + Job Zone) |
| Index Columns | `$index_jobZone`, `$index_socCode` |
| Stats Pushdown | Yes for range on jobZone, prefix on socCode |
| Pattern | Filter + Graph traversal |

---

### 2. Job Posting and Matching Systems

Applications that match job requirements to candidate profiles or vice versa.

#### Q5: Find occupations requiring specific skill at high level

**Business Question**: "What jobs need Programming skill at level 5+?"

```typescript
// Filter on OccupationSkill
{
  skill: { $ref: 'skills/2-B-2-i' }, // Programming
  level: { $gte: 5.0 }
}
```

| Metric | Value |
|--------|-------|
| Selectivity | ~15% of occupation-skill pairs |
| Index Columns | `$index_skill`, `$index_level` |
| Stats Pushdown | Yes - FLOAT64 range queries work well |
| Composite Index | `$index_skill + $index_level` ideal |

---

#### Q6: Find occupations by technology stack

**Business Question**: "What occupations use Python?"

```typescript
// Filter on OccupationTechnology
{
  technology: { $ref: 'technology/43232603' }, // Data management software
  exampleProduct: { $regex: /python/i }
}

// Or search by hot technology flag
{ isHotTechnology: true }
```

| Metric | Value |
|--------|-------|
| Selectivity | High (technology-specific) |
| Index Columns | `$index_technology`, `$index_isHotTechnology` |
| Stats Pushdown | Boolean for hot tech, ID for technology |
| Full-text | Needed for exampleProduct search |

---

#### Q7: Match job posting to O*NET occupation

**Business Question**: "Given a job title 'Data Scientist', find matching O*NET occupation"

```typescript
// Search AlternativeTitle
{ title: { $regex: /data scientist/i } }

// Or full-text search on Occupation
{
  $or: [
    { title: { $regex: /data scientist/i } },
    { description: { $text: 'data scientist' } }
  ]
}
```

| Metric | Value |
|--------|-------|
| Selectivity | Very low (single match expected) |
| Index Columns | `$index_title` (for prefix), FTS index |
| Stats Pushdown | Limited - string operations |
| Recommendation | Full-text index on title + description |

---

#### Q8: Build job requirements from occupation

**Business Question**: "What skills, knowledge, and abilities are required for 'Software Developer'?"

```typescript
// Multiple queries with importance threshold
const occupation = 'occupations/15-1252-00'

// Skills with importance >= 3.5
{ occupation: { $ref: occupation }, importance: { $gte: 3.5 } }
// Collection: occupation-skills

// Knowledge with importance >= 3.5
{ occupation: { $ref: occupation }, importance: { $gte: 3.5 } }
// Collection: occupation-knowledge

// Abilities with importance >= 3.5
{ occupation: { $ref: occupation }, importance: { $gte: 3.5 } }
// Collection: occupation-abilities
```

| Metric | Value |
|--------|-------|
| Selectivity | Medium (~10-15 items per category) |
| Index Columns | `$index_occupation`, `$index_importance` |
| Stats Pushdown | Yes for both ID and float range |
| Pattern | Parallel queries on rating tables |

---

### 3. Skills Gap Analysis Platforms

Applications that compare current skills to target occupation requirements.

#### Q9: Calculate skill gaps for career change

**Business Question**: "What skills does a 'Graphic Designer' need to become a 'UX Designer'?"

```typescript
// 1. Get source occupation skills
{ occupation: { $ref: 'occupations/27-1024-00' } } // Graphic Designer

// 2. Get target occupation skills
{ occupation: { $ref: 'occupations/15-1255-01' } } // UX Designer

// 3. Application-level: Compute difference in skill requirements
```

| Metric | Value |
|--------|-------|
| Selectivity | Low (all skills for 2 occupations) |
| Index Column | `$index_occupation` |
| Stats Pushdown | Yes for occupation ID |
| Pattern | Two queries + client-side diff |

---

#### Q10: Find training requirements for occupation

**Business Question**: "What education and training is needed for 'Nurse Practitioner'?"

```typescript
// Education requirements
{ occupation: { $ref: 'occupations/29-1171-00' } }
// Collection: occupation-education

// Experience requirements
{ occupation: { $ref: 'occupations/29-1171-00' } }
// Collection: occupation-experience

// Licensing requirements
{ occupation: { $ref: 'occupations/29-1171-00' } }
// Collection: occupation-licenses
```

| Metric | Value |
|--------|-------|
| Selectivity | Very low (single occupation) |
| Index Column | `$index_occupation` |
| Stats Pushdown | Yes |
| Pattern | Multi-collection single-key lookup |

---

#### Q11: Find occupations matching skill profile

**Business Question**: "Given my skills (Critical Thinking: 4, Writing: 5, Programming: 3), what careers am I qualified for?"

```typescript
// Complex multi-skill query
const userSkills = [
  { skill: 'skills/2-A-2-a', level: 4 },  // Critical Thinking
  { skill: 'skills/2-A-1-a', level: 5 },  // Writing
  { skill: 'skills/2-B-2-i', level: 3 },  // Programming
]

// For each skill, find occupations where required level <= user level
// Then intersect results
{
  skill: { $ref: 'skills/2-A-2-a' },
  level: { $lte: 4 }
}
```

| Metric | Value |
|--------|-------|
| Selectivity | Medium per skill, low after intersection |
| Index Columns | `$index_skill`, `$index_level` |
| Stats Pushdown | Yes for range queries |
| Pattern | N parallel queries + set intersection |

---

### 4. Workforce Analytics Dashboards

Applications for HR planning, labor market analysis, and workforce development.

#### Q12: Aggregate occupations by Job Zone

**Business Question**: "How many occupations are in each Job Zone?"

```typescript
// Group by jobZone, count
db.Occupations.aggregate([
  { $group: { _id: '$jobZone', count: { $sum: 1 } } }
])

// Filter for statistics:
{ jobZone: { $exists: true } }
```

| Metric | Value |
|--------|-------|
| Selectivity | None (full scan for aggregation) |
| Index Column | `$index_jobZone` |
| Stats Pushdown | Metadata only (distinct values) |
| Optimization | Pre-computed in metadata |

---

#### Q13: Find high-growth occupations

**Business Question**: "What occupations have 'Bright Outlook' designation?"

```typescript
// Filter OccupationalOutlook
{ brightOutlook: true }

// With growth rate detail
{
  brightOutlook: true,
  growthRate: { $in: ['Much faster than average', 'Faster than average'] }
}
```

| Metric | Value |
|--------|-------|
| Selectivity | ~10-15% of occupations |
| Index Column | `$index_brightOutlook` |
| Stats Pushdown | Yes for boolean |
| Pattern | Simple filter + optional join |

---

#### Q14: Analyze skill demand across industries

**Business Question**: "What's the average importance of 'Data Analysis' skill across all occupations?"

```typescript
// Filter by skill, aggregate importance
{
  skill: { $ref: 'skills/2-A-2-b' } // Data Analysis
}
// Aggregate: AVG(importance), COUNT

// Or grouped by SOC major group
// First filter, then group by socCode prefix
```

| Metric | Value |
|--------|-------|
| Selectivity | Low (~1,000 rows for one skill) |
| Index Column | `$index_skill` |
| Stats Pushdown | Yes for skill ID |
| Pattern | Filter + aggregation |

---

#### Q15: Labor market wage analysis

**Business Question**: "What's the salary distribution for STEM occupations?"

```typescript
// Filter LaborMarketInfo by SOC code ranges
{
  socCode: { $regex: '^(15-|17-)' }  // Computer and Engineering
}
// Return: medianWage, hourlyWage50, employment

// For detailed percentile analysis
{ socCode: '15-1252.00' }
// Return: hourlyWage10, hourlyWage25, hourlyWage50, hourlyWage75, hourlyWage90
```

| Metric | Value |
|--------|-------|
| Selectivity | ~15% for STEM filter |
| Index Column | `$index_socCode` |
| Stats Pushdown | Yes for string prefix |
| Pattern | Filter + projection |

---

### 5. Education and Training Recommendation Systems

Applications that suggest learning paths and credential requirements.

#### Q16: Find certifications for occupation

**Business Question**: "What licenses/certifications are required for 'Financial Analyst'?"

```typescript
// Query OccupationLicense
{
  occupation: { $ref: 'occupations/13-2051-00' },
  isRequired: true
}

// Or all certifications (required + preferred)
{ occupation: { $ref: 'occupations/13-2051-00' } }
```

| Metric | Value |
|--------|-------|
| Selectivity | Very low (0-5 per occupation) |
| Index Column | `$index_occupation` |
| Stats Pushdown | Yes |
| Pattern | Simple key lookup |

---

#### Q17: Find occupations by education level required

**Business Question**: "What careers can I pursue with an Associate's degree?"

```typescript
// Filter OccupationEducation
{
  education: { $ref: 'education/5' }, // Associate's
  percentRequired: { $gte: 30 } // At least 30% require this level
}

// Join to get occupation details
```

| Metric | Value |
|--------|-------|
| Selectivity | ~15% of education requirements |
| Index Columns | `$index_education`, `$index_percentRequired` |
| Stats Pushdown | Yes for ID and float range |
| Pattern | Filter + join to occupations |

---

#### Q18: Map knowledge areas to training programs

**Business Question**: "What occupations require 'Computer Science' knowledge at high importance?"

```typescript
{
  knowledge: { $ref: 'knowledge/2-C-3-a' }, // Computers and Electronics
  importance: { $gte: 4.0 }
}
```

| Metric | Value |
|--------|-------|
| Selectivity | ~10% of knowledge ratings |
| Index Columns | `$index_knowledge`, `$index_importance` |
| Stats Pushdown | Yes |
| Pattern | Filter on rating table |

---

### 6. Task and Work Activity Analysis

Applications analyzing actual job duties and activities.

#### Q19: Find core tasks for occupation

**Business Question**: "What are the essential tasks for 'Registered Nurse'?"

```typescript
// Filter Task by occupation and core status
{
  occupation: { $ref: 'occupations/29-1141-00' },
  taskType: 'Core'
}
// Or using isCore boolean
{
  occupation: { $ref: 'occupations/29-1141-00' },
  isCore: true
}
```

| Metric | Value |
|--------|-------|
| Selectivity | Low (~10-20 core tasks per occupation) |
| Index Columns | `$index_occupation`, `$index_taskType` |
| Stats Pushdown | Yes for both |
| Pattern | Simple composite filter |

---

#### Q20: Find occupations by work activity

**Business Question**: "What occupations involve 'Analyzing Data or Information' as a primary activity?"

```typescript
// Filter OccupationWorkActivity
{
  workActivity: { $ref: 'work-activities/4-A-2-a-4' },
  importance: { $gte: 4.0 }
}
```

| Metric | Value |
|--------|-------|
| Selectivity | ~15% of work activity ratings |
| Index Columns | `$index_workActivity`, `$index_importance` |
| Stats Pushdown | Yes |
| Pattern | Filter on rating table + join |

---

## Query Pattern Summary Table

| # | Query Pattern | Use Case | Primary Index | Stats Benefit | Selectivity |
|---|---------------|----------|---------------|---------------|-------------|
| Q1 | Job Zone filter | Career exploration | `$index_jobZone` | High | ~20% |
| Q2 | Holland code interests | Career exploration | `$index_interest` | High | ~17% |
| Q3 | Related occupations | Career exploration | `$index_fromOccupation` | High | <5% |
| Q4 | Career pathway | Career exploration | `$index_jobZone`, `$index_socCode` | High | <5% |
| Q5 | Skill level threshold | Job matching | `$index_skill`, `$index_level` | High | ~15% |
| Q6 | Technology search | Job matching | `$index_technology` | Medium | High |
| Q7 | Title matching | Job matching | `$index_title` (FTS) | Low | <1% |
| Q8 | Occupation requirements | Job matching | `$index_occupation` | High | <5% |
| Q9 | Skill gap analysis | Skills gap | `$index_occupation` | High | <5% |
| Q10 | Training requirements | Skills gap | `$index_occupation` | High | <1% |
| Q11 | Skill profile matching | Skills gap | `$index_skill`, `$index_level` | High | Medium |
| Q12 | Job Zone aggregation | Analytics | `$index_jobZone` | Metadata | 100% |
| Q13 | Bright Outlook filter | Analytics | `$index_brightOutlook` | High | ~15% |
| Q14 | Skill demand analysis | Analytics | `$index_skill` | High | Low |
| Q15 | Wage analysis | Analytics | `$index_socCode` | Medium | ~15% |
| Q16 | License lookup | Training | `$index_occupation` | High | <1% |
| Q17 | Education level filter | Training | `$index_education` | High | ~15% |
| Q18 | Knowledge requirements | Training | `$index_knowledge` | High | ~10% |
| Q19 | Core tasks | Work analysis | `$index_occupation`, `$index_taskType` | High | <5% |
| Q20 | Work activity filter | Work analysis | `$index_workActivity` | High | ~15% |

---

## Index Column Recommendations

### High-Priority Index Columns

These columns appear in multiple high-frequency queries and benefit significantly from Parquet statistics:

| Column | Type | Queries | Rationale |
|--------|------|---------|-----------|
| `$index_occupation` | STRING | Q8, Q9, Q10, Q16, Q19 | Foreign key in all rating tables |
| `$index_importance` | FLOAT64 | Q5, Q8, Q11, Q18, Q20 | Threshold filtering on all ratings |
| `$index_level` | FLOAT64 | Q5, Q9, Q11 | Skill level matching |
| `$index_skill` | STRING | Q5, Q11, Q14 | Skill-based queries |
| `$index_jobZone` | INT32 | Q1, Q4, Q12 | Education level filtering |
| `$index_socCode` | STRING | Q4, Q15 | SOC prefix queries |

### Medium-Priority Index Columns

| Column | Type | Queries | Rationale |
|--------|------|---------|-----------|
| `$index_brightOutlook` | BOOLEAN | Q13 | Outlook filtering |
| `$index_isHotTechnology` | BOOLEAN | Q6 | Technology trends |
| `$index_taskType` | STRING | Q19 | Core vs supplemental |
| `$index_workActivity` | STRING | Q20 | Activity filtering |
| `$index_education` | STRING | Q17 | Education level |
| `$index_knowledge` | STRING | Q18 | Knowledge area |

### Full-Text Search Indexes

Required for title/description searches (Q7):

| Collection | Fields | Index Type |
|------------|--------|------------|
| Occupation | `title`, `description` | FTS |
| AlternativeTitle | `title`, `shortTitle` | FTS |
| Task | `statement` | FTS |

---

## Row Group Statistics Pushdown Analysis

### Best Candidates for Statistics Pushdown

These query patterns benefit most from Parquet row group statistics:

| Pattern | Column | Stats Type | Skip Rate |
|---------|--------|-----------|-----------|
| Job Zone equality | `$index_jobZone` | min/max | ~80% |
| Importance threshold | `$index_importance` | min/max | ~50-85% |
| Level threshold | `$index_level` | min/max | ~50-85% |
| Boolean filters | `$index_brightOutlook`, `$index_isHotTechnology` | min/max | ~85-90% |
| SOC prefix | `$index_socCode` | min/max | ~70-90% |

### Limited Statistics Benefit

| Pattern | Reason |
|---------|--------|
| Title/description search | String content varies too much |
| Occupation ID lookup | Single value, must scan metadata |
| Full aggregations | Need all rows |

---

## Composite Index Patterns

For queries combining multiple filters, consider composite shredded fields:

### Recommended Composites

```typescript
// For skill matching queries (Q5, Q11)
$index_skill_level: `${skillId}:${level}`  // Enables combined filtering

// For occupation-based ratings (Q8)
$index_occupation_importance: `${occupationId}:${importance}`

// For education requirements (Q17)
$index_education_percent: `${educationId}:${percentRequired}`
```

### Composite Index Benefits

| Composite | Queries | Benefit |
|-----------|---------|---------|
| `skill + level` | Q5, Q11 | Single scan vs two |
| `occupation + importance` | Q8, Q10 | Selective on both |
| `workActivity + importance` | Q20 | Common pattern |

---

## Query Optimization Tips

### 1. Use ID References Over Joins

Instead of:
```typescript
// Slow: Join through skill name
{ skill: { name: 'Programming' } }
```

Use:
```typescript
// Fast: Direct ID reference
{ skill: { $ref: 'skills/2-B-2-i' } }
```

### 2. Filter Early with Indexed Columns

Instead of:
```typescript
// Slow: Filter on $data field
{ '$data.category': 'Basic Skills' }
```

Use:
```typescript
// Fast: Filter on index column
{ '$index_category': 'Basic Skills' }
```

### 3. Leverage Boolean Columns for Flags

Instead of:
```typescript
// Slow: String comparison
{ taskType: 'Core' }
```

Consider:
```typescript
// Fast: Boolean (if available)
{ isCore: true }
```

### 4. Batch Relationship Queries

Instead of N queries:
```typescript
// Slow: N separate queries
for (const skillId of skillIds) {
  await db.OccupationSkills.find({ skill: { $ref: skillId } })
}
```

Use batch:
```typescript
// Fast: Single query with $in
await db.OccupationSkills.find({
  skill: { $in: skillIds.map(id => ({ $ref: id })) }
})
```

---

## Performance Expectations

### Single-Entity Lookups
- **By ID**: <5ms (direct Parquet row access)
- **By indexed field**: 10-20ms (stats pushdown)

### Rating Table Queries
- **By occupation ID**: 20-50ms (~35 rows per occupation)
- **By skill/ability ID with threshold**: 50-100ms (~15% selectivity)

### Aggregation Queries
- **Simple count by category**: 100-200ms (metadata scan)
- **Full aggregation with filters**: 200-500ms

### Graph Traversals
- **Single hop**: 50-100ms
- **Multi-hop with filters**: 200-500ms

---

## References

- [O*NET Content Model](https://www.onetcenter.org/content.html)
- [O*NET Database Documentation](https://www.onetcenter.org/database.html)
- [ParqueDB Variant Shredding](../docs/architecture/VARIANT_SHREDDING.md)
- [ParqueDB Secondary Indexes](../docs/architecture/SECONDARY_INDEXES.md)
