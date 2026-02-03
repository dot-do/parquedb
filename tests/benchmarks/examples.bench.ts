/**
 * Example Dataset Benchmarks for ParqueDB
 *
 * Benchmarks using realistic example datasets:
 * - IMDB: Search titles, get filmography, find collaborations
 * - Wiktionary: Word lookup, synonym search
 * - UNSPSC: Hierarchy traversal
 * - O*NET: Skill matching
 */

import { describe, bench, beforeAll, beforeEach } from 'vitest'
import { Collection } from '../../src/Collection'
import type { Entity, EntityId } from '../../src/types'
import {
  randomElement,
  randomInt,
  randomString,
  randomSubset,
} from './setup'

// =============================================================================
// IMDB-like Types
// =============================================================================

interface Title {
  tconst: string
  titleType: 'movie' | 'tvSeries' | 'tvEpisode' | 'short' | 'videoGame'
  primaryTitle: string
  originalTitle: string
  isAdult: boolean
  startYear: number
  endYear?: number | undefined
  runtimeMinutes?: number | undefined
  genres: string[]
  averageRating?: number | undefined
  numVotes?: number | undefined
}

interface Person {
  nconst: string
  primaryName: string
  birthYear?: number | undefined
  deathYear?: number | undefined
  primaryProfession: string[]
  knownForTitles: string[]
}

interface Principal {
  tconst: string
  nconst: string
  ordering: number
  category: string
  job?: string | undefined
  characters?: string[] | undefined
}

// =============================================================================
// Wiktionary-like Types
// =============================================================================

interface Word {
  word: string
  language: string
  partOfSpeech: string
  definitions: Array<{
    text: string
    examples?: string[] | undefined
  }>
  etymology?: string | undefined
  pronunciation?: {
    ipa: string
    audio?: string | undefined
  }
  synonyms?: string[] | undefined
  antonyms?: string[] | undefined
  relatedWords?: string[] | undefined
}

// =============================================================================
// UNSPSC-like Types (Product Classification)
// =============================================================================

interface UNSPSCSegment {
  code: string
  title: string
  level: 'segment' | 'family' | 'class' | 'commodity'
  parentCode?: string | undefined
  description?: string | undefined
}

// =============================================================================
// O*NET-like Types (Occupations/Skills)
// =============================================================================

interface Occupation {
  code: string
  title: string
  description: string
  skills: Array<{
    skillId: string
    level: number
    importance: number
  }>
  interests: string[]
  workStyles: string[]
  education: string
  experience: string
  salary: {
    median: number
    low: number
    high: number
  }
}

interface Skill {
  skillId: string
  name: string
  description: string
  category: string
}

// =============================================================================
// Data Generators
// =============================================================================

const movieGenres = ['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 'Romance', 'Thriller', 'Documentary']
const professions = ['actor', 'actress', 'director', 'writer', 'producer', 'composer', 'cinematographer']
const languages = ['en', 'es', 'fr', 'de', 'ja', 'zh', 'pt', 'ru']
const partsOfSpeech = ['noun', 'verb', 'adjective', 'adverb', 'preposition']

function generateTitle(index: number): Title {
  const types: Title['titleType'][] = ['movie', 'tvSeries', 'tvEpisode', 'short']
  return {
    tconst: `tt${index.toString().padStart(7, '0')}`,
    titleType: types[index % types.length],
    primaryTitle: `Movie Title ${index}: ${randomString(20)}`,
    originalTitle: `Original Title ${index}`,
    isAdult: false,
    startYear: randomInt(1920, 2024),
    endYear: Math.random() > 0.7 ? randomInt(1920, 2024) : undefined,
    runtimeMinutes: randomInt(60, 180),
    genres: randomSubset(movieGenres, randomInt(1, 3)),
    averageRating: Math.random() > 0.3 ? randomInt(10, 100) / 10 : undefined,
    numVotes: Math.random() > 0.3 ? randomInt(100, 1000000) : undefined,
  }
}

function generatePerson(index: number): Person {
  return {
    nconst: `nm${index.toString().padStart(7, '0')}`,
    primaryName: `Person ${index} ${randomString(10)}`,
    birthYear: randomInt(1920, 2000),
    deathYear: Math.random() > 0.8 ? randomInt(1950, 2024) : undefined,
    primaryProfession: randomSubset(professions, randomInt(1, 3)),
    knownForTitles: Array.from(
      { length: randomInt(1, 5) },
      () => `tt${randomInt(1, 10000).toString().padStart(7, '0')}`
    ),
  }
}

function generateWord(index: number): Word {
  const word: Word = {
    word: `word${index}${randomString(5)}`.toLowerCase(),
    language: randomElement(languages),
    partOfSpeech: randomElement(partsOfSpeech),
    definitions: [
      {
        text: `Definition of word ${index}: ${randomString(50)}`,
        examples: Math.random() > 0.5 ? [`Example sentence ${index}.`] : undefined,
      },
    ],
    etymology: Math.random() > 0.5 ? `From Latin word${index}` : undefined,
    pronunciation: Math.random() > 0.5 ? { ipa: `/wɜːd${index}/` } : undefined,
    synonyms: Math.random() > 0.5 ? [`syn${index}a`, `syn${index}b`] : undefined,
    antonyms: Math.random() > 0.5 ? [`ant${index}`] : undefined,
  }
  return word
}

function generateUNSPSC(index: number, level: UNSPSCSegment['level'], parentCode?: string): UNSPSCSegment {
  const code = parentCode
    ? `${parentCode}${(index % 100).toString().padStart(2, '0')}`
    : (index * 1000000).toString().padStart(8, '0')
  return {
    code,
    title: `${level.charAt(0).toUpperCase() + level.slice(1)} ${index}: ${randomString(15)}`,
    level,
    parentCode,
    description: `Description for ${level} ${index}`,
  }
}

function generateOccupation(index: number): Occupation {
  return {
    code: `${(index % 100).toString().padStart(2, '0')}-${(index % 10000).toString().padStart(4, '0')}`,
    title: `Occupation ${index}: ${randomString(20)}`,
    description: randomString(200),
    skills: Array.from({ length: randomInt(5, 15) }, (_, i) => ({
      skillId: `skill-${(index * 100 + i) % 500}`,
      level: randomInt(1, 5),
      importance: randomInt(1, 5),
    })),
    interests: randomSubset(['Realistic', 'Investigative', 'Artistic', 'Social', 'Enterprising', 'Conventional'], 3),
    workStyles: randomSubset(['Leadership', 'Independence', 'Cooperation', 'Innovation', 'Detail'], 3),
    education: randomElement(['High School', 'Associate', 'Bachelor', 'Master', 'Doctoral']),
    experience: randomElement(['None', '< 1 year', '1-2 years', '2-5 years', '5+ years']),
    salary: {
      median: randomInt(30000, 150000),
      low: randomInt(20000, 50000),
      high: randomInt(100000, 300000),
    },
  }
}

function generateSkill(index: number): Skill {
  const categories = ['Basic Skills', 'Cross-Functional Skills', 'Technical Skills', 'Systems Skills']
  return {
    skillId: `skill-${index}`,
    name: `Skill ${index}: ${randomString(15)}`,
    description: `Description for skill ${index}: ${randomString(100)}`,
    category: randomElement(categories),
  }
}

// =============================================================================
// IMDB Benchmarks
// =============================================================================

describe('Example Dataset Benchmarks', () => {
  describe('IMDB Dataset', () => {
    let titles: Collection<Title>
    let persons: Collection<Person>
    let principals: Collection<Principal>

    let titleIds: string[] = []
    let personIds: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()

      titles = new Collection<Title>(`titles-${suffix}`)
      persons = new Collection<Person>(`persons-${suffix}`)
      principals = new Collection<Principal>(`principals-${suffix}`)

      // Generate 5000 titles
      for (let i = 0; i < 5000; i++) {
        const title = await titles.create({
          $type: 'Title',
          name: `Title ${i}`,
          ...generateTitle(i),
        })
        titleIds.push(title.$id as string)
      }

      // Generate 2000 persons
      for (let i = 0; i < 2000; i++) {
        const person = await persons.create({
          $type: 'Person',
          name: `Person ${i}`,
          ...generatePerson(i),
        })
        personIds.push(person.$id as string)
      }

      // Generate principals (cast/crew connections)
      for (let i = 0; i < 10000; i++) {
        const titleIdx = i % 5000
        const personIdx = i % 2000
        await principals.create({
          $type: 'Principal',
          name: `Principal ${i}`,
          tconst: `tt${titleIdx.toString().padStart(7, '0')}`,
          nconst: `nm${personIdx.toString().padStart(7, '0')}`,
          ordering: (i % 10) + 1,
          category: randomElement(['actor', 'actress', 'director', 'writer', 'producer']),
          job: Math.random() > 0.5 ? randomElement(['lead', 'supporting', 'cameo']) : undefined,
          characters: Math.random() > 0.5 ? [`Character ${i}`] : undefined,
        })
      }
    })

    // Title search benchmarks
    bench('search titles by name (text match)', async () => {
      await titles.find({ primaryTitle: { $regex: 'Title 1', $options: 'i' } }, { limit: 20 })
    })

    bench('search titles by genre', async () => {
      await titles.find({ genres: { $in: ['Action', 'Sci-Fi'] } }, { limit: 20 })
    })

    bench('search titles by year range', async () => {
      await titles.find({ startYear: { $gte: 2000, $lte: 2020 } }, { limit: 20 })
    })

    bench('search titles by rating (top rated)', async () => {
      await titles.find(
        { averageRating: { $gte: 8.0 }, numVotes: { $gte: 10000 } },
        { sort: { averageRating: -1 }, limit: 20 }
      )
    })

    bench('search titles - complex filter (genre + year + rating)', async () => {
      await titles.find(
        {
          $and: [
            { genres: { $in: ['Drama'] } },
            { startYear: { $gte: 2010 } },
            { averageRating: { $gte: 7.0 } },
          ],
        },
        { sort: { numVotes: -1 }, limit: 20 }
      )
    })

    // Filmography benchmarks
    bench('get person filmography', async () => {
      const personId = randomElement(personIds)
      const person = await persons.get(personId)
      if (person) {
        // Find all titles this person is known for
        await principals.find({ nconst: person.nconst }, { limit: 50 })
      }
    })

    bench('get title cast and crew', async () => {
      const titleId = randomElement(titleIds)
      const title = await titles.get(titleId)
      if (title) {
        await principals.find(
          { tconst: title.tconst },
          { sort: { ordering: 1 } }
        )
      }
    })

    bench('find collaborations (actors who worked together)', async () => {
      // Get two random actors
      const person1 = await persons.find({ primaryProfession: { $in: ['actor', 'actress'] } }, { limit: 1 })
      const person2 = await persons.find(
        { primaryProfession: { $in: ['actor', 'actress'] }, nconst: { $ne: person1[0]?.nconst } },
        { limit: 1 }
      )

      if (person1[0] && person2[0]) {
        // Find titles where both appeared
        const p1Titles = await principals.find({ nconst: person1[0].nconst })
        const p1TitleIds = p1Titles.map(p => p.tconst)

        await principals.find({
          nconst: person2[0].nconst,
          tconst: { $in: p1TitleIds },
        })
      }
    })

    bench('aggregate titles by genre', async () => {
      await titles.aggregate([
        { $unwind: '$genres' },
        {
          $group: {
            _id: '$genres',
            count: { $sum: 1 },
            avgRating: { $avg: '$averageRating' },
          },
        },
        { $sort: { count: -1 } },
      ])
    })
  })

  // ===========================================================================
  // Wiktionary Benchmarks
  // ===========================================================================

  describe('Wiktionary Dataset', () => {
    let words: Collection<Word>
    let wordIds: string[] = []
    let wordList: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()
      words = new Collection<Word>(`words-${suffix}`)

      // Generate 10000 words
      for (let i = 0; i < 10000; i++) {
        const wordData = generateWord(i)
        const entity = await words.create({
          $type: 'Word',
          name: wordData.word,
          ...wordData,
        })
        wordIds.push(entity.$id as string)
        wordList.push(wordData.word)
      }
    })

    bench('word lookup by exact match', async () => {
      const word = randomElement(wordList)
      await words.find({ word }, { limit: 1 })
    })

    bench('word lookup by prefix (autocomplete)', async () => {
      await words.find({ word: { $regex: '^word1' } }, { limit: 10 })
    })

    bench('search words by language', async () => {
      await words.find({ language: 'en' }, { limit: 50 })
    })

    bench('search words by part of speech', async () => {
      await words.find({ partOfSpeech: 'noun' }, { limit: 50 })
    })

    bench('search words with synonyms', async () => {
      await words.find({ synonyms: { $exists: true, $ne: [] } }, { limit: 50 })
    })

    bench('synonym lookup', async () => {
      // Get a word and find its synonyms
      const wordId = randomElement(wordIds)
      const word = await words.get(wordId)
      const synonyms = word?.synonyms as string[] | undefined
      if (synonyms && synonyms.length > 0) {
        await words.find({ word: { $in: synonyms } })
      }
    })

    bench('find words with similar definitions (text search simulation)', async () => {
      await words.find(
        { 'definitions.text': { $regex: 'Definition of word 1', $options: 'i' } },
        { limit: 20 }
      )
    })

    bench('aggregate words by language', async () => {
      await words.aggregate([
        {
          $group: {
            _id: '$language',
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
      ])
    })

    bench('aggregate words by part of speech', async () => {
      await words.aggregate([
        {
          $group: {
            _id: '$partOfSpeech',
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
      ])
    })
  })

  // ===========================================================================
  // UNSPSC Benchmarks (Hierarchy Traversal)
  // ===========================================================================

  describe('UNSPSC Dataset (Hierarchy)', () => {
    let segments: Collection<UNSPSCSegment>
    let segmentCodes: string[] = []
    let familyCodes: string[] = []
    let classCodes: string[] = []
    let commodityCodes: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()
      segments = new Collection<UNSPSCSegment>(`unspsc-${suffix}`)

      // Generate hierarchical data
      // 10 segments
      for (let s = 0; s < 10; s++) {
        const segment = await segments.create({
          $type: 'UNSPSCSegment',
          name: `Segment ${s}`,
          ...generateUNSPSC(s + 1, 'segment'),
        })
        segmentCodes.push(segment.code as string)

        // 10 families per segment
        for (let f = 0; f < 10; f++) {
          const family = await segments.create({
            $type: 'UNSPSCSegment',
            name: `Family ${s}-${f}`,
            ...generateUNSPSC(f + 1, 'family', segment.code as string),
          })
          familyCodes.push(family.code as string)

          // 10 classes per family
          for (let c = 0; c < 10; c++) {
            const classItem = await segments.create({
              $type: 'UNSPSCSegment',
              name: `Class ${s}-${f}-${c}`,
              ...generateUNSPSC(c + 1, 'class', family.code as string),
            })
            classCodes.push(classItem.code as string)

            // 5 commodities per class
            for (let m = 0; m < 5; m++) {
              const commodity = await segments.create({
                $type: 'UNSPSCSegment',
                name: `Commodity ${s}-${f}-${c}-${m}`,
                ...generateUNSPSC(m + 1, 'commodity', classItem.code as string),
              })
              commodityCodes.push(commodity.code as string)
            }
          }
        }
      }
    })

    bench('get all top-level segments', async () => {
      await segments.find({ level: 'segment' })
    })

    bench('get children of segment (families)', async () => {
      const segmentCode = randomElement(segmentCodes)
      await segments.find({ parentCode: segmentCode, level: 'family' })
    })

    bench('get children of family (classes)', async () => {
      const familyCode = randomElement(familyCodes)
      await segments.find({ parentCode: familyCode, level: 'class' })
    })

    bench('get children of class (commodities)', async () => {
      const classCode = randomElement(classCodes)
      await segments.find({ parentCode: classCode, level: 'commodity' })
    })

    bench('traverse hierarchy: segment -> commodity (3 levels)', async () => {
      const segmentCode = randomElement(segmentCodes)
      // Get families
      const families = await segments.find({ parentCode: segmentCode, level: 'family' })
      if (families.length > 0) {
        // Get classes of first family
        const familyCode = families[0].code as string
        const classes = await segments.find({ parentCode: familyCode, level: 'class' })
        if (classes.length > 0) {
          // Get commodities of first class
          const classCode = classes[0].code as string
          await segments.find({ parentCode: classCode, level: 'commodity' })
        }
      }
    })

    bench('search by title (text search)', async () => {
      await segments.find({ title: { $regex: 'Segment 1', $options: 'i' } }, { limit: 20 })
    })

    bench('find all ancestors of commodity', async () => {
      const commodityCode = randomElement(commodityCodes)
      const commodity = await segments.find({ code: commodityCode }, { limit: 1 })

      if (commodity[0]?.parentCode) {
        const ancestors: Entity<UNSPSCSegment>[] = []
        let currentCode: string | undefined = commodity[0].parentCode as string | undefined

        while (currentCode) {
          const parent = await segments.find({ code: currentCode }, { limit: 1 })
          if (parent[0]) {
            ancestors.push(parent[0])
            currentCode = parent[0].parentCode as string | undefined
          } else {
            break
          }
        }
      }
    })

    bench('aggregate by level', async () => {
      await segments.aggregate([
        {
          $group: {
            _id: '$level',
            count: { $sum: 1 },
          },
        },
      ])
    })
  })

  // ===========================================================================
  // O*NET Benchmarks (Skill Matching)
  // ===========================================================================

  describe('O*NET Dataset (Skills)', () => {
    let occupations: Collection<Occupation>
    let skills: Collection<Skill>

    let occupationIds: string[] = []
    let skillIds: string[] = []

    beforeAll(async () => {
      const suffix = Date.now()
      occupations = new Collection<Occupation>(`occupations-${suffix}`)
      skills = new Collection<Skill>(`skills-${suffix}`)

      // Generate 500 skills
      for (let i = 0; i < 500; i++) {
        const skill = await skills.create({
          $type: 'Skill',
          name: `Skill ${i}`,
          ...generateSkill(i),
        })
        skillIds.push(skill.$id as string)
      }

      // Generate 1000 occupations
      for (let i = 0; i < 1000; i++) {
        const occupation = await occupations.create({
          $type: 'Occupation',
          name: `Occupation ${i}`,
          ...generateOccupation(i),
        })
        occupationIds.push(occupation.$id as string)
      }
    })

    bench('search occupations by title', async () => {
      await occupations.find({ title: { $regex: 'Occupation 1', $options: 'i' } }, { limit: 20 })
    })

    bench('search occupations by education level', async () => {
      await occupations.find({ education: 'Bachelor' }, { limit: 20 })
    })

    bench('search occupations by salary range', async () => {
      await occupations.find(
        { 'salary.median': { $gte: 50000, $lte: 100000 } },
        { limit: 20 }
      )
    })

    bench('find occupations requiring specific skill', async () => {
      const targetSkillId = `skill-${randomInt(0, 499)}`
      await occupations.find(
        { 'skills.skillId': targetSkillId },
        { limit: 20 }
      )
    })

    bench('find occupations requiring skill at high level', async () => {
      const targetSkillId = `skill-${randomInt(0, 499)}`
      await occupations.find(
        {
          skills: {
            $elemMatch: {
              skillId: targetSkillId,
              level: { $gte: 4 },
            },
          },
        },
        { limit: 20 }
      )
    })

    bench('skill matching: find occupations matching skill set', async () => {
      // Given a set of skills, find matching occupations
      const userSkills = [`skill-${randomInt(0, 99)}`, `skill-${randomInt(100, 199)}`, `skill-${randomInt(200, 299)}`]

      await occupations.find(
        { 'skills.skillId': { $in: userSkills } },
        { limit: 20 }
      )
    })

    bench('get occupation skills with details', async () => {
      const occupationId = randomElement(occupationIds)
      const occupation = await occupations.get(occupationId)

      if (occupation) {
        const skillsArray = occupation.skills as Array<{ skillId: string; level: number; importance: number }>
        const skillIdsToFetch = skillsArray.map(s => s.skillId)
        await skills.find({ skillId: { $in: skillIdsToFetch } })
      }
    })

    bench('aggregate occupations by education', async () => {
      await occupations.aggregate([
        {
          $group: {
            _id: '$education',
            count: { $sum: 1 },
            avgSalary: { $avg: '$salary.median' },
          },
        },
        { $sort: { avgSalary: -1 } },
      ])
    })

    bench('aggregate skills by category', async () => {
      await skills.aggregate([
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
          },
        },
        { $sort: { count: -1 } },
      ])
    })

    bench('find similar occupations (by interests)', async () => {
      const occupationId = randomElement(occupationIds)
      const occupation = await occupations.get(occupationId)

      if (occupation) {
        const interests = occupation.interests as string[]
        const code = occupation.code as string
        await occupations.find(
          {
            interests: { $all: interests.slice(0, 2) },
            code: { $ne: code },
          },
          { limit: 10 }
        )
      }
    })

    bench('top paying occupations by category', async () => {
      await occupations.find(
        {},
        { sort: { 'salary.median': -1 }, limit: 10 }
      )
    })
  })
})
