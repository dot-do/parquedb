#!/usr/bin/env bun
import { ShardedHashIndex } from '../src/indexes/secondary/sharded-hash.ts'
import { ShardedSSTIndex } from '../src/indexes/secondary/sharded-sst.ts'
import { FsBackend } from '../src/storage/FsBackend.ts'
import { readdirSync } from 'fs'

const storage = new FsBackend('.')
// ShardedHashIndex adds 'indexes/secondary/' prefix, so basePath is just the namespace root
const basePath = 'data-v3/imdb-1m/titles'

// List available indexes
console.log('Available indexes:')
const files = readdirSync('data-v3/imdb-1m/titles/indexes/secondary')
console.log(files.join(', '))

interface RangeQuery {
  $gte?: number
  $lte?: number
}

interface RangeTest {
  name: string
  query: RangeQuery
}

async function main(): Promise<void> {
  // Test hash index for titleType
  console.log('\n--- Hash Index: titleType ---')
  const hashIndex = new ShardedHashIndex(
    storage,
    'titles',
    { name: 'titleType', type: 'hash', field: '$index_titleType', unique: false },
    basePath
  )
  const loadStart = performance.now()
  await hashIndex.load()
  console.log(`  Load time: ${(performance.now() - loadStart).toFixed(2)}ms`)

  const types = ['movie', 'tvSeries', 'short', 'tvEpisode', 'videoGame', 'tvMovie']
  console.log(`  Loaded: ${hashIndex.ready}`)
  console.log(`  Sharded: ${hashIndex.isSharded}`)
  for (const type of types) {
    const start = performance.now()
    try {
      const result = await hashIndex.lookup(type)
      const elapsed = performance.now() - start
      console.log(`  ${type}: ${result.docIds.length.toLocaleString()} docs, ${result.rowGroups.length} row groups in ${elapsed.toFixed(2)}ms`)
    } catch (err) {
      const elapsed = performance.now() - start
      console.log(`  ${type}: ERROR - ${(err as Error).message} (${elapsed.toFixed(2)}ms)`)
    }
  }

  // Test SST index for startYear
  console.log('\n--- SST Index: startYear ---')
  const sstIndex = new ShardedSSTIndex(
    storage,
    'titles',
    { name: 'startYear', type: 'sst', field: '$index_startYear', unique: false },
    basePath
  )
  const sstLoadStart = performance.now()
  await sstIndex.load()
  console.log(`  Load time: ${(performance.now() - sstLoadStart).toFixed(2)}ms`)

  const ranges: RangeTest[] = [
    { name: '2020-2025', query: { $gte: 2020, $lte: 2025 } },
    { name: '2010-2019', query: { $gte: 2010, $lte: 2019 } },
    { name: '2000-2009', query: { $gte: 2000, $lte: 2009 } },
    { name: '1990-1999', query: { $gte: 1990, $lte: 1999 } },
    { name: '1950-1959', query: { $gte: 1950, $lte: 1959 } }
  ]

  for (const { name, query } of ranges) {
    const start = performance.now()
    try {
      const result = await sstIndex.range(query)
      const elapsed = performance.now() - start
      console.log(`  ${name}: ${result.docIds.length.toLocaleString()} docs, ${result.rowGroups.length} row groups in ${elapsed.toFixed(2)}ms`)
    } catch (err) {
      const elapsed = performance.now() - start
      console.log(`  ${name}: ERROR - ${(err as Error).message} (${elapsed.toFixed(2)}ms)`)
    }
  }

  // Test SST index for averageRating
  console.log('\n--- SST Index: averageRating ---')
  const ratingIndex = new ShardedSSTIndex(
    storage,
    'titles',
    { name: 'averageRating', type: 'sst', field: '$index_averageRating', unique: false },
    basePath
  )
  const ratingLoadStart = performance.now()
  await ratingIndex.load()
  console.log(`  Load time: ${(performance.now() - ratingLoadStart).toFixed(2)}ms`)

  const ratingRanges: RangeTest[] = [
    { name: '>= 9.0 (elite)', query: { $gte: 9.0, $lte: 10.0 } },
    { name: '>= 8.0 (excellent)', query: { $gte: 8.0, $lte: 10.0 } },
    { name: '>= 7.0 (good)', query: { $gte: 7.0, $lte: 10.0 } },
    { name: '6.0-7.0 (average)', query: { $gte: 6.0, $lte: 7.0 } }
  ]

  for (const { name, query } of ratingRanges) {
    const start = performance.now()
    try {
      const result = await ratingIndex.range(query)
      const elapsed = performance.now() - start
      console.log(`  ${name}: ${result.docIds.length.toLocaleString()} docs, ${result.rowGroups.length} row groups in ${elapsed.toFixed(2)}ms`)
    } catch (err) {
      const elapsed = performance.now() - start
      console.log(`  ${name}: ERROR - ${(err as Error).message} (${elapsed.toFixed(2)}ms)`)
    }
  }
}

main().catch(console.error)
