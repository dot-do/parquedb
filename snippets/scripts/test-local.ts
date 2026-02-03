#!/usr/bin/env bun
/**
 * Local Test Script for Snippets
 *
 * Tests snippets using local parquet files instead of CDN.
 *
 * Usage:
 *   bun scripts/test-local.ts
 */

import { readFileSync } from 'fs'
import { resolve } from 'path'

// Override fetch to serve local files
const originalFetch = globalThis.fetch
globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
  const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url

  // Map CDN URLs to local files (using JSON for reliability)
  const localFiles: Record<string, string> = {
    'https://cdn.workers.do/parquedb-benchmarks/snippets/onet-occupations.json':
      resolve(import.meta.dirname, '../../data/snippets/onet-occupations.json'),
    'https://cdn.workers.do/parquedb-benchmarks/snippets/unspsc.json':
      resolve(import.meta.dirname, '../../data/snippets/unspsc.json'),
    'https://cdn.workers.do/parquedb-benchmarks/snippets/imdb-titles.json':
      resolve(import.meta.dirname, '../../data/snippets/imdb-titles.json'),
  }

  if (localFiles[url]) {
    try {
      const data = readFileSync(localFiles[url])
      return new Response(data)
    } catch (error) {
      return new Response('File not found', { status: 404 })
    }
  }

  return originalFetch(input, init)
}

// Import snippets
const onetSearch = await import('../examples/onet-search/snippet')
const unspscLookup = await import('../examples/unspsc-lookup/snippet')
const imdbSearch = await import('../examples/imdb-search/snippet')

// =============================================================================
// Test Functions
// =============================================================================

async function testOnetSearch() {
  console.log('\n=== O*NET Search Tests ===\n')

  // Test search by query
  let req = new Request('http://localhost/occupations?q=engineer')
  let res = await onetSearch.default.fetch(req)
  let data = await res.json()
  console.log(`Search "engineer": ${data.pagination?.total ?? 0} results`)
  if (data.data?.[0]) console.log(`  First: ${data.data[0].title}`)

  // Test search by code prefix
  req = new Request('http://localhost/occupations?code=11-1011')
  res = await onetSearch.default.fetch(req)
  data = await res.json()
  console.log(`Search code "11-1011": ${data.pagination?.total ?? 0} results`)

  // Test get single occupation
  req = new Request('http://localhost/occupations/11-1011.00')
  res = await onetSearch.default.fetch(req)
  data = await res.json()
  console.log(`Get 11-1011.00: ${data.title ?? 'not found'}`)
}

async function testUnspscLookup() {
  console.log('\n=== UNSPSC Lookup Tests ===\n')

  // Test search by query
  let req = new Request('http://localhost/unspsc?q=pet')
  let res = await unspscLookup.default.fetch(req)
  let data = await res.json()
  console.log(`Search "pet": ${data.pagination?.total ?? 0} results`)
  if (data.data?.[0]) console.log(`  First: ${data.data[0].commodityTitle}`)

  // Test get segments
  req = new Request('http://localhost/unspsc/segments')
  res = await unspscLookup.default.fetch(req)
  data = await res.json()
  console.log(`Segments: ${data.total ?? 0} total`)
  if (data.data?.[0]) console.log(`  First: ${data.data[0].title}`)

  // Test get single commodity
  req = new Request('http://localhost/unspsc/10111302')
  res = await unspscLookup.default.fetch(req)
  data = await res.json()
  console.log(`Get 10111302: ${data.commodityTitle ?? 'not found'}`)
}

async function testImdbSearch() {
  console.log('\n=== IMDB Search Tests ===\n')

  // Test search by query
  let req = new Request('http://localhost/titles?q=love')
  let res = await imdbSearch.default.fetch(req)
  let data = await res.json()
  console.log(`Search "love": ${data.pagination?.total ?? 0} results`)
  if (data.data?.[0]) console.log(`  First: ${data.data[0].primaryTitle} (${data.data[0].startYear})`)

  // Test get types
  req = new Request('http://localhost/titles/types')
  res = await imdbSearch.default.fetch(req)
  data = await res.json()
  console.log(`Title types: ${data.total ?? 0} types`)
  if (data.data?.[0]) console.log(`  Most common: ${data.data[0].type} (${data.data[0].count} titles)`)

  // Test filter by type and year
  req = new Request('http://localhost/titles?type=movie&minYear=2023')
  res = await imdbSearch.default.fetch(req)
  data = await res.json()
  console.log(`Movies from 2023+: ${data.pagination?.total ?? 0} results`)
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log('Testing ParqueDB Snippets with Local Data')
  console.log('=========================================')

  const start = performance.now()

  try {
    await testOnetSearch()
  } catch (error) {
    console.error('O*NET test failed:', error)
  }

  try {
    await testUnspscLookup()
  } catch (error) {
    console.error('UNSPSC test failed:', error)
  }

  try {
    await testImdbSearch()
  } catch (error) {
    console.error('IMDB test failed:', error)
  }

  const elapsed = performance.now() - start
  console.log(`\n=========================================`)
  console.log(`All tests completed in ${elapsed.toFixed(2)}ms`)
}

main().catch(console.error)
