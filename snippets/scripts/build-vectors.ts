#!/usr/bin/env npx tsx
/**
 * Build Vector Indexes for Semantic Search
 *
 * Generates embeddings for documents and stores them in vector shards.
 * Currently uses TF-IDF as a simple vector representation.
 * For production, replace with OpenAI/Cohere/etc embeddings.
 */

import { readFile, writeFile, mkdir } from 'fs/promises'
import { existsSync } from 'fs'
import { join } from 'path'

const VECTOR_DIM = 128  // Dimensionality of our TF-IDF vectors
const SHARD_SIZE = 500

interface DatasetConfig {
  sourceFile: string
  textFields: string[]
}

const DATASETS: Record<string, DatasetConfig> = {
  onet: { sourceFile: 'onet-occupations.json', textFields: ['title', 'description'] },
  imdb: { sourceFile: 'imdb-titles.json', textFields: ['primaryTitle', 'originalTitle'] },
  unspsc: { sourceFile: 'unspsc-codes.json', textFields: ['commodityTitle', 'classTitle', 'familyTitle', 'segmentTitle'] },
}

// Simple tokenizer
function tokenize(text: string): string[] {
  return text.toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(t => t.length >= 2)
}

// Build vocabulary from all documents
function buildVocab(docs: Record<string, unknown>[], textFields: string[]): Map<string, number> {
  const termFreq = new Map<string, number>()

  for (const doc of docs) {
    for (const field of textFields) {
      const text = doc[field]
      if (typeof text === 'string') {
        for (const term of tokenize(text)) {
          termFreq.set(term, (termFreq.get(term) || 0) + 1)
        }
      }
    }
  }

  // Take top N terms by frequency
  const sortedTerms = [...termFreq.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, VECTOR_DIM)
    .map(([term], idx) => [term, idx] as [string, number])

  return new Map(sortedTerms)
}

// Compute TF-IDF vector for a document
function computeVector(
  doc: Record<string, unknown>,
  textFields: string[],
  vocab: Map<string, number>,
  idf: Map<string, number>
): number[] {
  const vector = new Array(VECTOR_DIM).fill(0)
  const termCounts = new Map<string, number>()

  // Count terms in document
  for (const field of textFields) {
    const text = doc[field]
    if (typeof text === 'string') {
      for (const term of tokenize(text)) {
        termCounts.set(term, (termCounts.get(term) || 0) + 1)
      }
    }
  }

  // Compute TF-IDF
  for (const [term, count] of termCounts) {
    const idx = vocab.get(term)
    if (idx !== undefined) {
      const tf = count
      const idfVal = idf.get(term) || 1
      vector[idx] = tf * idfVal
    }
  }

  // Normalize (L2)
  const norm = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
  if (norm > 0) {
    for (let i = 0; i < vector.length; i++) {
      vector[i] = vector[i]! / norm
    }
  }

  return vector
}

// Compute IDF for each term
function computeIDF(
  docs: Record<string, unknown>[],
  textFields: string[],
  vocab: Map<string, number>
): Map<string, number> {
  const docFreq = new Map<string, number>()

  for (const doc of docs) {
    const docTerms = new Set<string>()
    for (const field of textFields) {
      const text = doc[field]
      if (typeof text === 'string') {
        for (const term of tokenize(text)) {
          docTerms.add(term)
        }
      }
    }
    for (const term of docTerms) {
      docFreq.set(term, (docFreq.get(term) || 0) + 1)
    }
  }

  const idf = new Map<string, number>()
  const N = docs.length
  for (const [term] of vocab) {
    const df = docFreq.get(term) || 1
    idf.set(term, Math.log(N / df) + 1)
  }

  return idf
}

async function buildVectorIndex(name: string, config: DatasetConfig): Promise<void> {
  console.log(`\n=== Building vector index for ${name} ===`)

  const dataPath = join('data', config.sourceFile)
  if (!existsSync(dataPath)) {
    console.log(`  Skipping - source file not found: ${dataPath}`)
    return
  }

  const rawData = await readFile(dataPath, 'utf-8')
  const docs = JSON.parse(rawData) as Record<string, unknown>[]
  console.log(`  Loaded ${docs.length} documents`)

  // Build vocabulary
  console.log('  Building vocabulary...')
  const vocab = buildVocab(docs, config.textFields)
  console.log(`  Vocabulary size: ${vocab.size}`)

  // Compute IDF
  console.log('  Computing IDF...')
  const idf = computeIDF(docs, config.textFields, vocab)

  // Generate vectors
  console.log('  Generating vectors...')
  const vectors: number[][] = []
  for (const doc of docs) {
    vectors.push(computeVector(doc, config.textFields, vocab, idf))
  }

  // Write vector shards
  const outputDir = join('indexes', name)
  if (!existsSync(outputDir)) {
    await mkdir(outputDir, { recursive: true })
  }

  const shardCount = Math.ceil(vectors.length / SHARD_SIZE)
  console.log(`  Writing ${shardCount} vector shards...`)

  for (let i = 0; i < shardCount; i++) {
    const start = i * SHARD_SIZE
    const end = Math.min(start + SHARD_SIZE, vectors.length)
    const shard = vectors.slice(start, end)

    const shardPath = join(outputDir, `vectors-${i}.json`)
    await writeFile(shardPath, JSON.stringify(shard))
    console.log(`    vectors-${i}.json: ${end - start} vectors`)
  }

  // Update meta.json
  const metaPath = join(outputDir, 'meta.json')
  let meta: Record<string, unknown> = {}
  if (existsSync(metaPath)) {
    meta = JSON.parse(await readFile(metaPath, 'utf-8'))
  }

  meta.hasVectors = true
  meta.vectorDim = VECTOR_DIM
  meta.vectorShardCount = shardCount

  await writeFile(metaPath, JSON.stringify(meta, null, 2))
  console.log(`  Updated meta.json with vector info`)

  // Write vocabulary for debugging
  const vocabPath = join(outputDir, 'vocab.json')
  await writeFile(vocabPath, JSON.stringify([...vocab.entries()]))
  console.log(`  Wrote vocab.json`)

  console.log(`  ✓ Vector index complete for ${name}`)
}

async function main() {
  console.log('Building vector indexes...')
  console.log(`Vector dimension: ${VECTOR_DIM}`)
  console.log(`Shard size: ${SHARD_SIZE}`)

  for (const [name, config] of Object.entries(DATASETS)) {
    await buildVectorIndex(name, config)
  }

  console.log('\n✓ All vector indexes built!')
  console.log('\nTo upload to R2:')
  console.log('  wrangler r2 object put parquedb-search-data/indexes/{dataset}/vectors-{N}.json --file indexes/{dataset}/vectors-{N}.json')
}

main().catch(console.error)
