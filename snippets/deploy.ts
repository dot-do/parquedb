#!/usr/bin/env bun
/**
 * Deploy Script for ParqueDB Cloudflare Snippets
 *
 * Uses the Cloudflare API to deploy snippets to a zone.
 *
 * Prerequisites:
 *   1. Set CF_API_TOKEN environment variable (or CLOUDFLARE_API_TOKEN)
 *   2. Set CF_ZONE_ID environment variable
 *   3. Build snippets first: pnpm build
 *
 * Usage:
 *   pnpm deploy                # Deploy all snippets
 *   pnpm deploy onet-search    # Deploy specific snippet
 *   pnpm deploy --list         # List deployed snippets
 *   pnpm deploy --delete NAME  # Delete a snippet
 */

import * as fs from 'node:fs'
import * as path from 'node:path'

// =============================================================================
// Configuration
// =============================================================================

const DIST_DIR = './dist'

// Snippet route configurations
// Note: Snippet names can only contain a-z, 0-9, and _ (no hyphens)
const SNIPPET_ROUTES: Record<string, SnippetConfig> = {
  'onet-search': {
    name: 'parquedb_onet_search',
    routes: ['cdn.workers.do/search/occupations*', 'cdn.workers.do/search/onet*'],
    description: 'Search O*NET occupations from parquet data',
  },
  'unspsc-lookup': {
    name: 'parquedb_unspsc_lookup',
    routes: ['cdn.workers.do/search/unspsc*'],
    description: 'Look up UNSPSC categories from parquet data',
  },
  'imdb-search': {
    name: 'parquedb_imdb_search',
    routes: ['cdn.workers.do/search/titles*', 'cdn.workers.do/search/imdb*'],
    description: 'Search IMDB titles from parquet data',
  },
  'product-lookup': {
    name: 'parquedb_product_lookup',
    routes: ['cdn.workers.do/search/products*'],
    description: 'Product lookup example',
  },
  'category-filter': {
    name: 'parquedb_category_filter',
    routes: ['cdn.workers.do/search/categories*'],
    description: 'Category filter example',
  },
}

interface SnippetConfig {
  name: string
  routes: string[]
  description: string
}

interface CloudflareResponse<T> {
  success: boolean
  errors: Array<{ code: number; message: string }>
  messages: string[]
  result: T
}

interface Snippet {
  snippet_name: string
  created_on: string
  modified_on: string
}

interface SnippetRule {
  id?: string
  description?: string
  enabled: boolean
  expression: string
  snippet_name: string
  last_updated?: string
}

// =============================================================================
// API Helpers
// =============================================================================

function getCredentials(): { apiToken: string; zoneId: string } {
  const apiToken = process.env.CLOUDFLARE_API_TOKEN || process.env.CF_API_TOKEN
  const zoneId = process.env.CF_ZONE_ID

  if (!apiToken) {
    console.error('Error: CLOUDFLARE_API_TOKEN or CF_API_TOKEN environment variable required')
    process.exit(1)
  }

  if (!zoneId) {
    console.error('Error: CF_ZONE_ID environment variable required')
    process.exit(1)
  }

  return { apiToken, zoneId }
}

async function cfApi<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<CloudflareResponse<T>> {
  const { apiToken, zoneId } = getCredentials()

  const url = `https://api.cloudflare.com/client/v4/zones/${zoneId}${endpoint}`

  const response = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${apiToken}`,
      'Content-Type': 'application/json',
      ...options.headers,
    },
  })

  const data = await response.json() as CloudflareResponse<T>

  if (!data.success) {
    console.error('API Error:', data.errors)
    throw new Error(data.errors.map(e => e.message).join(', '))
  }

  return data
}

// =============================================================================
// Snippet Operations
// =============================================================================

/**
 * List all deployed snippets
 */
async function listSnippets(): Promise<void> {
  console.log('Fetching deployed snippets...\n')

  const data = await cfApi<Snippet[]>('/snippets')

  if (data.result.length === 0) {
    console.log('No snippets deployed.')
    return
  }

  console.log('Deployed Snippets:')
  console.log('-'.repeat(60))

  for (const snippet of data.result) {
    console.log(`  ${snippet.snippet_name}`)
    console.log(`    Created: ${snippet.created_on}`)
    console.log(`    Modified: ${snippet.modified_on}`)
    console.log()
  }
}

/**
 * Deploy a single snippet using multipart form data
 */
async function deploySnippet(name: string): Promise<void> {
  const config = SNIPPET_ROUTES[name]
  if (!config) {
    console.error(`Unknown snippet: ${name}`)
    console.log('Available snippets:', Object.keys(SNIPPET_ROUTES).join(', '))
    process.exit(1)
  }

  const snippetPath = path.join(DIST_DIR, name, 'snippet.min.js')
  if (!fs.existsSync(snippetPath)) {
    console.error(`Built snippet not found: ${snippetPath}`)
    console.log('Run "pnpm build" first.')
    process.exit(1)
  }

  const code = fs.readFileSync(snippetPath, 'utf-8')
  const size = code.length

  console.log(`Deploying ${name}...`)
  console.log(`  Name: ${config.name}`)
  console.log(`  Size: ${(size / 1024).toFixed(2)} KB`)
  console.log(`  Routes: ${config.routes.join(', ')}`)

  const { apiToken, zoneId } = getCredentials()

  // Create or update snippet using multipart form data (required by API)
  try {
    const formData = new FormData()

    // Add the JavaScript file as a blob
    const blob = new Blob([code], { type: 'application/javascript' })
    formData.append('files', blob, 'snippet.js')

    // Add metadata as JSON
    formData.append('metadata', JSON.stringify({ main_module: 'snippet.js' }))

    const url = `https://api.cloudflare.com/client/v4/zones/${zoneId}/snippets/${config.name}`
    const response = await fetch(url, {
      method: 'PUT',
      headers: {
        'Authorization': `Bearer ${apiToken}`,
      },
      body: formData,
    })

    const data = await response.json() as CloudflareResponse<unknown>

    if (!response.ok || !data.success) {
      console.error('API Error:', data.errors)
      throw new Error(data.errors?.map(e => e.message).join(', ') || 'Unknown error')
    }

    console.log(`  ✓ Snippet deployed`)
    console.log()
  } catch (error) {
    console.error(`  ✗ Deployment failed:`, error)
    throw error
  }
}

/**
 * Delete a snippet
 */
async function deleteSnippet(name: string): Promise<void> {
  console.log(`Deleting snippet: ${name}...`)

  await cfApi(`/snippets/${name}`, {
    method: 'DELETE',
  })

  console.log(`✓ Snippet deleted`)
}

/**
 * Update all snippet rules at once (zone-level API)
 */
async function updateSnippetRules(deployedSnippets: string[]): Promise<void> {
  console.log('Configuring snippet rules...')

  // Get existing rules first
  let existingRules: SnippetRule[] = []
  try {
    const existing = await cfApi<{ rules: SnippetRule[] }>('/snippets/snippet_rules')
    existingRules = existing.result?.rules || []
  } catch {
    // No existing rules
  }

  // Build new rules for deployed snippets
  const newRules: SnippetRule[] = []
  for (const name of deployedSnippets) {
    const config = SNIPPET_ROUTES[name]
    if (!config) continue

    // Create a rule for each route pattern
    for (const route of config.routes) {
      // Parse route like "cdn.workers.do/search/occupations*"
      const [host, ...pathParts] = route.split('/')
      const pathPattern = '/' + pathParts.join('/').replace('*', '')

      // Build Cloudflare expression for matching this route
      // Use http.host and http.request.uri.path
      const expression = `(http.host eq "${host}" and starts_with(http.request.uri.path, "${pathPattern}"))`

      newRules.push({
        description: config.description,
        enabled: true,
        expression,
        snippet_name: config.name,
      })
    }
  }

  // Keep rules for snippets we didn't deploy (preserve other rules)
  const deployedSnippetNames = new Set(deployedSnippets.map(n => SNIPPET_ROUTES[n]?.name).filter(Boolean))
  const preservedRules = existingRules.filter(r => !deployedSnippetNames.has(r.snippet_name))

  const allRules = [...preservedRules, ...newRules]

  // Update all rules
  await cfApi('/snippets/snippet_rules', {
    method: 'PUT',
    body: JSON.stringify({ rules: allRules }),
  })

  console.log(`  ✓ ${newRules.length} rules configured for ${deployedSnippets.length} snippet(s)`)
}

/**
 * Deploy all snippets
 */
async function deployAll(): Promise<void> {
  console.log('Deploying all snippets...\n')

  const snippets = Object.keys(SNIPPET_ROUTES)
  const deployedSnippets: string[] = []

  for (const name of snippets) {
    const snippetPath = path.join(DIST_DIR, name, 'snippet.min.js')
    if (!fs.existsSync(snippetPath)) {
      console.log(`Skipping ${name} (not built)`)
      continue
    }

    try {
      await deploySnippet(name)
      deployedSnippets.push(name)
    } catch {
      console.error(`Failed to deploy ${name}`)
    }
  }

  // Configure rules for all deployed snippets
  if (deployedSnippets.length > 0) {
    try {
      await updateSnippetRules(deployedSnippets)
    } catch (error) {
      console.error('Failed to configure rules:', error)
    }
  }

  console.log('\nDeployment complete!')
}

// =============================================================================
// Data Upload
// =============================================================================

/**
 * Upload benchmark data to R2 via Workers
 *
 * Note: This requires a Worker endpoint that accepts uploads.
 * For direct R2 uploads, use wrangler CLI.
 */
async function uploadData(): Promise<void> {
  console.log('Data upload via API is not implemented.')
  console.log('Use wrangler CLI to upload parquet files:')
  console.log()
  console.log('  wrangler r2 object put cdn/parquedb-benchmarks/onet/occupation-data.parquet \\')
  console.log('    --file="./data/onet/Occupation Data.parquet" --remote')
  console.log()
  console.log('  wrangler r2 object put cdn/parquedb-benchmarks/onet/unspsc-reference.parquet \\')
  console.log('    --file="./data/onet/UNSPSC Reference.parquet" --remote')
  console.log()
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2)

  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
ParqueDB Snippets Deployment

Usage:
  pnpm deploy                 Deploy all snippets
  pnpm deploy <name>          Deploy specific snippet
  pnpm deploy --list          List deployed snippets
  pnpm deploy --delete <name> Delete a snippet
  pnpm deploy --upload        Upload data to R2 (instructions)

Available snippets:
${Object.keys(SNIPPET_ROUTES).map(n => `  - ${n}`).join('\n')}

Environment variables:
  CLOUDFLARE_API_TOKEN  Cloudflare API token
  CF_ZONE_ID            Target zone ID (workers.do)
`)
    return
  }

  if (args.includes('--list')) {
    await listSnippets()
    return
  }

  if (args.includes('--delete')) {
    const nameIdx = args.indexOf('--delete') + 1
    const name = args[nameIdx]
    if (!name) {
      console.error('Usage: pnpm deploy --delete <name>')
      process.exit(1)
    }
    await deleteSnippet(name)
    return
  }

  if (args.includes('--upload')) {
    await uploadData()
    return
  }

  // Deploy specific snippet or all
  if (args.length > 0 && !args[0].startsWith('-')) {
    await deploySnippet(args[0])
  } else {
    await deployAll()
  }
}

main().catch(error => {
  console.error('Fatal error:', error)
  process.exit(1)
})
