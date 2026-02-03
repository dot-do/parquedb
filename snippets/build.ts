/**
 * Build Script for ParqueDB Cloudflare Snippets
 *
 * Uses esbuild to bundle snippets with minimal size.
 * Reports bundle sizes and warns if over 1MB (Snippets limit).
 *
 * Usage:
 *   pnpm build           # Build all snippets
 *   pnpm build --watch   # Watch mode
 */

import * as esbuild from 'esbuild'
import * as fs from 'node:fs'
import * as path from 'node:path'

// =============================================================================
// Configuration
// =============================================================================

const EXAMPLES_DIR = './examples'
const DIST_DIR = './dist'
const SIZE_WARN_KB = 500 // Warn at 500KB
const SIZE_ERROR_KB = 1024 // Error at 1MB

// =============================================================================
// Build Functions
// =============================================================================

interface BuildResult {
  name: string
  inputPath: string
  outputPath: string
  size: number
  minifiedSize: number
  gzipSize: number
}

/**
 * Find all snippet entry points
 */
function findSnippets(): { name: string; path: string }[] {
  const snippets: { name: string; path: string }[] = []

  if (!fs.existsSync(EXAMPLES_DIR)) {
    console.error(`Examples directory not found: ${EXAMPLES_DIR}`)
    return snippets
  }

  const dirs = fs.readdirSync(EXAMPLES_DIR, { withFileTypes: true })

  for (const dir of dirs) {
    if (!dir.isDirectory()) continue

    const snippetPath = path.join(EXAMPLES_DIR, dir.name, 'snippet.ts')
    if (fs.existsSync(snippetPath)) {
      snippets.push({
        name: dir.name,
        path: snippetPath,
      })
    }
  }

  return snippets
}

/**
 * Build a single snippet
 */
async function buildSnippet(
  name: string,
  inputPath: string
): Promise<BuildResult> {
  const outputDir = path.join(DIST_DIR, name)
  const outputPath = path.join(outputDir, 'snippet.js')
  const outputMinPath = path.join(outputDir, 'snippet.min.js')

  // Ensure output directory exists
  fs.mkdirSync(outputDir, { recursive: true })

  // Build unminified (for debugging)
  await esbuild.build({
    entryPoints: [inputPath],
    outfile: outputPath,
    bundle: true,
    format: 'esm',
    target: 'es2022',
    platform: 'browser',
    minify: false,
    sourcemap: true,
    treeShaking: true,
  })

  // Build minified (for production)
  const minResult = await esbuild.build({
    entryPoints: [inputPath],
    outfile: outputMinPath,
    bundle: true,
    format: 'esm',
    target: 'es2022',
    platform: 'browser',
    minify: true,
    sourcemap: false,
    treeShaking: true,
    metafile: true,
  })

  // Get file sizes
  const size = fs.statSync(outputPath).size
  const minifiedSize = fs.statSync(outputMinPath).size

  // Estimate gzip size (rough approximation)
  const gzipSize = Math.round(minifiedSize * 0.3) // ~70% compression typical

  return {
    name,
    inputPath,
    outputPath: outputMinPath,
    size,
    minifiedSize,
    gzipSize,
  }
}

/**
 * Build the shared library separately (for analysis)
 */
async function buildLib(): Promise<void> {
  const libOutputDir = path.join(DIST_DIR, '_lib')
  fs.mkdirSync(libOutputDir, { recursive: true })

  // Build parquet-tiny
  await esbuild.build({
    entryPoints: ['./lib/parquet-tiny.ts'],
    outfile: path.join(libOutputDir, 'parquet-tiny.min.js'),
    bundle: true,
    format: 'esm',
    target: 'es2022',
    platform: 'browser',
    minify: true,
    treeShaking: true,
  })

  // Build filter
  await esbuild.build({
    entryPoints: ['./lib/filter.ts'],
    outfile: path.join(libOutputDir, 'filter.min.js'),
    bundle: true,
    format: 'esm',
    target: 'es2022',
    platform: 'browser',
    minify: true,
    treeShaking: true,
  })

  // Report lib sizes
  const parquetSize = fs.statSync(
    path.join(libOutputDir, 'parquet-tiny.min.js')
  ).size
  const filterSize = fs.statSync(path.join(libOutputDir, 'filter.min.js')).size

  console.log('\nLibrary sizes (minified):')
  console.log(`  parquet-tiny: ${formatSize(parquetSize)}`)
  console.log(`  filter:       ${formatSize(filterSize)}`)
}

/**
 * Format byte size as human readable
 */
function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

/**
 * Print size report with warnings
 */
function printReport(results: BuildResult[]): void {
  console.log('\nBuild Results:')
  console.log('─'.repeat(70))
  console.log(
    'Snippet'.padEnd(25) +
      'Size'.padStart(12) +
      'Minified'.padStart(12) +
      '~Gzip'.padStart(12) +
      '  Status'
  )
  console.log('─'.repeat(70))

  let hasWarnings = false
  let hasErrors = false

  for (const result of results) {
    const sizeKb = result.minifiedSize / 1024
    let status = '✓'

    if (sizeKb > SIZE_ERROR_KB) {
      status = '✗ ERROR: Over 1MB limit!'
      hasErrors = true
    } else if (sizeKb > SIZE_WARN_KB) {
      status = '⚠ Warning: Getting large'
      hasWarnings = true
    }

    console.log(
      result.name.padEnd(25) +
        formatSize(result.size).padStart(12) +
        formatSize(result.minifiedSize).padStart(12) +
        formatSize(result.gzipSize).padStart(12) +
        `  ${status}`
    )
  }

  console.log('─'.repeat(70))

  if (hasErrors) {
    console.log('\n❌ BUILD FAILED: Some snippets exceed the 1MB size limit.')
    console.log('   Cloudflare Snippets have a 1MB bundle size limit.')
    console.log('   Consider splitting the snippet or reducing dependencies.\n')
    process.exitCode = 1
  } else if (hasWarnings) {
    console.log('\n⚠️  WARNING: Some snippets are approaching the size limit.')
    console.log('   Consider optimizing bundle size.\n')
  } else {
    console.log('\n✅ All snippets built successfully!\n')
  }
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  const watchMode = args.includes('--watch')

  console.log('Building ParqueDB Cloudflare Snippets...\n')

  // Clean dist directory
  if (fs.existsSync(DIST_DIR)) {
    fs.rmSync(DIST_DIR, { recursive: true })
  }
  fs.mkdirSync(DIST_DIR, { recursive: true })

  // Find all snippets
  const snippets = findSnippets()

  if (snippets.length === 0) {
    console.log('No snippets found in', EXAMPLES_DIR)
    return
  }

  console.log(`Found ${snippets.length} snippet(s):`)
  for (const snippet of snippets) {
    console.log(`  - ${snippet.name}`)
  }

  // Build all snippets
  const results: BuildResult[] = []

  for (const snippet of snippets) {
    try {
      const result = await buildSnippet(snippet.name, snippet.path)
      results.push(result)
    } catch (error) {
      console.error(`\nError building ${snippet.name}:`, error)
      process.exitCode = 1
      return
    }
  }

  // Build shared library (for size analysis)
  await buildLib()

  // Print report
  printReport(results)

  // Watch mode
  if (watchMode) {
    console.log('Watching for changes... (Ctrl+C to stop)\n')

    const watchDirs = [
      './lib',
      ...snippets.map((s) => path.dirname(s.path)),
    ]

    for (const dir of watchDirs) {
      fs.watch(dir, { recursive: true }, async (eventType, filename) => {
        if (!filename?.endsWith('.ts')) return

        console.log(`\nFile changed: ${filename}`)
        console.log('Rebuilding...\n')

        try {
          results.length = 0
          for (const snippet of snippets) {
            const result = await buildSnippet(snippet.name, snippet.path)
            results.push(result)
          }
          printReport(results)
        } catch (error) {
          console.error('Build error:', error)
        }
      })
    }
  }
}

main().catch(console.error)
