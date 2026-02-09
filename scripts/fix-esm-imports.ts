#!/usr/bin/env npx tsx
/**
 * Post-build script: Add .js extensions to relative imports in dist/
 *
 * TypeScript with moduleResolution: "bundler" emits extensionless imports
 * (e.g. `from './db'`) which break Node.js ESM resolution. This script
 * walks dist/ and rewrites them to include explicit .js extensions.
 *
 * Resolution rules:
 *   from './foo'  → from './foo.js'       (if dist/foo.js exists)
 *   from './foo'  → from './foo/index.js'  (if dist/foo/index.js exists)
 *   from './foo.js' → unchanged            (already has extension)
 *
 * Also handles:
 *   - export ... from '...'
 *   - Parent-relative imports (../../)
 *   - Both single and double quotes
 */

import { readdir, readFile, writeFile, stat } from 'fs/promises'
import { join, dirname, resolve } from 'path'
import { existsSync } from 'fs'

const ROOT = import.meta.dirname
const DIST = join(ROOT, '..', 'dist')

// Match: from './path' or from "../path" or export ... from './path'
const IMPORT_RE = /(from\s+['"])(\.\.?\/[^'"]*?)(['"])/g
// Match: import('./path') or import("../path") — dynamic imports
const DYNAMIC_IMPORT_RE = /(import\s*\(\s*['"])(\.\.?\/[^'"]*?)(['"]\s*\))/g

let filesProcessed = 0
let importsFixed = 0
let alreadyCorrect = 0
let unresolved: string[] = []

async function getAllJsFiles(dir: string): Promise<string[]> {
  const files: string[] = []
  const entries = await readdir(dir, { withFileTypes: true })
  for (const entry of entries) {
    const fullPath = join(dir, entry.name)
    if (entry.isDirectory()) {
      files.push(...await getAllJsFiles(fullPath))
    } else if (entry.name.endsWith('.js')) {
      files.push(fullPath)
    }
  }
  return files
}

function resolveImport(importPath: string, fromFile: string): string | null {
  const dir = dirname(fromFile)

  // Already has a file extension
  if (/\.\w+$/.test(importPath) && !importPath.endsWith('/')) {
    return importPath
  }

  // Try direct .js
  const asJs = resolve(dir, importPath + '.js')
  if (existsSync(asJs)) {
    return importPath + '.js'
  }

  // Try directory/index.js
  const asIndex = resolve(dir, importPath, 'index.js')
  if (existsSync(asIndex)) {
    return importPath + '/index.js'
  }

  return null
}

function fixImportMatch(filePath: string, modified: { value: boolean }) {
  return (match: string, prefix: string, importPath: string, suffix: string) => {
    // Skip if already has extension
    if (/\.\w+$/.test(importPath) && !importPath.endsWith('/')) {
      alreadyCorrect++
      return match
    }

    const resolved = resolveImport(importPath, filePath)
    if (resolved && resolved !== importPath) {
      importsFixed++
      modified.value = true
      return `${prefix}${resolved}${suffix}`
    }

    // Could not resolve
    unresolved.push(`${filePath}: ${importPath}`)
    return match
  }
}

async function fixFile(filePath: string): Promise<boolean> {
  const content = await readFile(filePath, 'utf-8')
  const modified = { value: false }
  const fixer = fixImportMatch(filePath, modified)

  let newContent = content.replace(IMPORT_RE, fixer)
  newContent = newContent.replace(DYNAMIC_IMPORT_RE, fixer)

  if (modified.value) {
    await writeFile(filePath, newContent)
    return true
  }
  return false
}

/**
 * Fix the main barrel export to not eagerly load optional integrations
 * (MCP, Express, Fastify, Payload) that have peer deps which may not be installed.
 */
async function fixBarrelExport() {
  const indexPath = join(DIST, 'index.js')
  let content = await readFile(indexPath, 'utf-8')

  // Replace the integrations barrel import with direct iceberg imports
  const barrelPattern = /export \{[^}]*\} from '\.\/integrations\/index\.js';/s
  if (barrelPattern.test(content)) {
    content = content.replace(barrelPattern, [
      "export { IcebergMetadataManager, IcebergStorageAdapter, createIcebergMetadataManager, enableIcebergMetadata, parqueDBTypeToIceberg, icebergTypeToParqueDB, } from './integrations/iceberg.js';",
      "export { NativeIcebergMetadataManager, NativeIcebergStorageAdapter, createNativeIcebergManager, enableNativeIcebergMetadata, } from './integrations/iceberg-native.js';",
    ].join('\n'))
    await writeFile(indexPath, content)
    console.log('Fixed barrel export: integrations/index.js → direct iceberg imports')
  }
}

async function main() {
  console.log('Fixing ESM imports in dist/...\n')

  const files = await getAllJsFiles(DIST)
  filesProcessed = files.length

  let filesChanged = 0
  for (const file of files) {
    if (await fixFile(file)) {
      filesChanged++
    }
  }

  // Fix barrel export for optional integrations
  await fixBarrelExport()

  console.log(`Files scanned:    ${filesProcessed}`)
  console.log(`Files changed:    ${filesChanged}`)
  console.log(`Imports fixed:    ${importsFixed}`)
  console.log(`Already correct:  ${alreadyCorrect}`)

  if (unresolved.length > 0) {
    console.log(`\nUnresolved (${unresolved.length}):`)
    for (const u of unresolved) {
      console.log(`  ${u}`)
    }
  }

  console.log('\nDone!')
}

main().catch(console.error)
