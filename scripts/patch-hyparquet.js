#!/usr/bin/env node
/**
 * Patch hyparquet for Parquet VARIANT column support
 *
 * This script applies three patches to hyparquet that are required for
 * reading Parquet files with VARIANT logical type columns:
 *
 * 1. thrift.js: Support non-delta field IDs in Thrift Compact Protocol
 *    (VARIANT is field_16 in LogicalType, which exceeds the delta encoding max of 15)
 *
 * 2. metadata.js: Recognize VARIANT logical type (field_16)
 *
 * 3. convert.js: Skip UTF-8 conversion for BYTE_ARRAY columns inside VARIANT groups
 *    (VARIANT metadata/value sub-columns contain binary data, not text)
 *
 * These patches should be upstreamed to the dot-do/hyparquet fork.
 * Until then, this script is run as a postinstall hook.
 */

import { readFileSync, writeFileSync } from 'fs'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const hyparquetDir = resolve(__dirname, '../node_modules/hyparquet/src')

function patchFile(filename, patches) {
  const filepath = resolve(hyparquetDir, filename)
  let content
  try {
    content = readFileSync(filepath, 'utf8')
  } catch {
    console.log(`  [skip] ${filename} not found`)
    return
  }

  let modified = false
  for (const { check, find, replace } of patches) {
    // Skip if already patched
    if (check && content.includes(check)) {
      continue
    }
    if (content.includes(find)) {
      content = content.replace(find, replace)
      modified = true
    }
  }

  if (modified) {
    writeFileSync(filepath, content, 'utf8')
    console.log(`  [patched] ${filename}`)
  } else {
    console.log(`  [ok] ${filename} (already patched or no match)`)
  }
}

console.log('Patching hyparquet for VARIANT support...')

// Patch 1: thrift.js - Support non-delta field IDs
patchFile('thrift.js', [{
  check: 'Non-delta encoding: field ID follows as zigzag-encoded',
  find: `  } else {
    throw new Error('non-delta field id not supported')
  }`,
  replace: `  } else {
    // Non-delta encoding: field ID follows as zigzag-encoded i16 varint
    fid = readZigZag(reader)
  }`,
}])

// Patch 2: metadata.js - Recognize VARIANT logical type
patchFile('metadata.js', [{
  check: "type: 'VARIANT'",
  find: `  if (logicalType?.field_15) return { type: 'FLOAT16' }
  return logicalType`,
  replace: `  if (logicalType?.field_15) return { type: 'FLOAT16' }
  if (logicalType?.field_16 !== undefined) return { type: 'VARIANT' }
  return logicalType`,
}])

// Patch 3: convert.js - Skip UTF-8 for VARIANT sub-columns
patchFile('convert.js', [{
  check: "VARIANT",
  find: `  const { element, parsers, utf8 = true, schemaPath } = columnDecoder
  const { type, converted_type: ctype, logical_type: ltype } = element`,
  replace: `  const { element, parsers, utf8 = true, schemaPath } = columnDecoder
  const { type, converted_type: ctype, logical_type: ltype } = element

  // Skip UTF-8 conversion for BYTE_ARRAY columns inside a VARIANT group.
  // VARIANT sub-columns (metadata, value) contain binary data, not text.
  if (type === 'BYTE_ARRAY' && schemaPath) {
    for (const node of schemaPath) {
      if (node.element?.logical_type?.type === 'VARIANT') {
        return data // Return raw Uint8Array values
      }
    }
  }`,
}])

console.log('Done.')
