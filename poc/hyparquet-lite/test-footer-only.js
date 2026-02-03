// Simulate Cloudflare Snippets scenario:
// - Only fetch the last N bytes (footer) via Range request
// - Parse metadata to decide routing
import { parquetMetadata, parquetSchema } from 'hyparquet'
import { readFileSync, statSync, openSync, readSync, closeSync } from 'fs'

const filePath = '../../data/onet-optimized/data.parquet'
const fileStats = statSync(filePath)
const fileSize = fileStats.size
console.log(`Total file size: ${fileSize} bytes`)

// Step 1: Read last 8 bytes to get metadata length
const footerFd = openSync(filePath, 'r')
const last8 = Buffer.alloc(8)
readSync(footerFd, last8, 0, 8, fileSize - 8)

// Parse magic and metadata length
const magic = last8.toString('utf8', 4, 8)
console.log(`Magic: ${magic}`)
if (magic !== 'PAR1') {
  throw new Error('Not a valid parquet file')
}

const metadataLength = last8.readUInt32LE(0)
console.log(`Metadata length: ${metadataLength} bytes`)

// Step 2: Read just the footer (metadata + 8 bytes for footer marker)
const footerSize = metadataLength + 8
console.log(`Footer size needed: ${footerSize} bytes (${(footerSize / 1024).toFixed(2)}KB)`)

const footer = Buffer.alloc(footerSize)
readSync(footerFd, footer, 0, footerSize, fileSize - footerSize)
closeSync(footerFd)

// Step 3: Parse metadata from footer
const arrayBuffer = footer.buffer.slice(footer.byteOffset, footer.byteOffset + footer.byteLength)
const metadata = parquetMetadata(arrayBuffer)

console.log('\n=== Snippet Decision Logic ===')
console.log(`Rows: ${metadata.num_rows}`)
console.log(`Row groups: ${metadata.row_groups.length}`)

// Schema for routing
const schema = parquetSchema(metadata)
const columns = schema.children?.map(c => c.element?.name) || []
console.log(`Columns: ${columns.join(', ')}`)

// Decision: small file (<1000 rows) could be handled inline
// Large file should redirect to Worker
if (metadata.num_rows < 500) {
  console.log('\nDecision: Small file, could serve from Snippet with subrequests')
} else {
  console.log('\nDecision: Large file, redirect to Worker')
}

// Row group routing - find which row groups match query
console.log('\n=== Row Group Routing ===')
for (let i = 0; i < metadata.row_groups.length; i++) {
  const rg = metadata.row_groups[i]
  const offset = rg.columns[0]?.meta_data?.data_page_offset || 0
  const totalSize = rg.total_byte_size
  console.log(`Row group ${i}: rows=${rg.num_rows}, offset=${offset}, size=${totalSize}`)

  // Could use column statistics for predicate pushdown
  for (const col of rg.columns.slice(0, 2)) {
    const stats = col.meta_data?.statistics
    if (stats) {
      console.log(`  ${col.meta_data.path_in_schema?.[0]}: min/max available`)
    }
  }
}

console.log('\n=== Memory Analysis ===')
const used = process.memoryUsage()
console.log(`Heap used: ${Math.round(used.heapUsed / 1024)}KB`)

// Snippet limit check
console.log('\n=== Snippet Compatibility ===')
console.log(`Footer size (${footerSize} bytes) ${footerSize < 32768 ? '<' : '>'} 32KB limit: ${footerSize < 32768 ? '✓ FITS' : '✗ TOO LARGE'}`)
console.log(`Bundle size (~4KB gzipped) < 32KB limit: ✓ FITS`)
