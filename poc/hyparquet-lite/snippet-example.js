/**
 * Cloudflare Snippet Example: Parquet Query Router
 *
 * This snippet demonstrates using hyparquet metadata-only imports
 * to route parquet queries at the edge.
 *
 * Constraints:
 * - 32KB total package size (our bundle: ~4KB gzipped)
 * - 5ms execution time
 * - 2MB memory (we use <6KB for metadata parsing)
 * - 2-5 subrequests depending on plan
 *
 * Flow:
 * 1. Intercept /query/* requests
 * 2. Fetch parquet footer via Range request (1 subrequest)
 * 3. Parse metadata to determine routing
 * 4. Either serve directly or redirect to Worker
 */

// In production, these would be bundled from hyparquet
// import { parquetMetadata, parquetSchema } from 'hyparquet'

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)

    // Only handle /query/* paths
    if (!url.pathname.startsWith('/query/')) {
      return fetch(request)
    }

    // Parse query parameters
    const table = url.searchParams.get('table')
    const query = url.searchParams.get('q')

    if (!table) {
      return new Response('Missing table parameter', { status: 400 })
    }

    // Construct parquet file URL
    const parquetUrl = `${env.R2_PUBLIC_URL}/data/${table}/data.parquet`

    try {
      // Step 1: Fetch last 8 bytes to get metadata length
      const footerSizeResp = await fetch(parquetUrl, {
        headers: { Range: 'bytes=-8' }
      })
      const footerSizeBytes = await footerSizeResp.arrayBuffer()

      // Parse footer size and validate magic
      const view = new DataView(footerSizeBytes)
      const metadataLength = view.getUint32(0, true)
      const magic = new TextDecoder().decode(footerSizeBytes.slice(4))

      if (magic !== 'PAR1') {
        return new Response('Invalid parquet file', { status: 500 })
      }

      // Step 2: Fetch full footer if small enough
      const footerSize = metadataLength + 8
      if (footerSize > 30000) {
        // Footer too large for snippet, redirect to worker
        return Response.redirect(`${env.WORKER_URL}/query?${url.searchParams}`, 307)
      }

      const footerResp = await fetch(parquetUrl, {
        headers: { Range: `bytes=-${footerSize}` }
      })
      const footerBuffer = await footerResp.arrayBuffer()

      // Step 3: Parse metadata (uses hyparquet)
      // const metadata = parquetMetadata(footerBuffer)
      // const schema = parquetSchema(metadata)

      // For this example, simulate the metadata parsing result
      const metadata = {
        num_rows: 1170,
        row_groups: [{ num_rows: 1000 }, { num_rows: 170 }]
      }

      // Step 4: Routing decision
      const decision = {
        table,
        rows: metadata.num_rows,
        rowGroups: metadata.row_groups.length,
        footerSize
      }

      // Small tables (<10k rows) - could serve from edge cache
      if (metadata.num_rows < 10000 && !query) {
        // Return cached list or redirect to cached endpoint
        return Response.redirect(`${env.EDGE_CACHE_URL}/${table}/list.json`, 302)
      }

      // Large tables or complex queries - redirect to worker
      const workerUrl = new URL(`${env.WORKER_URL}/query`)
      workerUrl.searchParams.set('table', table)
      if (query) workerUrl.searchParams.set('q', query)

      // Pass routing hints to worker
      workerUrl.searchParams.set('_rows', metadata.num_rows.toString())
      workerUrl.searchParams.set('_rg', metadata.row_groups.length.toString())

      return Response.redirect(workerUrl.toString(), 307)

    } catch (error) {
      // On error, fall through to worker
      return Response.redirect(`${env.WORKER_URL}/query?${url.searchParams}`, 307)
    }
  }
}
