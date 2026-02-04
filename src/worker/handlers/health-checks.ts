/**
 * Deep Health Check Implementations
 *
 * Individual health check implementations for deep health endpoint:
 * - R2 connectivity check
 * - Read path verification
 * - Write path verification
 *
 * Each check returns a standardized result with success, latencyMs, and error.
 */

// =============================================================================
// Types
// =============================================================================

/** Result of a single health check */
export interface HealthCheckResult {
  /** Whether the check passed */
  success: boolean
  /** Time taken to complete the check in milliseconds */
  latencyMs: number
  /** Error message if check failed */
  error?: string | undefined
  /** Additional details about the check */
  details?: Record<string, unknown> | undefined
}

/** Options for health checks */
export interface HealthCheckOptions {
  /** Timeout in milliseconds */
  timeoutMs?: number | undefined
}

/** Deep health check response structure */
export interface DeepHealthResult {
  /** Overall status */
  status: 'healthy' | 'degraded' | 'unhealthy'
  /** Individual check results */
  checks: {
    r2?: HealthCheckResult | undefined
    read?: HealthCheckResult | undefined
    write?: HealthCheckResult | undefined
  }
  /** Latency summary */
  latencyMs: {
    total: number
    r2?: number | undefined
    read?: number | undefined
    write?: number | undefined
  }
  /** Timestamp */
  timestamp: string
}

// =============================================================================
// R2 Connectivity Check
// =============================================================================

/**
 * Check R2 bucket connectivity by listing with limit=1
 *
 * This verifies:
 * - R2 binding is valid
 * - Network connectivity to R2
 * - Basic read permissions
 */
export async function checkR2Connectivity(
  bucket: R2Bucket,
  options: HealthCheckOptions = {}
): Promise<HealthCheckResult> {
  const timeoutMs = options.timeoutMs ?? 5000
  const start = performance.now()

  try {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

    // List with limit=1 is the cheapest R2 operation
    const result = await bucket.list({ limit: 1 })
    clearTimeout(timeoutId)

    const latencyMs = performance.now() - start

    return {
      success: true,
      latencyMs,
      details: {
        objectCount: result.objects.length,
        truncated: result.truncated,
      },
    }
  } catch (error) {
    const latencyMs = performance.now() - start
    const errorMessage = error instanceof Error
      ? error.name === 'AbortError'
        ? `Timeout after ${timeoutMs}ms`
        : error.message
      : 'Unknown error'

    return {
      success: false,
      latencyMs,
      error: `R2 connectivity check failed: ${errorMessage}`,
    }
  }
}

// =============================================================================
// Read Path Check
// =============================================================================

/** Expected content of the probe file */
const PROBE_FILE_CONTENT = '{"probe":true,"version":"1.0"}'
const PROBE_FILE_KEY = '_health/probe.json'

/**
 * Check R2 read path by reading a known probe file
 *
 * This verifies:
 * - R2 read operations work
 * - Data integrity (content matches expected)
 * - CDN/cache path if applicable
 */
export async function checkReadPath(
  bucket: R2Bucket,
  options: HealthCheckOptions = {}
): Promise<HealthCheckResult> {
  const timeoutMs = options.timeoutMs ?? 5000
  const start = performance.now()

  try {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

    const object = await bucket.get(PROBE_FILE_KEY)
    clearTimeout(timeoutId)

    const latencyMs = performance.now() - start

    if (!object) {
      // Probe file doesn't exist - create it
      try {
        await bucket.put(PROBE_FILE_KEY, PROBE_FILE_CONTENT, {
          httpMetadata: { contentType: 'application/json' },
        })
        return {
          success: true,
          latencyMs,
          details: { action: 'created_probe_file' },
        }
      } catch (putError) {
        return {
          success: false,
          latencyMs,
          error: `Probe file not found and failed to create: ${putError instanceof Error ? putError.message : 'Unknown error'}`,
        }
      }
    }

    // Verify content
    const content = await object.text()
    const contentValid = content === PROBE_FILE_CONTENT

    if (!contentValid) {
      return {
        success: false,
        latencyMs,
        error: 'Probe file content mismatch',
        details: {
          expected: PROBE_FILE_CONTENT,
          actual: content.substring(0, 100),
        },
      }
    }

    return {
      success: true,
      latencyMs,
      details: {
        size: object.size,
        etag: object.etag,
        contentValid: true,
      },
    }
  } catch (error) {
    const latencyMs = performance.now() - start
    const errorMessage = error instanceof Error
      ? error.name === 'AbortError'
        ? `Timeout after ${timeoutMs}ms`
        : error.message
      : 'Unknown error'

    return {
      success: false,
      latencyMs,
      error: `Read path check failed: ${errorMessage}`,
    }
  }
}

// =============================================================================
// Write Path Check
// =============================================================================

/**
 * Check R2 write path by writing, reading, and deleting a temp file
 *
 * This verifies:
 * - R2 write operations work
 * - Data can be read back correctly
 * - Delete operations work
 * - Full write path integrity
 */
export async function checkWritePath(
  bucket: R2Bucket,
  options: HealthCheckOptions = {}
): Promise<HealthCheckResult> {
  const timeoutMs = options.timeoutMs ?? 10000
  const start = performance.now()

  // Generate unique key for this check
  const timestamp = Date.now()
  const random = Math.random().toString(36).substring(2, 8)
  const probeKey = `_health/write-probe-${timestamp}-${random}.json`
  const probeContent = JSON.stringify({
    probe: true,
    timestamp,
    random,
    check: 'write_path',
  })

  try {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

    // Step 1: Write
    const writeStart = performance.now()
    await bucket.put(probeKey, probeContent, {
      httpMetadata: { contentType: 'application/json' },
    })
    const writeLatency = performance.now() - writeStart

    // Step 2: Read back
    const readStart = performance.now()
    const object = await bucket.get(probeKey)
    const readLatency = performance.now() - readStart

    if (!object) {
      clearTimeout(timeoutId)
      return {
        success: false,
        latencyMs: performance.now() - start,
        error: 'Write succeeded but read back failed (object not found)',
        details: { writeLatency },
      }
    }

    const readContent = await object.text()
    if (readContent !== probeContent) {
      clearTimeout(timeoutId)
      // Still try to delete
      await bucket.delete(probeKey).catch(() => {})
      return {
        success: false,
        latencyMs: performance.now() - start,
        error: 'Write succeeded but content mismatch on read back',
        details: { writeLatency, readLatency },
      }
    }

    // Step 3: Delete
    const deleteStart = performance.now()
    await bucket.delete(probeKey)
    const deleteLatency = performance.now() - deleteStart

    clearTimeout(timeoutId)
    const latencyMs = performance.now() - start

    return {
      success: true,
      latencyMs,
      details: {
        writeLatency,
        readLatency,
        deleteLatency,
        contentVerified: true,
      },
    }
  } catch (error) {
    const latencyMs = performance.now() - start
    const errorMessage = error instanceof Error
      ? error.name === 'AbortError'
        ? `Timeout after ${timeoutMs}ms`
        : error.message
      : 'Unknown error'

    // Try to clean up the probe file
    try {
      await bucket.delete(probeKey)
    } catch {
      // Ignore cleanup errors
    }

    return {
      success: false,
      latencyMs,
      error: `Write path check failed: ${errorMessage}`,
    }
  }
}

// =============================================================================
// Run All Checks
// =============================================================================

/**
 * Run all deep health checks and aggregate results
 */
export async function runDeepHealthChecks(
  bucket: R2Bucket,
  options: HealthCheckOptions = {}
): Promise<DeepHealthResult> {
  const start = performance.now()

  // Run checks in sequence (to avoid overloading R2)
  const r2Check = await checkR2Connectivity(bucket, options)
  const readCheck = await checkReadPath(bucket, options)
  const writeCheck = await checkWritePath(bucket, options)

  const totalLatency = performance.now() - start

  // Determine overall status
  const allPassed = r2Check.success && readCheck.success && writeCheck.success
  const status: DeepHealthResult['status'] =
    allPassed ? 'healthy' :
    !r2Check.success ? 'unhealthy' : // R2 connectivity is critical
    'degraded'

  return {
    status,
    checks: {
      r2: r2Check,
      read: readCheck,
      write: writeCheck,
    },
    latencyMs: {
      total: totalLatency,
      r2: r2Check.latencyMs,
      read: readCheck.latencyMs,
      write: writeCheck.latencyMs,
    },
    timestamp: new Date().toISOString(),
  }
}
