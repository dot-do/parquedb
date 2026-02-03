/**
 * Sync Commands
 *
 * Push, pull, and sync commands for syncing databases
 * between local filesystem and remote R2 storage.
 */

import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import type { Visibility } from '../../types/visibility'
import { isValidVisibility, DEFAULT_VISIBILITY } from '../../types/visibility'
import type { ConflictStrategy, SyncFileEntry, SyncManifest } from '../../sync/manifest'
import { createSyncClient } from '../../sync/client'
import { diffManifests, resolveConflicts } from '../../sync/manifest'

// =============================================================================
// Push Command
// =============================================================================

/**
 * Push command - upload local database to remote R2
 *
 * Usage: parquedb push [--visibility <public|unlisted|private>] [--slug <name>]
 */
export async function pushCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Authenticate
    const { ensureLoggedIn, getUser } = await import('oauth.do/node')

    print('Authenticating...')
    const { token } = await ensureLoggedIn({
      openBrowser: true,
      print: (msg: string) => print(msg),
    })

    const authResult = await getUser(token)
    const user = authResult.user
    if (!user) {
      printError('Failed to get user info')
      return 1
    }

    const username = (user as Record<string, unknown>).username as string | undefined
    const owner = username ?? user.id

    // Parse options
    const visibilityArg = findOption(parsed.args, '--visibility')
    const slugArg = findOption(parsed.args, '--slug')
    const dryRun = parsed.args.includes('--dry-run')

    let visibility: Visibility = DEFAULT_VISIBILITY
    if (visibilityArg) {
      if (!isValidVisibility(visibilityArg)) {
        printError(`Invalid visibility: ${visibilityArg}. Valid values: public, unlisted, private`)
        return 1
      }
      visibility = visibilityArg
    }

    // Validate slug
    if (slugArg && !isValidSlug(slugArg)) {
      printError('Invalid slug: must be 3-64 lowercase alphanumeric characters with hyphens')
      return 1
    }

    // Load config and create backends
    const { loadConfig } = await import('../../config/loader')
    const config = await loadConfig()
    if (!config) {
      printError('No parquedb.config.ts found. Run "parquedb init" first.')
      return 1
    }

    // Get visibility from config if not specified
    if (!visibilityArg && config.visibility) {
      visibility = config.visibility
    }

    // Create local storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const localBackend = new FsBackend(parsed.options.directory)

    // Create sync client
    const remoteUrl = process.env.PARQUEDB_REMOTE_URL ?? 'https://api.parque.db'
    const syncClient = createSyncClient({
      baseUrl: remoteUrl,
      token,
    })

    print('')
    print(`Pushing to ${remoteUrl}...`)
    print(`  Visibility: ${visibility}`)
    if (slugArg) {
      print(`  Slug: ${owner}/${slugArg}`)
    }
    print('')

    // Register database with remote service
    const registerResult = await syncClient.registerDatabase({
      name: config.defaultNamespace ?? 'default',
      visibility,
      slug: slugArg,
      owner,
    })

    if (!registerResult.success) {
      printError(`Failed to register database: ${registerResult.error}`)
      return 1
    }

    const databaseId = registerResult.databaseId!
    print(`Database registered: ${databaseId}`)
    print('')

    // Build local manifest
    print('Scanning local files...')
    const localManifest = await buildLocalManifest(localBackend, databaseId, config.defaultNamespace ?? 'default', owner, visibility)
    const localFiles = Object.values(localManifest.files)

    if (localFiles.length === 0) {
      print('No files to upload.')
      return 0
    }

    print(`Found ${localFiles.length} files to sync.`)

    // Get remote manifest
    const remoteManifest = await syncClient.getManifest(databaseId)

    // Compare manifests
    const diff = diffManifests(localManifest, remoteManifest)

    const toUpload = [...diff.toUpload]
    const { upload: conflictUploads } = resolveConflicts(diff.conflicts, 'local-wins')
    toUpload.push(...conflictUploads)

    if (toUpload.length === 0) {
      print('Already up to date.')
      return 0
    }

    if (dryRun) {
      print('[Dry run] Would upload the following files:')
      for (const file of toUpload) {
        print(`  - ${file.path} (${formatBytes(file.size)})`)
      }
      return 0
    }

    // Get presigned upload URLs
    print('')
    print(`Uploading ${toUpload.length} files...`)

    const uploadUrls = await syncClient.getUploadUrls(
      databaseId,
      toUpload.map(f => ({
        path: f.path,
        size: f.size,
        contentType: f.contentType,
      }))
    )

    // Upload each file
    let uploaded = 0
    let totalBytes = 0
    const errors: Array<{ path: string; error: string }> = []

    for (const urlInfo of uploadUrls) {
      const file = toUpload.find(f => f.path === urlInfo.path)
      if (!file) continue

      try {
        const data = await localBackend.read(file.path)
        await syncClient.uploadFile(urlInfo, data)
        uploaded++
        totalBytes += data.length
        printProgress(uploaded, toUpload.length, file.path)
      } catch (error) {
        errors.push({
          path: file.path,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    }

    // Update remote manifest
    localManifest.lastSyncedAt = new Date().toISOString()
    localManifest.syncedFrom = 'local'
    await syncClient.updateManifest(databaseId, localManifest)

    // Save local manifest
    await localBackend.write(
      '_meta/manifest.json',
      new TextEncoder().encode(JSON.stringify(localManifest, null, 2))
    )

    print('')
    if (errors.length > 0) {
      printError(`Uploaded ${uploaded} files with ${errors.length} errors:`)
      for (const err of errors) {
        print(`  - ${err.path}: ${err.error}`)
      }
    } else {
      printSuccess(`Uploaded ${uploaded} files (${formatBytes(totalBytes)})`)
    }

    if (visibility === 'public' || visibility === 'unlisted') {
      const publicUrl = `https://parque.db/${owner}/${slugArg ?? databaseId}`
      print(`  URL: ${publicUrl}`)
    }

    return errors.length > 0 ? 1 : 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Push failed: ${message}`)
    return 1
  }
}

// =============================================================================
// Pull Command
// =============================================================================

/**
 * Pull command - download remote database to local
 *
 * Usage: parquedb pull <owner/database> [--directory <path>]
 */
export async function pullCommand(parsed: ParsedArgs): Promise<number> {
  try {
    const dbRef = parsed.args[0]
    if (!dbRef) {
      printError('Usage: parquedb pull <owner/database> [--directory <path>]')
      return 1
    }

    // Parse owner/database reference
    const parts = dbRef.split('/')
    if (parts.length !== 2) {
      printError('Invalid database reference. Use format: owner/database')
      return 1
    }
    const [owner, slug] = parts

    const dryRun = parsed.args.includes('--dry-run')
    const remoteUrl = process.env.PARQUEDB_REMOTE_URL ?? 'https://api.parque.db'

    print(`Fetching database info from ${remoteUrl}...`)

    // Check if auth is needed - try unauthenticated first
    let token: string | undefined
    let syncClient = createSyncClient({
      baseUrl: remoteUrl,
      token: '',
    })

    // Lookup database
    let dbInfo = await syncClient.lookupDatabase(owner!, slug!)

    if (!dbInfo) {
      // Try with authentication
      print('Database not found or requires authentication...')
      const { ensureLoggedIn } = await import('oauth.do/node')
      const authResult = await ensureLoggedIn({
        openBrowser: true,
        print: (msg: string) => print(msg),
      })
      token = authResult.token

      syncClient = createSyncClient({
        baseUrl: remoteUrl,
        token,
      })

      dbInfo = await syncClient.lookupDatabase(owner!, slug!)
      if (!dbInfo) {
        printError(`Database not found: ${dbRef}`)
        return 1
      }
    }

    print('')
    print(`Found database: ${dbInfo.name}`)
    print(`  Visibility: ${dbInfo.visibility}`)
    print(`  Collections: ${dbInfo.collectionCount ?? 'unknown'}`)
    print('')

    const targetDir = parsed.options.directory

    // Create local storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const localBackend = new FsBackend(targetDir)

    // Ensure directories exist
    await localBackend.mkdir('data')
    await localBackend.mkdir('_meta')

    // Get remote manifest
    const remoteManifest = await syncClient.getManifest(dbInfo.id)
    if (!remoteManifest) {
      printError('Remote manifest not found. Database may be empty.')
      return 1
    }

    // Load local manifest if exists
    let localManifest: SyncManifest | null = null
    try {
      const data = await localBackend.read('_meta/manifest.json')
      localManifest = JSON.parse(new TextDecoder().decode(data)) as SyncManifest
    } catch {
      // No local manifest, will download everything
    }

    // Compare manifests
    const diff = diffManifests(localManifest, remoteManifest)
    const toDownload = [...diff.toDownload]
    const { download: conflictDownloads } = resolveConflicts(diff.conflicts, 'remote-wins')
    toDownload.push(...conflictDownloads)

    if (toDownload.length === 0) {
      print('Already up to date.')
      return 0
    }

    if (dryRun) {
      print(`[Dry run] Would download to: ${targetDir}`)
      print('Files to download:')
      for (const file of toDownload) {
        print(`  - ${file.path} (${formatBytes(file.size)})`)
      }
      return 0
    }

    print(`Downloading ${toDownload.length} files to ${targetDir}...`)

    // Get presigned download URLs
    const downloadUrls = await syncClient.getDownloadUrls(
      dbInfo.id,
      toDownload.map(f => f.path)
    )

    // Download each file
    let downloaded = 0
    let totalBytes = 0
    const errors: Array<{ path: string; error: string }> = []

    for (const urlInfo of downloadUrls) {
      const file = toDownload.find(f => f.path === urlInfo.path)
      if (!file) continue

      try {
        const data = await syncClient.downloadFile(urlInfo)
        await localBackend.write(file.path, data)
        downloaded++
        totalBytes += data.length
        printProgress(downloaded, toDownload.length, file.path)
      } catch (error) {
        errors.push({
          path: file.path,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    }

    // Save local manifest
    const updatedManifest: SyncManifest = {
      ...remoteManifest,
      lastSyncedAt: new Date().toISOString(),
      syncedFrom: `remote:${remoteUrl}`,
    }

    await localBackend.write(
      '_meta/manifest.json',
      new TextEncoder().encode(JSON.stringify(updatedManifest, null, 2))
    )

    print('')
    if (errors.length > 0) {
      printError(`Downloaded ${downloaded} files with ${errors.length} errors:`)
      for (const err of errors) {
        print(`  - ${err.path}: ${err.error}`)
      }
    } else {
      printSuccess(`Downloaded ${downloaded} files (${formatBytes(totalBytes)})`)
      print(`  Location: ${targetDir}`)
    }

    return errors.length > 0 ? 1 : 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Pull failed: ${message}`)
    return 1
  }
}

// =============================================================================
// Sync Command
// =============================================================================

/**
 * Sync command - bidirectional sync with conflict resolution
 *
 * Usage: parquedb sync [--strategy <local-wins|remote-wins|newest>]
 */
export async function syncCommand(parsed: ParsedArgs): Promise<number> {
  try {
    // Authenticate
    const { ensureLoggedIn, getUser } = await import('oauth.do/node')

    print('Authenticating...')
    const { token } = await ensureLoggedIn({
      openBrowser: true,
      print: (msg: string) => print(msg),
    })

    const authResult = await getUser(token)
    const user = authResult.user
    if (!user) {
      printError('Failed to get user info')
      return 1
    }

    // Parse options
    const strategyArg = findOption(parsed.args, '--strategy')
    const dryRun = parsed.args.includes('--dry-run')
    const statusOnly = parsed.args.includes('--status')

    let strategy: ConflictStrategy = 'newest'
    if (strategyArg) {
      if (!['local-wins', 'remote-wins', 'newest', 'manual'].includes(strategyArg)) {
        printError(`Invalid strategy: ${strategyArg}. Valid values: local-wins, remote-wins, newest, manual`)
        return 1
      }
      strategy = strategyArg as ConflictStrategy
    }

    // Load config
    const { loadConfig } = await import('../../config/loader')
    const config = await loadConfig()
    if (!config) {
      printError('No parquedb.config.ts found. Run "parquedb init" first.')
      return 1
    }

    // Create local storage backend
    const { FsBackend } = await import('../../storage/FsBackend')
    const localBackend = new FsBackend(parsed.options.directory)

    // Check for existing manifest
    let localManifest: SyncManifest
    try {
      const data = await localBackend.read('_meta/manifest.json')
      localManifest = JSON.parse(new TextDecoder().decode(data)) as SyncManifest
    } catch {
      printError('No manifest found. Run "parquedb push" first to establish a sync.')
      return 1
    }

    const databaseId = localManifest.databaseId
    if (!databaseId) {
      printError('Invalid manifest: missing databaseId. Run "parquedb push" to re-register.')
      return 1
    }

    // Create sync client
    const remoteUrl = process.env.PARQUEDB_REMOTE_URL ?? 'https://api.parque.db'
    const syncClient = createSyncClient({
      baseUrl: remoteUrl,
      token,
    })

    print('')
    print('Checking sync status...')

    // Build current local state
    const currentLocalManifest = await buildLocalManifest(
      localBackend,
      databaseId,
      localManifest.name,
      localManifest.owner,
      localManifest.visibility
    )

    // Get remote manifest
    const remoteManifest = await syncClient.getManifest(databaseId)

    // Compare
    const diff = diffManifests(currentLocalManifest, remoteManifest)

    const isSynced =
      diff.toUpload.length === 0 &&
      diff.toDownload.length === 0 &&
      diff.conflicts.length === 0

    if (statusOnly) {
      print('')
      print('Local manifest:')
      print(`  Database: ${localManifest.name}`)
      print(`  Last synced: ${localManifest.lastSyncedAt}`)
      print(`  Local files: ${Object.keys(currentLocalManifest.files).length}`)
      print('')
      print('Status:')
      if (isSynced) {
        printSuccess('  Up to date')
      } else {
        print(`  Files to upload: ${diff.toUpload.length}`)
        print(`  Files to download: ${diff.toDownload.length}`)
        print(`  Conflicts: ${diff.conflicts.length}`)
      }
      return 0
    }

    if (isSynced) {
      printSuccess('Already up to date.')
      return 0
    }

    print(`  Strategy: ${strategy}`)
    print(`  Files to upload: ${diff.toUpload.length}`)
    print(`  Files to download: ${diff.toDownload.length}`)
    print(`  Conflicts: ${diff.conflicts.length}`)
    print('')

    // Resolve conflicts
    const resolved = resolveConflicts(diff.conflicts, strategy)
    const toUpload = [...diff.toUpload, ...resolved.upload]
    const toDownload = [...diff.toDownload, ...resolved.download]

    if (resolved.manual.length > 0) {
      printError(`${resolved.manual.length} conflicts require manual resolution:`)
      for (const conflict of resolved.manual) {
        print(`  - ${conflict.path}`)
      }
      print('')
      print('Use --strategy=local-wins or --strategy=remote-wins to auto-resolve.')
      return 1
    }

    if (dryRun) {
      print('[Dry run] Would sync the following changes:')
      if (toUpload.length > 0) {
        print('  Upload:')
        for (const file of toUpload) {
          print(`    - ${file.path} (${formatBytes(file.size)})`)
        }
      }
      if (toDownload.length > 0) {
        print('  Download:')
        for (const file of toDownload) {
          print(`    - ${file.path} (${formatBytes(file.size)})`)
        }
      }
      return 0
    }

    print('Syncing...')
    const errors: Array<{ path: string; error: string }> = []

    // Upload files
    if (toUpload.length > 0) {
      print(`Uploading ${toUpload.length} files...`)
      const uploadUrls = await syncClient.getUploadUrls(
        databaseId,
        toUpload.map(f => ({
          path: f.path,
          size: f.size,
          contentType: f.contentType,
        }))
      )

      for (const urlInfo of uploadUrls) {
        const file = toUpload.find(f => f.path === urlInfo.path)
        if (!file) continue

        try {
          const data = await localBackend.read(file.path)
          await syncClient.uploadFile(urlInfo, data)
        } catch (error) {
          errors.push({
            path: file.path,
            error: error instanceof Error ? error.message : String(error),
          })
        }
      }
    }

    // Download files
    if (toDownload.length > 0) {
      print(`Downloading ${toDownload.length} files...`)
      const downloadUrls = await syncClient.getDownloadUrls(
        databaseId,
        toDownload.map(f => f.path)
      )

      for (const urlInfo of downloadUrls) {
        const file = toDownload.find(f => f.path === urlInfo.path)
        if (!file) continue

        try {
          const data = await syncClient.downloadFile(urlInfo)
          await localBackend.write(file.path, data)
        } catch (error) {
          errors.push({
            path: file.path,
            error: error instanceof Error ? error.message : String(error),
          })
        }
      }
    }

    // Update manifests
    const updatedManifest = await buildLocalManifest(
      localBackend,
      databaseId,
      currentLocalManifest.name,
      currentLocalManifest.owner,
      currentLocalManifest.visibility
    )
    updatedManifest.lastSyncedAt = new Date().toISOString()
    updatedManifest.syncedFrom = 'bidirectional'

    await syncClient.updateManifest(databaseId, updatedManifest)
    await localBackend.write(
      '_meta/manifest.json',
      new TextEncoder().encode(JSON.stringify(updatedManifest, null, 2))
    )

    print('')
    if (errors.length > 0) {
      printError(`Sync completed with ${errors.length} errors:`)
      for (const err of errors) {
        print(`  - ${err.path}: ${err.error}`)
      }
      return 1
    }

    printSuccess(`Sync complete! Uploaded ${toUpload.length}, downloaded ${toDownload.length} files.`)
    return 0
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    printError(`Sync failed: ${message}`)
    return 1
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Find an option value in args
 */
function findOption(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag)
  if (index >= 0 && index < args.length - 1) {
    return args[index + 1]
  }
  return undefined
}

/**
 * Validate slug format
 */
function isValidSlug(slug: string): boolean {
  return /^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$/.test(slug) || /^[a-z0-9]{1,3}$/.test(slug)
}

/**
 * Format bytes for display
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
}

/**
 * Print progress indicator
 */
function printProgress(current: number, total: number, file: string): void {
  const percent = Math.round((current / total) * 100)
  const shortFile = file.length > 40 ? '...' + file.slice(-37) : file
  print(`  [${percent}%] ${shortFile}`)
}

/**
 * Build a manifest from local files
 */
async function buildLocalManifest(
  backend: { list: (prefix: string) => Promise<{ files: string[] }>; stat: (path: string) => Promise<{ size: number; mtime: Date } | null>; read: (path: string) => Promise<Uint8Array> },
  databaseId: string,
  name: string,
  owner: string | undefined,
  visibility: Visibility
): Promise<SyncManifest> {
  const files: Record<string, SyncFileEntry> = {}

  const IGNORED_PATHS = [
    '_meta/manifest.json',
    '.git',
    '.DS_Store',
    'node_modules',
  ]

  // List all files
  const result = await backend.list('')

  for (const filePath of result.files) {
    // Skip ignored paths
    if (IGNORED_PATHS.some(p => filePath.startsWith(p))) {
      continue
    }

    // Get file info
    const stat = await backend.stat(filePath)
    if (!stat) continue

    // Calculate hash
    const hash = await hashFile(filePath, backend)

    files[filePath] = {
      path: filePath,
      size: stat.size,
      hash,
      hashAlgorithm: 'sha256',
      modifiedAt: stat.mtime.toISOString(),
      contentType: guessContentType(filePath),
    }
  }

  return {
    version: 1,
    databaseId,
    name,
    owner,
    visibility,
    lastSyncedAt: new Date().toISOString(),
    files,
  }
}

/**
 * Calculate file hash
 */
async function hashFile(
  path: string,
  storage: { read: (path: string) => Promise<Uint8Array> }
): Promise<string> {
  const data = await storage.read(path)

  // Use crypto.subtle
  if (typeof crypto === 'undefined' || !crypto.subtle) {
    throw new Error('crypto.subtle is not available')
  }

  // Create a fresh ArrayBuffer to avoid SharedArrayBuffer issues
  const buffer = new ArrayBuffer(data.length)
  const view = new Uint8Array(buffer)
  view.set(data)
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer)
  const hashArray = Array.from(new Uint8Array(hashBuffer))
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('')
}

/**
 * Guess content type from file extension
 */
function guessContentType(path: string): string {
  const ext = path.split('.').pop()?.toLowerCase()
  const types: Record<string, string> = {
    parquet: 'application/vnd.apache.parquet',
    json: 'application/json',
    jsonl: 'application/x-ndjson',
    csv: 'text/csv',
    txt: 'text/plain',
  }
  return types[ext ?? ''] ?? 'application/octet-stream'
}
