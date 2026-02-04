/**
 * ParqueDB Sync to R2 Example
 *
 * Shows how to sync a local database to Cloudflare R2:
 * - Push local changes to R2
 * - Pull remote changes from R2
 * - Bidirectional sync with conflict resolution
 *
 * Run: npx tsx examples/04-sync-to-r2/index.ts
 *
 * Note: This example demonstrates the API. For actual R2 sync,
 * you need Cloudflare credentials and an R2 bucket.
 */
import { DB, FsBackend, SyncEngine, createManifest, diffManifests } from '../../src'
import { rm, mkdir } from 'fs/promises'

async function main() {
  // Clean start
  await rm('.db', { recursive: true, force: true })
  await rm('.db-remote', { recursive: true, force: true })
  await mkdir('.db-remote', { recursive: true })

  // Create local storage
  const localStorage = new FsBackend('.db')
  const db = DB({
    User: {
      email: 'string!#',
      name: 'string'
    }
  }, { storage: localStorage })

  console.log('Created local database at .db/')

  // Create some data
  await db.User.create({
    $type: 'User',
    name: 'Alice',
    email: 'alice@example.com'
  })
  await db.User.create({
    $type: 'User',
    name: 'Bob',
    email: 'bob@example.com'
  })
  console.log('Added 2 users')

  // Create manifest for sync
  // -------------------------
  // A manifest tracks all files and their hashes for efficient sync
  const manifest = await createManifest(localStorage, 'private')
  console.log(`\nManifest created with ${manifest.files.length} files`)
  for (const file of manifest.files) {
    console.log(`  ${file.path} (${file.size} bytes)`)
  }

  // Simulate remote storage (use FsBackend for demo)
  // In production, use: new R2Backend(env.MY_BUCKET)
  const remoteStorage = new FsBackend('.db-remote')

  // Create a SyncEngine
  // -------------------
  const sync = new SyncEngine({
    local: localStorage,
    remote: remoteStorage,
    databaseId: 'my-database-id',
    name: 'my-database',
    owner: 'my-username',
    onProgress: (progress) => {
      console.log(`Sync progress: ${progress.phase} - ${progress.current}/${progress.total}`)
    }
  })

  // Push to remote
  // --------------
  // CLI: parquedb push
  console.log('\n--- Pushing to remote ---')
  const pushResult = await sync.push()
  console.log(`Push complete: ${pushResult.uploaded.length} files uploaded`)
  if (pushResult.errors.length > 0) {
    console.log(`Errors: ${pushResult.errors.map(e => e.message).join(', ')}`)
  }

  // Check remote manifest
  const remoteManifest = await createManifest(remoteStorage, 'private')
  console.log(`Remote now has ${remoteManifest.files.length} files`)

  // Diff manifests (useful for dry-run)
  // -----------------------------------
  const diff = diffManifests(manifest, remoteManifest)
  console.log(`\nDiff: ${diff.toUpload.length} to upload, ${diff.toDownload.length} to download, ${diff.conflicts.length} conflicts`)

  // Pull from remote
  // ----------------
  // CLI: parquedb pull
  console.log('\n--- Pulling from remote ---')
  const pullResult = await sync.pull()
  console.log(`Pull complete: ${pullResult.downloaded.length} files downloaded`)

  // Bidirectional sync
  // ------------------
  // CLI: parquedb sync
  console.log('\n--- Bidirectional sync ---')
  const syncResult = await sync.sync({
    conflictStrategy: 'newest'  // Use most recent version on conflicts
  })
  console.log(`Sync complete:`)
  console.log(`  Uploaded: ${syncResult.uploaded.length}`)
  console.log(`  Downloaded: ${syncResult.downloaded.length}`)
  console.log(`  Conflicts resolved: ${syncResult.conflictsResolved.length}`)

  // Clean up
  db.dispose()

  console.log('\n--- Sync CLI Commands ---')
  console.log(`
  # Push local database to R2
  parquedb push

  # Pull remote changes
  parquedb pull

  # Bidirectional sync (push + pull with conflict resolution)
  parquedb sync

  # Sync with specific conflict strategy
  parquedb sync --strategy newest   # Use most recent version
  parquedb sync --strategy local    # Prefer local changes
  parquedb sync --strategy remote   # Prefer remote changes

  # Dry run (show what would be synced)
  parquedb sync --dry-run
  `)

  console.log('Done!')
}

main().catch(console.error)
