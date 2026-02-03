# Database Sync

ParqueDB provides a complete sync system for sharing databases between local filesystems and remote cloud storage. This enables publishing public datasets, collaborating with teams, and backing up data to the cloud.

## Table of Contents

- [Overview](#overview)
- [Visibility Levels](#visibility-levels)
- [CLI Commands](#cli-commands)
- [Configuration](#configuration)
- [Remote Client](#remote-client)
- [Public API Routes](#public-api-routes)
- [Sync Manifest](#sync-manifest)
- [Architecture](#architecture)

---

## Overview

The sync system allows you to:

- **Push** local databases to remote R2 storage
- **Pull** remote databases to your local filesystem
- **Sync** bidirectionally with conflict resolution
- **Share** public and unlisted databases via HTTP

Key features:

| Feature | Description |
|---------|-------------|
| Visibility control | Public, unlisted, or private access |
| Conflict resolution | Multiple strategies (local-wins, remote-wins, newest) |
| Range requests | Efficient Parquet file reading via HTTP Range headers |
| Manifest tracking | File-level change detection with hashes |
| Authentication | OAuth integration for private databases |

---

## Visibility Levels

ParqueDB supports three visibility levels that control access to databases and collections:

| Visibility | Discoverable | Anonymous Read | Requires Auth |
|------------|--------------|----------------|---------------|
| `public` | Yes | Yes | No |
| `unlisted` | No | Yes (with link) | No |
| `private` | No | No | Yes |

### public

Public databases are:
- Listed in public database directories
- Accessible by anyone without authentication
- Queryable via the public HTTP API
- Ideal for open datasets and documentation

### unlisted

Unlisted databases are:
- Not listed in public directories
- Accessible to anyone who has the direct URL
- Useful for sharing with specific people without full public exposure
- Similar to "unlisted" YouTube videos

### private

Private databases are:
- Not discoverable or listed anywhere
- Require authentication to access
- Only accessible by the owner or authorized users
- Default visibility for new databases

### Visibility Helpers

```typescript
import {
  Visibility,
  DEFAULT_VISIBILITY,
  isValidVisibility,
  allowsAnonymousRead,
  allowsDiscovery,
} from 'parquedb/types/visibility'

// Default is 'private' for security
console.log(DEFAULT_VISIBILITY) // 'private'

// Check if anonymous users can read
allowsAnonymousRead('public')   // true
allowsAnonymousRead('unlisted') // true
allowsAnonymousRead('private')  // false

// Check if database is discoverable
allowsDiscovery('public')   // true
allowsDiscovery('unlisted') // false
allowsDiscovery('private')  // false
```

---

## CLI Commands

### push

Upload your local database to remote storage.

```bash
# Basic push (uses config file settings)
parquedb push

# Specify visibility
parquedb push --visibility public
parquedb push --visibility unlisted
parquedb push --visibility private

# Custom URL slug (for public URL)
parquedb push --slug my-dataset

# Preview what would be uploaded
parquedb push --dry-run

# From a specific directory
parquedb push --directory ./my-data
```

**Options:**

| Option | Description |
|--------|-------------|
| `--visibility <level>` | Set visibility (public, unlisted, private) |
| `--slug <name>` | URL-friendly name for public access |
| `--dry-run` | Preview changes without uploading |
| `--directory <path>` | Source directory (default: current) |

**Output:**

```
Authenticating...
Logged in as username

Pushing to https://api.parque.db...
  Visibility: public
  Slug: username/my-dataset

Database registered: db_abc123

Uploading files...
Uploaded 5 files.

Database pushed successfully!
  URL: https://parque.db/username/my-dataset
```

### pull

Download a remote database to your local filesystem.

```bash
# Pull by owner/slug reference
parquedb pull username/my-dataset

# Pull to specific directory
parquedb pull username/my-dataset --directory ./local-data

# Preview what would be downloaded
parquedb pull username/my-dataset --dry-run
```

**Options:**

| Option | Description |
|--------|-------------|
| `--directory <path>` | Target directory (default: current) |
| `--dry-run` | Preview changes without downloading |

**Output:**

```
Fetching database info from https://api.parque.db...

Found database: my-dataset
  Visibility: public
  Collections: 3

Downloading to ./local-data...

Database pulled successfully!
  Location: ./local-data
```

### sync

Bidirectional sync with conflict resolution.

```bash
# Sync with default strategy (newest wins)
parquedb sync

# Specify conflict strategy
parquedb sync --strategy local-wins
parquedb sync --strategy remote-wins
parquedb sync --strategy newest
parquedb sync --strategy manual

# Check sync status only
parquedb sync --status

# Preview changes without syncing
parquedb sync --dry-run
```

**Conflict Strategies:**

| Strategy | Description |
|----------|-------------|
| `local-wins` | Always use local version when conflicts occur |
| `remote-wins` | Always use remote version when conflicts occur |
| `newest` | Use the most recently modified version (default) |
| `manual` | Require manual resolution for conflicts |

**Output:**

```
Authenticating...
Logged in as username

Checking sync status...
  Strategy: newest

Local manifest:
  Database: my-dataset
  Last synced: 2026-02-03T10:30:00Z
  Files: 12

Syncing...

Sync complete!
```

---

## Configuration

### Database-Level Visibility

Set visibility in your `parquedb.config.ts`:

```typescript
import { defineConfig, defineSchema } from 'parquedb/config'

export default defineConfig({
  storage: { type: 'fs', path: './data' },

  // Database-level visibility
  $visibility: 'public',

  // Optional: Owner for public URL
  $owner: 'username',

  // Optional: URL slug
  $slug: 'my-dataset',

  schema: defineSchema({
    Post: {
      title: 'string!',
      content: 'text',
      status: 'string',
    },
  }),
})
```

### Collection-Level Visibility

You can also set visibility per collection:

```typescript
export default defineConfig({
  storage: { type: 'fs', path: './data' },

  // Default visibility for all collections
  $visibility: 'private',

  schema: defineSchema({
    // Public collection
    Post: {
      $visibility: 'public',
      title: 'string!',
      content: 'text',
    },

    // Private collection (uses default)
    User: {
      email: 'string!',
      name: 'string',
    },

    // Unlisted collection
    Draft: {
      $visibility: 'unlisted',
      content: 'text',
    },
  }),
})
```

### Environment Variables

Configure sync behavior via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PARQUEDB_REMOTE_URL` | `https://api.parque.db` | Remote API URL |

---

## Remote Client

Query public and unlisted databases directly from your application using the remote client.

### Opening a Remote Database

```typescript
import { openRemoteDB } from 'parquedb/client/remote'

// Open a public database
const db = await openRemoteDB('username/my-dataset')

// Query a collection (MongoDB-style API)
const posts = await db.Posts.find({ status: 'published' })
console.log(posts.items)

// Get a single entity
const post = await db.Posts.get('posts/abc123')

// Check if entity exists
const exists = await db.Posts.exists('posts/abc123')

// Count entities
const count = await db.Posts.count({ status: 'draft' })
```

### With Authentication

For private databases, provide an authentication token:

```typescript
const privateDb = await openRemoteDB('username/private-data', {
  token: 'your-auth-token',
})

const secrets = await privateDb.Secrets.find()
```

### Configuration Options

```typescript
interface OpenRemoteDBOptions {
  /** Authentication token (required for private databases) */
  token?: string

  /** Custom base URL (defaults to https://parque.db) */
  baseUrl?: string

  /** Request timeout in milliseconds */
  timeout?: number

  /** Custom headers */
  headers?: Record<string, string>
}
```

### Check Database Existence

```typescript
import { checkRemoteDB } from 'parquedb/client/remote'

const exists = await checkRemoteDB('username/my-dataset')
if (exists) {
  const db = await openRemoteDB('username/my-dataset')
}
```

### List Public Databases

```typescript
import { listPublicDatabases } from 'parquedb/client/remote'

const databases = await listPublicDatabases({
  limit: 20,
  offset: 0,
})

for (const db of databases) {
  console.log(`${db.owner}/${db.slug}: ${db.description}`)
}
```

### Remote Collection API

The remote collection interface is read-only and supports:

```typescript
interface RemoteCollection<T> {
  /** Collection namespace */
  readonly namespace: string

  /** Find entities matching a filter */
  find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>

  /** Find a single entity */
  findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null>

  /** Get entity by ID */
  get(id: string): Promise<Entity<T> | null>

  /** Count entities matching filter */
  count(filter?: Filter): Promise<number>

  /** Check if entity exists */
  exists(id: string): Promise<boolean>
}
```

---

## Public API Routes

The ParqueDB worker exposes HTTP endpoints for accessing public databases.

### List Public Databases

```
GET /api/public
```

Query parameters:
- `limit` - Maximum results (default: 50)
- `offset` - Pagination offset (default: 0)

Response:
```json
{
  "databases": [
    {
      "id": "db_abc123",
      "name": "my-dataset",
      "owner": "username",
      "slug": "my-dataset",
      "visibility": "public",
      "description": "A sample dataset",
      "collectionCount": 3,
      "entityCount": 1000
    }
  ],
  "total": 42,
  "hasMore": true
}
```

### Get Database Metadata

```
GET /api/db/:owner/:slug
```

Response:
```json
{
  "id": "db_abc123",
  "name": "my-dataset",
  "owner": "username",
  "slug": "my-dataset",
  "visibility": "public",
  "description": "A sample dataset",
  "collectionCount": 3,
  "entityCount": 1000,
  "createdAt": "2026-01-15T10:00:00Z"
}
```

### Query Collection

```
GET /api/db/:owner/:slug/:collection
```

Query parameters:
- `filter` - JSON filter object
- `limit` - Maximum results (default: 100)
- `offset` - Pagination offset (default: 0)

Response:
```json
{
  "items": [...],
  "total": 100,
  "hasMore": false,
  "collection": "posts",
  "database": "username/my-dataset"
}
```

### Raw File Access

```
GET /db/:owner/:slug/:path
```

Direct file access with Range request support for efficient Parquet reading.

**Headers:**
- `Range: bytes=0-1023` - Request specific byte range
- `Range: bytes=-8` - Request last 8 bytes (Parquet footer)

**Response Headers:**
- `Content-Type` - File content type
- `Accept-Ranges: bytes` - Indicates range request support
- `Content-Range` - For partial responses (206)
- `ETag` - For caching

**Example - Read Parquet Footer:**

```bash
# Get last 8 bytes to read footer length
curl -H "Range: bytes=-8" \
  https://parque.db/db/username/my-dataset/data/posts/data.parquet

# Response: 206 Partial Content
# Content-Range: bytes 1000-1007/1008
```

### CORS Support

All public routes include CORS headers:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, HEAD, OPTIONS
Access-Control-Allow-Headers: Content-Type, Range, Authorization
Access-Control-Expose-Headers: Content-Range, Content-Length, ETag, Accept-Ranges
```

---

## Sync Manifest

The sync system tracks file state using manifests stored at `_meta/manifest.json` in both local and remote storage.

### Manifest Structure

```typescript
interface SyncManifest {
  /** Schema version for manifest format */
  version: 1

  /** Unique database identifier */
  databaseId: string

  /** Human-readable database name */
  name: string

  /** Owner username (for public URL) */
  owner?: string

  /** URL-friendly slug (for public URL) */
  slug?: string

  /** Database visibility level */
  visibility: Visibility

  /** ISO timestamp of last sync */
  lastSyncedAt: string

  /** Sync source identifier */
  syncedFrom?: string

  /** All tracked files */
  files: Record<string, SyncFileEntry>

  /** Optional database metadata */
  metadata?: Record<string, unknown>
}
```

### File Entry Structure

```typescript
interface SyncFileEntry {
  /** File path relative to database root */
  path: string

  /** File size in bytes */
  size: number

  /** MD5 or SHA-256 hash of file contents */
  hash: string

  /** Hash algorithm used */
  hashAlgorithm: 'md5' | 'sha256'

  /** ISO timestamp of last modification */
  modifiedAt: string

  /** ETag from storage (for conditional updates) */
  etag?: string

  /** Content type */
  contentType?: string
}
```

### Conflict Detection

When syncing, the engine compares local and remote manifests to detect:

1. **New files** - Exist locally but not remotely (upload) or vice versa (download)
2. **Changed files** - Same path but different hash
3. **Conflicts** - Both local and remote changed since last sync

### Conflict Resolution

```typescript
interface SyncConflict {
  /** File path */
  path: string

  /** Local file entry */
  local: SyncFileEntry

  /** Remote file entry */
  remote: SyncFileEntry

  /** Suggested resolution based on timestamps */
  suggestedResolution: 'keep-local' | 'keep-remote' | 'manual'
}
```

Resolution strategies:

| Strategy | Behavior |
|----------|----------|
| `local-wins` | Always upload local version |
| `remote-wins` | Always download remote version |
| `newest` | Use most recently modified (by timestamp) |
| `manual` | Report conflict, require user decision |

### Diff Result

```typescript
interface SyncDiff {
  /** Files to upload (local only) */
  toUpload: SyncFileEntry[]

  /** Files to download (remote only) */
  toDownload: SyncFileEntry[]

  /** Files that differ */
  conflicts: SyncConflict[]

  /** Files that are identical */
  unchanged: string[]

  /** Files deleted locally */
  deletedLocally: string[]

  /** Files deleted remotely */
  deletedRemotely: string[]
}
```

---

## Architecture

### Component Diagram

```
+----------------+     +----------------+     +----------------+
|   CLI (push/   |---->|  SyncEngine    |---->| RemoteBackend  |
|   pull/sync)   |     |                |     | (HTTP client)  |
+----------------+     +----------------+     +----------------+
                              |                      |
                              v                      v
                       +----------------+     +----------------+
                       |  FsBackend     |     |  R2/S3 Storage |
                       |  (local files) |     |  (cloud)       |
                       +----------------+     +----------------+
```

### SyncEngine

The `SyncEngine` class orchestrates sync operations:

```typescript
import { SyncEngine, createSyncEngine } from 'parquedb/sync/engine'

const engine = createSyncEngine({
  local: localBackend,
  remote: remoteBackend,
  databaseId: 'db_abc123',
  name: 'my-dataset',
  owner: 'username',
  onProgress: (progress) => {
    console.log(`${progress.operation}: ${progress.currentFile}`)
  },
})

// Check status
const { diff, isSynced } = await engine.status()

// Push local changes to remote
const pushResult = await engine.push({ dryRun: false })

// Pull remote changes to local
const pullResult = await engine.pull({ conflictStrategy: 'remote-wins' })

// Bidirectional sync
const syncResult = await engine.sync({ conflictStrategy: 'newest' })
```

### RemoteBackend

The `RemoteBackend` provides read-only HTTP access to remote databases:

```typescript
import { RemoteBackend, createRemoteBackend } from 'parquedb/storage/RemoteBackend'

// Create backend for a public database
const backend = createRemoteBackend('username/my-dataset')

// Read entire file
const data = await backend.read('data/posts/data.parquet')

// Range read (efficient for Parquet)
const footer = await backend.readRange('data/posts/data.parquet', -8, -1)

// Check if file exists
const exists = await backend.exists('data/users/data.parquet')

// Get file metadata
const stat = await backend.stat('data/posts/data.parquet')
```

### Storage Layout

```
database-root/
  _meta/
    manifest.json      # Sync manifest
  data/
    posts/
      data.parquet     # Post entities
    users/
      data.parquet     # User entities
  rels/
    forward/
      posts.parquet    # Forward relationships
    reverse/
      users.parquet    # Reverse relationships
  events/
    current.parquet    # Event log
```

### Public URL Structure

For public and unlisted databases:

```
https://parque.db/
  ├── db/
  │   └── :owner/
  │       └── :slug/
  │           └── *    # Raw file access
  └── api/
      ├── public       # List public databases
      └── db/
          └── :owner/
              └── :slug/
                  └── :collection  # Query collection
```

### Security

- **Authentication**: OAuth integration via `oauth.do/node`
- **Authorization**: Visibility checks on every request
- **Read-only remote**: `RemoteBackend` only supports read operations
- **Write via sync**: Modifications require push/sync through authenticated CLI

---

## Next Steps

- [Getting Started](./getting-started.md) - Basic ParqueDB usage
- [Configuration](./deployment/configuration.md) - Complete configuration reference
- [Cloudflare Workers](./deployment/cloudflare-workers.md) - Deploy to Cloudflare
- [Storage Backends](./backends.md) - Available storage options
