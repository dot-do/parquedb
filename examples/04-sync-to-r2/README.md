# Sync to R2

ParqueDB supports syncing your local database to Cloudflare R2 (or any compatible storage).

## Run

```bash
npx tsx examples/04-sync-to-r2/index.ts
```

## What it demonstrates

1. **Manifest creation** - Track files and hashes for efficient sync
2. **Push** - Upload local changes to remote
3. **Pull** - Download remote changes to local
4. **Sync** - Bidirectional sync with conflict resolution

## CLI Commands

```bash
# Push local database to R2
parquedb push

# Pull remote changes
parquedb pull

# Bidirectional sync
parquedb sync

# Sync with conflict strategy
parquedb sync --strategy newest
parquedb sync --strategy local
parquedb sync --strategy remote

# Dry run
parquedb sync --dry-run
```

## Programmatic API

```typescript
import { SyncEngine, FsBackend, R2Backend } from 'parquedb'

// Local storage
const local = new FsBackend('.db')

// Remote storage (R2)
const remote = new R2Backend(env.MY_BUCKET)

// Create sync engine
const sync = new SyncEngine({
  local,
  remote,
  databaseId: 'my-db-id',
  name: 'my-database',
  owner: 'username',
  onProgress: (p) => console.log(`${p.phase}: ${p.current}/${p.total}`)
})

// Push to R2
const pushResult = await sync.push()

// Pull from R2
const pullResult = await sync.pull()

// Bidirectional sync
const syncResult = await sync.sync({
  conflictStrategy: 'newest'
})
```

## Setting up R2

1. Create an R2 bucket in Cloudflare Dashboard
2. Generate API tokens with R2 access
3. Configure environment variables:

```bash
export CLOUDFLARE_ACCOUNT_ID=your-account-id
export R2_ACCESS_KEY_ID=your-access-key
export R2_SECRET_ACCESS_KEY=your-secret-key
```

Or use wrangler for local development:

```bash
npx wrangler r2 object put my-bucket/test.txt --file=test.txt
```

## Conflict Strategies

| Strategy | Behavior |
|----------|----------|
| `newest` | Keep the most recently modified version |
| `local` | Always prefer local changes |
| `remote` | Always prefer remote changes |
| `manual` | Report conflicts for manual resolution |

## Use Cases

- **Backup** - Push local database to cloud storage
- **Share** - Sync public datasets for others to pull
- **Collaboration** - Multiple developers syncing to shared R2 bucket
- **Edge deployment** - Push schema/data to R2 for Worker consumption

## Next Steps

- [05-cloudflare-worker](../05-cloudflare-worker/) - Deploy a full worker with R2
