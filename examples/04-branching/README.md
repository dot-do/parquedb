# Database Branching

ParqueDB supports git-style branching for version control of your database.

## Run

```bash
npx tsx examples/04-branching/index.ts
```

## What it demonstrates

1. **Branch listing** - See all branches
2. **Branch creation** - Create feature branches
3. **Branch switching** - Checkout different branches
4. **Status checking** - Detect uncommitted changes

## CLI Commands

ParqueDB provides a CLI for branch operations:

```bash
# List all branches
parquedb branch

# Create a new branch
parquedb branch feature/my-feature

# Switch to a branch
parquedb checkout feature/my-feature

# Create and switch in one command
parquedb checkout -b feature/my-feature

# Merge a branch
parquedb merge feature/my-feature --strategy newest

# Delete a branch
parquedb branch -d feature/my-feature
```

## Programmatic API

```typescript
import { BranchManager, FsBackend } from 'parquedb'

const storage = new FsBackend('.db')
const branches = new BranchManager({ storage })

// List branches
const all = await branches.list()

// Create branch
await branches.create('feature/new-schema')

// Switch branch (with state reconstruction)
await branches.checkout('feature/new-schema')

// Check for uncommitted changes
const status = await branches.hasUncommittedChanges()

// Delete branch
await branches.delete('feature/new-schema')
```

## Merge Strategies

When merging branches with conflicts, you can choose a strategy:

| Strategy | Behavior |
|----------|----------|
| `newest` | Keep the most recently modified version |
| `oldest` | Keep the original version |
| `local` | Always prefer local changes |
| `remote` | Always prefer remote changes |
| `manual` | Stop and wait for manual resolution |

## Use Cases

- **Feature development** - Develop new features without affecting production data
- **Testing migrations** - Test schema changes on a branch before merging
- **Data experimentation** - Try different data transformations safely
- **Rollback** - Easily revert to previous database states

## Next Steps

- [05-sync-to-r2](../05-sync-to-r2/) - Sync branches to cloud storage
