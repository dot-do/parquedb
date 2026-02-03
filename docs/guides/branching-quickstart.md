# Database Branching Quickstart

Database branching lets you create isolated copies of your database state, make changes safely, and merge them back - just like git branches for code. Use it to test schema migrations, develop features against realistic data, and run CI/CD pipelines with preview environments that automatically clean up when pull requests close.

---

## Table of Contents

- [Key Concepts](#key-concepts)
- [Prerequisites](#prerequisites)
- [Quick Start (5 minutes)](#quick-start-5-minutes)
- [GitHub Actions Setup](#github-actions-setup)
- [Conflict Resolution](#conflict-resolution)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Key Concepts

### What is Database Branching?

ParqueDB implements git-style version control for your database. Each **commit** captures a complete snapshot of your database state, and **branches** let you diverge from the main history to work in isolation.

```
main:     A---B---C---D  (production data)
               \
feature:        E---F    (your changes)
```

When you're ready, you **merge** your branch back, combining changes from both histories.

### Core Components

| Component | Description |
|-----------|-------------|
| **Commit** | Immutable snapshot of database state with a unique hash |
| **Branch** | Named pointer to a commit (e.g., `main`, `feature/users`) |
| **HEAD** | Points to your current branch or commit |
| **Merge** | Combines changes from two branches |

### How Branching Works

1. **Commits are content-addressed**: Each commit contains hashes of all data files, creating an immutable history chain
2. **Branches are lightweight**: A branch is just a pointer to a commit hash stored in `_meta/refs/heads/{branch-name}`
3. **HEAD tracks your position**: Stored in `_meta/HEAD`, points to either a branch name or a specific commit (detached HEAD)
4. **State reconstruction**: When you checkout a branch, ParqueDB reconstructs the database state from that commit's snapshot

### Storage Model

Branching metadata is stored alongside your data:

```
your-database/
  _meta/
    HEAD                    # Current branch (e.g., "refs/heads/main")
    refs/
      heads/
        main                # Commit hash for main branch
        feature/users       # Commit hash for feature branch
    commits/
      abc123.json           # Commit object with state snapshot
      def456.json           # Another commit
  data/                     # Your actual data files
    users/data.parquet
    posts/data.parquet
```

---

## Prerequisites

- ParqueDB CLI installed (`npm install -g parquedb`)
- Existing database with at least one commit (run `parquedb init` if starting fresh)
- Git repository (optional, but recommended for CI integration)

Verify your setup:

```bash
parquedb --version
parquedb log --limit 1
```

---

## Quick Start (5 minutes)

### 1. Create a Branch

```bash
# Create a new branch from current HEAD
parquedb branch feature/add-users

# Or create from a specific commit/branch
parquedb branch feature/add-users main
```

### 2. Switch to Your Branch

```bash
# Switch to the new branch
parquedb checkout feature/add-users

# Or create and switch in one command
parquedb checkout -b feature/add-users
```

### 3. Make Changes

```bash
# Your database operations now target the branch
parquedb create users --data '{"name": "Alice", "email": "alice@example.com"}'
parquedb update users/user-001 --set '{"status": "active"}'

# Commit your changes
parquedb commit -m "Add initial user data"
```

### 4. Preview the Diff

```bash
# Compare your branch against main
parquedb diff main

# Get detailed statistics
parquedb diff main --stat

# Output as JSON for scripting
parquedb diff main --json
```

### 5. Merge Back

```bash
# Switch back to main
parquedb checkout main

# Merge your feature branch
parquedb merge feature/add-users

# Clean up the branch
parquedb branch -d feature/add-users
```

---

## GitHub Actions Setup

### Step 1: Add Workflow File

Create `.github/workflows/parquedb-preview.yml`:

```yaml
name: ParqueDB Preview

on:
  pull_request:
    types: [opened, synchronize, reopened, closed]

env:
  PARQUEDB_REMOTE_URL: ${{ secrets.PARQUEDB_REMOTE_URL }}
  PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

jobs:
  preview:
    if: github.event.action != 'closed'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install ParqueDB CLI
        run: npm install -g parquedb

      - name: Create preview branch
        run: parquedb ci preview create --pr ${{ github.event.pull_request.number }}

      - name: Check for merge conflicts
        run: |
          parquedb ci check-merge \
            --base ${{ github.base_ref }} \
            --head ${{ github.head_ref }}

      - name: Generate diff summary
        run: |
          parquedb ci diff \
            --base ${{ github.base_ref }} \
            --head ${{ github.head_ref }} \
            --summary

  cleanup:
    if: github.event.action == 'closed'
    runs-on: ubuntu-latest
    steps:
      - name: Install ParqueDB CLI
        run: npm install -g parquedb

      - name: Delete preview branch
        run: parquedb ci preview delete --pr ${{ github.event.pull_request.number }}
        env:
          PARQUEDB_REMOTE_URL: ${{ secrets.PARQUEDB_REMOTE_URL }}
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}
```

### Step 2: Configure Secrets

Add these secrets in your repository settings (Settings > Secrets and variables > Actions):

| Secret | Description |
|--------|-------------|
| `PARQUEDB_REMOTE_URL` | Your ParqueDB remote URL (e.g., `https://api.parque.db`) |
| `PARQUEDB_TOKEN` | Authentication token for push/pull operations |

### Step 3: Test with a PR

1. Create a branch with database changes
2. Open a pull request
3. Watch the workflow create a preview branch and check for conflicts
4. Review the diff summary in the PR checks
5. Merge or close the PR to trigger cleanup

---

## Conflict Resolution

### Detecting Conflicts

Conflicts occur when both branches modify the same entity. Check for conflicts before merging:

```bash
# Dry-run merge to detect conflicts
parquedb merge feature/add-users --dry-run

# Check merge status in CI
parquedb ci check-merge --base main --head feature/add-users
```

### Resolution Strategies

| Strategy | Command | When to Use |
|----------|---------|-------------|
| **ours** | `--strategy ours` | Keep your branch's version |
| **theirs** | `--strategy theirs` | Accept incoming branch's version |
| **newest** | `--strategy newest` | Use most recently updated value |
| **manual** | `--strategy manual` | Review each conflict individually |

```bash
# Auto-resolve all conflicts using newest timestamp
parquedb merge feature/add-users --strategy newest

# Or resolve manually
parquedb merge feature/add-users --strategy manual
```

### Using `parquedb resolve`

When using manual resolution:

```bash
# View current conflicts
parquedb conflicts

# Resolve a specific entity
parquedb resolve users/user-123 --ours
parquedb resolve posts/post-456 --theirs
parquedb resolve comments/comment-789 --newest

# Resolve all remaining conflicts with one strategy
parquedb resolve --all --ours

# Continue the merge after resolving
parquedb merge --continue
```

### Aborting a Merge

If you need to cancel:

```bash
parquedb merge --abort
```

---

## Best Practices

### Branch Naming Conventions

Use descriptive prefixes to organize branches:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `feature/` | New functionality | `feature/user-profiles` |
| `fix/` | Bug fixes | `fix/duplicate-entries` |
| `migration/` | Schema changes | `migration/add-email-index` |
| `experiment/` | Exploratory work | `experiment/new-schema` |
| `pr-{n}` | CI preview branches | `pr-123` (auto-generated) |

### When to Branch vs Direct Commits

**Use branches when:**
- Testing schema migrations before applying to production
- Developing features that modify shared data
- Running CI/CD pipelines that need isolated data
- Collaborating with others on data changes

**Direct commits are fine when:**
- Making small, additive-only changes
- Working alone on a development database
- Changes are easily reversible

### Cleaning Up Old Branches

Regularly clean up merged or stale branches:

```bash
# List all branches
parquedb branch

# Delete a merged branch
parquedb branch -d feature/completed

# Force delete an unmerged branch (use with caution)
parquedb branch -d experiment/abandoned --force
```

---

## Troubleshooting

### Branch not found

```
Error: Branch not found: feature/xyz
```

**Solution:** Check the branch name spelling, or create it:

```bash
parquedb branch              # List existing branches
parquedb checkout -b feature/xyz  # Create and switch
```

### Cannot delete current branch

```
Error: Cannot delete current branch: main
```

**Solution:** Switch to a different branch first:

```bash
parquedb checkout other-branch
parquedb branch -d main
```

### Merge conflicts detected

```
Error: Merge has conflicts
Found 3 conflicts
```

**Solution:** Resolve conflicts manually or with a strategy:

```bash
# View conflicts
parquedb conflicts

# Resolve all with a strategy
parquedb resolve --all --newest

# Or resolve individually
parquedb resolve users/user-123 --theirs

# Continue merge
parquedb merge --continue
```

### HEAD does not point to any commit

```
Error: Cannot create branch: HEAD does not point to any commit
```

**Solution:** Your database has no commits yet. Create an initial commit:

```bash
parquedb commit -m "Initial commit"
parquedb branch feature/new
```

### CI preview branch already exists

```
Updating existing preview branch: pr-123
```

This is normal - ParqueDB updates the preview branch on each push to the PR.

### No CI environment detected

```
Error: No CI environment detected
```

**Solution:** The `ci` commands require running in a supported CI environment (GitHub Actions, GitLab CI, or CircleCI). For local testing:

```bash
# Use explicit flags instead of auto-detection
parquedb diff main feature/xyz
parquedb merge feature/xyz --dry-run
```

---

## Next Steps

- [Sync Guide](../SYNC.md) - Push/pull databases to remote storage
- [Schema Evolution](../schema-evolution.md) - Safely migrate schemas
- [Deployment](../deployment/cloudflare-workers.md) - Deploy to production
