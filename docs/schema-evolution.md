# Schema Evolution & Type Generation

ParqueDB tracks schema changes over time, enabling strongly-typed time travel queries and safe schema migrations.

## Overview

Every commit includes a schema snapshot, allowing you to:
- Generate TypeScript types for any point in history
- Compare schemas between versions
- Detect breaking changes automatically
- Get migration hints for schema updates
- Query with compile-time type safety at any commit

## Quick Start

### 1. Define Your Schema

```typescript
// parquedb.config.ts
import { defineConfig } from 'parquedb/config'

export default defineConfig({
  storage: { type: 'fs', path: './data' },
  schema: {
    User: {
      email: 'string!#',      // required + indexed
      name: 'string',
      age: 'int?',            // optional
      verified: 'boolean',
      tags: 'string[]',       // array
      profile: '-> Profile'   // relationship
    },
    Post: {
      title: 'string!',
      content: 'text',
      author: '-> User',
      comments: '<- Comment.post[]'  // reverse relationship
    }
  }
})
```

### 2. Generate Types

```bash
# Generate types from current schema
parquedb types generate

# Generate from specific version
parquedb types generate --at v1.0.0

# Custom output path
parquedb types generate -o src/db.d.ts
```

This creates a `.d.ts` file with:

```typescript
// types/db.d.ts
export interface UserEntity extends Entity {
  $type: 'User'
  email: string
  name?: string
  age?: number
  verified?: boolean
  tags?: string[]
  profile?: EntityRef<ProfileEntity>
}

export interface UserInput {
  email?: string
  name?: string
  age?: number
  verified?: boolean
  tags?: string[]
  profile?: EntityRef<ProfileEntity>
}

export interface UserCollection {
  create(input: UserInput): Promise<UserEntity>
  get(id: string, options?: GetOptions): Promise<UserEntity | null>
  find(filter?: Filter<UserEntity>, options?: FindOptions): Promise<UserEntity[]>
  update(id: string, update: UpdateOperators<UserEntity> | Partial<UserInput>): Promise<UserEntity>
  // ... more methods
}

export interface Database {
  User: UserCollection
  Post: PostCollection
}
```

### 3. Use Typed Database

```typescript
import type { UserEntity, Database } from './types/db'
import { DB } from 'parquedb'
import config from './parquedb.config'

const db = DB(config.schema) as Database

// Fully typed!
const user: UserEntity = await db.User.create({
  email: 'alice@example.com',
  name: 'Alice'
})

const users: UserEntity[] = await db.User.find({
  verified: true,
  age: { $gt: 21 }
})
```

## Schema Management

### View Current Schema

```bash
# Show current schema
parquedb schema show

# Show schema at specific version
parquedb schema show --at v1.0.0

# JSON output
parquedb schema show --json > schema.json
```

Example output:

```
Schema: current
Hash: abc123...
Captured: 2024-02-03T10:00:00Z

Collections (2):

  User
    Hash: def456...
    Version: 1
    Fields (3):
      - email: string! (required, indexed)
      - name: string
      - age: int? (optional)

  Post
    Hash: ghi789...
    Version: 1
    Fields (2):
      - title: string! (required)
      - author: -> User (rel:outbound:User)
```

### Compare Schemas

```bash
# Compare current schema to main branch
parquedb schema diff main

# Compare two versions
parquedb schema diff v1.0.0 v2.0.0

# Show only breaking changes
parquedb schema diff main --breaking-only
```

Example output:

```
Schema diff: v1.0.0..v2.0.0

⚠️  2 breaking changes, - 1 field, + 1 field

Breaking Changes:

  ⚠️  Removed field: User.age
  Severity: high
  Impact: Field 'User.age' will be removed. Queries referencing this field will fail.

  Migration hint:
    Consider:
      1. Update all queries to remove references to 'age'
      2. If data should be preserved, migrate to a new field name
      3. Add a migration script to transform existing data

  ⚠️  Changed type: User.verified from string to boolean
  Severity: critical
  Impact: Field 'User.verified' type changed from string to boolean. Existing data may not be compatible.

  Migration hint:
    Required actions:
      1. Write migration script to convert existing values
      2. Test conversion on backup data first
      3. Update application code to handle new type

All Changes:
  ⚠️  Removed field: User.age
  ⚠️  Changed type: User.verified from string to boolean
  ✓  Added field: User.role

Migration Hints:
  📋 Recommended workflow:
    1. Review all breaking changes above
    2. Create a backup: parquedb export --all backup/
    3. Write and test migration scripts
    4. Update application code to handle changes
    5. Deploy schema changes to staging first
    6. Run migration scripts
    7. Verify application behavior
    8. Deploy to production

  💡 Generate updated types:
    parquedb types generate
```

### Check Schema Compatibility

```bash
# Check if current schema has breaking changes
parquedb schema check

# Check staged changes
parquedb schema check --staged
```

Example output (compatible):

```
Schema compatibility check

✓ Schema changes are compatible (no breaking changes)

Changes:
  ✓ Added field: User.role
  ✓ Added index: User.email
```

Example output (incompatible):

```
Schema compatibility check

⚠️  Schema changes include breaking changes!

⚠️  Removed field: User.age
   Severity: high
   Impact: Field 'User.age' will be removed. Queries referencing this field will fail.

   Migration hint:
     Consider:
       1. Update all queries to remove references to 'age'
       2. If data should be preserved, migrate to a new field name

💡 Generate updated types:
   parquedb types generate
```

## Time Travel Queries with Types

Generate types for historical versions:

```bash
# Generate types for v1.0.0
parquedb types generate --at v1.0.0 -o types/db-v1.d.ts

# Generate types for current version
parquedb types generate -o types/db-v2.d.ts
```

Query with historical types:

```typescript
import type { UserEntity as UserV1 } from './types/db-v1'
import type { UserEntity as UserV2 } from './types/db-v2'
import { DB } from 'parquedb'

// Query v1.0.0
const dbV1 = DB({ /* v1 schema */ })
const oldUsers: UserV1[] = await dbV1.User.find({ age: { $gt: 21 } })

// Query v2.0.0
const dbV2 = DB({ /* v2 schema */ })
const newUsers: UserV2[] = await dbV2.User.find({ verified: true })
```

## Schema Evolution Best Practices

### 1. Non-Breaking Changes

Safe changes that don't require migration:

- **Add optional field**: `newField: 'string?'`
- **Add index**: Change `'string'` to `'string#'`
- **Add new collection**: Create new collection
- **Make required field optional**: Change `'string!'` to `'string?'`

### 2. Breaking Changes

Require careful migration:

- **Remove field**: Requires updating all queries
- **Change field type**: Requires data conversion
- **Make optional field required**: Requires populating existing records
- **Remove collection**: Requires data backup/migration

### 3. Migration Workflow

For breaking changes:

```bash
# 1. Compare schemas
parquedb schema diff main feature/new-schema

# 2. Backup data
parquedb export --all backup/

# 3. Write migration script
cat > migrate.ts << 'EOF'
import { DB } from 'parquedb'

async function migrate() {
  const db = DB(oldSchema)

  // Convert string 'verified' to boolean
  const users = await db.User.find({})
  for (const user of users) {
    await db.User.update(user.$id, {
      verified: user.verified === 'true'
    })
  }
}

migrate()
EOF

# 4. Test migration on staging
node migrate.ts

# 5. Update schema
git checkout feature/new-schema

# 6. Generate new types
parquedb types generate

# 7. Update application code
# ... update to use new types

# 8. Deploy to production
git push origin main
```

### 4. Versioned Types

Maintain types for multiple versions:

```typescript
// types/v1.d.ts - Historical
export interface UserV1 {
  email: string
  age: number
}

// types/v2.d.ts - Current
export interface UserV2 {
  email: string
  verified: boolean
}

// Migration helper
function migrateUser(v1: UserV1): UserV2 {
  return {
    email: v1.email,
    verified: v1.age >= 18
  }
}
```

## CLI Reference

### `parquedb types generate`

Generate TypeScript types from schema.

**Options:**
- `--at <ref>` - Generate from specific commit/branch/tag
- `-o, --output <file>` - Output file path (default: `types/db.d.ts`)
- `--namespace <name>` - Wrap types in namespace
- `-h, --help` - Show help

**Examples:**
```bash
parquedb types generate
parquedb types generate --at v1.0.0
parquedb types generate -o src/db.d.ts
parquedb types generate --namespace DB
```

### `parquedb types diff`

Compare types between versions.

**Arguments:**
- `<from>` - Base reference (commit/branch/tag)
- `<to>` - Target reference (default: HEAD)

**Examples:**
```bash
parquedb types diff main
parquedb types diff v1.0.0 v2.0.0
```

### `parquedb schema show`

Display schema details.

**Options:**
- `--at <ref>` - Show schema at specific version
- `--json` - Output as JSON

**Examples:**
```bash
parquedb schema show
parquedb schema show --at v1.0.0
parquedb schema show --json
```

### `parquedb schema diff`

Compare schemas between versions.

**Arguments:**
- `<from>` - Base reference
- `<to>` - Target reference (default: HEAD)

**Options:**
- `--breaking-only` - Show only breaking changes

**Examples:**
```bash
parquedb schema diff main
parquedb schema diff v1.0.0 v2.0.0
parquedb schema diff main --breaking-only
```

### `parquedb schema check`

Check schema compatibility.

**Options:**
- `--staged` - Check staged changes (not committed)

**Examples:**
```bash
parquedb schema check
parquedb schema check --staged
```

## API Reference

### Schema Snapshot

```typescript
import { captureSchema, loadSchemaAtCommit, diffSchemas } from 'parquedb/sync'

// Capture current schema
const snapshot = await captureSchema(config)

// Load historical schema
const oldSchema = await loadSchemaAtCommit(storage, commitHash)

// Compare schemas
const changes = diffSchemas(oldSchema, snapshot)

console.log(changes.summary)
console.log(changes.compatible)
console.log(changes.breakingChanges)
```

### Schema Evolution

```typescript
import { detectBreakingChanges, generateMigrationHints, isSafeToApply } from 'parquedb/sync'

const changes = diffSchemas(before, after)

// Detect breaking changes with severity
const breaking = detectBreakingChanges(changes)
for (const change of breaking) {
  console.log(change.severity)      // 'critical' | 'high' | 'medium'
  console.log(change.impact)        // Human-readable impact
  console.log(change.migrationHint) // Migration guidance
}

// Generate migration hints
const hints = generateMigrationHints(changes)
console.log(hints.join('\n'))

// Check if safe to apply
if (isSafeToApply(changes)) {
  // No breaking changes, safe to deploy
}
```

### TypeScript Generation

```typescript
import { generateTypeScript } from 'parquedb/codegen'

const code = generateTypeScript(schema, {
  namespace: 'DB',
  includeMetadata: true,
  includeImports: true
})

fs.writeFileSync('types/db.d.ts', code)
```

## Advanced Usage

### Custom Type Mapping

Extend type generation:

```typescript
import { generateTypeScript, mapType } from 'parquedb/codegen'

// Override mapType for custom types
const originalMapType = mapType
function customMapType(type: string): string {
  if (type.startsWith('geo:')) {
    return 'GeoPoint'
  }
  return originalMapType(type)
}

// Generate with custom mapping
const code = generateTypeScript(schema)
```

### CI/CD Integration

Add schema checks to your pipeline:

```yaml
# .github/workflows/schema-check.yml
name: Schema Check

on: [pull_request]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: pnpm install

      # Check for breaking changes
      - name: Schema compatibility
        run: |
          parquedb schema diff origin/main
          if [ $? -ne 0 ]; then
            echo "⚠️  Breaking schema changes detected!"
            exit 1
          fi

      # Generate types
      - name: Generate types
        run: parquedb types generate

      # Verify types compile
      - name: Type check
        run: tsc --noEmit
```

### Pre-commit Hook

```bash
# .git/hooks/pre-commit
#!/bin/bash

# Check for breaking changes
echo "Checking schema compatibility..."
if ! parquedb schema check; then
  echo ""
  echo "⚠️  Schema has breaking changes!"
  echo "Run 'parquedb schema diff HEAD' to see details"
  echo ""
  read -p "Continue anyway? (y/N) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

# Regenerate types
echo "Regenerating types..."
parquedb types generate

# Stage generated types
git add types/db.d.ts
```

## Troubleshooting

### Schema snapshot not found

```
Error: Schema snapshot not found for commit: abc123
```

**Solution**: Older commits may not have schema snapshots. Regenerate:

```bash
# Capture schema at current HEAD
parquedb schema show --at HEAD
```

### Type generation fails

```
Error: No parquedb.config.ts found
```

**Solution**: Ensure config file exists and has valid schema:

```typescript
// parquedb.config.ts
import { defineConfig } from 'parquedb/config'

export default defineConfig({
  schema: {
    User: { email: 'string!' }
  }
})
```

### Breaking changes detected but ignored

Use `--breaking-only` to see critical changes:

```bash
parquedb schema diff main --breaking-only
```

Review migration hints:

```bash
parquedb types diff main
```
