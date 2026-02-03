---
title: ParqueDB Studio
description: Admin interface for viewing and editing Parquet data using Payload CMS
---

# ParqueDB Studio

ParqueDB Studio is a built-in admin interface powered by Payload CMS. It auto-discovers your Parquet files and generates a full-featured admin UI with zero configuration.

## Quick Start

```bash
# Launch studio in current directory
npx parquedb studio

# Specify data directory
npx parquedb studio ./my-data

# Custom port
npx parquedb studio --port 8080

# Read-only mode
npx parquedb studio --read-only
```

Then open http://localhost:3000 in your browser.

## Features

- **Auto-discovery**: Automatically finds and introspects Parquet files
- **Schema visualization**: View column types, statistics, and metadata
- **CRUD operations**: Create, read, update, delete records through the UI
- **Filtering and sorting**: MongoDB-style filters in a visual interface
- **Relationship navigation**: Click through relationships between entities
- **Bulk operations**: Import/export, batch updates

## Configuration

### Command Line Options

| Option | Description |
|--------|-------------|
| `-p, --port <port>` | Port to run server on (default: 3000) |
| `-r, --read-only` | Disable write operations |
| `-m, --metadata-dir <dir>` | UI metadata directory (default: .studio) |
| `--auth <mode>` | Authentication: none, local, env |
| `--admin-email <email>` | Admin email for local auth |
| `--admin-password <pass>` | Admin password for local auth |
| `--debug` | Enable debug logging |

### Config File

Create a `parquedb.config.ts` for persistent configuration:

```typescript
import { defineConfig } from 'parquedb/config'

export default defineConfig({
  storage: { type: 'fs', path: './data' },

  schema: {
    Post: {
      title: 'string!',
      slug: 'string!',
      content: 'text',
      status: 'string',
      author: '-> User',

      // Layout configuration
      $layout: [['title', 'slug'], 'content'],
      $sidebar: ['$id', 'status', 'createdAt', 'author'],
      $studio: {
        label: 'Blog Posts',
        useAsTitle: 'title',
        status: { options: ['draft', 'published', 'archived'] }
      }
    },

    User: {
      email: 'email!#',
      name: 'string!',
      role: 'string',
      posts: '<- Post.author[]',

      $layout: [['name', 'email'], 'role'],
      $sidebar: ['$id', 'createdAt'],
      $studio: {
        label: 'Users',
        useAsTitle: 'name',
        role: { options: ['admin', 'editor', 'viewer'] }
      }
    }
  },

  studio: {
    port: 3000,
    theme: 'auto',  // 'light', 'dark', or 'auto'
    defaultSidebar: ['$id', 'createdAt', 'updatedAt']
  }
})
```

## Layout Configuration

The `$layout` field controls how fields are arranged in the edit form.

### Without Tabs (Array)

```typescript
// Simple rows
$layout: ['title', 'content', 'status']

// Grouped fields in rows
$layout: [
  ['title', 'slug'],      // Row 1: title and slug side-by-side
  'content',              // Row 2: content full width
  ['status', 'author']    // Row 3: status and author side-by-side
]
```

### With Tabs (Object)

```typescript
// Object keys become tab names
$layout: {
  Content: [
    ['title', 'slug'],
    'content'
  ],
  Settings: [
    'status',
    ['publishedAt', 'author']
  ],
  SEO: [
    'metaTitle',
    'metaDescription'
  ]
}
```

## Field Configuration

Use `$studio` for field-level UI configuration:

```typescript
$studio: {
  // Collection-level config
  label: 'Blog Posts',
  useAsTitle: 'title',
  defaultColumns: ['title', 'status', 'createdAt'],
  group: 'Content',

  // Field-level config
  status: {
    label: 'Publication Status',
    options: ['draft', 'published', 'archived'],
    description: 'Current status of the post'
  },
  content: {
    label: 'Body Content',
    description: 'Markdown supported'
  }
}
```

### Field Options

| Option | Description |
|--------|-------------|
| `label` | Display label |
| `description` | Help text shown below field |
| `options` | For select fields: array of values or `{label, value}` objects |
| `hideInList` | Hide from list view |
| `readOnly` | Disable editing |
| `min`, `max` | Numeric validation |
| `minLength`, `maxLength` | String validation |

## Sidebar Configuration

The `$sidebar` field specifies which fields appear in the sidebar:

```typescript
$sidebar: ['$id', 'status', 'createdAt', 'updatedAt', 'author']
```

System fields (prefixed with `$`) are automatically formatted:
- `$id` - Shows the entity ID
- `createdAt`, `updatedAt` - Formatted dates
- Relationship fields show linked entity names

## UI Metadata Storage

Studio stores UI customizations in `.studio/metadata.json`:

```json
{
  "version": "1.0",
  "collections": {
    "posts": {
      "label": "Blog Posts",
      "admin": {
        "useAsTitle": "title",
        "defaultColumns": ["title", "status", "createdAt"]
      },
      "fields": {
        "status": {
          "options": [
            { "label": "Draft", "value": "draft" },
            { "label": "Published", "value": "published" }
          ]
        }
      }
    }
  }
}
```

This file is auto-generated but can be manually edited. It's merged with schema-level `$studio` configuration, with the metadata file taking precedence.

## Authentication

### No Authentication (Default)

```bash
parquedb studio --auth none
```

Suitable for local development.

### Local Authentication

```bash
parquedb studio --auth local --admin-email admin@example.com --admin-password secret
```

Creates a local admin user with specified credentials.

### Environment Variables

```bash
parquedb studio --auth env
```

Reads credentials from environment variables:
- `PARQUEDB_ADMIN_EMAIL`
- `PARQUEDB_ADMIN_PASSWORD`

## Deploying Studio

### Cloudflare Workers

Use OpenNext to deploy Studio to Cloudflare Workers:

```bash
# Install OpenNext
pnpm add -D @opennextjs/cloudflare

# Build for Workers
pnpm opennextjs-cloudflare build

# Deploy
wrangler deploy
```

See the [Payload Adapter](./integrations/payload.md) documentation for full deployment instructions.

### Docker

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
CMD ["npx", "parquedb", "studio", "--port", "3000"]
EXPOSE 3000
```

## Programmatic Usage

You can also run Studio programmatically:

```typescript
import { createStudioServer, discoverCollections } from 'parquedb/studio'
import { FsBackend } from 'parquedb'

const storage = new FsBackend('./data')
const collections = await discoverCollections(storage, './data')

const server = await createStudioServer({
  port: 3000,
  dataDir: './data',
  metadataDir: '.studio',
  readOnly: false,
  debug: false
}, storage)

await server.start()
console.log('Studio running at http://localhost:3000')
```

## Next Steps

- [Payload Adapter](./integrations/payload.md) - Use ParqueDB as Payload's database
- [Configuration Reference](./deployment/configuration.md) - All config options
- [Schema Definition](./schemas.md) - Define your data model
