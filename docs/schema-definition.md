---
title: Schema Definition Guide
description: Complete guide to defining schemas using the DB() factory function
---

ParqueDB provides a powerful schema definition system through the `DB()` factory function. This guide covers everything from basic field types to advanced configuration options.

## Table of Contents

1. [The DB() Factory](#the-db-factory)
2. [ID and Name Directives](#id-and-name-directives)
3. [Field Type Syntax](#field-type-syntax)
4. [Relationships](#relationships)
5. [Collection Options ($options)](#collection-options-options)
6. [Layout Configuration](#layout-configuration)
7. [Complete Examples](#complete-examples)

---

## The DB() Factory

The `DB()` function is the main entry point for creating typed ParqueDB instances. It accepts a schema object and optional configuration.

### Basic Usage

```typescript
import { DB } from 'parquedb'

// Create a typed database instance
const db = DB({
  User: {
    email: 'string!#',
    name: 'string',
    age: 'int?'
  },
  Post: {
    title: 'string!',
    content: 'text',
    author: '-> User'
  }
})

// Access collections with full TypeScript support
await db.User.create({ email: 'alice@example.com', name: 'Alice' })
await db.Post.find({ author: 'users/alice' })
```

### Flexible Mode

For schema-less operation, use flexible mode:

```typescript
// Completely flexible - no schema required
const db = DB({ schema: 'flexible' })

// Or use the default exported instance
import { db } from 'parquedb'

// Create any entity in any collection
await db.Posts.create({
  title: 'Hello World',
  tags: ['intro', 'welcome'],
  customField: 'anything'
})
```

### Mixed Mode

Combine typed and flexible collections in a single database:

```typescript
const db = DB({
  User: {
    email: 'string!#',
    name: 'string'
  },
  Post: {
    title: 'string!',
    author: '-> User'
  },
  Logs: 'flexible'  // No schema for this collection
})

// User and Post are validated
await db.User.create({ email: 'alice@example.com', name: 'Alice' })

// Logs can accept any shape
await db.Logs.create({
  level: 'info',
  message: 'User created',
  customData: { anything: 'goes here' }
})
```

### Configuration Options

Pass additional options as the second parameter:

```typescript
import { FsBackend } from 'parquedb'

const db = DB({
  User: { email: 'string!#', name: 'string' }
}, {
  storage: new FsBackend('./data'),
  defaultNamespace: 'app'
})
```

---

## ID and Name Directives

ParqueDB provides two powerful directives for using meaningful field values as entity identifiers and display names.

### $id Directive

The `$id` directive specifies which field's value should be used as the entity ID instead of an auto-generated ULID:

```typescript
const db = DB({
  User: {
    $id: 'email',           // Use email as the entity ID
    email: 'string!#',      // alice@example.com → user/alice@example.com
    name: 'string!',
  },
  Post: {
    $id: 'slug',            // Use slug as the entity ID
    slug: 'string!#',       // hello-world → post/hello-world
    title: 'string!',
    content: 'text',
  }
})
```

**Benefits:**
- **Human-readable IDs**: `user/alice@example.com` instead of `user/01hwm3x8g6kj4...`
- **Intuitive lookups**: `db.User.get('alice@example.com')` instead of requiring a ULID
- **URL-friendly**: Slugs work naturally in URLs

**Validation rules:**
- The referenced field must exist in the schema
- The field value cannot be empty
- The field value cannot contain `/` (slashes separate namespace from local ID)

```typescript
// Get by short ID (using field value)
const user = await db.User.get('alice@example.com')
const post = await db.Post.get('hello-world')

// Also works with full ID
const user = await db.User.get('user/alice@example.com')
```

### $name Directive

The `$name` directive specifies which field's value should be used as the entity's display name:

```typescript
const db = DB({
  Post: {
    $id: 'slug',
    $name: 'title',         // Use title as display name
    slug: 'string!#',
    title: 'string!',       // "Hello World" becomes the entity's name
  },
  User: {
    $id: 'email',
    $name: 'displayName',   // Use displayName as display name
    email: 'string!#',
    displayName: 'string!',
  }
})
```

**Fallback behavior:**
- If `$name` field value is empty, null, or undefined, falls back to the entity's local ID
- If no `$name` directive, ParqueDB looks for `name`, `title`, or `label` fields

```typescript
const post = await db.Post.create({
  slug: 'hello-world',
  title: 'Hello World',
})

console.log(post.name)  // 'Hello World' (from $name directive)
console.log(post.$id)   // 'post/hello-world' (from $id directive)
```

### Using Both Directives Together

You can use `$id` and `$name` together, and they can even reference the same field:

```typescript
const db = DB({
  // Different fields for ID and name
  User: {
    $id: 'email',
    $name: 'fullName',
    email: 'string!#',
    fullName: 'string!',
  },

  // Same field for both ID and name
  Tag: {
    $id: 'name',
    $name: 'name',
    name: 'string!#',
  }
})
```

---

## Field Type Syntax

ParqueDB uses a concise string notation for field types. The basic format is:

```
type[modifiers][array][required/optional]
```

### Basic Types

| Type | Description | Example |
|------|-------------|---------|
| `string` | Short text (supports indexing) | `'name: string'` |
| `text` | Long text (no column shredding) | `'content: text'` |
| `markdown` | Markdown content | `'body: markdown'` |
| `number` | Generic number type | `'count: number'` |
| `int` | 32-bit integer | `'age: int'` |
| `float` | Single-precision float | `'rating: float'` |
| `double` | Double-precision float | `'price: double'` |
| `boolean` | True/false value | `'isActive: boolean'` |
| `date` | Date without time | `'birthDate: date'` |
| `datetime` | Date with time | `'publishedAt: datetime'` |
| `timestamp` | Unix timestamp | `'createdAt: timestamp'` |
| `uuid` | UUID identifier | `'id: uuid'` |
| `email` | Email address | `'email: email'` |
| `url` | URL string | `'website: url'` |
| `json` | Arbitrary JSON data | `'metadata: json'` |
| `binary` | Binary data | `'file: binary'` |

### Parametric Types

Some types accept parameters in parentheses:

```typescript
const db = DB({
  Product: {
    // Decimal with precision and scale
    price: 'decimal(10,2)',

    // Fixed-length string
    sku: 'varchar(50)',

    // Character type
    code: 'char(36)',

    // Vector embeddings
    embedding: 'vector(1536)',

    // Enum with allowed values
    status: 'enum(draft,published,archived)'
  }
})
```

### Required Modifier (`!`)

Mark fields as required with the `!` suffix:

```typescript
const db = DB({
  User: {
    email: 'string!',      // Required
    name: 'string',        // Optional (default)
    nickname: 'string?',   // Explicitly optional
  }
})
```

### Indexed Modifier (`#`)

Index fields for fast lookups with the `#` modifier:

```typescript
const db = DB({
  User: {
    email: 'string#!',     // Indexed and required
    username: 'string##',  // Unique index (optional)
    bio: 'text#fts',       // Full-text search index
    apiKey: 'string#hash', // Hash index (O(1) lookups)
  },
  Product: {
    embedding: 'vector(1536)#vec',  // Vector similarity index
  }
})
```

Index modifiers:

| Modifier | Description | Example |
|----------|-------------|---------|
| `#` | Boolean index (implies column shredding) | `'email: string#'` |
| `##` | Unique index | `'username: string##'` |
| `#fts` | Full-text search index | `'content: text#fts'` |
| `#vec` | Vector similarity index | `'embedding: vector(1536)#vec'` |
| `#hash` | Hash index for exact lookups | `'apiKey: string#hash'` |

**Note:** When combining with the required modifier (`!`), the index modifier comes first: `string#!` (not `string!#`).

### Optional Modifier (`?`)

Explicitly mark fields as optional:

```typescript
const db = DB({
  User: {
    age: 'int?',           // Explicitly optional
    nickname: 'string?',   // Explicitly optional
  }
})
```

### Combining Modifiers

Modifiers can be combined. The order matters for parsing:

1. Base type (e.g., `string`, `int`)
2. Index modifier (e.g., `#`, `##`, `#fts`)
3. Required/optional modifier (`!` or `?`)
4. Array modifier (`[]`)

```typescript
const db = DB({
  Product: {
    sku: 'string#!',       // Required, indexed
    slug: 'string##!',     // Required, unique index
    name: 'string#fts!',   // Required, full-text search
    tags: 'string[]',      // Optional array
    categories: 'string[]!', // Required array
  }
})
```

Note: When combining index and required modifiers, the index modifier should come first (e.g., `string#!` not `string!#`).

### Array Types

Append `[]` to any type to create an array:

```typescript
const db = DB({
  Post: {
    title: 'string!',
    tags: 'string[]',         // Optional array of strings
    categories: 'string[]!',  // Required array of strings
    scores: 'int[]',          // Array of integers
    metadata: 'json[]',       // Array of JSON objects
  }
})
```

### Default Values

Specify default values with `= value`:

```typescript
const db = DB({
  Post: {
    status: 'string = "draft"',
    viewCount: 'int = 0',
    isPublic: 'boolean = true',
    tags: 'string[] = []',
  },
  User: {
    role: 'enum(admin,user,guest) = "user"'
  }
})
```

---

## Relationships

ParqueDB provides first-class support for bidirectional relationships. Relationships are defined using arrow operators that specify both the target type and the reverse field name.

**Key Concepts:**
- Forward relationships (`->`) define outbound links from this entity to another
- Reverse relationships (`<-`) document inbound links (automatically populated)
- The reverse field name creates a bidirectional index for efficient traversal in both directions
- Use `[]` suffix for to-many relationships (one entity linking to multiple entities)

### Forward Relationships (`->`)

Forward relationships define outbound references to other entities:

```typescript
const db = DB({
  Post: {
    title: 'string!',
    // Single reference: this post has one author
    // Creates 'posts' backref on User
    author: '-> User.posts',
  },
  Comment: {
    text: 'string!',
    // Reference to parent post
    post: '-> Post.comments',
    // Reference to comment author
    author: '-> User.comments',
  }
})
```

The syntax is: `'-> TargetType.reverseFieldName'` or `'-> TargetType.reverseFieldName[]'`

- `TargetType`: The collection being referenced
- `reverseFieldName`: The field name on the target for reverse traversal
- `[]`: Optional array suffix for to-many relationships

### Reverse Relationships (`<-`)

Reverse relationships document inbound references (automatically populated):

```typescript
const db = DB({
  User: {
    name: 'string!',
    email: 'string!#',
    // Array of posts by this user (from Post.author)
    posts: '<- Post.author[]',
    // Array of comments by this user
    comments: '<- Comment.author[]',
  },
  Post: {
    title: 'string!',
    author: '-> User.posts',
    // Array of comments on this post
    comments: '<- Comment.post[]',
  },
  Comment: {
    text: 'string!',
    post: '-> Post.comments',
    author: '-> User.comments',
  }
})
```

The syntax is: `'<- SourceType.forwardFieldName[]'`

- `SourceType`: The collection that references this type
- `forwardFieldName`: The forward relationship field name
- `[]`: Always use array suffix for reverse relationships

### Relationship Predicates

The predicate is the name of the forward relationship field. The reverse name is specified after the dot in the relationship definition:

```typescript
// Define the schema
const db = DB({
  Person: {
    name: 'string!',
    // Forward relationship: "worksAt" is the predicate
    worksAt: '-> Person.colleagues[]',
    // Reverse relationship: "colleagues" is the reverse name
    colleagues: '<- Person.worksAt[]',
  }
})

// Create entities
const alice = await db.Person.create({ name: 'Alice' })
const bob = await db.Person.create({ name: 'Bob' })

// Link entities using the predicate name
await db.Person.link(
  alice.$id,
  'worksAt',  // predicate (matches forward field name)
  bob.$id,
  { reverse: 'colleagues' }  // reverse name (matches the reverse field)
)
```

**Important:** The predicate name should match the forward relationship field name in your schema, and the reverse name should match the reverse field name.

### To-One Relationships

Reference a single entity:

```typescript
const db = DB({
  Post: {
    title: 'string!',
    author: '-> User.posts',        // One author
    category: '-> Category.posts',  // One category
  }
})

// Create and link
const user = await db.User.create({ name: 'Alice' })
const post = await db.Post.create({
  title: 'Hello World',
  author: user.$id  // Link to user
})
```

### To-Many Relationships

Reference multiple entities with `[]`:

```typescript
const db = DB({
  Post: {
    title: 'string!',
    // Many-to-many with tags
    tags: '-> Tag.posts[]',
  },
  Tag: {
    name: 'string!',
    // Reverse: posts with this tag
    posts: '<- Post.tags[]',
  }
})

// Create and link multiple
const post = await db.Post.create({ title: 'Hello World' })
const tag1 = await db.Tag.create({ name: 'tutorial' })
const tag2 = await db.Tag.create({ name: 'intro' })

// Link to multiple tags
await db.Post.update(post.$id, {
  $link: {
    tags: [tag1.$id, tag2.$id]
  }
})
```

### Self-Referential Relationships

Types can reference themselves for hierarchical structures:

```typescript
const db = DB({
  Employee: {
    name: 'string!',
    // Manager reference (one-to-one)
    manager: '-> Employee.reports',
    // Direct reports (one-to-many)
    reports: '<- Employee.manager[]',
  },
  Category: {
    name: 'string!',
    // Parent category
    parent: '-> Category.children',
    // Child categories
    children: '<- Category.parent[]',
  }
})

// Create hierarchy
const alice = await db.Employee.create({ name: 'Alice' })
const bob = await db.Employee.create({ name: 'Bob' })
const charlie = await db.Employee.create({ name: 'Charlie' })

// Alice manages Bob and Charlie
await db.Employee.update(bob.$id, { $link: { manager: alice.$id } })
await db.Employee.update(charlie.$id, { $link: { manager: alice.$id } })

// Query relationships
const aliceWithReports = await db.Employee.get(alice.$id, {
  populate: ['reports']
})
// aliceWithReports.reports = [bob, charlie]
```

### Fuzzy (Semantic) Relationships

Use `~>` and `<~` for AI-matched relationships based on semantic similarity:

```typescript
const db = DB({
  Article: {
    title: 'string!',
    content: 'text!',
    embedding: 'vector(1536)#vec',
    // AI-matched related topics
    relatedTopics: '~> Topic.articles[]',
  },
  Topic: {
    name: 'string!',
    embedding: 'vector(1536)#vec',
    // Reverse fuzzy relationship
    articles: '<~ Article.relatedTopics[]',
  },
  Product: {
    name: 'string!',
    embedding: 'vector(1536)#vec',
    // AI-matched similar products (self-referential)
    similar: '~> Product.similarTo[]',
    similarTo: '<~ Product.similar[]',
  }
})
```

Note: Fuzzy relationships require vector embeddings to be configured and use cosine similarity or other distance metrics for matching.

---

## Collection Options ($options)

Use the `$options` key to configure collection-level behavior:

```typescript
const db = DB({
  Occupation: {
    $options: {
      includeDataVariant: true,  // Include $data variant column (default)
    },
    name: 'string!',
    socCode: 'string!#',
    description: 'text',
  },
  Logs: {
    $options: {
      includeDataVariant: false,  // Omit $data for write-heavy collections
    },
    level: 'string',
    message: 'string',
    timestamp: 'datetime',
  }
})
```

### Available Options

#### includeDataVariant

**Type:** `boolean`
**Default:** `true`

Controls whether the `$data` Variant column is included in the Parquet schema.

- **`true` (default)**: Include `$data` column for fast full-row reads
- **`false`**: Omit `$data` column to reduce storage for write-heavy collections

**When to use `includeDataVariant: false`:**

1. **Write-heavy collections** (logs, events, metrics) where you rarely need full row data
2. **Large collections** where storage is a concern and you mainly query indexed columns
3. **Append-only data** where the variant overhead isn't worth it

**When to use `includeDataVariant: true` (default):**

1. **Read-heavy collections** where you frequently fetch complete entities
2. **Dynamic schemas** where fields change frequently
3. **General-purpose collections** where flexibility is important

Example comparison:

```typescript
const db = DB({
  // User profiles - read full entities frequently
  User: {
    $options: { includeDataVariant: true },
    email: 'string!#',
    name: 'string',
    bio: 'text',
    // Many other fields...
  },

  // Access logs - only query indexed fields
  AccessLog: {
    $options: { includeDataVariant: false },
    userId: 'string#',
    endpoint: 'string#',
    timestamp: 'datetime#',
    // Saves ~50% storage by omitting $data
  }
})
```

### Future Options

The `$options` interface is designed to be extensible. Planned future options include:

```typescript
interface CollectionOptions {
  includeDataVariant?: boolean

  // Future options:
  // compression?: 'snappy' | 'gzip' | 'zstd' | 'none'
  // partitionBy?: string[]
  // rowGroupSize?: number
  // enableVersioning?: boolean
  // ttl?: number  // Auto-expire in seconds
}
```

---

## Layout Configuration

ParqueDB provides special keys for UI and admin configuration: `$layout`, `$sidebar`, and `$studio`.

### $layout - Field Layout

The `$layout` key defines how fields are arranged in admin UIs. It supports both simple layouts and tab-based layouts.

#### Simple Layout (No Tabs)

Use an array of rows, where each row can be a single field or an array of fields:

```typescript
const db = DB({
  Post: {
    title: 'string!',
    slug: 'string!',
    content: 'text',
    excerpt: 'text',
    status: 'string',
    publishedAt: 'datetime',
    author: '-> User',

    // Layout: rows of fields
    $layout: [
      ['title', 'slug'],     // Row with 2 columns
      'content',             // Full-width row
      'excerpt',             // Full-width row
      ['status', 'publishedAt', 'author']  // Row with 3 columns
    ]
  }
})
```

#### Tabbed Layout

Use an object where keys are tab names and values are arrays of rows:

```typescript
const db = DB({
  Post: {
    title: 'string!',
    slug: 'string!',
    content: 'text',
    excerpt: 'text',
    status: 'string',
    publishedAt: 'datetime',
    author: '-> User',
    seoTitle: 'string',
    seoDescription: 'text',
    ogImage: 'string',

    // Tabbed layout
    $layout: {
      Content: [
        ['title', 'slug'],
        'content',
        'excerpt'
      ],
      Metadata: [
        ['status', 'publishedAt'],
        'author'
      ],
      SEO: [
        'seoTitle',
        'seoDescription',
        'ogImage'
      ]
    }
  }
})
```

### $sidebar - Sidebar Fields

The `$sidebar` key defines which fields appear in the sidebar (typically metadata):

```typescript
const db = DB({
  Post: {
    title: 'string!',
    content: 'text',
    status: 'string',

    $layout: [
      'title',
      'content'
    ],

    // Sidebar shows ID, status, and timestamps
    $sidebar: [
      '$id',
      'status',
      'createdAt',
      'updatedAt',
      'publishedAt'
    ]
  }
})
```

### $studio - UI Configuration

The `$studio` key provides additional UI/admin configuration:

```typescript
const db = DB({
  Post: {
    title: 'string!',
    slug: 'string!',
    content: 'text',
    status: 'string',
    author: '-> User',

    $studio: {
      // Collection label in admin
      label: 'Blog Posts',

      // Field to use as title/name
      useAsTitle: 'title',

      // Default columns in list view
      defaultColumns: ['title', 'status', 'author', 'createdAt'],

      // Group in admin navigation
      group: 'Content',

      // Per-field UI config
      status: {
        label: 'Post Status',
        description: 'Current publication status',
        options: ['draft', 'published', 'archived']
      },

      content: {
        label: 'Post Content',
        description: 'Main article content in Markdown',
        hideInList: true  // Don't show in list view
      },

      author: {
        label: 'Author',
        readOnly: true  // Can't edit after creation
      }
    }
  }
})
```

### Field-Level Studio Config

Individual field configurations in `$studio`:

```typescript
interface InlineFieldStudio {
  /** Display label */
  label?: string

  /** Help text */
  description?: string

  /** For select fields - simple array or label/value pairs */
  options?: string[] | Array<{ label: string; value: string }>

  /** Hide from list view */
  hideInList?: boolean

  /** Read-only field */
  readOnly?: boolean
}
```

Example with all field options:

```typescript
const db = DB({
  Product: {
    name: 'string!',
    status: 'string',
    category: '-> Category',
    internalNotes: 'text',

    $studio: {
      label: 'Products',
      useAsTitle: 'name',
      group: 'Catalog',

      status: {
        label: 'Status',
        description: 'Product availability status',
        options: [
          { label: 'In Stock', value: 'available' },
          { label: 'Out of Stock', value: 'unavailable' },
          { label: 'Coming Soon', value: 'preorder' }
        ]
      },

      category: {
        label: 'Category',
        description: 'Primary product category'
      },

      internalNotes: {
        label: 'Internal Notes',
        description: 'For internal use only',
        hideInList: true  // Don't show in list view
      }
    }
  }
})
```

### Complete Layout Example

A fully configured collection with all layout options:

```typescript
const db = DB({
  Post: {
    // Fields
    title: 'string!',
    slug: 'string!#',
    content: 'markdown!',
    excerpt: 'text',
    status: 'enum(draft,published,archived)',
    publishedAt: 'datetime',
    featuredImage: 'string',
    author: '-> User.posts',
    tags: '-> Tag.posts[]',

    // Storage options
    $options: {
      includeDataVariant: true
    },

    // Tabbed layout
    $layout: {
      Content: [
        ['title', 'slug'],
        'content',
        'excerpt'
      ],
      Settings: [
        ['status', 'publishedAt'],
        'featuredImage',
        'author',
        'tags'
      ]
    },

    // Sidebar metadata
    $sidebar: [
      '$id',
      'status',
      'createdAt',
      'updatedAt',
      'createdBy'
    ],

    // UI configuration
    $studio: {
      label: 'Blog Posts',
      useAsTitle: 'title',
      defaultColumns: ['title', 'status', 'author', 'publishedAt'],
      group: 'Content',

      status: {
        options: ['draft', 'published', 'archived']
      },

      content: {
        description: 'Main post content in Markdown format',
        hideInList: true
      },

      excerpt: {
        description: 'Short summary for post listings',
        hideInList: true
      },

      featuredImage: {
        label: 'Featured Image',
        description: 'URL to featured image'
      }
    }
  }
})
```

---

## Complete Examples

### Blog Schema

A complete blog with users, posts, comments, and tags:

```typescript
import { DB } from 'parquedb'

const db = DB({
  User: {
    // Storage
    $options: { includeDataVariant: true },

    // Fields
    name: 'string!',
    email: 'string##!',  // Required, unique index
    bio: 'text#fts',    // Full-text search
    avatar: 'string',
    role: 'enum(admin,editor,author) = "author"',

    // Relationships
    posts: '<- Post.author[]',
    comments: '<- Comment.author[]',

    // Layout
    $layout: [
      ['name', 'email'],
      'bio',
      ['role', 'avatar']
    ],
    $sidebar: ['$id', 'role', 'createdAt'],
    $studio: {
      label: 'Users',
      useAsTitle: 'name',
      defaultColumns: ['name', 'email', 'role', 'createdAt'],
      group: 'Users',

      role: {
        options: ['admin', 'editor', 'author']
      }
    }
  },

  Post: {
    $options: { includeDataVariant: true },

    // Fields
    title: 'string#fts!',
    slug: 'string##!',  // Unique index
    content: 'markdown#fts!',
    excerpt: 'text',
    status: 'enum(draft,published,archived) = "draft"',
    publishedAt: 'datetime',
    viewCount: 'int = 0',

    // Relationships
    author: '-> User.posts',
    tags: '-> Tag.posts[]',
    comments: '<- Comment.post[]',

    // Layout
    $layout: {
      Content: [
        ['title', 'slug'],
        'content',
        'excerpt'
      ],
      Settings: [
        ['status', 'publishedAt'],
        ['author', 'viewCount'],
        'tags'
      ]
    },
    $sidebar: ['$id', 'status', 'createdAt', 'updatedAt'],
    $studio: {
      label: 'Blog Posts',
      useAsTitle: 'title',
      defaultColumns: ['title', 'status', 'author', 'publishedAt'],
      group: 'Content',

      status: {
        options: ['draft', 'published', 'archived']
      }
    }
  },

  Comment: {
    $options: { includeDataVariant: true },

    // Fields
    text: 'string!',
    isApproved: 'boolean = false',

    // Relationships
    post: '-> Post.comments',
    author: '-> User.comments',
    parent: '-> Comment.replies',
    replies: '<- Comment.parent[]',

    // Layout
    $layout: [
      'text',
      ['isApproved', 'author', 'post']
    ],
    $sidebar: ['$id', 'isApproved', 'createdAt'],
    $studio: {
      label: 'Comments',
      useAsTitle: 'text',
      defaultColumns: ['text', 'author', 'post', 'isApproved'],
      group: 'Content'
    }
  },

  Tag: {
    $options: { includeDataVariant: true },

    // Fields
    name: 'string##!',  // Unique
    slug: 'string##!',  // Unique
    description: 'text',

    // Relationships
    posts: '<- Post.tags[]',

    // Layout
    $layout: [
      ['name', 'slug'],
      'description'
    ],
    $studio: {
      label: 'Tags',
      useAsTitle: 'name',
      defaultColumns: ['name', 'slug'],
      group: 'Content'
    }
  }
})

// Use the database
const user = await db.User.create({
  name: 'Alice',
  email: 'alice@example.com',
  bio: 'Tech writer and developer'
})

const post = await db.Post.create({
  title: 'Getting Started with ParqueDB',
  slug: 'getting-started-parquedb',
  content: '# Introduction\n\nParqueDB is...',
  excerpt: 'Learn the basics of ParqueDB',
  author: user.$id
})

const comment = await db.Comment.create({
  text: 'Great post!',
  post: post.$id,
  author: user.$id,
  isApproved: true
})
```

### E-Commerce Schema

A complete e-commerce schema with products, orders, and customers:

```typescript
import { DB } from 'parquedb'

const db = DB({
  Customer: {
    $options: { includeDataVariant: true },

    name: 'string!',
    email: 'string##!',
    phone: 'string',

    orders: '<- Order.customer[]',
    addresses: '<- Address.customer[]',

    $layout: [
      ['name', 'email'],
      'phone'
    ],
    $sidebar: ['$id', 'createdAt'],
    $studio: {
      label: 'Customers',
      useAsTitle: 'name',
      group: 'Sales'
    }
  },

  Product: {
    $options: { includeDataVariant: true },

    name: 'string#fts!',
    description: 'markdown#fts',
    sku: 'string##!',
    price: 'float!',
    stock: 'int = 0',
    isActive: 'boolean = true',

    category: '-> Category.products',
    orderItems: '<- OrderItem.product[]',

    $layout: {
      Details: [
        ['name', 'sku'],
        'description',
        ['price', 'stock']
      ],
      Settings: [
        ['isActive', 'category']
      ]
    },
    $sidebar: ['$id', 'isActive', 'createdAt', 'updatedAt'],
    $studio: {
      label: 'Products',
      useAsTitle: 'name',
      defaultColumns: ['name', 'sku', 'price', 'stock'],
      group: 'Catalog'
    }
  },

  Category: {
    $options: { includeDataVariant: true },

    name: 'string!',
    slug: 'string##!',

    parent: '-> Category.children',
    children: '<- Category.parent[]',
    products: '<- Product.category[]',

    $layout: [
      ['name', 'slug'],
      'parent'
    ],
    $studio: {
      label: 'Categories',
      useAsTitle: 'name',
      group: 'Catalog'
    }
  },

  Order: {
    $options: { includeDataVariant: true },

    orderNumber: 'string##!',
    total: 'float!',
    status: 'enum(pending,paid,shipped,delivered,cancelled) = "pending"',

    customer: '-> Customer.orders',
    items: '<- OrderItem.order[]',
    shippingAddress: '-> Address.orders',

    $layout: {
      Details: [
        ['orderNumber', 'status'],
        ['total', 'customer']
      ],
      Items: [
        'items'
      ],
      Shipping: [
        'shippingAddress'
      ]
    },
    $sidebar: ['$id', 'status', 'createdAt'],
    $studio: {
      label: 'Orders',
      useAsTitle: 'orderNumber',
      defaultColumns: ['orderNumber', 'customer', 'total', 'status'],
      group: 'Sales',

      status: {
        options: ['pending', 'paid', 'shipped', 'delivered', 'cancelled']
      }
    }
  },

  OrderItem: {
    $options: { includeDataVariant: false },  // Write-heavy, don't need full rows

    quantity: 'int!',
    unitPrice: 'float!',
    subtotal: 'float!',

    order: '-> Order.items',
    product: '-> Product.orderItems',

    $layout: [
      ['product', 'quantity'],
      ['unitPrice', 'subtotal']
    ]
  },

  Address: {
    $options: { includeDataVariant: true },

    street: 'string!',
    city: 'string!',
    state: 'string',
    postalCode: 'string!',
    country: 'string!',
    isDefault: 'boolean = false',

    customer: '-> Customer.addresses',
    orders: '<- Order.shippingAddress[]',

    $layout: [
      'street',
      ['city', 'state', 'postalCode'],
      ['country', 'isDefault']
    ]
  }
})
```

### SaaS Application Schema

A SaaS schema with organizations, teams, and users:

```typescript
import { DB } from 'parquedb'

const db = DB({
  Organization: {
    $options: { includeDataVariant: true },

    name: 'string!',
    slug: 'string##!',
    plan: 'enum(free,pro,enterprise) = "free"',
    billingEmail: 'string!',

    teams: '<- Team.organization[]',
    members: '<- Member.organization[]',

    $layout: [
      ['name', 'slug'],
      ['plan', 'billingEmail']
    ],
    $sidebar: ['$id', 'plan', 'createdAt'],
    $studio: {
      label: 'Organizations',
      useAsTitle: 'name',
      group: 'Admin',

      plan: {
        options: ['free', 'pro', 'enterprise']
      }
    }
  },

  Team: {
    $options: { includeDataVariant: true },

    name: 'string!',

    organization: '-> Organization.teams',
    members: '<- Member.team[]',
    projects: '<- Project.team[]',

    $layout: [
      'name',
      'organization'
    ],
    $studio: {
      label: 'Teams',
      useAsTitle: 'name',
      group: 'Admin'
    }
  },

  Member: {
    $options: { includeDataVariant: true },

    email: 'string#!',
    name: 'string!',
    role: 'enum(owner,admin,member) = "member"',

    organization: '-> Organization.members',
    team: '-> Team.members',

    $layout: [
      ['name', 'email'],
      ['role', 'team']
    ],
    $sidebar: ['$id', 'role', 'createdAt'],
    $studio: {
      label: 'Members',
      useAsTitle: 'name',
      group: 'Admin',

      role: {
        options: ['owner', 'admin', 'member']
      }
    }
  },

  Project: {
    $options: { includeDataVariant: true },

    name: 'string!',
    description: 'text',
    status: 'enum(active,archived) = "active"',

    team: '-> Team.projects',
    tasks: '<- Task.project[]',

    $layout: [
      ['name', 'status'],
      'description',
      'team'
    ],
    $sidebar: ['$id', 'status', 'createdAt', 'updatedAt'],
    $studio: {
      label: 'Projects',
      useAsTitle: 'name',
      group: 'Work',

      status: {
        options: ['active', 'archived']
      }
    }
  },

  Task: {
    $options: { includeDataVariant: true },

    title: 'string!',
    description: 'markdown',
    status: 'enum(todo,in_progress,done) = "todo"',
    priority: 'enum(low,medium,high) = "medium"',
    dueDate: 'date',

    project: '-> Project.tasks',
    assignee: '-> Member',

    $layout: {
      Details: [
        'title',
        'description',
        ['status', 'priority']
      ],
      Assignment: [
        ['assignee', 'dueDate'],
        'project'
      ]
    },
    $sidebar: ['$id', 'status', 'priority', 'createdAt'],
    $studio: {
      label: 'Tasks',
      useAsTitle: 'title',
      defaultColumns: ['title', 'status', 'priority', 'assignee', 'dueDate'],
      group: 'Work',

      status: {
        options: ['todo', 'in_progress', 'done']
      },
      priority: {
        options: ['low', 'medium', 'high']
      }
    }
  }
})
```

---

## Common Pitfalls

### 1. Incorrect Modifier Ordering

**Wrong:**
```typescript
const db = DB({
  User: {
    email: 'string!#',  // This may not parse correctly
  }
})
```

**Correct:**
```typescript
const db = DB({
  User: {
    email: 'string#!',  // Index modifier before required modifier
  }
})
```

### 2. Missing Array Suffix on Reverse Relationships

**Wrong:**
```typescript
const db = DB({
  User: {
    posts: '<- Post.author',  // Missing []
  }
})
```

**Correct:**
```typescript
const db = DB({
  User: {
    posts: '<- Post.author[]',  // Always use [] for reverse relationships
  }
})
```

### 3. Inconsistent Relationship Names

**Wrong:**
```typescript
const db = DB({
  Post: {
    author: '-> User.articles',  // Reverse name doesn't match
  },
  User: {
    posts: '<- Post.author[]',   // Expecting 'articles' not 'posts'
  }
})
```

**Correct:**
```typescript
const db = DB({
  Post: {
    author: '-> User.posts',     // Matches User.posts
  },
  User: {
    posts: '<- Post.author[]',   // Matches Post.author
  }
})
```

---

## Best Practices

### 1. Use Flexible Mode for Prototyping

Start with flexible mode when exploring:

```typescript
const db = DB({ schema: 'flexible' })
```

Once your schema stabilizes, convert to typed mode for validation.

### 2. Index Strategically

Only index fields used in queries:

```typescript
const db = DB({
  User: {
    email: 'string!#',     // Indexed - used in lookups
    name: 'string',        // Not indexed - display only
    bio: 'text#fts',       // FTS indexed - for search
  }
})
```

### 3. Use includeDataVariant Wisely

Set `includeDataVariant: false` for write-heavy collections:

```typescript
const db = DB({
  AccessLog: {
    $options: { includeDataVariant: false },
    userId: 'string#',
    endpoint: 'string#',
    timestamp: 'datetime#'
  }
})
```

### 4. Document Relationships Both Ways

Always define both forward and reverse relationships:

```typescript
const db = DB({
  Post: {
    author: '-> User.posts',      // Forward
  },
  User: {
    posts: '<- Post.author[]',    // Reverse (documentation)
  }
})
```

### 5. Use Layouts for Better UX

Configure layouts to improve the admin experience:

```typescript
const db = DB({
  Post: {
    // ... fields ...

    $layout: {
      Content: ['title', 'content'],
      Settings: ['status', 'author']
    },
    $sidebar: ['$id', 'status', 'createdAt'],
    $studio: {
      label: 'Blog Posts',
      useAsTitle: 'title'
    }
  }
})
```

---

## Next Steps

- [Getting Started](./getting-started.md) - Basic CRUD operations with DB()
- [Query API](./queries.md) - MongoDB-style filtering and querying
- [Update Operators](./updates.md) - Update operators like $set, $inc, $push
- [Relationships](./relationships.md) - Working with graph relationships
- [ParqueDB Studio](./studio.md) - Admin UI for viewing and editing data
