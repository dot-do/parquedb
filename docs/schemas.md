---
title: Schema Definition Guide
description: Defining types, fields, relationships, and indexes in ParqueDB schemas
---

ParqueDB supports flexible schema definitions that enable type safety, relationships, and indexing. This guide covers how to define schemas using the native ParqueDB format, GraphDL integration, and IceType integration.

## Table of Contents

1. [Basic Schema Definition](#basic-schema-definition)
2. [Field Types](#field-types)
3. [Type Modifiers](#type-modifiers)
4. [Relationship Definitions](#relationship-definitions)
5. [Index Definitions](#index-definitions)
6. [Type-Level Metadata](#type-level-metadata)
7. [GraphDL Integration](#graphdl-integration)
8. [IceType Integration](#icetype-integration)
9. [Complete Examples](#complete-examples)

---

## Basic Schema Definition

A ParqueDB schema is a TypeScript object where keys are type names and values define the type structure:

```typescript
import type { Schema } from 'parquedb'

const schema: Schema = {
  User: {
    name: 'string!',
    email: 'email!',
    bio: 'text',
  },
  Post: {
    title: 'string!',
    content: 'markdown!',
    status: 'string = "draft"',
    author: '-> User.posts',
  },
}
```

---

## Field Types

### Primitive Types

ParqueDB supports the following primitive field types:

| Type | Description | Example |
|------|-------------|---------|
| `string` | Short text (supports shredding) | `'name: string'` |
| `text` | Long text (no shredding) | `'content: text'` |
| `markdown` | Markdown content | `'body: markdown'` |
| `number` | General numeric type | `'amount: number'` |
| `int` | 32-bit integer | `'count: int'` |
| `float` | Single-precision floating point | `'rating: float'` |
| `double` | Double-precision floating point | `'price: double'` |
| `boolean` | True/false value | `'isActive: boolean'` |
| `date` | Date without time | `'birthDate: date'` |
| `datetime` | Date with time | `'publishedAt: datetime'` |
| `timestamp` | Microsecond precision timestamp | `'createdAt: timestamp'` |
| `uuid` | UUID identifier | `'id: uuid'` |
| `email` | Email address (validated) | `'email: email'` |
| `url` | URL (validated) | `'website: url'` |
| `json` | Arbitrary JSON data | `'metadata: json'` |
| `binary` | Binary data | `'avatar: binary'` |

### Parametric Types

Some types accept parameters:

```typescript
const schema: Schema = {
  Product: {
    // Decimal with precision and scale
    price: 'decimal(10,2)',

    // Fixed-length string
    sku: 'varchar(50)',

    // Character type
    code: 'char(36)',

    // Vector for embeddings
    embedding: 'vector(1536)',

    // Enum with allowed values
    status: 'enum(draft,published,archived)',
  },
}
```

### Array Types

Append `[]` to any type to create an array:

```typescript
const schema: Schema = {
  Post: {
    title: 'string!',
    tags: 'string[]',           // Optional array of strings
    categories: 'string[]!',    // Required array of strings
    scores: 'int[]',            // Array of integers
  },
}
```

---

## Type Modifiers

### Required Fields (`!`)

Mark fields as required with the `!` suffix:

```typescript
const schema: Schema = {
  User: {
    name: 'string!',     // Required
    email: 'email!',     // Required
    bio: 'text',         // Optional (default)
  },
}
```

### Optional Fields (`?`)

Explicitly mark fields as optional with `?`:

```typescript
const schema: Schema = {
  User: {
    nickname: 'string?',  // Explicitly optional
  },
}
```

### Default Values (`= value`)

Provide default values for fields:

```typescript
const schema: Schema = {
  Post: {
    status: 'string = "draft"',
    viewCount: 'int = 0',
    isPublic: 'boolean = true',
    tags: 'string[] = []',
  },
}
```

### Combined Modifiers

Modifiers can be combined:

```typescript
const schema: Schema = {
  Post: {
    // Required with default
    status: 'string! = "draft"',

    // Required array
    categories: 'string[]!',
  },
}
```

---

## Relationship Definitions

ParqueDB supports bidirectional relationships with four operators:

| Operator | Direction | Match Mode | Description |
|----------|-----------|------------|-------------|
| `->` | Forward | Exact | Foreign key reference |
| `<-` | Backward | Exact | Reverse reference (auto-populated) |
| `~>` | Forward | Fuzzy | AI-matched semantic reference |
| `<~` | Backward | Fuzzy | AI-matched backlink |

### Forward Relationships (`->`)

Define outbound references to other types:

```typescript
const schema: Schema = {
  Post: {
    title: 'string!',
    // Single author, creates 'posts' backref on User
    author: '-> User.posts',
  },
  Comment: {
    text: 'string!',
    // Single post reference
    post: '-> Post.comments',
    // Single author reference
    author: '-> User.comments',
  },
}
```

### Backward Relationships (`<-`)

Document inbound references (automatically populated):

```typescript
const schema: Schema = {
  User: {
    name: 'string!',
    // Array of posts by this user (from Post.author)
    posts: '<- Post.author[]',
    // Array of comments by this user
    comments: '<- Comment.author[]',
  },
  Post: {
    title: 'string!',
    // Array of comments on this post
    comments: '<- Comment.post[]',
  },
}
```

### Many-to-Many Relationships

Use `[]` suffix for array relationships:

```typescript
const schema: Schema = {
  Post: {
    title: 'string!',
    // Many-to-many with tags
    tags: '-> Tag.posts[]',
  },
  Tag: {
    name: 'string!',
    // Reverse of Post.tags
    posts: '<- Post.tags[]',
  },
}
```

### Fuzzy (Semantic) Relationships

Use `~>` and `<~` for AI-matched relationships:

```typescript
const schema: Schema = {
  Article: {
    title: 'string!',
    content: 'text!',
    // AI-matched related topics
    relatedTopics: '~> Topic.articles[]',
  },
  Product: {
    name: 'string!',
    // AI-matched similar products
    similar: '~> Product.similarTo[]',
  },
}
```

### Self-Referential Relationships

Types can reference themselves:

```typescript
const schema: Schema = {
  Employee: {
    name: 'string!',
    // Manager reference
    manager: '-> Employee.reports',
    // Direct reports (auto-populated)
    reports: '<- Employee.manager[]',
  },
  Category: {
    name: 'string!',
    // Parent category
    parent: '-> Category.children',
    // Child categories
    children: '<- Category.parent[]',
  },
}
```

---

## Index Definitions

### Field-Level Index Modifiers

Use shorthand modifiers directly on field types:

```typescript
const schema: Schema = {
  User: {
    // Indexed field (boolean true - enables shredding)
    email: 'email#!',

    // Unique index
    username: 'string##!',

    // Full-text search index
    bio: 'text#fts',

    // Vector similarity index
    embedding: 'vector(1536)#vec',

    // Hash index (O(1) lookups)
    apiKey: 'string#hash',
  },
}
```

### Object-Form Field Definitions

For more control, use the object form:

```typescript
const schema: Schema = {
  User: {
    email: {
      type: 'email',
      required: true,
      index: 'unique',
      description: 'Primary email address',
    },
    age: {
      type: 'int',
      min: 0,
      max: 150,
    },
    username: {
      type: 'string',
      required: true,
      index: true,
      minLength: 3,
      maxLength: 50,
      pattern: '^[a-z0-9_]+$',
    },
  },
}
```

### Compound Indexes (`$indexes`)

Define multi-field indexes at the type level:

```typescript
const schema: Schema = {
  Post: {
    $indexes: [
      // Compound index on author + createdAt
      {
        name: 'posts_by_author_date',
        fields: [
          { field: 'author' },
          { field: 'createdAt', direction: -1 },
        ],
      },
      // Unique compound index
      {
        name: 'unique_slug_per_author',
        fields: [{ field: 'author' }, { field: 'slug' }],
        unique: true,
      },
      // Sparse index (only index documents with field)
      {
        name: 'posts_by_featured',
        fields: [{ field: 'featuredAt' }],
        sparse: true,
      },
      // TTL index (auto-expire documents)
      {
        name: 'expire_drafts',
        fields: [{ field: 'createdAt' }],
        expireAfterSeconds: 86400 * 30,
        partialFilterExpression: { status: 'draft' },
      },
    ],
    title: 'string!',
    slug: 'string!',
    author: '-> User.posts',
    status: 'string = "draft"',
    createdAt: 'timestamp!',
    featuredAt: 'timestamp',
  },
}
```

### Full-Text Search (FTS) Indexes

Configure FTS with language and weight options:

```typescript
const schema: Schema = {
  Article: {
    title: {
      type: 'string',
      required: true,
      index: 'fts',
      ftsOptions: {
        language: 'english',
        weight: 2.0,  // Title matches rank higher
      },
    },
    content: {
      type: 'text',
      index: 'fts',
      ftsOptions: {
        language: 'english',
        weight: 1.0,
      },
    },
  },
}
```

### Vector Indexes

Configure vector similarity search:

```typescript
const schema: Schema = {
  Document: {
    embedding: {
      type: 'vector(1536)',
      index: 'vector',
      dimensions: 1536,
      metric: 'cosine',  // or 'euclidean', 'dotProduct'
    },
  },
}
```

---

## Type-Level Metadata

### `$type` - JSON-LD Type URI

Link types to schema.org or custom vocabularies:

```typescript
const schema: Schema = {
  User: {
    $type: 'schema:Person',
    name: 'string!',
    email: 'email!',
  },
  Post: {
    $type: 'schema:BlogPosting',
    title: 'string!',
    content: 'markdown!',
  },
  Organization: {
    $type: 'https://schema.org/Organization',
    name: 'string!',
  },
}
```

### `$description` - Type Documentation

Add descriptions to types:

```typescript
const schema: Schema = {
  User: {
    $type: 'schema:Person',
    $description: 'A registered user in the system',
    name: 'string!',
  },
}
```

### `$shred` - Variant Shredding

Specify fields to extract from Variant for columnar efficiency:

```typescript
const schema: Schema = {
  Event: {
    // These fields will be stored as separate Parquet columns
    $shred: ['eventType', 'timestamp', 'userId'],

    eventType: 'string!',
    timestamp: 'timestamp!',
    userId: 'uuid!',
    payload: 'json',  // Kept in Variant blob
  },
}
```

### `$abstract` - Abstract Types

Define types that cannot be instantiated directly:

```typescript
const schema: Schema = {
  BaseContent: {
    $abstract: true,
    title: 'string!',
    createdAt: 'timestamp!',
    author: '-> User.content',
  },
  Post: {
    $extends: 'BaseContent',
    content: 'markdown!',
  },
  Video: {
    $extends: 'BaseContent',
    url: 'url!',
    duration: 'int!',
  },
}
```

### `$ns` - Default Namespace

Set the default namespace for entities of this type:

```typescript
const schema: Schema = {
  User: {
    $ns: 'https://example.com/users',
    name: 'string!',
  },
}
```

---

## GraphDL Integration

ParqueDB integrates with [GraphDL](https://github.com/primitives-org/graphdl) for schema definition:

```typescript
import { Graph } from '@graphdl/core'
import { fromGraphDL } from 'parquedb'

// Define schema using GraphDL's Graph() function
const graphdlSchema = Graph`
  # Users in the system
  User {
    $type: schema:Person
    name: string!
    email: email!
    bio: text
    posts: <- Post.author[]
  }

  # Blog posts
  Post {
    $type: schema:BlogPosting
    title: string!
    content: markdown!
    status: string = "draft"
    author: -> User.posts
    tags: -> Tag.posts[]
    comments: <- Comment.post[]
  }

  # Comments on posts
  Comment {
    text: string!
    post: -> Post.comments
    author: -> User.comments
  }

  # Tags for categorization
  Tag {
    name: string!
    posts: <- Post.tags[]
  }
`

// Convert to ParqueDB schema
const schema = fromGraphDL(graphdlSchema)

// Use with ParqueDB
const db = new ParqueDB({ schema })
```

### GraphDL Features

GraphDL supports:

- **Forward relationships**: `author: -> User.posts`
- **Backward relationships**: `posts: <- Post.author[]`
- **Fuzzy relationships**: `similar: ~> Product.similarTo[]`
- **Type URIs**: `$type: schema:Person`
- **Required fields**: `name: string!`
- **Array types**: `tags: string[]`
- **Default values**: `status: string = "draft"`

---

## IceType Integration

ParqueDB also integrates with [IceType](https://github.com/primitives-org/icetype) for advanced schema features:

```typescript
import { graphToIceType } from '@icetype/core'
import { fromIceType } from 'parquedb'

// Convert GraphDL to IceType (or define directly)
const iceSchemas = graphToIceType(graphdlSchema)

// Or use the Map<string, IceTypeSchema> format directly
const schema = fromIceType(iceSchemas)
```

### IceType-Specific Features

IceType adds support for:

#### Directives

```typescript
// IceType directives for advanced features
const schema = {
  Product: {
    // Partitioning (maps to $shred in ParqueDB)
    $partitionBy: ['category', 'createdAt'],

    // FTS indexes
    $fts: ['name', 'description'],

    // Vector indexes
    $vector: [
      { field: 'embedding', dimensions: 1536, metric: 'cosine' },
    ],

    // Compound indexes
    $index: [
      ['category', 'price'],
      { fields: ['sku'], unique: true },
    ],

    // Unique constraints
    $unique: [['sku'], ['slug', 'category']],

    // Fields
    name: 'string!',
    description: 'text',
    price: 'decimal(10,2)!',
    sku: 'string!',
    slug: 'string!',
    category: '-> Category.products',
  },
}
```

#### Field Validation

```typescript
const schema = {
  User: {
    username: {
      type: 'string',
      required: true,
      validation: {
        minLength: 3,
        maxLength: 50,
        pattern: '^[a-z0-9_]+$',
      },
      description: 'Unique username (3-50 lowercase alphanumeric)',
    },
    age: {
      type: 'int',
      validation: {
        min: 0,
        max: 150,
      },
    },
  },
}
```

---

## Complete Examples

### Blog Schema

```typescript
import type { Schema } from 'parquedb'

export const BlogSchema: Schema = {
  User: {
    $type: 'schema:Person',
    $description: 'A registered blog user',
    $shred: ['email', 'createdAt'],

    name: 'string!',
    email: {
      type: 'email',
      required: true,
      index: 'unique',
    },
    bio: {
      type: 'text',
      index: 'fts',
    },
    avatar: 'url',
    createdAt: 'timestamp!',

    // Relationships
    posts: '<- Post.author[]',
    comments: '<- Comment.author[]',
  },

  Post: {
    $type: 'schema:BlogPosting',
    $shred: ['status', 'publishedAt'],
    $indexes: [
      {
        name: 'posts_by_author_date',
        fields: [
          { field: 'authorId' },
          { field: 'publishedAt', direction: -1 },
        ],
      },
    ],

    title: {
      type: 'string',
      required: true,
      index: 'fts',
      ftsOptions: { weight: 2.0 },
    },
    slug: 'string##!',  // Unique index
    content: {
      type: 'markdown',
      required: true,
      index: 'fts',
    },
    excerpt: 'text',
    status: 'enum(draft,published,archived) = "draft"',
    publishedAt: 'datetime',
    createdAt: 'timestamp!',

    // Relationships
    author: '-> User.posts',
    tags: '-> Tag.posts[]',
    comments: '<- Comment.post[]',
  },

  Comment: {
    $shred: ['createdAt'],

    text: 'string!',
    createdAt: 'timestamp!',

    // Relationships
    post: '-> Post.comments',
    author: '-> User.comments',
    parent: '-> Comment.replies',
    replies: '<- Comment.parent[]',
  },

  Tag: {
    name: 'string##!',  // Unique
    slug: 'string##!',  // Unique
    description: 'text',

    // Relationships
    posts: '<- Post.tags[]',
  },
}
```

### E-Commerce Schema

```typescript
export const ECommerceSchema: Schema = {
  Customer: {
    $type: 'schema:Customer',

    name: 'string!',
    email: 'email##!',
    phone: 'string',

    orders: '<- Order.customer[]',
    addresses: '<- Address.customer[]',
  },

  Product: {
    $type: 'schema:Product',
    $shred: ['price', 'stock', 'category'],
    $indexes: [
      {
        name: 'products_by_category_price',
        fields: [{ field: 'category' }, { field: 'price' }],
      },
    ],

    name: {
      type: 'string',
      required: true,
      index: 'fts',
    },
    description: {
      type: 'markdown',
      index: 'fts',
    },
    sku: 'string##!',
    price: 'decimal(10,2)!',
    stock: 'int = 0',

    embedding: {
      type: 'vector(1536)',
      index: 'vector',
      dimensions: 1536,
      metric: 'cosine',
    },

    category: '-> Category.products',
    similar: '~> Product.similarTo[]',
    orderItems: '<- OrderItem.product[]',
  },

  Category: {
    name: 'string!',
    slug: 'string##!',

    parent: '-> Category.children',
    children: '<- Category.parent[]',
    products: '<- Product.category[]',
  },

  Order: {
    $shred: ['status', 'createdAt'],
    $indexes: [
      {
        name: 'orders_by_customer_date',
        fields: [
          { field: 'customerId' },
          { field: 'createdAt', direction: -1 },
        ],
      },
    ],

    orderNumber: 'string##!',
    total: 'decimal(10,2)!',
    status: 'enum(pending,paid,shipped,delivered,cancelled) = "pending"',
    createdAt: 'timestamp!',

    customer: '-> Customer.orders',
    items: '<- OrderItem.order[]',
    shippingAddress: '-> Address.orders',
  },

  OrderItem: {
    quantity: 'int!',
    unitPrice: 'decimal(10,2)!',
    subtotal: 'decimal(10,2)!',

    order: '-> Order.items',
    product: '-> Product.orderItems',
  },

  Address: {
    street: 'string!',
    city: 'string!',
    state: 'string',
    postalCode: 'string!',
    country: 'string!',
    isDefault: 'boolean = false',

    customer: '-> Customer.addresses',
    orders: '<- Order.shippingAddress[]',
  },
}
```

### UNSPSC Taxonomy Schema

```typescript
// Hierarchical taxonomy example
export const UNSPSCSchema: Schema = {
  Segment: {
    $type: 'schema:DefinedTerm',
    $description: 'UNSPSC Segment - Top-level 2-digit category',

    code: {
      type: 'string',
      index: 'unique',
      description: '2-digit segment code',
    },
    title: {
      type: 'string',
      index: 'fts',
    },
    isActive: 'boolean = true',

    families: '<- Family.segment[]',
  },

  Family: {
    $type: 'schema:DefinedTerm',

    code: 'string##!',
    title: 'string#fts!',
    segmentCode: 'string#!',
    isActive: 'boolean = true',

    segment: '-> Segment.families',
    classes: '<- Class.family[]',
  },

  Class: {
    $type: 'schema:DefinedTerm',

    code: 'string##!',
    title: 'string#fts!',
    familyCode: 'string#!',
    segmentCode: 'string#!',
    isActive: 'boolean = true',

    family: '-> Family.classes',
    commodities: '<- Commodity.class[]',
  },

  Commodity: {
    $type: 'schema:DefinedTerm',

    code: 'string##!',
    title: 'string#fts!',
    classCode: 'string#!',
    familyCode: 'string#!',
    segmentCode: 'string#!',
    isActive: 'boolean = true',

    class: '-> Class.commodities',
  },
}
```

---

## Best Practices

1. **Use Type URIs** - Link to schema.org or custom vocabularies with `$type` for interoperability.

2. **Specify Shred Fields** - Use `$shred` for frequently queried fields to enable predicate pushdown.

3. **Index Strategically** - Only index fields used in queries; excessive indexes slow writes.

4. **Use Compound Indexes** - For queries filtering on multiple fields, define compound indexes in the right order.

5. **Prefer Forward Relationships** - Define relationships from the "many" side pointing to the "one" side.

6. **Document Relationships** - Use backward relationships (`<-`) to document the reverse direction.

7. **Leverage FTS** - Use full-text search indexes for user-facing search features.

8. **Consider Vector Search** - For semantic search, add vector embeddings with appropriate metrics.

9. **Use Enums** - For fields with known value sets, use `enum(...)` for validation.

10. **Add Descriptions** - Use `$description` and field-level `description` for schema documentation.

---

## Next Steps

- [Getting Started](./getting-started.md) - Basic CRUD operations
- [Queries](./queries.md) - Advanced filtering and querying
- [Relationships](./relationships.md) - Working with graph relationships
- [Storage Backends](./storage-backends.md) - Configuring storage
