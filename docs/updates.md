---
title: Update Operators
description: MongoDB-style update operators for modifying documents in ParqueDB
---

ParqueDB uses MongoDB-style update operators to modify documents. This guide covers all available operators with examples.

## Basic Usage

Update operators are passed to the `update()` method:

```typescript
await db.Posts.update(post.$id, {
  $set: { status: 'published' }
})
```

Multiple operators can be combined in a single update:

```typescript
await db.Posts.update(post.$id, {
  $set: { status: 'published' },
  $inc: { version: 1 },
  $currentDate: { updatedAt: true }
})
```

## Field Operators

### $set - Set Field Values

Sets the value of one or more fields:

```typescript
// Set a single field
await db.Posts.update(post.$id, {
  $set: { status: 'published' }
})

// Set multiple fields
await db.Posts.update(post.$id, {
  $set: {
    status: 'published',
    publishedAt: new Date(),
    featured: true
  }
})

// Set nested fields using dot notation
await db.Users.update(user.$id, {
  $set: {
    'profile.bio': 'New bio',
    'settings.theme': 'dark'
  }
})

// Create nested structure if it doesn't exist
await db.Users.update(user.$id, {
  $set: {
    'preferences.notifications.email': true
  }
})
```

### $unset - Remove Fields

Removes one or more fields from a document:

```typescript
// Remove a single field
await db.Posts.update(post.$id, {
  $unset: { temporaryData: true }
})

// Remove multiple fields (value can be '', 1, or true)
await db.Posts.update(post.$id, {
  $unset: {
    draft: '',
    preview: 1,
    tempFlag: true
  }
})

// Remove nested fields
await db.Users.update(user.$id, {
  $unset: {
    'settings.deprecated': true
  }
})
```

### $rename - Rename Fields

Renames a field:

```typescript
// Rename a field
await db.Posts.update(post.$id, {
  $rename: { 'oldFieldName': 'newFieldName' }
})

// Rename multiple fields
await db.Posts.update(post.$id, {
  $rename: {
    'legacy_status': 'status',
    'old_category': 'category'
  }
})

// Move field to nested location
await db.Users.update(user.$id, {
  $rename: { 'bio': 'profile.bio' }
})
```

### $setOnInsert - Set on Insert Only

Sets field values only when creating a new document (during upsert):

```typescript
// Only sets these fields if the document is being inserted
await db.Posts.update(
  'new-post-id',
  {
    $set: { status: 'draft', title: 'New Post' },
    $setOnInsert: {
      createdAt: new Date(),
      viewCount: 0,
      version: 1
    }
  },
  { upsert: true }
)
```

This is particularly useful with upsert operations where you want default values only on creation.

## Numeric Operators

### $inc - Increment

Increments numeric fields by a specified amount:

```typescript
// Increment by 1
await db.Posts.update(post.$id, {
  $inc: { viewCount: 1 }
})

// Increment by any amount
await db.Posts.update(post.$id, {
  $inc: { viewCount: 10 }
})

// Decrement (use negative value)
await db.Products.update(product.$id, {
  $inc: { stock: -1 }
})

// Increment multiple fields
await db.Games.update(game.$id, {
  $inc: {
    score: 100,
    level: 1,
    lives: -1
  }
})

// Works with floating point
await db.Accounts.update(account.$id, {
  $inc: { balance: 49.99 }
})
```

### $mul - Multiply

Multiplies numeric fields by a specified value:

```typescript
// Double a value
await db.Products.update(product.$id, {
  $mul: { price: 2 }
})

// Apply a percentage (10% increase)
await db.Products.update(product.$id, {
  $mul: { price: 1.1 }
})

// Apply discount (20% off)
await db.Products.update(product.$id, {
  $mul: { price: 0.8 }
})

// Multiply multiple fields
await db.Stats.update(stats.$id, {
  $mul: {
    visits: 1.5,
    score: 2
  }
})
```

### $min - Set Minimum

Updates the field only if the specified value is less than the current value:

```typescript
// Set minimum (keeps the smaller value)
await db.Stats.update(stats.$id, {
  $min: { lowScore: 50 }
})

// If current lowScore is 100, it becomes 50
// If current lowScore is 30, it stays 30

// Works with dates (keeps earlier date)
await db.Events.update(event.$id, {
  $min: { firstSeen: new Date() }
})
```

### $max - Set Maximum

Updates the field only if the specified value is greater than the current value:

```typescript
// Set maximum (keeps the larger value)
await db.Stats.update(stats.$id, {
  $max: { highScore: 1000 }
})

// If current highScore is 500, it becomes 1000
// If current highScore is 1500, it stays 1500

// Works with dates (keeps later date)
await db.Events.update(event.$id, {
  $max: { lastSeen: new Date() }
})
```

## Array Operators

### $push - Add to Array

Appends a value to an array:

```typescript
// Push a single value
await db.Posts.update(post.$id, {
  $push: { tags: 'featured' }
})

// Push an object
await db.Posts.update(post.$id, {
  $push: {
    comments: {
      author: 'users/alice',
      text: 'Great post!',
      createdAt: new Date()
    }
  }
})
```

#### $push with Modifiers

Use modifiers for advanced array operations:

```typescript
// Push multiple values with $each
await db.Posts.update(post.$id, {
  $push: {
    tags: {
      $each: ['featured', 'pinned', 'trending']
    }
  }
})

// Push at specific position with $position
await db.Posts.update(post.$id, {
  $push: {
    tags: {
      $each: ['urgent'],
      $position: 0  // Insert at beginning
    }
  }
})

// Limit array size with $slice
await db.Posts.update(post.$id, {
  $push: {
    recentViews: {
      $each: [{ userId: 'users/bob', at: new Date() }],
      $slice: -10  // Keep only last 10 items
    }
  }
})

// Sort after push with $sort
await db.Leaderboards.update(board.$id, {
  $push: {
    scores: {
      $each: [{ player: 'alice', score: 1500 }],
      $sort: { score: -1 },  // Sort descending by score
      $slice: 10             // Keep top 10
    }
  }
})
```

### $pull - Remove from Array

Removes all matching values from an array:

```typescript
// Remove specific value
await db.Posts.update(post.$id, {
  $pull: { tags: 'deprecated' }
})

// Remove by condition (filter)
await db.Posts.update(post.$id, {
  $pull: {
    comments: { author: 'users/spammer' }
  }
})

// Remove with comparison operators
await db.Stats.update(stats.$id, {
  $pull: {
    scores: { $lt: 10 }  // Remove all scores less than 10
  }
})
```

### $pullAll - Remove Multiple Values

Removes all specified values from an array:

```typescript
// Remove multiple specific values
await db.Posts.update(post.$id, {
  $pullAll: {
    tags: ['obsolete', 'deprecated', 'legacy']
  }
})
```

### $addToSet - Add Unique Value

Adds a value to an array only if it doesn't already exist:

```typescript
// Add unique value
await db.Posts.update(post.$id, {
  $addToSet: { tags: 'featured' }
})
// If 'featured' already exists, no change is made

// Add multiple unique values with $each
await db.Posts.update(post.$id, {
  $addToSet: {
    tags: {
      $each: ['react', 'javascript', 'typescript']
    }
  }
})
// Only adds values that don't already exist
```

### $pop - Remove First or Last Element

Removes the first or last element from an array:

```typescript
// Remove last element (1)
await db.Queues.update(queue.$id, {
  $pop: { items: 1 }
})

// Remove first element (-1)
await db.Queues.update(queue.$id, {
  $pop: { items: -1 }
})
```

## Date Operators

### $currentDate - Set to Current Date

Sets a field to the current date:

```typescript
// Set to current date
await db.Posts.update(post.$id, {
  $currentDate: { updatedAt: true }
})

// Specify type
await db.Posts.update(post.$id, {
  $currentDate: {
    lastModified: { $type: 'date' },
    lastModifiedTs: { $type: 'timestamp' }
  }
})

// Combine with other operators
await db.Posts.update(post.$id, {
  $set: { status: 'published' },
  $currentDate: { publishedAt: true }
})
```

## Relationship Operators

ParqueDB extends MongoDB operators with relationship-specific operators.

### $link - Create Relationships

Creates relationships to other entities:

```typescript
// Link to single entity
await db.Posts.update(post.$id, {
  $link: { author: 'users/alice' }
})

// Link to multiple entities
await db.Posts.update(post.$id, {
  $link: {
    categories: ['categories/tech', 'categories/tutorial']
  }
})

// Multiple relationship types
await db.Posts.update(post.$id, {
  $link: {
    author: 'users/alice',
    categories: ['categories/tech'],
    relatedPosts: ['posts/intro', 'posts/advanced']
  }
})
```

### $unlink - Remove Relationships

Removes relationships to other entities:

```typescript
// Unlink specific entity
await db.Posts.update(post.$id, {
  $unlink: { author: 'users/alice' }
})

// Unlink multiple entities
await db.Posts.update(post.$id, {
  $unlink: {
    categories: ['categories/outdated', 'categories/draft']
  }
})

// Remove all relationships of a type
await db.Posts.update(post.$id, {
  $unlink: { relatedPosts: '$all' }
})
```

## Bitwise Operators

### $bit - Bitwise Operations

Performs bitwise operations on integer fields:

```typescript
// Bitwise AND
await db.Permissions.update(perm.$id, {
  $bit: { flags: { and: 0b1111 } }
})

// Bitwise OR
await db.Permissions.update(perm.$id, {
  $bit: { flags: { or: 0b0100 } }
})

// Bitwise XOR
await db.Permissions.update(perm.$id, {
  $bit: { flags: { xor: 0b0010 } }
})

// Example: Toggle a permission flag
const READ = 0b0001
const WRITE = 0b0010
const DELETE = 0b0100

// Grant write permission
await db.Permissions.update(perm.$id, {
  $bit: { flags: { or: WRITE } }
})

// Revoke delete permission
await db.Permissions.update(perm.$id, {
  $bit: { flags: { and: ~DELETE } }
})
```

## Update Options

### Upsert

Creates a new document if no match is found:

```typescript
await db.Posts.update(
  'posts/new-post',
  {
    $set: { title: 'New Post', content: 'Content here' },
    $setOnInsert: { createdAt: new Date(), viewCount: 0 }
  },
  { upsert: true }
)
```

### Expected Version (Optimistic Concurrency)

Fails if the document version doesn't match:

```typescript
const post = await db.Posts.get(postId)

await db.Posts.update(
  post.$id,
  { $set: { title: 'Updated Title' } },
  { expectedVersion: post.version }
)
// Throws error if version changed since read
```

### Actor Tracking

Records who performed the update:

```typescript
await db.Posts.update(
  post.$id,
  { $set: { status: 'published' } },
  { actor: 'users/admin' }
)

// Entity will have updatedBy: 'users/admin'
```

## Combining Operators

Multiple operators can be combined in a single update for atomic operations:

```typescript
await db.Posts.update(post.$id, {
  // Field updates
  $set: { status: 'published', featured: true },
  $unset: { draft: true },

  // Numeric updates
  $inc: { version: 1 },

  // Array updates
  $push: { tags: 'trending' },
  $pull: { tags: 'draft' },

  // Date updates
  $currentDate: { publishedAt: true },

  // Relationship updates
  $link: { categories: ['categories/featured'] }
})
```

## Update Many

Update multiple documents matching a filter:

```typescript
// Mark all old drafts as archived
await db.Posts.updateMany(
  {
    status: 'draft',
    createdAt: { $lt: new Date('2023-01-01') }
  },
  {
    $set: { status: 'archived' }
  }
)
```

## Error Handling

Updates can fail for several reasons:

```typescript
try {
  await db.Posts.update(post.$id, {
    $set: { status: 'published' }
  }, {
    expectedVersion: 1
  })
} catch (error) {
  if (error.code === 'VERSION_MISMATCH') {
    // Document was modified since read
    console.log('Concurrent modification detected')
  } else if (error.code === 'NOT_FOUND') {
    // Document doesn't exist
    console.log('Document not found')
  } else if (error.code === 'VALIDATION_ERROR') {
    // Update violates schema
    console.log('Validation failed:', error.message)
  }
}
```

## Performance Tips

1. **Batch updates**: Combine multiple field changes in a single update
2. **Use $inc for counters**: More efficient than read-modify-write
3. **Limit array growth**: Use $slice with $push to cap array sizes
4. **Avoid large $pullAll**: For many values, consider restructuring data
5. **Use expectedVersion**: Prevents lost updates in concurrent scenarios

## See Also

- [Query API](./queries.md) - Filter documents with $eq, $gt, $in, etc.
- [Getting Started](./getting-started.md) - Basic CRUD operations
- [Graph-First Architecture](./architecture/GRAPH_FIRST_ARCHITECTURE.md) - Relationship design
