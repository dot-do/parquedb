---
title: Update Operators
description: Complete reference for ParqueDB update operators
---

ParqueDB provides MongoDB-style update operators for modifying documents. This reference covers all available operators with detailed examples and usage patterns.

## Basic Updates

### update() - Update Matching Documents

Updates one or more documents that match a filter:

```typescript
// Update single document by ID
await db.Posts.update(post.$id, {
  $set: { status: 'published' }
})

// Update with filter (updates first match by default)
await db.Posts.update(
  { status: 'draft' },
  { $set: { status: 'archived' } }
)
```

### updateOne() - Update Single Document

Updates exactly one document matching the filter:

```typescript
// Update single matching document
await db.Posts.updateOne(
  { slug: 'hello-world' },
  { $set: { views: 100 } }
)
```

### updateMany() - Update Multiple Documents

Updates all documents matching a filter:

```typescript
// Update all matching documents
await db.Posts.updateMany(
  { status: 'draft', createdAt: { $lt: new Date('2024-01-01') } },
  { $set: { status: 'archived' } }
)
```

### upsert - Update or Insert

Creates a document if no match is found:

```typescript
await db.Posts.update(
  'posts/new-id',
  {
    $set: { title: 'New Post', content: 'Hello' },
    $setOnInsert: { createdAt: new Date(), viewCount: 0 }
  },
  { upsert: true }
)
```

## Field Operators

### $set - Set Field Value

Sets the value of one or more fields in a document.

**Syntax:**

```typescript
{ $set: { <field1>: <value1>, <field2>: <value2>, ... } }
```

**Examples:**

```typescript
// Set single field
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

// Set nested fields with dot notation
await db.Users.update(user.$id, {
  $set: {
    'profile.bio': 'Software Engineer',
    'settings.theme': 'dark',
    'preferences.notifications.email': true
  }
})

// Creates nested structure if it doesn't exist
await db.Users.update(user.$id, {
  $set: { 'address.city': 'New York' }
})
// Result: { address: { city: 'New York' } }
```

**Notes:**
- Creates the field if it doesn't exist
- Replaces the existing value if field exists
- Supports dot notation for nested fields
- Automatically creates intermediate nested objects

### $unset - Remove Field

Removes one or more fields from a document.

**Syntax:**

```typescript
{ $unset: { <field1>: '', <field2>: 1, <field3>: true } }
```

**Examples:**

```typescript
// Remove single field
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

// Remove nested field
await db.Users.update(user.$id, {
  $unset: { 'settings.deprecated': true }
})
```

**Notes:**
- The value (true, 1, or '') is ignored; any truthy value works
- No-op if field doesn't exist
- Removes the entire field from the document

### $rename - Rename Field

Renames a field in a document.

**Syntax:**

```typescript
{ $rename: { <oldFieldName>: <newFieldName> } }
```

**Examples:**

```typescript
// Rename single field
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

// Move from nested to top-level
await db.Users.update(user.$id, {
  $rename: { 'profile.name': 'displayName' }
})
```

**Notes:**
- No-op if source field doesn't exist
- Creates nested structure for target if needed
- Cannot rename to an existing field (will overwrite)

### $setOnInsert - Set Only on Insert

Sets field values only when creating a new document during an upsert operation.

**Syntax:**

```typescript
{ $setOnInsert: { <field1>: <value1>, ... } }
```

**Examples:**

```typescript
// Set default values only on insert
await db.Posts.update(
  'posts/new-id',
  {
    $set: { title: 'New Post', content: 'Hello World' },
    $setOnInsert: {
      createdAt: new Date(),
      viewCount: 0,
      version: 1,
      status: 'draft'
    }
  },
  { upsert: true }
)

// If document exists: only $set is applied
// If document is new: both $set and $setOnInsert are applied
```

**Notes:**
- Only applies during upsert operations when creating new documents
- Ignored during normal updates
- Useful for setting creation timestamps and default values
- Works with other operators

## Numeric Operators

### $inc - Increment

Increments a numeric field by a specified amount.

**Syntax:**

```typescript
{ $inc: { <field1>: <amount1>, <field2>: <amount2>, ... } }
```

**Examples:**

```typescript
// Increment by 1
await db.Posts.update(post.$id, {
  $inc: { viewCount: 1 }
})

// Increment by any amount
await db.Posts.update(post.$id, {
  $inc: { viewCount: 10, shareCount: 5 }
})

// Decrement (use negative value)
await db.Products.update(product.$id, {
  $inc: { stock: -1, reserved: 1 }
})

// Works with floating point
await db.Accounts.update(account.$id, {
  $inc: { balance: 49.99 }
})

// Increment nested field
await db.Stats.update(stats.$id, {
  $inc: { 'analytics.pageViews': 1 }
})
```

**Notes:**
- Creates field with value 0 if it doesn't exist, then applies increment
- More efficient than read-modify-write pattern
- Supports positive and negative values
- Works with integers and floating-point numbers
- Throws error if field exists but is not numeric

### $mul - Multiply

Multiplies a numeric field by a specified value.

**Syntax:**

```typescript
{ $mul: { <field1>: <multiplier1>, <field2>: <multiplier2>, ... } }
```

**Examples:**

```typescript
// Double a value
await db.Products.update(product.$id, {
  $mul: { price: 2 }
})

// Apply percentage increase (10%)
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
    score: 2,
    rating: 0.95
  }
})
```

**Notes:**
- Creates field with value 0 if it doesn't exist
- Result is 0 * multiplier = 0 for non-existent fields
- Supports positive and negative multipliers
- Works with integers and floating-point numbers

### $min - Update if Less Than

Updates the field only if the specified value is less than the current value.

**Syntax:**

```typescript
{ $min: { <field1>: <value1>, <field2>: <value2>, ... } }
```

**Examples:**

```typescript
// Set minimum score
await db.Stats.update(stats.$id, {
  $min: { lowScore: 50 }
})
// If current lowScore is 100, it becomes 50
// If current lowScore is 30, it stays 30

// Track earliest date
await db.Events.update(event.$id, {
  $min: { firstSeen: new Date() }
})

// Works with strings (lexicographic comparison)
await db.Records.update(record.$id, {
  $min: { status: 'active' }
})

// Multiple fields
await db.Stats.update(stats.$id, {
  $min: {
    lowScore: 100,
    minValue: 5.5,
    earliestDate: new Date('2024-01-01')
  }
})
```

**Notes:**
- Sets the field if it doesn't exist
- Compares numbers numerically
- Compares strings lexicographically
- Compares dates chronologically
- No change if current value is already smaller

### $max - Update if Greater Than

Updates the field only if the specified value is greater than the current value.

**Syntax:**

```typescript
{ $max: { <field1>: <value1>, <field2>: <value2>, ... } }
```

**Examples:**

```typescript
// Set maximum score
await db.Stats.update(stats.$id, {
  $max: { highScore: 1000 }
})
// If current highScore is 500, it becomes 1000
// If current highScore is 1500, it stays 1500

// Track latest date
await db.Events.update(event.$id, {
  $max: { lastSeen: new Date() }
})

// Works with strings (lexicographic comparison)
await db.Records.update(record.$id, {
  $max: { priority: 'urgent' }
})

// Multiple fields
await db.Stats.update(stats.$id, {
  $max: {
    highScore: 1000,
    maxValue: 99.9,
    latestDate: new Date()
  }
})
```

**Notes:**
- Sets the field if it doesn't exist
- Compares numbers numerically
- Compares strings lexicographically
- Compares dates chronologically
- No change if current value is already larger

## Array Operators

### $push - Add to Array

Appends a value or values to an array field.

**Syntax:**

```typescript
{ $push: { <arrayField>: <value> } }
// or with modifiers
{ $push: { <arrayField>: { $each: [...], $position: N, $slice: N, $sort: 1|-1 } } }
```

**Examples:**

```typescript
// Push single value
await db.Posts.update(post.$id, {
  $push: { tags: 'featured' }
})

// Push object
await db.Posts.update(post.$id, {
  $push: {
    comments: {
      author: 'users/alice',
      text: 'Great post!',
      createdAt: new Date()
    }
  }
})

// Creates array if field doesn't exist
await db.Posts.update(post.$id, {
  $push: { tags: 'first-tag' }
})
// Result: { tags: ['first-tag'] }
```

**Notes:**
- Creates array with single element if field doesn't exist
- Allows duplicate values (use $addToSet for uniqueness)
- Can push any value type: primitives, objects, arrays

#### Array Modifiers

##### $each - Push Multiple Values

```typescript
// Push multiple values
await db.Posts.update(post.$id, {
  $push: {
    tags: { $each: ['featured', 'pinned', 'trending'] }
  }
})

// Push multiple objects
await db.Posts.update(post.$id, {
  $push: {
    comments: {
      $each: [
        { author: 'users/alice', text: 'Great!' },
        { author: 'users/bob', text: 'Thanks!' }
      ]
    }
  }
})
```

##### $position - Insert at Position

```typescript
// Insert at beginning (position 0)
await db.Posts.update(post.$id, {
  $push: {
    tags: {
      $each: ['urgent', 'breaking'],
      $position: 0
    }
  }
})

// Insert at specific position
await db.Posts.update(post.$id, {
  $push: {
    items: {
      $each: ['new-item'],
      $position: 2
    }
  }
})
```

##### $slice - Limit Array Size

```typescript
// Keep only last 10 items
await db.Posts.update(post.$id, {
  $push: {
    recentViews: {
      $each: [{ userId: 'users/alice', timestamp: new Date() }],
      $slice: -10  // Negative keeps last N
    }
  }
})

// Keep only first 5 items
await db.Posts.update(post.$id, {
  $push: {
    topItems: {
      $each: ['new-item'],
      $slice: 5  // Positive keeps first N
    }
  }
})

// Empty array
await db.Posts.update(post.$id, {
  $push: {
    temp: {
      $each: [],
      $slice: 0
    }
  }
})
```

##### $sort - Sort After Push

```typescript
// Sort ascending
await db.Leaderboard.update(board.$id, {
  $push: {
    scores: {
      $each: [75, 92, 88],
      $sort: 1
    }
  }
})

// Sort descending
await db.Leaderboard.update(board.$id, {
  $push: {
    scores: {
      $each: [75, 92, 88],
      $sort: -1
    }
  }
})

// Sort by field in objects
await db.Leaderboard.update(board.$id, {
  $push: {
    players: {
      $each: [
        { name: 'alice', score: 1500 },
        { name: 'bob', score: 1200 }
      ],
      $sort: { score: -1 }  // Sort by score descending
    }
  }
})

// Sort by multiple fields
await db.Records.update(record.$id, {
  $push: {
    entries: {
      $each: [{ category: 'A', value: 100 }],
      $sort: { category: 1, value: -1 }
    }
  }
})
```

##### Combined Modifiers

```typescript
// Maintain a sorted, limited leaderboard
await db.Leaderboard.update(board.$id, {
  $push: {
    topScores: {
      $each: [{ player: 'alice', score: 1500 }],
      $sort: { score: -1 },  // Sort descending by score
      $slice: 10             // Keep only top 10
    }
  }
})

// Insert at position, then sort and limit
await db.Queue.update(queue.$id, {
  $push: {
    items: {
      $each: ['high-priority'],
      $position: 0,
      $sort: 1,
      $slice: 100
    }
  }
})
```

**Modifier Notes:**
- $each is required when using other modifiers
- Modifiers are applied in order: $each → $position → $sort → $slice
- $sort can be 1 (ascending), -1 (descending), or object for field-based sorting
- $slice can be positive (first N), negative (last N), or 0 (empty)

### $pull - Remove from Array

Removes all array elements that match a specified condition.

**Syntax:**

```typescript
{ $pull: { <arrayField>: <value | condition> } }
```

**Examples:**

```typescript
// Remove specific value
await db.Posts.update(post.$id, {
  $pull: { tags: 'deprecated' }
})

// Remove all occurrences
await db.Posts.update(post.$id, {
  $pull: { tags: 'spam' }
})
// Removes all 'spam' entries from tags array

// Remove by condition with comparison operators
await db.Stats.update(stats.$id, {
  $pull: { scores: { $lt: 10 } }
})
// Removes all scores less than 10

await db.Posts.update(post.$id, {
  $pull: { scores: { $gte: 100 } }
})
// Removes all scores >= 100

// Remove objects matching condition
await db.Posts.update(post.$id, {
  $pull: {
    comments: { author: 'users/spammer' }
  }
})
// Removes all comments by spammer

// Remove with complex condition
await db.Posts.update(post.$id, {
  $pull: {
    items: { status: 'deleted', age: { $gt: 30 } }
  }
})
// Removes items where status='deleted' AND age > 30

// Remove with $in operator
await db.Posts.update(post.$id, {
  $pull: {
    tags: { $in: ['obsolete', 'deprecated', 'old'] }
  }
})
```

**Supported Comparison Operators:**
- `$eq` - Equal to
- `$ne` - Not equal to
- `$gt` - Greater than
- `$gte` - Greater than or equal to
- `$lt` - Less than
- `$lte` - Less than or equal to
- `$in` - In array
- `$nin` - Not in array

**Notes:**
- Removes all matching elements, not just first match
- No-op if field doesn't exist or is not an array
- For objects, can match exact object or use conditions on fields
- All conditions in object must match (AND logic)

### $pullAll - Remove Multiple Values

Removes all specified values from an array.

**Syntax:**

```typescript
{ $pullAll: { <arrayField>: [<value1>, <value2>, ...] } }
```

**Examples:**

```typescript
// Remove multiple specific values
await db.Posts.update(post.$id, {
  $pullAll: {
    tags: ['obsolete', 'deprecated', 'legacy']
  }
})

// Remove all occurrences of each value
await db.Stats.update(stats.$id, {
  $pullAll: {
    scores: [0, -1, null]
  }
})

// Remove multiple objects
await db.Posts.update(post.$id, {
  $pullAll: {
    items: [
      { id: 1, name: 'a' },
      { id: 2, name: 'b' }
    ]
  }
})
```

**Notes:**
- Removes all occurrences of each specified value
- Values must match exactly (deep equality for objects)
- No-op if field doesn't exist or is not an array
- Empty array in $pullAll has no effect

### $addToSet - Add Unique Value

Adds a value to an array only if it doesn't already exist (maintains uniqueness).

**Syntax:**

```typescript
{ $addToSet: { <arrayField>: <value> } }
// or with $each
{ $addToSet: { <arrayField>: { $each: [<value1>, <value2>, ...] } } }
```

**Examples:**

```typescript
// Add unique value
await db.Posts.update(post.$id, {
  $addToSet: { tags: 'featured' }
})
// Only adds 'featured' if not already in tags

// Add multiple unique values with $each
await db.Posts.update(post.$id, {
  $addToSet: {
    tags: {
      $each: ['react', 'javascript', 'typescript']
    }
  }
})
// Only adds values not already present

// Add unique object
await db.Collections.update(collection.$id, {
  $addToSet: {
    items: { id: 'item-1', name: 'Widget' }
  }
})

// Creates array if doesn't exist
await db.Posts.update(post.$id, {
  $addToSet: { tags: 'first' }
})
// Result: { tags: ['first'] }
```

**Notes:**
- Uses deep equality for comparison (works with objects)
- Creates array if field doesn't exist
- No-op if value already exists
- $each allows adding multiple unique values
- Order is not guaranteed

### $pop - Remove First or Last Element

Removes the first or last element from an array.

**Syntax:**

```typescript
{ $pop: { <arrayField>: 1 | -1 } }
```

**Examples:**

```typescript
// Remove last element
await db.Queue.update(queue.$id, {
  $pop: { items: 1 }
})

// Remove first element
await db.Queue.update(queue.$id, {
  $pop: { items: -1 }
})

// Use as FIFO queue (remove from front)
await db.Queue.update(queue.$id, {
  $pop: { pending: -1 }
})

// Use as stack (remove from back)
await db.Stack.update(stack.$id, {
  $pop: { items: 1 }
})
```

**Notes:**
- Value must be 1 (remove last) or -1 (remove first)
- No-op on empty array
- Creates empty array if field doesn't exist

## Date Operators

### $currentDate - Set to Current Date

Sets a field to the current date/time.

**Syntax:**

```typescript
{ $currentDate: { <field1>: true | { $type: 'date' | 'timestamp' } } }
```

**Examples:**

```typescript
// Set to current Date object
await db.Posts.update(post.$id, {
  $currentDate: { updatedAt: true }
})

// Explicit date type
await db.Posts.update(post.$id, {
  $currentDate: {
    lastModified: { $type: 'date' }
  }
})
// Stores as Date object

// Timestamp type (milliseconds)
await db.Posts.update(post.$id, {
  $currentDate: {
    lastModifiedTs: { $type: 'timestamp' }
  }
})
// Stores as number (Date.now())

// Multiple date fields
await db.Posts.update(post.$id, {
  $currentDate: {
    updatedAt: true,
    lastModified: { $type: 'date' },
    timestamp: { $type: 'timestamp' }
  }
})

// Combine with other operators
await db.Posts.update(post.$id, {
  $set: { status: 'published' },
  $currentDate: { publishedAt: true }
})
```

**Type Options:**
- `true` or `{ $type: 'date' }` - Stores as Date object
- `{ $type: 'timestamp' }` - Stores as number (milliseconds since epoch)

**Notes:**
- Always uses current server time
- Creates field if it doesn't exist
- Useful for automatic timestamp tracking

## Relationship Operators

ParqueDB extends MongoDB operators with graph-specific relationship operators.

### $link - Create Relationships

Creates relationships from the current entity to other entities.

**Syntax:**

```typescript
{ $link: { <predicate>: <entityId> | [<entityId>, ...] } }
```

**Examples:**

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

// Combine with field updates
await db.Posts.update(post.$id, {
  $set: { status: 'published' },
  $link: { publishedBy: 'users/admin' }
})
```

**Notes:**
- Predicate is the relationship name (edge type)
- Supports both single entity and array of entities
- Creates bidirectional relationships (forward and reverse indexes)
- Does not remove existing links (additive)

### $unlink - Remove Relationships

Removes relationships from the current entity to other entities.

**Syntax:**

```typescript
{ $unlink: { <predicate>: <entityId> | [<entityId>, ...] | '$all' } }
```

**Examples:**

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

// Multiple unlink operations
await db.Posts.update(post.$id, {
  $unlink: {
    oldAuthor: 'users/former',
    categories: '$all'
  }
})
```

**Notes:**
- '$all' removes all relationships for the predicate
- Specific entity IDs remove only those relationships
- No-op if relationship doesn't exist
- Updates both forward and reverse relationship indexes

## Bitwise Operators

### $bit - Bitwise Operations

Performs bitwise operations on integer fields.

**Syntax:**

```typescript
{ $bit: { <field>: { and?: <mask>, or?: <mask>, xor?: <mask> } } }
```

**Examples:**

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

// Multiple operations (applied in order: and, or, xor)
await db.Permissions.update(perm.$id, {
  $bit: {
    flags: {
      and: 0b1010,  // First: AND
      or: 0b0001    // Then: OR
    }
  }
})

// Permission flag example
const READ = 0b0001    // 1
const WRITE = 0b0010   // 2
const DELETE = 0b0100  // 4
const ADMIN = 0b1000   // 8

// Grant write permission
await db.Permissions.update(perm.$id, {
  $bit: { permissions: { or: WRITE } }
})

// Revoke delete permission
await db.Permissions.update(perm.$id, {
  $bit: { permissions: { and: ~DELETE } }
})

// Toggle admin flag
await db.Permissions.update(perm.$id, {
  $bit: { permissions: { xor: ADMIN } }
})
```

**Operation Order:**
1. `and` - Applied first
2. `or` - Applied second
3. `xor` - Applied last

**Notes:**
- Creates field with value 0 if it doesn't exist
- All operations are applied sequentially
- Useful for permission flags, feature flags, and bit masks
- Works only with integer values

## Update Options

### upsert

Creates a document if no match is found:

```typescript
await db.Posts.update(
  { slug: 'unique-slug' },
  {
    $set: { title: 'Title', content: 'Content' },
    $setOnInsert: { createdAt: new Date() }
  },
  { upsert: true }
)
```

### arrayFilters

Applies update to array elements matching a condition:

```typescript
await db.Posts.update(
  post.$id,
  {
    $set: { 'comments.$[elem].status': 'approved' }
  },
  {
    arrayFilters: [{ 'elem.author': 'users/alice' }]
  }
)
```

### multi

Updates multiple documents (for updateMany):

```typescript
await db.Posts.updateMany(
  { status: 'draft' },
  { $set: { status: 'archived' } }
)
```

### expectedVersion (Optimistic Concurrency)

Fails if document version doesn't match:

```typescript
const post = await db.Posts.get(postId)

await db.Posts.update(
  post.$id,
  { $set: { title: 'Updated' } },
  { expectedVersion: post.version }
)
// Throws error if version changed since read
```

### actor

Records who performed the update:

```typescript
await db.Posts.update(
  post.$id,
  { $set: { status: 'published' } },
  { actor: 'users/admin' }
)
// Sets updatedBy: 'users/admin'
```

## Combining Operators

Multiple operators can be combined in a single atomic update:

```typescript
await db.Posts.update(post.$id, {
  // Field updates
  $set: { status: 'published', featured: true },
  $unset: { draft: true },

  // Numeric updates
  $inc: { version: 1, viewCount: 0 },

  // Array updates
  $push: { tags: 'trending' },
  $pull: { tags: 'draft' },
  $addToSet: { keywords: 'important' },

  // Date updates
  $currentDate: { publishedAt: true, updatedAt: true },

  // Relationship updates
  $link: { categories: ['categories/featured'] },
  $unlink: { relatedDrafts: '$all' }
})
```

**Operator Conflicts:**

You cannot use conflicting operators on the same field:

```typescript
// ERROR: Conflicting operators on 'count'
await db.Posts.update(post.$id, {
  $set: { count: 10 },
  $inc: { count: 1 }
})
// Throws: "Conflicting operators: field 'count' modified by multiple operators"
```

## Error Handling

Updates can fail for various reasons:

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

**Common Error Codes:**
- `VERSION_MISMATCH` - Optimistic locking failure
- `NOT_FOUND` - Document doesn't exist (and upsert not enabled)
- `VALIDATION_ERROR` - Schema validation failure
- `INVALID_OPERATOR` - Unknown or invalid operator
- `CONFLICTING_OPERATORS` - Same field modified by multiple operators
- `TYPE_ERROR` - Operator applied to wrong type (e.g., $inc on string)

## Security Considerations

### Prototype Pollution Protection

ParqueDB validates all field paths to prevent prototype pollution:

```typescript
// UNSAFE: These throw errors
await db.Posts.update(post.$id, {
  $set: { '__proto__.isAdmin': true }  // Error: Unsafe path
})

await db.Posts.update(post.$id, {
  $set: { 'constructor.prototype.isAdmin': true }  // Error: Unsafe path
})
```

**Protected Path Segments:**
- `__proto__`
- `constructor`
- `prototype`

## Performance Tips

1. **Batch Updates**: Combine multiple field changes in a single update

```typescript
// Good: Single update
await db.Posts.update(post.$id, {
  $set: { status: 'published', featured: true },
  $inc: { version: 1 }
})

// Bad: Multiple updates
await db.Posts.update(post.$id, { $set: { status: 'published' } })
await db.Posts.update(post.$id, { $set: { featured: true } })
await db.Posts.update(post.$id, { $inc: { version: 1 } })
```

2. **Use $inc for Counters**: More efficient than read-modify-write

```typescript
// Good: Atomic increment
await db.Posts.update(post.$id, { $inc: { viewCount: 1 } })

// Bad: Read-modify-write
const post = await db.Posts.get(postId)
await db.Posts.update(post.$id, { $set: { viewCount: post.viewCount + 1 } })
```

3. **Limit Array Growth**: Use $slice with $push to cap array sizes

```typescript
// Keep only last 100 items
await db.Logs.update(log.$id, {
  $push: {
    entries: {
      $each: [newEntry],
      $slice: -100
    }
  }
})
```

4. **Avoid Large $pullAll**: For many values, consider restructuring data

```typescript
// If removing many items frequently, consider using a Set structure
// or maintaining a separate "active" flag instead
```

5. **Use expectedVersion**: Prevents lost updates in concurrent scenarios

```typescript
const post = await db.Posts.get(postId)
await db.Posts.update(
  post.$id,
  { $inc: { likes: 1 } },
  { expectedVersion: post.version }
)
```

## See Also

- [Getting Started](./getting-started.md) - Basic CRUD operations
- [Query API](./queries.md) - Filter documents with MongoDB-style queries
- [Schema Definition](./schemas.md) - Define types, validation, and relationships
- [Graph-First Architecture](./architecture/GRAPH_FIRST_ARCHITECTURE.md) - Relationship design
