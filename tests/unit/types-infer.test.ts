/**
 * Type-level tests for ParqueDB schema inference
 *
 * These tests verify that the type inference utilities correctly map
 * schema definition strings to TypeScript types at compile time.
 *
 * Run with: pnpm test tests/types/infer.test-d.ts
 */

import { describe, test, expectTypeOf } from 'vitest'
import type { InferFieldType, InferEntityData, InferCreateData, InferEntity, TypedDBInstance, InferCollections } from '../../src/types/infer'
import type { Entity, EntityData } from '../../src/types/entity'
import type { Collection } from '../../src/ParqueDB/types'
import { DB } from '../../src'

// =============================================================================
// InferFieldType Tests - Basic Types
// =============================================================================

describe('InferFieldType - basic types', () => {
  test('string! -> string', () => {
    expectTypeOf<InferFieldType<'string!'>>().toEqualTypeOf<string>()
  })

  test('string -> string | undefined', () => {
    expectTypeOf<InferFieldType<'string'>>().toEqualTypeOf<string | undefined>()
  })

  test('string? -> string | undefined', () => {
    expectTypeOf<InferFieldType<'string?'>>().toEqualTypeOf<string | undefined>()
  })

  test('text! -> string', () => {
    expectTypeOf<InferFieldType<'text!'>>().toEqualTypeOf<string>()
  })

  test('markdown -> string | undefined', () => {
    expectTypeOf<InferFieldType<'markdown'>>().toEqualTypeOf<string | undefined>()
  })

  test('uuid! -> string', () => {
    expectTypeOf<InferFieldType<'uuid!'>>().toEqualTypeOf<string>()
  })

  test('email! -> string', () => {
    expectTypeOf<InferFieldType<'email!'>>().toEqualTypeOf<string>()
  })

  test('url! -> string', () => {
    expectTypeOf<InferFieldType<'url!'>>().toEqualTypeOf<string>()
  })
})

// =============================================================================
// InferFieldType Tests - Numeric Types
// =============================================================================

describe('InferFieldType - numeric types', () => {
  test('int! -> number', () => {
    expectTypeOf<InferFieldType<'int!'>>().toEqualTypeOf<number>()
  })

  test('int -> number | undefined', () => {
    expectTypeOf<InferFieldType<'int'>>().toEqualTypeOf<number | undefined>()
  })

  test('float! -> number', () => {
    expectTypeOf<InferFieldType<'float!'>>().toEqualTypeOf<number>()
  })

  test('double -> number | undefined', () => {
    expectTypeOf<InferFieldType<'double'>>().toEqualTypeOf<number | undefined>()
  })

  test('number! -> number', () => {
    expectTypeOf<InferFieldType<'number!'>>().toEqualTypeOf<number>()
  })
})

// =============================================================================
// InferFieldType Tests - Boolean Types
// =============================================================================

describe('InferFieldType - boolean types', () => {
  test('boolean! -> boolean', () => {
    expectTypeOf<InferFieldType<'boolean!'>>().toEqualTypeOf<boolean>()
  })

  test('boolean -> boolean | undefined', () => {
    expectTypeOf<InferFieldType<'boolean'>>().toEqualTypeOf<boolean | undefined>()
  })

  test('bool! -> boolean', () => {
    expectTypeOf<InferFieldType<'bool!'>>().toEqualTypeOf<boolean>()
  })
})

// =============================================================================
// InferFieldType Tests - Date Types
// =============================================================================

describe('InferFieldType - date types', () => {
  test('date! -> Date', () => {
    expectTypeOf<InferFieldType<'date!'>>().toEqualTypeOf<Date>()
  })

  test('date -> Date | undefined', () => {
    expectTypeOf<InferFieldType<'date'>>().toEqualTypeOf<Date | undefined>()
  })

  test('datetime! -> Date', () => {
    expectTypeOf<InferFieldType<'datetime!'>>().toEqualTypeOf<Date>()
  })

  test('timestamp -> Date | undefined', () => {
    expectTypeOf<InferFieldType<'timestamp'>>().toEqualTypeOf<Date | undefined>()
  })
})

// =============================================================================
// InferFieldType Tests - Special Types
// =============================================================================

describe('InferFieldType - special types', () => {
  test('json -> unknown', () => {
    expectTypeOf<InferFieldType<'json'>>().toEqualTypeOf<unknown>()
  })

  test('binary -> Uint8Array | undefined', () => {
    expectTypeOf<InferFieldType<'binary'>>().toEqualTypeOf<Uint8Array | undefined>()
  })

  test('binary! -> Uint8Array', () => {
    expectTypeOf<InferFieldType<'binary!'>>().toEqualTypeOf<Uint8Array>()
  })
})

// =============================================================================
// InferFieldType Tests - Indexed Types
// =============================================================================

describe('InferFieldType - indexed types', () => {
  test('string!# -> string (required + indexed)', () => {
    expectTypeOf<InferFieldType<'string!#'>>().toEqualTypeOf<string>()
  })

  test('string!## -> string (required + unique)', () => {
    expectTypeOf<InferFieldType<'string!##'>>().toEqualTypeOf<string>()
  })

  test('string# -> string | undefined (indexed only)', () => {
    expectTypeOf<InferFieldType<'string#'>>().toEqualTypeOf<string | undefined>()
  })

  test('string## -> string | undefined (unique only)', () => {
    expectTypeOf<InferFieldType<'string##'>>().toEqualTypeOf<string | undefined>()
  })

  test('string#! -> string (index + required)', () => {
    expectTypeOf<InferFieldType<'string#!'>>().toEqualTypeOf<string>()
  })
})

// =============================================================================
// InferFieldType Tests - Array Types
// =============================================================================

describe('InferFieldType - array types', () => {
  test('string[] -> string[]', () => {
    expectTypeOf<InferFieldType<'string[]'>>().toEqualTypeOf<string[]>()
  })

  test('int[]! -> number[]', () => {
    expectTypeOf<InferFieldType<'int[]!'>>().toEqualTypeOf<number[]>()
  })

  test('boolean[] -> boolean[]', () => {
    expectTypeOf<InferFieldType<'boolean[]'>>().toEqualTypeOf<boolean[]>()
  })
})

// =============================================================================
// InferFieldType Tests - Forward Relationships
// =============================================================================

describe('InferFieldType - forward relationships', () => {
  test('-> User -> string | null', () => {
    expectTypeOf<InferFieldType<'-> User'>>().toEqualTypeOf<string | null>()
  })

  test('-> User[] -> string[]', () => {
    expectTypeOf<InferFieldType<'-> User[]'>>().toEqualTypeOf<string[]>()
  })

  test('-> Industry.parent -> string | null', () => {
    expectTypeOf<InferFieldType<'-> Industry.parent'>>().toEqualTypeOf<string | null>()
  })

  test('-> User.posts[] -> string[]', () => {
    expectTypeOf<InferFieldType<'-> User.posts[]'>>().toEqualTypeOf<string[]>()
  })
})

// =============================================================================
// InferFieldType Tests - Backward Relationships
// =============================================================================

describe('InferFieldType - backward relationships', () => {
  test('<- Post.author -> string[]', () => {
    expectTypeOf<InferFieldType<'<- Post.author'>>().toEqualTypeOf<string[]>()
  })

  test('<- Industry.parent[] -> string[]', () => {
    expectTypeOf<InferFieldType<'<- Industry.parent[]'>>().toEqualTypeOf<string[]>()
  })

  test('<- Post[] -> string[]', () => {
    expectTypeOf<InferFieldType<'<- Post[]'>>().toEqualTypeOf<string[]>()
  })
})

// =============================================================================
// InferFieldType Tests - Fuzzy Relationships
// =============================================================================

describe('InferFieldType - fuzzy relationships', () => {
  test('~> Topic -> string | null', () => {
    expectTypeOf<InferFieldType<'~> Topic'>>().toEqualTypeOf<string | null>()
  })

  test('~> Topic[] -> string[]', () => {
    expectTypeOf<InferFieldType<'~> Topic[]'>>().toEqualTypeOf<string[]>()
  })

  test('<~ Article -> string[]', () => {
    expectTypeOf<InferFieldType<'<~ Article'>>().toEqualTypeOf<string[]>()
  })

  test('<~ Article[] -> string[]', () => {
    expectTypeOf<InferFieldType<'<~ Article[]'>>().toEqualTypeOf<string[]>()
  })
})

// =============================================================================
// InferEntityData Tests
// =============================================================================

describe('InferEntityData', () => {
  test('infers simple schema', () => {
    type Schema = {
      name: 'string!'
      age: 'int'
      active: 'boolean!'
    }

    type Expected = {
      name: string
      age: number | undefined
      active: boolean
    }

    expectTypeOf<InferEntityData<Schema>>().toEqualTypeOf<Expected>()
  })

  test('infers schema with relationships', () => {
    type Schema = {
      title: 'string!'
      author: '-> User'
      comments: '<- Comment.post[]'
    }

    type Expected = {
      title: string
      author: string | null
      comments: string[]
    }

    expectTypeOf<InferEntityData<Schema>>().toEqualTypeOf<Expected>()
  })

  test('excludes $-prefixed fields', () => {
    type Schema = {
      name: 'string!'
      $id: 'customId'
      $layout: string[]
      $options: { includeDataVariant: true }
    }

    type Expected = {
      name: string
    }

    expectTypeOf<InferEntityData<Schema>>().toEqualTypeOf<Expected>()
  })

  test('Industry schema from user example', () => {
    type IndustrySchema = {
      code: 'string!##'
      name: 'string!#'
      parent: '-> Industry'
      children: '<- Industry.parent[]'
    }

    type Expected = {
      code: string
      name: string
      parent: string | null
      children: string[]
    }

    expectTypeOf<InferEntityData<IndustrySchema>>().toEqualTypeOf<Expected>()
  })
})

// =============================================================================
// InferCreateData Tests
// =============================================================================

describe('InferCreateData', () => {
  test('excludes backward relationships from create data', () => {
    type UserSchema = {
      email: 'string!#'
      name: 'string!'
      role: 'string'
      posts: '<- Post.author[]'  // Backward relationship - should be excluded
    }

    type Expected = {
      email: string
      name: string
      role: string | undefined
      // posts should NOT be here - it's a backward relationship
    }

    expectTypeOf<InferCreateData<UserSchema>>().toEqualTypeOf<Expected>()
  })

  test('includes forward relationships in create data', () => {
    type PostSchema = {
      title: 'string!'
      content: 'text'
      author: '-> User'  // Forward relationship - should be included
      comments: '<- Comment.post[]'  // Backward - should be excluded
    }

    type Expected = {
      title: string
      content: string | undefined
      author: string | null
      // comments should NOT be here
    }

    expectTypeOf<InferCreateData<PostSchema>>().toEqualTypeOf<Expected>()
  })

  test('excludes backward fuzzy relationships', () => {
    type TopicSchema = {
      name: 'string!'
      articles: '<~ Article[]'  // Backward fuzzy - should be excluded
    }

    type Expected = {
      name: string
      // articles should NOT be here
    }

    expectTypeOf<InferCreateData<TopicSchema>>().toEqualTypeOf<Expected>()
  })
})

// =============================================================================
// InferEntity Tests
// =============================================================================

describe('InferEntity', () => {
  test('includes base entity fields plus data', () => {
    type Schema = {
      title: 'string!'
    }

    type InferredEntity = InferEntity<Schema>

    // Should have data field
    expectTypeOf<InferredEntity['title']>().toEqualTypeOf<string>()

    // Should have base entity fields
    expectTypeOf<InferredEntity['$id']>().not.toBeNever()
    expectTypeOf<InferredEntity['$type']>().not.toBeNever()
    expectTypeOf<InferredEntity['name']>().not.toBeNever()
  })
})

// =============================================================================
// InferCollections Tests
// =============================================================================

describe('InferCollections', () => {
  test('infers collections from schema', () => {
    type Schema = {
      User: { email: 'string!'; name: 'string' }
      Post: 'flexible'
    }

    type Collections = InferCollections<Schema>

    // User collection should be typed
    type UserCollection = Collections['User']
    expectTypeOf<UserCollection>().toMatchTypeOf<Collection<{ email: string; name: string | undefined }>>()

    // Post collection should be EntityData (flexible)
    type PostCollection = Collections['Post']
    expectTypeOf<PostCollection>().toMatchTypeOf<Collection<EntityData>>()
  })
})

// =============================================================================
// DB() Integration Tests
// =============================================================================

describe('DB() inference', () => {
  test('db.Industry returns typed collection', async () => {
    const db = DB({
      Industry: {
        code: 'string!##',
        name: 'string!#',
        parent: '-> Industry',
        children: '<- Industry.parent[]',
      },
    })

    // Type assertions on the db object
    const result = await db.Industry.find()

    // These should type-check correctly
    const item = result.items[0]
    if (item) {
      expectTypeOf(item.code).toEqualTypeOf<string>()
      expectTypeOf(item.name).toEqualTypeOf<string>()
      expectTypeOf(item.parent).toEqualTypeOf<string | null>()
      expectTypeOf(item.children).toEqualTypeOf<string[]>()
    }
  })

  test('db with multiple collections', async () => {
    const db = DB({
      User: {
        email: 'string!#',
        displayName: 'string',
        age: 'int',
      },
      Post: {
        title: 'string!',
        content: 'text',
        author: '-> User',
        views: 'int = 0',
      },
    })

    // User collection
    const users = await db.User.find()
    if (users.items[0]) {
      expectTypeOf(users.items[0].email).toEqualTypeOf<string>()
      expectTypeOf(users.items[0].displayName).toEqualTypeOf<string | undefined>()
      expectTypeOf(users.items[0].age).toEqualTypeOf<number | undefined>()
    }

    // Post collection
    const posts = await db.Post.find()
    if (posts.items[0]) {
      expectTypeOf(posts.items[0].title).toEqualTypeOf<string>()
      expectTypeOf(posts.items[0].content).toEqualTypeOf<string | undefined>()
      expectTypeOf(posts.items[0].author).toEqualTypeOf<string | null>()
      expectTypeOf(posts.items[0].views).toEqualTypeOf<number | undefined>()
    }
  })

  test('db with flexible collection', async () => {
    const db = DB({
      Strict: {
        name: 'string!',
      },
      Flexible: 'flexible' as const,
    })

    // Strict collection is typed
    const strict = await db.Strict.find()
    if (strict.items[0]) {
      expectTypeOf(strict.items[0].name).toEqualTypeOf<string>()
    }

    // Flexible collection is EntityData
    const flexible = await db.Flexible.find()
    expectTypeOf(flexible.items).toMatchTypeOf<Entity<EntityData>[]>()
  })
})
