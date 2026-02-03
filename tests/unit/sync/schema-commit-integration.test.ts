/**
 * Schema-Commit Integration Tests
 *
 * Tests for tracking schema evolution in commits with type generation.
 * This validates that:
 * 1. Schema snapshots are stored in commits
 * 2. Types can be generated from schema at any commit
 * 3. Schema diff between commits works
 * 4. Breaking changes are detected between commits
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  createCommit,
  createCommitWithSchema,
  loadCommit,
  saveCommit,
  type DatabaseState
} from '../../../src/sync/commit'
import {
  captureSchema,
  diffSchemas,
  loadSchemaAtCommit,
  saveSchemaSnapshot,
  type SchemaSnapshot
} from '../../../src/sync/schema-snapshot'
import { detectBreakingChanges } from '../../../src/sync/schema-evolution'
import { generateTypeScript } from '../../../src/codegen/typescript'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { ParqueDBConfig } from '../../../src/config/loader'

describe('Schema-Commit Integration', () => {
  let storage: MemoryBackend
  let baseState: DatabaseState

  beforeEach(() => {
    storage = new MemoryBackend()
    baseState = {
      collections: {
        users: {
          dataHash: 'abc123',
          schemaHash: 'def456',
          rowCount: 100
        }
      },
      relationships: {
        forwardHash: 'fwd789',
        reverseHash: 'rev012'
      },
      eventLogPosition: {
        segmentId: 'seg1',
        offset: 42
      }
    }
  })

  describe('createCommitWithSchema', () => {
    it('should create a commit with schema snapshot embedded', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!#',
            name: 'string'
          }
        }
      }

      const schema = await captureSchema(config)
      const commit = await createCommitWithSchema(baseState, schema, {
        message: 'Add user schema',
        author: 'test@example.com'
      })

      expect(commit.state.schema).toBeDefined()
      expect(commit.state.schema.hash).toBe(schema.hash)
      expect(commit.state.schema.collections).toEqual(schema.collections)
    })

    it('should set commitHash on schema snapshot', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          }
        }
      }

      const schema = await captureSchema(config)
      const commit = await createCommitWithSchema(baseState, schema, {
        message: 'Initial commit'
      })

      expect(commit.state.schema.commitHash).toBe(commit.hash)
    })
  })

  describe('loadSchemaAtCommit', () => {
    it('should load schema from commit state', async () => {
      const config: ParqueDBConfig = {
        schema: {
          Post: {
            title: 'string!',
            content: 'text'
          }
        }
      }

      const schema = await captureSchema(config)
      const commit = await createCommitWithSchema(baseState, schema, {
        message: 'Add post schema'
      })

      await saveCommit(storage, commit)

      const loadedSchema = await loadSchemaAtCommit(storage, commit.hash)

      expect(loadedSchema.hash).toBe(schema.hash)
      expect(loadedSchema.collections.Post).toBeDefined()
      expect(loadedSchema.collections.Post.fields).toHaveLength(2)
    })

    it('should load schema from legacy snapshot file as fallback', async () => {
      // Create a commit without embedded schema (legacy format)
      const commit = await createCommit(baseState, {
        message: 'Legacy commit'
      })
      await saveCommit(storage, commit)

      // Save schema snapshot separately
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          }
        }
      }
      const schema = await captureSchema(config)
      schema.commitHash = commit.hash
      await saveSchemaSnapshot(storage, schema)

      const loadedSchema = await loadSchemaAtCommit(storage, commit.hash)

      expect(loadedSchema.hash).toBe(schema.hash)
      expect(loadedSchema.commitHash).toBe(commit.hash)
    })
  })

  describe('Schema evolution across commits', () => {
    it('should track schema changes across commits', async () => {
      // First commit - initial schema
      const config1: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          }
        }
      }
      const schema1 = await captureSchema(config1)
      const commit1 = await createCommitWithSchema(baseState, schema1, {
        message: 'Initial schema'
      })
      await saveCommit(storage, commit1)

      // Second commit - add a field
      const config2: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!',
            name: 'string'
          }
        }
      }
      const schema2 = await captureSchema(config2)
      const commit2 = await createCommitWithSchema(baseState, schema2, {
        message: 'Add name field',
        parents: [commit1.hash]
      })
      await saveCommit(storage, commit2)

      // Diff schemas between commits
      const loadedSchema1 = await loadSchemaAtCommit(storage, commit1.hash)
      const loadedSchema2 = await loadSchemaAtCommit(storage, commit2.hash)

      const diff = diffSchemas(loadedSchema1, loadedSchema2)

      expect(diff.changes.length).toBeGreaterThan(0)
      const addField = diff.changes.find(c => c.type === 'ADD_FIELD')
      expect(addField).toBeDefined()
      expect(addField?.field).toBe('name')
      expect(addField?.breaking).toBe(false)
    })

    it('should detect breaking changes between commits', async () => {
      // First commit
      const config1: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!',
            age: 'int'
          }
        }
      }
      const schema1 = await captureSchema(config1)
      const commit1 = await createCommitWithSchema(baseState, schema1, {
        message: 'Initial schema'
      })
      await saveCommit(storage, commit1)

      // Second commit - remove field (breaking change)
      const config2: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          }
        }
      }
      const schema2 = await captureSchema(config2)
      const commit2 = await createCommitWithSchema(baseState, schema2, {
        message: 'Remove age field',
        parents: [commit1.hash]
      })
      await saveCommit(storage, commit2)

      const loadedSchema1 = await loadSchemaAtCommit(storage, commit1.hash)
      const loadedSchema2 = await loadSchemaAtCommit(storage, commit2.hash)

      const diff = diffSchemas(loadedSchema1, loadedSchema2)

      expect(diff.compatible).toBe(false)
      expect(diff.breakingChanges.length).toBeGreaterThan(0)

      const breaking = detectBreakingChanges(diff)
      expect(breaking.some(b => b.type === 'REMOVE_FIELD')).toBe(true)
    })
  })

  describe('Type generation from commits', () => {
    it('should generate TypeScript types from schema at commit', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!#',
            name: 'string',
            active: 'boolean!'
          },
          Post: {
            title: 'string!',
            author: '-> User'
          }
        }
      }

      const schema = await captureSchema(config)
      const commit = await createCommitWithSchema(baseState, schema, {
        message: 'Add schema'
      })
      await saveCommit(storage, commit)

      const loadedSchema = await loadSchemaAtCommit(storage, commit.hash)
      const types = generateTypeScript(loadedSchema)

      // Verify generated types include expected interfaces
      expect(types).toContain('export interface UserEntity extends Entity')
      expect(types).toContain('export interface PostEntity extends Entity')
      expect(types).toContain('email: string')
      expect(types).toContain('name?: string')
      expect(types).toContain('active: boolean')
      expect(types).toContain('export interface Database')
      expect(types).toContain('SCHEMA_METADATA')
      expect(types).toContain(commit.hash)
    })

    it('should generate different types for different schema versions', async () => {
      // V1 schema
      const config1: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          }
        }
      }
      const schema1 = await captureSchema(config1)
      const commit1 = await createCommitWithSchema(baseState, schema1, {
        message: 'V1 schema'
      })
      await saveCommit(storage, commit1)

      // V2 schema with new field
      const config2: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!',
            avatar: 'string'
          }
        }
      }
      const schema2 = await captureSchema(config2)
      const commit2 = await createCommitWithSchema(baseState, schema2, {
        message: 'V2 schema',
        parents: [commit1.hash]
      })
      await saveCommit(storage, commit2)

      // Generate types for both versions
      const loadedSchema1 = await loadSchemaAtCommit(storage, commit1.hash)
      const loadedSchema2 = await loadSchemaAtCommit(storage, commit2.hash)

      const types1 = generateTypeScript(loadedSchema1)
      const types2 = generateTypeScript(loadedSchema2)

      // V1 should NOT have avatar field
      expect(types1).not.toContain('avatar')

      // V2 should have avatar field
      expect(types2).toContain('avatar?: string')
    })
  })

  describe('DatabaseState with schema', () => {
    it('should include schema in DatabaseState type', async () => {
      const config: ParqueDBConfig = {
        schema: {
          User: {
            email: 'string!'
          }
        }
      }

      const schema = await captureSchema(config)
      const commit = await createCommitWithSchema(baseState, schema, {
        message: 'Test commit'
      })

      // Verify the state includes schema
      expect(commit.state.schema).toBeDefined()
      expect(commit.state.schema.hash).toBeTruthy()
      expect(commit.state.schema.collections).toBeDefined()
    })
  })
})
