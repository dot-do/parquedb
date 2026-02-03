import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  parseCommand,
  parseCommands,
  handlePreviewCommand,
  handleDiffCommand,
  handleResolveCommand,
  handleSchemaCommand,
  handleHelpCommand,
  handleUnknownCommand,
  checkPermissions,
} from '../../../../src/worker/github/commands'

const mockParqueDB = {
  branch: { create: vi.fn(), exists: vi.fn() },
  diff: vi.fn(),
  merge: vi.fn(),
  resolve: vi.fn(),
  schema: { diff: vi.fn() },
}

const mockOctokit = {
  rest: {
    issues: {
      createComment: vi.fn(),
    },
    reactions: {
      createForIssueComment: vi.fn(),
    },
    repos: {
      getCollaboratorPermissionLevel: vi.fn(),
    },
  },
  checks: {
    update: vi.fn(),
  },
}

describe('slash commands', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('command parsing', () => {
    it('parses /parquedb preview', () => {
      const comment = '/parquedb preview'
      const parsed = parseCommand(comment)
      expect(parsed).toEqual({
        command: 'preview',
        args: [],
        flags: {},
      })
    })

    it('parses /parquedb diff users', () => {
      const comment = '/parquedb diff users'
      const parsed = parseCommand(comment)
      expect(parsed).toEqual({
        command: 'diff',
        args: ['users'],
        flags: {},
      })
    })

    it('parses /parquedb resolve posts/123 --ours', () => {
      const comment = '/parquedb resolve posts/123 --ours'
      const parsed = parseCommand(comment)
      expect(parsed).toEqual({
        command: 'resolve',
        args: ['posts/123'],
        flags: { ours: true },
      })
    })

    it('ignores non-parquedb commands', () => {
      const comment = '/other command'
      const parsed = parseCommand(comment)
      expect(parsed).toBeNull()
    })

    it('handles command in middle of comment', () => {
      const comment = 'Some text /parquedb preview more text'
      const parsed = parseCommand(comment)
      expect(parsed).toEqual({
        command: 'preview',
        args: [],
        flags: {},
      })
    })

    it('handles multiple commands in one comment', () => {
      const comment = '/parquedb diff\n/parquedb preview'
      const commands = parseCommands(comment)
      expect(commands).toHaveLength(2)
      expect(commands[0].command).toBe('diff')
      expect(commands[1].command).toBe('preview')
    })
  })

  describe('/parquedb preview', () => {
    const context = {
      issueNumber: 42,
      repo: { owner: 'testuser', repo: 'mydb' },
      commentId: 123,
      prBranch: 'feature/test',
    }

    it('creates preview branch if not exists', async () => {
      mockParqueDB.branch.exists.mockResolvedValue(false)
      mockParqueDB.branch.create.mockResolvedValue({
        name: 'preview/pr-42',
        url: 'https://preview.parquedb.com/testuser/mydb/preview-pr-42',
      })

      await handlePreviewCommand(context, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.exists).toHaveBeenCalledWith('preview/pr-42')
      expect(mockParqueDB.branch.create).toHaveBeenCalledWith({
        name: 'preview/pr-42',
        from: 'feature/test',
      })
    })

    it('refreshes existing preview', async () => {
      mockParqueDB.branch.exists.mockResolvedValue(true)
      mockParqueDB.branch.create.mockResolvedValue({
        name: 'preview/pr-42',
        url: 'https://preview.parquedb.com/testuser/mydb/preview-pr-42',
      })

      await handlePreviewCommand(context, mockParqueDB, mockOctokit)

      expect(mockParqueDB.branch.create).toHaveBeenCalledWith({
        name: 'preview/pr-42',
        from: 'feature/test',
        overwrite: true,
      })
    })

    it('posts preview URL as reply', async () => {
      mockParqueDB.branch.exists.mockResolvedValue(false)
      mockParqueDB.branch.create.mockResolvedValue({
        name: 'preview/pr-42',
        url: 'https://preview.parquedb.com/testuser/mydb/preview-pr-42',
      })

      await handlePreviewCommand(context, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        issue_number: 42,
        body: expect.stringContaining('Preview environment created'),
      })
      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('https://preview.parquedb.com/testuser/mydb/preview-pr-42'),
        })
      )
    })

    it('reacts with rocket emoji on success', async () => {
      mockParqueDB.branch.exists.mockResolvedValue(false)
      mockParqueDB.branch.create.mockResolvedValue({
        name: 'preview/pr-42',
        url: 'https://preview.parquedb.com/testuser/mydb/preview-pr-42',
      })

      await handlePreviewCommand(context, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.reactions.createForIssueComment).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        comment_id: 123,
        content: 'rocket',
      })
    })

    it('reacts with confused emoji on error', async () => {
      mockParqueDB.branch.exists.mockResolvedValue(false)
      mockParqueDB.branch.create.mockRejectedValue(new Error('fail'))

      await handlePreviewCommand(context, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.reactions.createForIssueComment).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        comment_id: 123,
        content: 'confused',
      })
    })

    it('posts error message on failure', async () => {
      mockParqueDB.branch.exists.mockResolvedValue(false)
      mockParqueDB.branch.create.mockRejectedValue(new Error('Branch creation failed'))

      await handlePreviewCommand(context, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        issue_number: 42,
        body: expect.stringContaining('Error creating preview'),
      })
      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('Branch creation failed'),
        })
      )
    })
  })

  describe('/parquedb diff', () => {
    const context = {
      issueNumber: 42,
      repo: { owner: 'testuser', repo: 'mydb' },
      baseBranch: 'main',
      headBranch: 'feature/test',
      commentId: 123,
    }

    it('posts full diff when no collection specified', async () => {
      mockParqueDB.diff.mockResolvedValue({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
          posts: { added: 10, removed: 0, modified: 1 },
        },
      })

      await handleDiffCommand(context, [], mockParqueDB, mockOctokit)

      expect(mockParqueDB.diff).toHaveBeenCalledWith({
        base: 'main',
        head: 'feature/test',
      })
      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('users'),
        })
      )
      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('posts'),
        })
      )
    })

    it('filters by collection when specified', async () => {
      mockParqueDB.diff.mockResolvedValue({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
        },
      })

      await handleDiffCommand(context, ['users'], mockParqueDB, mockOctokit)

      expect(mockParqueDB.diff).toHaveBeenCalledWith({
        base: 'main',
        head: 'feature/test',
        collection: 'users',
      })
    })

    it('formats diff as markdown table', async () => {
      mockParqueDB.diff.mockResolvedValue({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
        },
      })

      await handleDiffCommand(context, [], mockParqueDB, mockOctokit)

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('| Collection | Added | Removed | Modified |')
      expect(comment).toContain('| users | 5 | 2 | 3 |')
    })

    it('shows sample entities when configured', async () => {
      mockParqueDB.diff.mockResolvedValue({
        collections: {
          users: {
            added: 2,
            removed: 0,
            modified: 0,
            samples: {
              added: [
                { $id: 'u1', name: 'Alice' },
                { $id: 'u2', name: 'Bob' },
              ],
            },
          },
        },
      })

      await handleDiffCommand(
        { ...context, config: { show_samples: true } },
        [],
        mockParqueDB,
        mockOctokit
      )

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('Alice')
      expect(comment).toContain('Bob')
    })

    it('respects max_entities limit', async () => {
      mockParqueDB.diff.mockResolvedValue({
        collections: {
          users: {
            added: 100,
            removed: 0,
            modified: 0,
            samples: {
              added: Array.from({ length: 100 }, (_, i) => ({ $id: `u${i}`, name: `User ${i}` })),
            },
          },
        },
      })

      await handleDiffCommand(
        { ...context, config: { show_samples: true, max_entities: 10 } },
        [],
        mockParqueDB,
        mockOctokit
      )

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('(showing 10 of 100)')
    })

    it('handles no changes', async () => {
      mockParqueDB.diff.mockResolvedValue({ collections: {} })

      await handleDiffCommand(context, [], mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('No changes'),
        })
      )
    })

    it('reacts with eyes emoji', async () => {
      mockParqueDB.diff.mockResolvedValue({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
        },
      })

      await handleDiffCommand(context, [], mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.reactions.createForIssueComment).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        comment_id: 123,
        content: 'eyes',
      })
    })
  })

  describe('/parquedb resolve', () => {
    const context = {
      issueNumber: 42,
      repo: { owner: 'testuser', repo: 'mydb' },
      commentId: 123,
      checkRunId: 789,
    }

    it('resolves conflict with --ours', async () => {
      mockParqueDB.resolve.mockResolvedValue({
        resolved: ['posts/123'],
        strategy: 'ours',
      })

      await handleResolveCommand(
        context,
        ['posts/123'],
        { ours: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockParqueDB.resolve).toHaveBeenCalledWith({
        entities: ['posts/123'],
        strategy: 'ours',
      })
    })

    it('resolves conflict with --theirs', async () => {
      mockParqueDB.resolve.mockResolvedValue({
        resolved: ['posts/123'],
        strategy: 'theirs',
      })

      await handleResolveCommand(
        context,
        ['posts/123'],
        { theirs: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockParqueDB.resolve).toHaveBeenCalledWith({
        entities: ['posts/123'],
        strategy: 'theirs',
      })
    })

    it('resolves conflict with --newest', async () => {
      mockParqueDB.resolve.mockResolvedValue({
        resolved: ['posts/123'],
        strategy: 'newest',
      })

      await handleResolveCommand(
        context,
        ['posts/123'],
        { newest: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockParqueDB.resolve).toHaveBeenCalledWith({
        entities: ['posts/123'],
        strategy: 'newest',
      })
    })

    it('resolves multiple entities with glob pattern', async () => {
      mockParqueDB.resolve.mockResolvedValue({
        resolved: ['posts/123', 'posts/456', 'posts/789'],
        strategy: 'ours',
      })

      await handleResolveCommand(
        context,
        ['posts/*'],
        { ours: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockParqueDB.resolve).toHaveBeenCalledWith({
        entities: ['posts/*'],
        strategy: 'ours',
      })
    })

    it('posts success message with resolved count', async () => {
      mockParqueDB.resolve.mockResolvedValue({
        resolved: ['posts/123', 'posts/456'],
        strategy: 'ours',
      })

      await handleResolveCommand(
        context,
        ['posts/*'],
        { ours: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        issue_number: 42,
        body: expect.stringContaining('Resolved 2 conflicts'),
      })
    })

    it('posts error on invalid entity', async () => {
      mockParqueDB.resolve.mockRejectedValue(new Error('Entity not found'))

      await handleResolveCommand(
        context,
        ['posts/invalid'],
        { ours: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('Entity not found'),
        })
      )
    })

    it('posts error when no merge in progress', async () => {
      mockParqueDB.resolve.mockRejectedValue(new Error('No merge in progress'))

      await handleResolveCommand(
        context,
        ['posts/123'],
        { ours: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('No merge in progress'),
        })
      )
    })

    it('triggers merge check re-run after resolution', async () => {
      mockParqueDB.resolve.mockResolvedValue({
        resolved: ['posts/123'],
        strategy: 'ours',
      })

      await handleResolveCommand(
        context,
        ['posts/123'],
        { ours: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockOctokit.checks.update).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        check_run_id: 789,
        status: 'queued',
      })
    })

    it('reacts with checkmark on success', async () => {
      mockParqueDB.resolve.mockResolvedValue({
        resolved: ['posts/123'],
        strategy: 'ours',
      })

      await handleResolveCommand(
        context,
        ['posts/123'],
        { ours: true },
        mockParqueDB,
        mockOctokit
      )

      expect(mockOctokit.rest.reactions.createForIssueComment).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        comment_id: 123,
        content: '+1',
      })
    })
  })

  describe('/parquedb schema', () => {
    const context = {
      issueNumber: 42,
      repo: { owner: 'testuser', repo: 'mydb' },
      baseBranch: 'main',
      headBranch: 'feature/test',
      commentId: 123,
    }

    it('shows schema diff between base and head', async () => {
      mockParqueDB.schema.diff.mockResolvedValue({
        changes: [
          { type: 'ADD_FIELD', collection: 'users', field: 'avatar', fieldType: 'string' },
        ],
        breakingChanges: [],
      })

      await handleSchemaCommand(context, mockParqueDB, mockOctokit)

      expect(mockParqueDB.schema.diff).toHaveBeenCalledWith({
        base: 'main',
        head: 'feature/test',
      })
      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('ADD_FIELD')
      expect(comment).toContain('users')
      expect(comment).toContain('avatar')
    })

    it('highlights breaking changes with warning', async () => {
      mockParqueDB.schema.diff.mockResolvedValue({
        changes: [],
        breakingChanges: [
          { type: 'REMOVE_FIELD', collection: 'users', field: 'name' },
        ],
      })

      await handleSchemaCommand(context, mockParqueDB, mockOctokit)

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('⚠️')
      expect(comment).toContain('Breaking Changes')
      expect(comment).toContain('REMOVE_FIELD')
    })

    it('shows migration hints for breaking changes', async () => {
      mockParqueDB.schema.diff.mockResolvedValue({
        changes: [],
        breakingChanges: [
          {
            type: 'CHANGE_TYPE',
            collection: 'users',
            field: 'age',
            oldType: 'string',
            newType: 'number',
            migrationHint: 'Consider using parseInt() for existing values',
          },
        ],
      })

      await handleSchemaCommand(context, mockParqueDB, mockOctokit)

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('Migration hint')
      expect(comment).toContain('parseInt()')
    })

    it('handles no schema changes', async () => {
      mockParqueDB.schema.diff.mockResolvedValue({
        changes: [],
        breakingChanges: [],
      })

      await handleSchemaCommand(context, mockParqueDB, mockOctokit)

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.stringContaining('No schema changes'),
        })
      )
    })
  })

  describe('/parquedb help', () => {
    const context = {
      issueNumber: 42,
      repo: { owner: 'testuser', repo: 'mydb' },
      commentId: 123,
    }

    it('posts help message with available commands', async () => {
      await handleHelpCommand(context, mockOctokit)

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('preview')
      expect(comment).toContain('diff')
      expect(comment).toContain('resolve')
      expect(comment).toContain('schema')
    })

    it('shows command syntax', async () => {
      await handleHelpCommand(context, mockOctokit)

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('/parquedb preview')
      expect(comment).toContain('/parquedb diff [collection]')
      expect(comment).toContain('/parquedb resolve <entity> --ours|--theirs|--newest')
      expect(comment).toContain('/parquedb schema')
    })
  })

  describe('unknown command', () => {
    const context = {
      issueNumber: 42,
      repo: { owner: 'testuser', repo: 'mydb' },
      commentId: 123,
    }

    it('posts help message for unknown commands', async () => {
      await handleUnknownCommand(context, 'unknown', mockOctokit)

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('Unknown command')
      expect(comment).toContain('/parquedb help')
    })

    it('suggests similar commands', async () => {
      await handleUnknownCommand(context, 'preveiw', mockOctokit)

      const comment = mockOctokit.rest.issues.createComment.mock.calls[0][0].body
      expect(comment).toContain('Did you mean')
      expect(comment).toContain('preview')
    })
  })

  describe('permissions', () => {
    const context = {
      issueNumber: 42,
      repo: { owner: 'testuser', repo: 'mydb' },
      commentId: 123,
      commentAuthor: 'contributor',
    }

    it('checks user has write access', async () => {
      mockOctokit.rest.repos.getCollaboratorPermissionLevel.mockResolvedValue({
        data: { permission: 'write' },
      })

      const hasPermission = await checkPermissions(context, mockOctokit)

      expect(hasPermission).toBe(true)
      expect(mockOctokit.rest.repos.getCollaboratorPermissionLevel).toHaveBeenCalledWith({
        owner: 'testuser',
        repo: 'mydb',
        username: 'contributor',
      })
    })

    it('rejects commands from users without access', async () => {
      mockOctokit.rest.repos.getCollaboratorPermissionLevel.mockResolvedValue({
        data: { permission: 'read' },
      })

      const hasPermission = await checkPermissions(context, mockOctokit)

      expect(hasPermission).toBe(false)
    })

    it('allows repo collaborators', async () => {
      mockOctokit.rest.repos.getCollaboratorPermissionLevel.mockResolvedValue({
        data: { permission: 'admin' },
      })

      const hasPermission = await checkPermissions(context, mockOctokit)

      expect(hasPermission).toBe(true)
    })
  })
})
