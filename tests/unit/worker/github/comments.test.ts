import { describe, it, expect, vi, beforeEach } from 'vitest'

const mockOctokit = {
  rest: {
    issues: {
      listComments: vi.fn(),
      createComment: vi.fn(),
      updateComment: vi.fn(),
    },
  },
}

describe('PR Comments', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('formatDiffComment', () => {
    it('includes header with emoji', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const result = formatDiffComment({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
        },
      })

      expect(result).toContain('## \uD83D\uDDC4\uFE0F Database Changes')
    })

    it('formats as markdown table', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const result = formatDiffComment({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
          posts: { added: 10, removed: 0, modified: 1 },
        },
      })

      expect(result).toContain('| Collection | Added | Removed | Modified |')
      expect(result).toContain('|------------|-------|---------|----------|')
    })

    it('shows +N for added, -N for removed, ~N for modified', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const result = formatDiffComment({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
        },
      })

      expect(result).toMatch(/\+5/)
      expect(result).toMatch(/-2/)
      expect(result).toMatch(/~3/)
    })

    it('shows total row', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const result = formatDiffComment({
        collections: {
          users: { added: 5, removed: 2, modified: 3 },
          posts: { added: 10, removed: 0, modified: 1 },
        },
      })

      expect(result).toContain('**Total**')
      expect(result).toMatch(/\+15/) // 5 + 10
      expect(result).toMatch(/-2/)
      expect(result).toMatch(/~4/) // 3 + 1
    })

    it('handles empty diff', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const result = formatDiffComment({
        collections: {},
      })

      expect(result).toContain('No database changes')
    })

    it('truncates long collection lists', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const collections: Record<string, { added: number; removed: number; modified: number }> = {}
      for (let i = 0; i < 50; i++) {
        collections[`collection${i}`] = { added: 1, removed: 0, modified: 0 }
      }

      const result = formatDiffComment({ collections })

      // Should show max 20 collections
      expect(result).toContain('and 30 more')
    })
  })

  describe('formatPreviewUrl', () => {
    it('includes preview URL', async () => {
      const { formatPreviewUrl } = await import('../../../../src/worker/github/comments')
      const result = formatPreviewUrl({
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
      })

      expect(result).toContain('http')
      expect(result).toContain('preview')
    })

    it('uses correct domain', async () => {
      const { formatPreviewUrl } = await import('../../../../src/worker/github/comments')
      const result = formatPreviewUrl({
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
      })

      expect(result).toContain('parque.db')
    })

    it('includes owner and PR number', async () => {
      const { formatPreviewUrl } = await import('../../../../src/worker/github/comments')
      const result = formatPreviewUrl({
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
      })

      expect(result).toContain('test-owner')
      expect(result).toContain('test-repo')
      expect(result).toContain('42')
    })
  })

  describe('formatMergeStatus', () => {
    it('shows checkmark for clean merge', async () => {
      const { formatMergeStatus } = await import('../../../../src/worker/github/comments')
      const result = formatMergeStatus({
        status: 'clean',
      })

      expect(result).toContain('\u2705')
      expect(result).toContain('Clean merge')
    })

    it('shows warning for conflicts', async () => {
      const { formatMergeStatus } = await import('../../../../src/worker/github/comments')
      const result = formatMergeStatus({
        status: 'conflicts',
        conflictCount: 3,
      })

      expect(result).toContain('\u26A0\uFE0F')
      expect(result).toContain('3 conflicts')
    })

    it('shows info for schema warnings', async () => {
      const { formatMergeStatus } = await import('../../../../src/worker/github/comments')
      const result = formatMergeStatus({
        status: 'warnings',
      })

      expect(result).toContain('\u2139\uFE0F')
      expect(result).toContain('Schema changes detected')
    })
  })

  describe('formatConflictDetails', () => {
    it('lists each conflicting entity', async () => {
      const { formatConflictDetails } = await import('../../../../src/worker/github/comments')
      const result = formatConflictDetails([
        {
          ns: 'users',
          entityId: 'user1',
          field: 'email',
          ours: 'old@example.com',
          theirs: 'new@example.com',
        },
        {
          ns: 'users',
          entityId: 'user2',
          field: 'name',
          ours: 'Old Name',
          theirs: 'New Name',
        },
      ])

      expect(result).toContain('user1')
      expect(result).toContain('user2')
    })

    it('shows conflicting fields', async () => {
      const { formatConflictDetails } = await import('../../../../src/worker/github/comments')
      const result = formatConflictDetails([
        {
          ns: 'users',
          entityId: 'user1',
          field: 'email',
          ours: 'old@example.com',
          theirs: 'new@example.com',
        },
      ])

      expect(result).toContain('email')
    })

    it('shows ours vs theirs values', async () => {
      const { formatConflictDetails } = await import('../../../../src/worker/github/comments')
      const result = formatConflictDetails([
        {
          ns: 'users',
          entityId: 'user1',
          field: 'email',
          ours: 'old@example.com',
          theirs: 'new@example.com',
        },
      ])

      expect(result).toContain('old@example.com')
      expect(result).toContain('new@example.com')
      expect(result).toMatch(/ours|base/i)
      expect(result).toMatch(/theirs|incoming/i)
    })

    it('includes resolution commands', async () => {
      const { formatConflictDetails } = await import('../../../../src/worker/github/comments')
      const result = formatConflictDetails([
        {
          ns: 'users',
          entityId: 'user1',
          field: 'email',
          ours: 'old@example.com',
          theirs: 'new@example.com',
        },
      ])

      expect(result).toContain('/parquedb resolve')
      expect(result).toContain('user1')
    })
  })

  describe('formatSchemaChanges', () => {
    it('lists added fields', async () => {
      const { formatSchemaChanges } = await import('../../../../src/worker/github/comments')
      const result = formatSchemaChanges({
        users: {
          added: [{ name: 'avatar', type: 'string' }],
          removed: [],
          modified: [],
        },
      })

      expect(result).toContain('+ users.avatar: string')
    })

    it('lists removed fields with warning', async () => {
      const { formatSchemaChanges } = await import('../../../../src/worker/github/comments')
      const result = formatSchemaChanges({
        users: {
          added: [],
          removed: [{ name: 'name', type: 'string' }],
          modified: [],
        },
      })

      expect(result).toContain('- users.name: string')
      expect(result).toContain('\u26A0\uFE0F BREAKING')
    })

    it('lists modified fields', async () => {
      const { formatSchemaChanges } = await import('../../../../src/worker/github/comments')
      const result = formatSchemaChanges({
        users: {
          added: [],
          removed: [],
          modified: [
            {
              name: 'email',
              oldType: 'string',
              newType: 'string!',
            },
          ],
        },
      })

      expect(result).toContain('~ users.email: string \u2192 string!')
    })

    it('groups by collection', async () => {
      const { formatSchemaChanges } = await import('../../../../src/worker/github/comments')
      const result = formatSchemaChanges({
        users: {
          added: [{ name: 'avatar', type: 'string' }],
          removed: [],
          modified: [],
        },
        posts: {
          added: [{ name: 'views', type: 'number' }],
          removed: [],
          modified: [],
        },
      })

      expect(result).toContain('### users')
      expect(result).toContain('### posts')
    })
  })

  describe('findExistingComment', () => {
    it('finds comment by marker', async () => {
      mockOctokit.rest.issues.listComments.mockResolvedValue({
        data: [
          {
            id: 1,
            user: { login: 'parquedb[bot]' },
            body: '## \uD83D\uDDC4\uFE0F Database Changes\n\nSome content',
          },
          {
            id: 2,
            user: { login: 'other-user' },
            body: 'Regular comment',
          },
        ],
      })

      const { findExistingComment } = await import('../../../../src/worker/github/comments')
      const result = await findExistingComment(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
      })

      expect(result).toBe(1)
    })

    it('returns null if not found', async () => {
      mockOctokit.rest.issues.listComments.mockResolvedValue({
        data: [
          {
            id: 1,
            user: { login: 'other-user' },
            body: 'Regular comment',
          },
        ],
      })

      const { findExistingComment } = await import('../../../../src/worker/github/comments')
      const result = await findExistingComment(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
      })

      expect(result).toBeNull()
    })

    it('matches bot username', async () => {
      mockOctokit.rest.issues.listComments.mockResolvedValue({
        data: [
          {
            id: 1,
            user: { login: 'parquedb[bot]' },
            body: '## \uD83D\uDDC4\uFE0F Database Changes\n\nSome content',
          },
          {
            id: 2,
            user: { login: 'imposter[bot]' },
            body: '## \uD83D\uDDC4\uFE0F Database Changes\n\nFake content',
          },
        ],
      })

      const { findExistingComment } = await import('../../../../src/worker/github/comments')
      const result = await findExistingComment(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
      })

      expect(result).toBe(1)
    })
  })

  describe('upsertComment', () => {
    it('creates new comment if none exists', async () => {
      mockOctokit.rest.issues.listComments.mockResolvedValue({
        data: [],
      })
      mockOctokit.rest.issues.createComment.mockResolvedValue({
        data: { id: 123 },
      })

      const { upsertComment } = await import('../../../../src/worker/github/comments')
      await upsertComment(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        body: 'Test comment',
      })

      expect(mockOctokit.rest.issues.createComment).toHaveBeenCalledWith({
        owner: 'test-owner',
        repo: 'test-repo',
        issue_number: 42,
        body: 'Test comment',
      })
      expect(mockOctokit.rest.issues.updateComment).not.toHaveBeenCalled()
    })

    it('updates existing comment', async () => {
      mockOctokit.rest.issues.listComments.mockResolvedValue({
        data: [
          {
            id: 456,
            user: { login: 'parquedb[bot]' },
            body: '## \uD83D\uDDC4\uFE0F Database Changes\n\nOld content',
          },
        ],
      })
      mockOctokit.rest.issues.updateComment.mockResolvedValue({
        data: { id: 456 },
      })

      const { upsertComment } = await import('../../../../src/worker/github/comments')
      await upsertComment(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        body: 'Test comment',
      })

      expect(mockOctokit.rest.issues.updateComment).toHaveBeenCalledWith({
        owner: 'test-owner',
        repo: 'test-repo',
        comment_id: 456,
        body: 'Test comment',
      })
      expect(mockOctokit.rest.issues.createComment).not.toHaveBeenCalled()
    })

    it('preserves comment ID for future updates', async () => {
      mockOctokit.rest.issues.listComments.mockResolvedValue({
        data: [
          {
            id: 789,
            user: { login: 'parquedb[bot]' },
            body: '## \uD83D\uDDC4\uFE0F Database Changes\n\nOld content',
          },
        ],
      })
      mockOctokit.rest.issues.updateComment.mockResolvedValue({
        data: { id: 789 },
      })

      const { upsertComment } = await import('../../../../src/worker/github/comments')
      const result = await upsertComment(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        body: 'Test comment',
      })

      expect(result.id).toBe(789)
    })
  })

  describe('comment footer', () => {
    it('includes powered by ParqueDB link', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const result = formatDiffComment({
        collections: {},
      })

      expect(result).toContain('ParqueDB')
      expect(result).toMatch(/https?:\/\/.*parque\.?db/i)
    })

    it('includes timestamp', async () => {
      const { formatDiffComment } = await import('../../../../src/worker/github/comments')
      const result = formatDiffComment({
        collections: {},
      })

      expect(result).toMatch(/updated|generated/i)
      // Should contain some form of timestamp/date
      expect(result).toMatch(/\d{4}|\d{1,2}:\d{2}/i)
    })
  })
})
