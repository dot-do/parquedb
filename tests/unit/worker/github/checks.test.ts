import { describe, it, expect, vi, beforeEach } from 'vitest'

const mockOctokit = {
  rest: {
    checks: {
      create: vi.fn(),
      update: vi.fn(),
    },
  },
}

describe('GitHub Checks', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('createMergeCheck', () => {
    it('creates check with pending status', async () => {
      mockOctokit.rest.checks.create.mockResolvedValue({
        data: { id: 123, status: 'in_progress' },
      })

      const { createMergeCheck } = await import('../../../../src/worker/github/checks')
      const result = await createMergeCheck(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        headSha: 'abc123',
      })

      expect(mockOctokit.rest.checks.create).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'in_progress',
        })
      )
      expect(result.id).toBe(123)
    })

    it('uses correct check name', async () => {
      mockOctokit.rest.checks.create.mockResolvedValue({
        data: { id: 123 },
      })

      const { createMergeCheck } = await import('../../../../src/worker/github/checks')
      await createMergeCheck(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        headSha: 'abc123',
      })

      expect(mockOctokit.rest.checks.create).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'ParqueDB Merge Check',
        })
      )
    })

    it('includes PR head SHA', async () => {
      mockOctokit.rest.checks.create.mockResolvedValue({
        data: { id: 123 },
      })

      const { createMergeCheck } = await import('../../../../src/worker/github/checks')
      await createMergeCheck(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        headSha: 'abc123',
      })

      expect(mockOctokit.rest.checks.create).toHaveBeenCalledWith(
        expect.objectContaining({
          head_sha: 'abc123',
        })
      )
    })

    it('sets status to in_progress', async () => {
      mockOctokit.rest.checks.create.mockResolvedValue({
        data: { id: 123, status: 'in_progress' },
      })

      const { createMergeCheck } = await import('../../../../src/worker/github/checks')
      const result = await createMergeCheck(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        headSha: 'abc123',
      })

      expect(mockOctokit.rest.checks.create).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'in_progress',
        })
      )
    })

    it('includes initial summary', async () => {
      mockOctokit.rest.checks.create.mockResolvedValue({
        data: { id: 123 },
      })

      const { createMergeCheck } = await import('../../../../src/worker/github/checks')
      await createMergeCheck(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        pr: 42,
        headSha: 'abc123',
      })

      expect(mockOctokit.rest.checks.create).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            title: 'Analyzing database changes...',
            summary: expect.stringContaining('Checking for merge conflicts'),
          }),
        })
      )
    })
  })

  describe('updateCheckSuccess', () => {
    it('updates check to success', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123, conclusion: 'success' },
      })

      const { updateCheckSuccess } = await import('../../../../src/worker/github/checks')
      await updateCheckSuccess(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        mergePreview: {
          collections: {
            users: { added: 5, removed: 2, modified: 3 },
          },
        },
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          conclusion: 'success',
        })
      )
    })

    it('includes merge preview in summary', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckSuccess } = await import('../../../../src/worker/github/checks')
      await updateCheckSuccess(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        mergePreview: {
          collections: {
            users: { added: 5, removed: 2, modified: 3 },
          },
        },
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            summary: expect.stringContaining('Clean merge'),
          }),
        })
      )
    })

    it('shows entity counts in details', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckSuccess } = await import('../../../../src/worker/github/checks')
      await updateCheckSuccess(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        mergePreview: {
          collections: {
            users: { added: 5, removed: 2, modified: 3 },
            posts: { added: 10, removed: 0, modified: 1 },
          },
        },
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            text: expect.stringMatching(/users.*\+5.*-2.*~3/s),
          }),
        })
      )
    })

    it('shows no conflicts message', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckSuccess } = await import('../../../../src/worker/github/checks')
      await updateCheckSuccess(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        mergePreview: {
          collections: {},
        },
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            title: expect.stringContaining('Clean merge'),
          }),
        })
      )
    })
  })

  describe('updateCheckFailure', () => {
    it('updates check to failure', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123, conclusion: 'failure' },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
          {
            ns: 'users',
            entityId: 'user1',
            field: 'email',
            ours: 'old@example.com',
            theirs: 'new@example.com',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          conclusion: 'failure',
        })
      )
    })

    it('includes conflict count in title', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
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
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            title: expect.stringMatching(/2 conflicts?/i),
          }),
        })
      )
    })

    it('lists conflicting entities', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
          {
            ns: 'users',
            entityId: 'user1',
            field: 'email',
            ours: 'old@example.com',
            theirs: 'new@example.com',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            text: expect.stringContaining('user1'),
          }),
        })
      )
    })

    it('shows conflict details per entity', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
          {
            ns: 'users',
            entityId: 'user1',
            field: 'email',
            ours: 'old@example.com',
            theirs: 'new@example.com',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            text: expect.stringMatching(/email.*old@example\.com.*new@example\.com/s),
          }),
        })
      )
    })

    it('includes resolution instructions', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
          {
            ns: 'users',
            entityId: 'user1',
            field: 'email',
            ours: 'old@example.com',
            theirs: 'new@example.com',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            text: expect.stringContaining('/parquedb resolve'),
          }),
        })
      )
    })
  })

  describe('updateCheckWithSchemaWarnings', () => {
    it('sets conclusion to neutral for warnings', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123, conclusion: 'neutral' },
      })

      const { updateCheckWithSchemaWarnings } = await import('../../../../src/worker/github/checks')
      await updateCheckWithSchemaWarnings(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        warnings: [
          {
            type: 'breaking',
            collection: 'users',
            field: 'name',
            message: 'Field removed',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          conclusion: 'neutral',
        })
      )
    })

    it('includes breaking changes in summary', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckWithSchemaWarnings } = await import('../../../../src/worker/github/checks')
      await updateCheckWithSchemaWarnings(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        warnings: [
          {
            type: 'breaking',
            collection: 'users',
            field: 'name',
            message: 'Field removed',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            summary: expect.stringContaining('breaking'),
          }),
        })
      )
    })

    it('shows migration hints', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckWithSchemaWarnings } = await import('../../../../src/worker/github/checks')
      await updateCheckWithSchemaWarnings(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        warnings: [
          {
            type: 'breaking',
            collection: 'users',
            field: 'name',
            message: 'Field removed',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            text: expect.stringContaining('migration'),
          }),
        })
      )
    })
  })

  describe('annotations', () => {
    it('adds annotation for each conflict', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
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
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            annotations: expect.arrayContaining([
              expect.objectContaining({
                message: expect.stringContaining('email'),
              }),
              expect.objectContaining({
                message: expect.stringContaining('name'),
              }),
            ]),
          }),
        })
      )
    })

    it('annotation points to entity location', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
          {
            ns: 'users',
            entityId: 'user1',
            field: 'email',
            ours: 'old@example.com',
            theirs: 'new@example.com',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            annotations: expect.arrayContaining([
              expect.objectContaining({
                path: expect.stringContaining('users'),
                start_line: expect.any(Number),
                end_line: expect.any(Number),
              }),
            ]),
          }),
        })
      )
    })

    it('annotation has warning level', async () => {
      mockOctokit.rest.checks.update.mockResolvedValue({
        data: { id: 123 },
      })

      const { updateCheckFailure } = await import('../../../../src/worker/github/checks')
      await updateCheckFailure(mockOctokit as any, {
        owner: 'test-owner',
        repo: 'test-repo',
        checkId: 123,
        conflicts: [
          {
            ns: 'users',
            entityId: 'user1',
            field: 'email',
            ours: 'old@example.com',
            theirs: 'new@example.com',
          },
        ],
      })

      expect(mockOctokit.rest.checks.update).toHaveBeenCalledWith(
        expect.objectContaining({
          output: expect.objectContaining({
            annotations: expect.arrayContaining([
              expect.objectContaining({
                annotation_level: 'warning',
              }),
            ]),
          }),
        })
      )
    })
  })
})
