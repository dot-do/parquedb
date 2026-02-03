/**
 * GitHub App Integration for ParqueDB
 *
 * Provides webhook handlers and GitHub API integration for
 * database CI/CD workflows:
 *
 * - Installation management (link/unlink GitHub installations)
 * - Branch synchronization (create/delete database branches with git)
 * - Pull request integration (preview branches, diff comments, merge checks)
 * - Slash commands (/parquedb preview, diff, resolve, schema, help)
 *
 * @module
 */

// Webhook router
export { handleGitHubWebhook, type GitHubAppEnv } from './webhooks'

// Webhook handlers
export {
  verifyWebhookSignature,
  handleInstallationCreated,
  handleInstallationDeleted,
  handleCreate,
  handleDelete,
  handlePullRequestOpened,
  handlePullRequestSynchronize,
  handlePullRequestClosed,
  type DatabaseIndexService,
  type ParqueDBService,
  type OctokitClient,
  type InstallationCreatedPayload,
  type InstallationDeletedPayload,
  type CreatePayload,
  type DeletePayload,
  type PullRequestPayload,
  type DatabaseInfo,
  type DatabaseConfig,
  type BranchConfig,
  type PreviewConfig,
  type MergeConfig,
} from './handlers'

// Slash commands
export {
  parseCommand,
  parseCommands,
  handlePreviewCommand,
  handleDiffCommand,
  handleResolveCommand,
  handleSchemaCommand,
  handleHelpCommand,
  handleUnknownCommand,
  checkPermissions,
  type ParsedCommand,
  type CommandContext,
  type ParqueDBClient,
} from './commands'

// GitHub Checks API
export {
  createMergeCheck,
  updateCheckSuccess,
  updateCheckFailure,
  updateCheckWithSchemaWarnings,
  type CreateMergeCheckOptions,
  type UpdateCheckSuccessOptions,
  type UpdateCheckFailureOptions,
  type UpdateCheckWithSchemaWarningsOptions,
  type MergePreview,
  type Conflict,
  type SchemaWarning,
  type Octokit,
} from './checks'

// PR comment formatting
export {
  formatDiffComment,
  formatPreviewUrl,
  formatMergeStatus,
  formatConflictDetails,
  formatSchemaChanges,
  findExistingComment,
  upsertComment,
  type DiffData,
  type CollectionDiffCounts,
  type PreviewUrlOptions,
  type MergeStatusOptions,
  type SchemaChanges,
} from './comments'

// Config parser
export {
  parseConfig,
  loadConfigFromRepo,
  shouldCreateBranch,
  shouldIgnoreBranch,
  parseTTL,
  invalidateConfigCache,
  clearConfigCache,
  type ParqueDBGitHubConfig,
  type DatabaseSection,
  type BranchesSection,
  type PreviewSection,
  type MergeSection,
  type DiffSection,
} from './config'
