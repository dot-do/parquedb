/**
 * ParqueDB Deployment Module
 *
 * Provides deployment capabilities for ParqueDB to various platforms.
 */

// Cloudflare Snippets integration
export {
  SnippetsClient,
  createSnippetsClientFromEnv,
  isValidSnippetName,
  normalizeSnippetName,
  type CloudflareResponse,
  type Snippet,
  type SnippetRule,
  type SnippetsConfig,
  type CreateSnippetOptions,
  type CreateRulesOptions,
  type DeployResult,
} from './snippets'
