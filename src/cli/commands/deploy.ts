/**
 * Deploy Command
 *
 * Deploy ParqueDB endpoints to Cloudflare Snippets.
 *
 * Usage:
 *   parquedb deploy snippets <name> <file> [--rule <expression>] [--description <desc>]
 *   parquedb deploy snippets list
 *   parquedb deploy snippets delete <name>
 */

import { promises as fs } from 'node:fs'
import { resolve, basename, isAbsolute } from 'node:path'
import type { ParsedArgs } from '../types'
import { print, printError, printSuccess } from '../types'
import {
  validateFilePathWithAllowedDirs,
  PathValidationError,
} from '../../utils/fs-path-safety'

/**
 * Print warning message (yellow)
 */
function printWarning(message: string): void {
  process.stdout.write(`Warning: ${message}\n`)
}

/**
 * Print info message (blue)
 */
function printInfo(message: string): void {
  process.stdout.write(`Info: ${message}\n`)
}
import {
  SnippetsClient,
  createSnippetsClientFromEnv,
  isValidSnippetName,
  normalizeSnippetName,
} from '../../deploy/snippets'

/**
 * Parse deploy-specific arguments
 *
 * Supports these patterns:
 *   parquedb deploy snippets <name> <file> [options]
 *   parquedb deploy snippets list
 *   parquedb deploy snippets rules
 *   parquedb deploy snippets delete <name>
 */
function parseDeployArgs(args: string[]): {
  target?: string | undefined
  name?: string | undefined
  file?: string | undefined
  rule?: string | undefined
  description?: string | undefined
  enabled?: boolean | undefined
  dryRun?: boolean | undefined
} {
  const result: ReturnType<typeof parseDeployArgs> = {}
  const positional: string[] = []

  let i = 0
  while (i < args.length) {
    const arg = args[i]

    if (arg === '--rule' || arg === '-r') {
      result.rule = args[++i]
    } else if (arg === '--description' || arg === '-D') {
      result.description = args[++i]
    } else if (arg === '--disabled') {
      result.enabled = false
    } else if (arg === '--dry-run') {
      result.dryRun = true
    } else if (arg && !arg.startsWith('-')) {
      positional.push(arg)
    }
    i++
  }

  // Assign positional arguments: target, name, file
  result.target = positional[0]
  result.name = positional[1]
  result.file = positional[2]

  return result
}

/**
 * Deploy to Cloudflare Snippets
 */
async function deploySnippets(
  parsed: ParsedArgs,
  deployArgs: ReturnType<typeof parseDeployArgs>
): Promise<number> {
  const { name, file, rule, description, enabled, dryRun } = deployArgs

  // Handle list subcommand
  if (name === 'list') {
    return await listSnippets()
  }

  // Handle rules subcommand
  if (name === 'rules') {
    return await listRules()
  }

  // Handle delete subcommand
  if (name === 'delete') {
    const snippetName = file
    if (!snippetName) {
      printError('Snippet name is required for delete')
      return 1
    }
    return await deleteSnippet(snippetName)
  }

  // Deploy a snippet - require name
  if (!name) {
    printError('Snippet name is required')
    print('')
    print('Usage: parquedb deploy snippets <name> <file> [options]')
    print('')
    print('Options:')
    print('  --rule, -r <expression>    Cloudflare expression to trigger the snippet')
    print('  --description, -D <desc>   Description for the rule')
    print('  --disabled                 Create rule in disabled state')
    print('  --dry-run                  Show what would be deployed without deploying')
    return 1
  }

  // Require file for deployment
  if (!file) {
    printError('JavaScript file is required')
    print('')
    print('Usage: parquedb deploy snippets <name> <file>')
    return 1
  }

  // Validate and normalize snippet name
  let snippetName = name
  if (!isValidSnippetName(name)) {
    const normalized = normalizeSnippetName(name)
    printWarning(`Snippet name "${name}" contains invalid characters`)
    printInfo(`Using normalized name: "${normalized}"`)
    snippetName = normalized
  }

  // Validate file path for security (path traversal, dangerous characters)
  const cwd = process.cwd()
  const allowedDirs = [cwd, resolve(parsed.options.directory)]

  try {
    validateFilePathWithAllowedDirs(cwd, file, allowedDirs)
  } catch (error) {
    if (error instanceof PathValidationError) {
      printError(`Invalid file path: ${error.message}`)
      return 1
    }
    throw error
  }

  // Read the file
  const filePath = isAbsolute(file) ? file : resolve(parsed.options.directory, file)
  let code: string
  try {
    code = await fs.readFile(filePath, 'utf-8')
  } catch (error) {
    printError(`Failed to read file: ${filePath}`)
    return 1
  }

  // Show dry run info
  if (dryRun) {
    print('Dry run - would deploy:')
    print('')
    print(`  Snippet name: ${snippetName}`)
    print(`  File: ${filePath}`)
    print(`  Code size: ${code.length} bytes`)
    if (rule) {
      print(`  Rule expression: ${rule}`)
      if (description) {
        print(`  Rule description: ${description}`)
      }
      print(`  Rule enabled: ${enabled !== false}`)
    }
    return 0
  }

  // Create the client
  let client: SnippetsClient
  try {
    client = createSnippetsClientFromEnv()
  } catch (error) {
    printError(error instanceof Error ? error.message : String(error))
    print('')
    print('Set the following environment variables:')
    print('  CLOUDFLARE_API_TOKEN - API token with Zone > Snippets > Edit permission')
    print('  CLOUDFLARE_ZONE_ID   - Zone ID from Cloudflare dashboard')
    return 1
  }

  // Deploy
  print(`Deploying snippet "${snippetName}"...`)

  const result = await client.deploy({
    name: snippetName,
    code,
    rule: rule
      ? {
          expression: rule,
          description: description || `Route to ${snippetName}`,
          enabled: enabled !== false,
        }
      : undefined,
  })

  if (!result.success) {
    printError(`Deployment failed: ${result.error}`)
    return 1
  }

  printSuccess(`Deployed snippet "${snippetName}"`)

  if (result.snippet) {
    print(`  Created: ${result.snippet.created_on}`)
    print(`  Modified: ${result.snippet.modified_on}`)
  }

  if (result.rules && rule) {
    const snippetRule = result.rules.find(r => r.snippet_name === snippetName)
    if (snippetRule) {
      print(`  Rule: ${snippetRule.expression}`)
      print(`  Rule enabled: ${snippetRule.enabled}`)
    }
  }

  return 0
}

/**
 * List all snippets
 */
async function listSnippets(): Promise<number> {
  let client: SnippetsClient
  try {
    client = createSnippetsClientFromEnv()
  } catch (error) {
    printError(error instanceof Error ? error.message : String(error))
    return 1
  }

  const snippets = await client.listSnippets()

  if (snippets.length === 0) {
    print('No snippets found')
    return 0
  }

  print(`Found ${snippets.length} snippet(s):`)
  print('')

  for (const snippet of snippets) {
    print(`  ${snippet.snippet_name}`)
    print(`    Created: ${snippet.created_on}`)
    print(`    Modified: ${snippet.modified_on}`)
    print('')
  }

  return 0
}

/**
 * List all snippet rules
 */
async function listRules(): Promise<number> {
  let client: SnippetsClient
  try {
    client = createSnippetsClientFromEnv()
  } catch (error) {
    printError(error instanceof Error ? error.message : String(error))
    return 1
  }

  const rules = await client.listRules()

  if (rules.length === 0) {
    print('No rules found')
    return 0
  }

  print(`Found ${rules.length} rule(s):`)
  print('')

  for (const rule of rules) {
    print(`  ${rule.snippet_name}`)
    print(`    Expression: ${rule.expression}`)
    if (rule.description) {
      print(`    Description: ${rule.description}`)
    }
    print(`    Enabled: ${rule.enabled}`)
    if (rule.last_updated) {
      print(`    Last updated: ${rule.last_updated}`)
    }
    print('')
  }

  return 0
}

/**
 * Delete a snippet
 */
async function deleteSnippet(name: string): Promise<number> {
  let client: SnippetsClient
  try {
    client = createSnippetsClientFromEnv()
  } catch (error) {
    printError(error instanceof Error ? error.message : String(error))
    return 1
  }

  print(`Deleting snippet "${name}"...`)

  const success = await client.undeploy(name)

  if (success) {
    printSuccess(`Deleted snippet "${name}"`)
    return 0
  } else {
    printWarning(`Snippet "${name}" not found or already deleted`)
    return 0
  }
}

/**
 * Main deploy command
 */
export async function deployCommand(parsed: ParsedArgs): Promise<number> {
  const deployArgs = parseDeployArgs(parsed.args)

  // Check for target platform
  if (!deployArgs.target) {
    print('Deploy ParqueDB to edge platforms')
    print('')
    print('Usage: parquedb deploy <platform> <command> [options]')
    print('')
    print('Platforms:')
    print('  snippets    Cloudflare Snippets (lightweight edge code)')
    print('')
    print('Commands:')
    print('  <name> <file>   Deploy a JavaScript file as a snippet')
    print('  list            List all deployed snippets')
    print('  rules           List all snippet rules')
    print('  delete <name>   Delete a snippet')
    print('')
    print('Examples:')
    print('  # Deploy a snippet with a routing rule')
    print('  parquedb deploy snippets my_api ./dist/worker.js --rule \'http.request.uri.path starts_with "/api"\'')
    print('')
    print('  # List deployed snippets')
    print('  parquedb deploy snippets list')
    print('')
    print('  # Delete a snippet')
    print('  parquedb deploy snippets delete my_api')
    print('')
    print('Environment Variables:')
    print('  CLOUDFLARE_API_TOKEN   API token with Zone > Snippets > Edit permission')
    print('  CLOUDFLARE_ZONE_ID     Zone ID from Cloudflare dashboard')
    return 0
  }

  // Route to the appropriate platform handler
  switch (deployArgs.target) {
    case 'snippets':
    case 'snippet':
      return await deploySnippets(parsed, deployArgs)

    default:
      printError(`Unknown platform: ${deployArgs.target}`)
      print('')
      print('Supported platforms: snippets')
      return 1
  }
}
