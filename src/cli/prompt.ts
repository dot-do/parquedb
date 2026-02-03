/**
 * CLI Prompt Utilities
 *
 * Simple interactive prompts using Node.js readline.
 * Provides a clean API for collecting user input in the CLI.
 */

import * as readline from 'node:readline'
import { isTTY, colors, isColorEnabled } from './utils'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for a text prompt
 */
export interface TextPromptOptions {
  /** Default value if user presses Enter */
  default?: string
  /** Validate the input, return error message or undefined/true for valid */
  validate?: (value: string) => string | undefined | true
}

/**
 * Options for a select prompt
 */
export interface SelectPromptOptions<T extends string = string> {
  /** Available choices */
  choices: Array<{ value: T; label: string; description?: string }>
  /** Default selected value */
  default?: T
}

/**
 * Options for a confirm prompt
 */
export interface ConfirmPromptOptions {
  /** Default value (true = yes, false = no) */
  default?: boolean
}

/**
 * Interface for dependency injection (testing)
 */
export interface PromptIO {
  input: NodeJS.ReadableStream
  output: NodeJS.WritableStream
}

// =============================================================================
// Prompt Implementation
// =============================================================================

/**
 * Create a readline interface
 */
function createInterface(io?: PromptIO): readline.Interface {
  return readline.createInterface({
    input: io?.input ?? process.stdin,
    output: io?.output ?? process.stdout,
  })
}

/**
 * Check if interactive mode is available
 */
export function isInteractive(io?: PromptIO): boolean {
  if (io) {
    // For testing - assume interactive
    return true
  }
  return isTTY()
}

/**
 * Prompt for text input
 *
 * @example
 * ```ts
 * const name = await promptText('Database name:', { default: 'mydb' })
 * ```
 */
export async function promptText(
  message: string,
  options: TextPromptOptions = {},
  io?: PromptIO
): Promise<string> {
  const rl = createInterface(io)

  const defaultText = options.default ? ` (${options.default})` : ''
  const prefix = isColorEnabled() ? `${colors.cyan}?${colors.reset}` : '?'
  const prompt = `${prefix} ${message}${defaultText} `

  return new Promise((resolve, reject) => {
    const ask = () => {
      rl.question(prompt, (answer) => {
        const value = answer.trim() || options.default || ''

        if (options.validate) {
          const result = options.validate(value)
          if (result !== true && result !== undefined) {
            // Invalid input, show error and ask again
            const errorPrefix = isColorEnabled()
              ? `${colors.red}!${colors.reset}`
              : '!'
            const output = io?.output ?? process.stdout
            output.write(`${errorPrefix} ${result}\n`)
            ask()
            return
          }
        }

        rl.close()
        resolve(value)
      })
    }

    rl.on('error', reject)
    rl.on('close', () => {
      // Handle Ctrl+C
    })

    ask()
  })
}

/**
 * Prompt for yes/no confirmation
 *
 * @example
 * ```ts
 * const confirmed = await promptConfirm('Continue?', { default: true })
 * ```
 */
export async function promptConfirm(
  message: string,
  options: ConfirmPromptOptions = {},
  io?: PromptIO
): Promise<boolean> {
  const rl = createInterface(io)

  const defaultHint =
    options.default === true ? 'Y/n' : options.default === false ? 'y/N' : 'y/n'
  const prefix = isColorEnabled() ? `${colors.cyan}?${colors.reset}` : '?'
  const prompt = `${prefix} ${message} (${defaultHint}) `

  return new Promise((resolve) => {
    rl.question(prompt, (answer) => {
      rl.close()

      const normalized = answer.trim().toLowerCase()

      if (normalized === '') {
        resolve(options.default ?? false)
      } else if (normalized === 'y' || normalized === 'yes') {
        resolve(true)
      } else if (normalized === 'n' || normalized === 'no') {
        resolve(false)
      } else {
        // Invalid input, use default
        resolve(options.default ?? false)
      }
    })
  })
}

/**
 * Prompt for selecting from a list of choices
 *
 * @example
 * ```ts
 * const storage = await promptSelect('Storage type:', {
 *   choices: [
 *     { value: 'fs', label: 'Filesystem', description: 'Local file storage' },
 *     { value: 'r2', label: 'R2', description: 'Cloudflare R2 storage' },
 *   ],
 *   default: 'fs'
 * })
 * ```
 */
export async function promptSelect<T extends string>(
  message: string,
  options: SelectPromptOptions<T>,
  io?: PromptIO
): Promise<T> {
  const rl = createInterface(io)
  const output = io?.output ?? process.stdout

  const prefix = isColorEnabled() ? `${colors.cyan}?${colors.reset}` : '?'
  output.write(`${prefix} ${message}\n`)

  // Show choices
  for (let i = 0; i < options.choices.length; i++) {
    const choice = options.choices[i]!
    const isDefault = choice.value === options.default
    const marker = isDefault
      ? isColorEnabled()
        ? `${colors.green}*${colors.reset}`
        : '*'
      : ' '
    const label = isColorEnabled() && isDefault
      ? `${colors.bold}${choice.label}${colors.reset}`
      : choice.label
    const desc = choice.description
      ? isColorEnabled()
        ? ` ${colors.gray}${choice.description}${colors.reset}`
        : ` (${choice.description})`
      : ''

    output.write(`  ${marker} ${i + 1}. ${label}${desc}\n`)
  }

  const defaultIndex = options.default
    ? options.choices.findIndex((c) => c.value === options.default) + 1
    : 1
  const prompt = `  Enter choice (1-${options.choices.length}) [${defaultIndex}]: `

  return new Promise((resolve) => {
    const ask = () => {
      rl.question(prompt, (answer) => {
        const trimmed = answer.trim()

        // Empty input means default
        if (trimmed === '') {
          rl.close()
          resolve(options.default ?? options.choices[0]!.value)
          return
        }

        // Parse number
        const num = parseInt(trimmed, 10)
        if (!isNaN(num) && num >= 1 && num <= options.choices.length) {
          rl.close()
          resolve(options.choices[num - 1]!.value)
          return
        }

        // Check if they typed the value directly
        const matchingChoice = options.choices.find(
          (c) => c.value.toLowerCase() === trimmed.toLowerCase()
        )
        if (matchingChoice) {
          rl.close()
          resolve(matchingChoice.value)
          return
        }

        // Invalid input, ask again
        const errorPrefix = isColorEnabled()
          ? `${colors.red}!${colors.reset}`
          : '!'
        output.write(`${errorPrefix} Please enter a number from 1 to ${options.choices.length}\n`)
        ask()
      })
    }

    ask()
  })
}

/**
 * Prompt for a list of items (comma-separated)
 *
 * @example
 * ```ts
 * const namespaces = await promptList('Initial namespaces:', {
 *   default: ['users', 'posts']
 * })
 * ```
 */
export async function promptList(
  message: string,
  options: { default?: string[]; validate?: (values: string[]) => string | undefined | true } = {},
  io?: PromptIO
): Promise<string[]> {
  const defaultText = options.default?.length
    ? options.default.join(', ')
    : ''

  const result = await promptText(
    `${message} (comma-separated)`,
    {
      default: defaultText,
      validate: (value) => {
        if (!value) return true
        const items = value.split(',').map((s) => s.trim()).filter(Boolean)
        if (options.validate) {
          return options.validate(items)
        }
        return true
      },
    },
    io
  )

  if (!result) return []
  return result.split(',').map((s) => s.trim()).filter(Boolean)
}

/**
 * Print a styled header for a wizard step
 */
export function printWizardHeader(title: string, step?: number, total?: number): void {
  const stepText =
    step !== undefined && total !== undefined ? ` (Step ${step}/${total})` : ''

  if (isColorEnabled()) {
    console.log(`\n${colors.bold}${colors.cyan}${title}${stepText}${colors.reset}`)
    console.log(`${colors.gray}${'─'.repeat(title.length + stepText.length)}${colors.reset}\n`)
  } else {
    console.log(`\n${title}${stepText}`)
    console.log(`${'-'.repeat(title.length + stepText.length)}\n`)
  }
}

/**
 * Print a summary of collected values
 */
export function printWizardSummary(
  title: string,
  values: Record<string, string | string[] | boolean | undefined>
): void {
  if (isColorEnabled()) {
    console.log(`\n${colors.bold}${colors.green}${title}${colors.reset}`)
    console.log(`${colors.gray}${'─'.repeat(title.length)}${colors.reset}`)
  } else {
    console.log(`\n${title}`)
    console.log('-'.repeat(title.length))
  }

  for (const [key, value] of Object.entries(values)) {
    if (value === undefined) continue

    const displayValue = Array.isArray(value)
      ? value.join(', ') || '(none)'
      : typeof value === 'boolean'
        ? value ? 'Yes' : 'No'
        : value || '(none)'

    if (isColorEnabled()) {
      console.log(`  ${colors.gray}${key}:${colors.reset} ${displayValue}`)
    } else {
      console.log(`  ${key}: ${displayValue}`)
    }
  }

  console.log()
}
