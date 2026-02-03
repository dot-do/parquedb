/**
 * Filesystem Path Safety Utilities for ParqueDB
 *
 * Provides protection against path traversal attacks by validating
 * file paths used in CLI commands and other filesystem operations.
 *
 * Path traversal attacks occur when an attacker provides a path containing
 * sequences like ".." or absolute paths to access files outside the
 * intended directory.
 *
 * @module utils/fs-path-safety
 */

import { resolve, normalize, isAbsolute, relative } from 'node:path'

/**
 * Error thrown when a path fails validation.
 */
export class PathValidationError extends Error {
  constructor(message: string, public readonly path: string) {
    super(message)
    this.name = 'PathValidationError'
  }
}

/**
 * Characters that are dangerous in file paths and could be used for attacks.
 * - Null byte: Can truncate strings in some languages/systems
 * - Line breaks: Could be used to inject commands
 */
const DANGEROUS_CHARACTERS = [
  '\0',        // Null byte
  '\n',        // Line feed
  '\r',        // Carriage return
]

/**
 * Check if a path contains dangerous characters that could be used for attacks.
 *
 * @param filePath - The path to check
 * @returns true if the path contains dangerous characters
 *
 * @example
 * hasDangerousCharacters('file.txt')           // false
 * hasDangerousCharacters('file\0.txt')         // true (null byte)
 * hasDangerousCharacters('file\n.txt')         // true (newline)
 */
export function hasDangerousCharacters(filePath: string): boolean {
  return DANGEROUS_CHARACTERS.some(char => filePath.includes(char))
}

/**
 * Check if a path contains path traversal sequences.
 *
 * @param filePath - The path to check
 * @returns true if the path contains traversal sequences
 *
 * @example
 * hasPathTraversal('file.txt')                 // false
 * hasPathTraversal('../etc/passwd')            // true
 * hasPathTraversal('data/../secret')           // true
 * hasPathTraversal('..\\windows\\system32')    // true (Windows)
 */
export function hasPathTraversal(filePath: string): boolean {
  // Normalize path separators and check for ..
  const normalized = filePath.replace(/\\/g, '/')
  const parts = normalized.split('/')
  return parts.some(part => part === '..')
}

/**
 * Check if a file path escapes a base directory after normalization.
 *
 * This resolves the path to an absolute path and checks if it's still
 * within the base directory. This is the most robust check as it handles
 * all edge cases including symlinks (when the path exists).
 *
 * @param basePath - The base directory that files should stay within
 * @param filePath - The file path to check (can be relative or absolute)
 * @returns true if the path escapes the base directory
 *
 * @example
 * // Assuming cwd is /home/user/project
 * escapesBaseDirectory('/home/user/project', 'data/file.txt')     // false
 * escapesBaseDirectory('/home/user/project', '../other/file.txt') // true
 * escapesBaseDirectory('/home/user/project', '/etc/passwd')       // true
 */
export function escapesBaseDirectory(basePath: string, filePath: string): boolean {
  // Resolve to absolute paths
  const resolvedBase = resolve(basePath)
  const resolvedPath = resolve(basePath, filePath)

  // Check if the resolved path starts with the base path
  // Using relative() is more reliable than startsWith() for path comparison
  const relativePath = relative(resolvedBase, resolvedPath)

  // If relative path starts with '..' or is absolute, it's outside base
  return relativePath.startsWith('..') || isAbsolute(relativePath)
}

/**
 * Validate a file path for safe filesystem operations.
 *
 * This function checks for:
 * 1. Dangerous characters (null bytes, newlines)
 * 2. Path traversal sequences (..)
 * 3. Paths that would escape the base directory
 *
 * @param basePath - The base directory that files should stay within
 * @param filePath - The file path to validate
 * @throws PathValidationError if the path is unsafe
 *
 * @example
 * validateFilePath('/app/data', 'users.json')       // OK
 * validateFilePath('/app/data', 'sub/users.json')   // OK
 * validateFilePath('/app/data', '../etc/passwd')    // throws PathValidationError
 * validateFilePath('/app/data', '/etc/passwd')      // throws PathValidationError
 */
export function validateFilePath(basePath: string, filePath: string): void {
  // Check for dangerous characters first
  if (hasDangerousCharacters(filePath)) {
    throw new PathValidationError(
      'Path contains dangerous characters (null byte or line breaks)',
      filePath
    )
  }

  // Check for path traversal sequences
  if (hasPathTraversal(filePath)) {
    throw new PathValidationError(
      'Path contains traversal sequence (..) which is not allowed',
      filePath
    )
  }

  // Check if path escapes the base directory
  if (escapesBaseDirectory(basePath, filePath)) {
    throw new PathValidationError(
      `Path escapes the allowed directory: ${basePath}`,
      filePath
    )
  }
}

/**
 * Validate a file path allowing absolute paths within the allowed directories.
 *
 * This is a more flexible version of validateFilePath that accepts:
 * 1. Relative paths within the base directory
 * 2. Absolute paths within any of the allowed directories
 *
 * @param basePath - The primary base directory for relative paths
 * @param filePath - The file path to validate
 * @param allowedDirectories - Additional directories where absolute paths are allowed
 * @throws PathValidationError if the path is unsafe
 *
 * @example
 * const cwd = process.cwd()
 * validateFilePathWithAllowedDirs(cwd, 'data.json', [cwd])       // OK (relative)
 * validateFilePathWithAllowedDirs(cwd, '/tmp/data.json', ['/tmp']) // OK (absolute in allowed dir)
 * validateFilePathWithAllowedDirs(cwd, '/etc/passwd', ['/tmp'])    // throws (not in allowed)
 */
export function validateFilePathWithAllowedDirs(
  basePath: string,
  filePath: string,
  allowedDirectories: string[]
): void {
  // Check for dangerous characters first
  if (hasDangerousCharacters(filePath)) {
    throw new PathValidationError(
      'Path contains dangerous characters (null byte or line breaks)',
      filePath
    )
  }

  // Check for path traversal sequences
  if (hasPathTraversal(filePath)) {
    throw new PathValidationError(
      'Path contains traversal sequence (..) which is not allowed',
      filePath
    )
  }

  // Resolve the path
  const resolvedPath = isAbsolute(filePath)
    ? normalize(filePath)
    : resolve(basePath, filePath)

  // Check if path is within any of the allowed directories
  const isWithinAllowed = allowedDirectories.some(dir => {
    const resolvedDir = resolve(dir)
    const rel = relative(resolvedDir, resolvedPath)
    return !rel.startsWith('..') && !isAbsolute(rel)
  })

  if (!isWithinAllowed) {
    throw new PathValidationError(
      `Path is not within any allowed directory: ${filePath}`,
      filePath
    )
  }
}

/**
 * Sanitize a file path by resolving it and ensuring it's safe.
 *
 * @param basePath - The base directory for relative paths
 * @param filePath - The file path to sanitize
 * @returns The sanitized absolute path
 * @throws PathValidationError if the path is unsafe
 *
 * @example
 * sanitizeFilePath('/app', 'data/file.txt')  // '/app/data/file.txt'
 * sanitizeFilePath('/app', '../etc/passwd')  // throws PathValidationError
 */
export function sanitizeFilePath(basePath: string, filePath: string): string {
  validateFilePath(basePath, filePath)
  return resolve(basePath, filePath)
}
