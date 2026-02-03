import type { StorageBackend } from '../types/storage'

/**
 * Reference types supported by ParqueDB
 */
export type RefType = 'heads' | 'tags'

/**
 * HEAD state - either points to a branch or is detached
 */
export interface HeadState {
  readonly type: 'branch' | 'detached'
  readonly ref: string  // Branch name (e.g., 'main') or commit hash
}

/**
 * Manages git-style references (branches, tags, HEAD)
 *
 * Storage layout:
 * - _meta/HEAD                 - contains "refs/heads/main" or commit hash
 * - _meta/refs/heads/main      - contains commit hash
 * - _meta/refs/tags/v1.0.0     - contains commit hash
 * - _meta/commits/{hash}.json  - commit objects
 */
export class RefManager {
  constructor(private storage: StorageBackend) {}

  /**
   * Resolve a ref to a commit hash
   * Supports: 'HEAD', 'refs/heads/main', 'main', 'refs/tags/v1.0.0', 'v1.0.0'
   *
   * @param ref Reference to resolve
   * @returns Commit hash or null if not found
   */
  async resolveRef(ref: string): Promise<string | null> {
    // Normalize ref
    const normalizedRef = this.normalizeRef(ref)

    // Special case: HEAD
    if (normalizedRef === 'HEAD') {
      const headState = await this.getHead()
      if (headState.type === 'detached') {
        return headState.ref // Return commit hash directly
      }
      // HEAD points to a branch, resolve that branch
      return this.resolveRef(headState.ref)
    }

    // Try to read the ref file
    const path = `_meta/${normalizedRef}`
    const exists = await this.storage.exists(path)

    if (!exists) {
      return null
    }

    const data = await this.storage.read(path)
    const content = new TextDecoder().decode(data).trim()

    // If content looks like a ref (starts with 'refs/'), recursively resolve
    if (content.startsWith('refs/')) {
      return this.resolveRef(content)
    }

    // Otherwise, it should be a commit hash
    return content
  }

  /**
   * Update a ref to point to a commit hash
   *
   * @param ref Reference to update (e.g., 'main', 'refs/heads/main', 'refs/tags/v1.0.0')
   * @param hash Commit hash to point to
   */
  async updateRef(ref: string, hash: string): Promise<void> {
    const normalizedRef = this.normalizeRef(ref)

    if (normalizedRef === 'HEAD') {
      throw new Error('Cannot directly update HEAD. Use setHead() to change branches.')
    }

    const path = `_meta/${normalizedRef}`
    await this.storage.write(path, new TextEncoder().encode(hash))
  }

  /**
   * List all refs of a specific type
   *
   * @param type 'heads' (branches) or 'tags', or undefined for all
   * @returns Array of ref names (e.g., ['refs/heads/main', 'refs/tags/v1.0.0'])
   */
  async listRefs(type?: RefType): Promise<string[]> {
    const refs: string[] = []

    // List heads (branches)
    if (!type || type === 'heads') {
      const headsPath = '_meta/refs/heads/'
      const heads = await this.listDirectory(headsPath)
      refs.push(...heads.map(name => `refs/heads/${name}`))
    }

    // List tags
    if (!type || type === 'tags') {
      const tagsPath = '_meta/refs/tags/'
      const tags = await this.listDirectory(tagsPath)
      refs.push(...tags.map(name => `refs/tags/${name}`))
    }

    return refs
  }

  /**
   * Delete a ref
   *
   * @param ref Reference to delete
   */
  async deleteRef(ref: string): Promise<void> {
    const normalizedRef = this.normalizeRef(ref)

    if (normalizedRef === 'HEAD') {
      throw new Error('Cannot delete HEAD')
    }

    const path = `_meta/${normalizedRef}`
    const exists = await this.storage.exists(path)

    if (!exists) {
      throw new Error(`Ref not found: ${ref}`)
    }

    await this.storage.delete(path)
  }

  /**
   * Get current HEAD state
   *
   * @returns HeadState indicating branch or detached commit
   */
  async getHead(): Promise<HeadState> {
    const path = '_meta/HEAD'
    const exists = await this.storage.exists(path)

    if (!exists) {
      // HEAD doesn't exist - return default
      return { type: 'branch', ref: 'main' }
    }

    const data = await this.storage.read(path)
    const content = new TextDecoder().decode(data).trim()

    // If content starts with 'refs/heads/', it's a branch
    if (content.startsWith('refs/heads/')) {
      const branch = content.substring('refs/heads/'.length)
      return { type: 'branch', ref: branch }
    }

    // Otherwise, it's a detached commit hash
    return { type: 'detached', ref: content }
  }

  /**
   * Set HEAD to point to a branch
   *
   * @param branch Branch name (e.g., 'main')
   */
  async setHead(branch: string): Promise<void> {
    const path = '_meta/HEAD'
    const content = `refs/heads/${branch}`
    await this.storage.write(path, new TextEncoder().encode(content))
  }

  /**
   * Set HEAD to a specific commit (detached state)
   *
   * @param hash Commit hash
   */
  async detachHead(hash: string): Promise<void> {
    const path = '_meta/HEAD'
    await this.storage.write(path, new TextEncoder().encode(hash))
  }

  /**
   * Normalize ref names to canonical form
   *
   * Examples:
   * - 'main' -> 'refs/heads/main'
   * - 'v1.0.0' -> 'refs/tags/v1.0.0'
   * - 'refs/heads/main' -> 'refs/heads/main'
   * - 'HEAD' -> 'HEAD'
   *
   * @param ref Reference name
   * @returns Normalized reference
   */
  private normalizeRef(ref: string): string {
    if (ref === 'HEAD') {
      return 'HEAD'
    }

    if (ref.startsWith('refs/')) {
      return ref
    }

    // Check if it looks like a tag (starts with 'v' and has dots)
    if (ref.startsWith('v') && ref.includes('.')) {
      return `refs/tags/${ref}`
    }

    // Default to branch
    return `refs/heads/${ref}`
  }

  /**
   * List files in a directory (helper method)
   *
   * @param path Directory path
   * @returns Array of file names (not full paths)
   */
  private async listDirectory(path: string): Promise<string[]> {
    try {
      // Use the storage backend's list method
      const result = await this.storage.list(path, {})

      // Extract just the file names (not full paths)
      const names = result.files.map((filePath: string) => {
        // Remove the path prefix to get just the filename
        const relativePath = filePath.startsWith(path)
          ? filePath.substring(path.length)
          : filePath
        // Remove leading slash if present
        return relativePath.startsWith('/') ? relativePath.substring(1) : relativePath
      })

      return names
    } catch (error) {
      // Directory doesn't exist or can't be read
      return []
    }
  }
}

/**
 * Create a new RefManager instance
 *
 * @param storage StorageBackend to use
 * @returns RefManager instance
 */
export function createRefManager(storage: StorageBackend): RefManager {
  return new RefManager(storage)
}
