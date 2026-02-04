/**
 * Global type augmentations for runtime environments
 *
 * This file augments globalThis with optional Bun/Deno properties
 * for runtime detection without requiring casts to `any`.
 */

/**
 * Bun runtime global type
 * Type declarations for runtime detection and environment access
 */
interface BunGlobal {
  version: string
  env?: Record<string, string | undefined>
}

/**
 * Deno runtime global types
 * Type declarations for runtime detection and environment access
 */
interface DenoGlobal {
  version: {
    deno: string
  }
  cwd(): string
  env: {
    get(key: string): string | undefined
    set(key: string, value: string): void
    delete(key: string): void
    toObject(): Record<string, string>
  }
}

/**
 * Augment globalThis with optional Bun/Deno properties
 */
declare global {
  // eslint-disable-next-line no-var
  var Bun: BunGlobal | undefined
  // eslint-disable-next-line no-var
  var Deno: DenoGlobal | undefined
}

// Make this a module so declare global works
export {}
