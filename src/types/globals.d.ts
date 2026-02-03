/**
 * Global type augmentations for runtime environments
 *
 * This file augments globalThis with optional Bun/Deno properties
 * for runtime detection without requiring casts to `any`.
 */

/**
 * Bun runtime global type
 * Minimal type declarations for runtime detection
 */
interface BunGlobal {
  version: string
}

/**
 * Deno runtime global types
 * Minimal type declarations for runtime detection
 */
interface DenoGlobal {
  version: {
    deno: string
  }
  cwd(): string
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
