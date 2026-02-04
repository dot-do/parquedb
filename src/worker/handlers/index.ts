/**
 * Route Handlers Index
 *
 * Re-exports all route handlers for use in the main worker.
 */

// Core handlers
export * from './types'
export * from './root'
export * from './health'
export * from './health-checks'
export * from './metrics'
export * from './debug'
export * from './datasets'
export * from './entity'
export * from './relationships'
export * from './ns'
export * from './csrf-validation'

// Route module handlers (used by route-registry)
export * from './benchmark'
export * from './migration'
export * from './vacuum'
export * from './compaction'
