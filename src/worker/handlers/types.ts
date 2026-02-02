/**
 * Shared types for route handlers
 */

import type { ParqueDBWorker } from '../index'

/**
 * Interface for the worker instance used by handlers.
 * Uses the concrete ParqueDBWorker type for full compatibility.
 */
export type WorkerInterface = ParqueDBWorker

/**
 * Context passed to route handlers
 */
export interface HandlerContext {
  /** The incoming HTTP request */
  request: Request
  /** Parsed URL */
  url: URL
  /** Base URL for building links */
  baseUrl: string
  /** Request path */
  path: string
  /** ParqueDB worker instance */
  worker: WorkerInterface
  /** Request start time for latency calculation */
  startTime: number
  /** Execution context for waitUntil */
  ctx: ExecutionContext
}
