/**
 * ParqueDB Streams Module
 *
 * Provides stream adapters for integrating ParqueDB with different
 * streaming environments:
 *
 * - **Node.js Streams**: Native Node.js Readable/Writable streams
 * - **Web Streams**: Web Streams API (ReadableStream/WritableStream)
 * - **Async Iterators**: Async iteration patterns
 *
 * ## Features
 *
 * - Bidirectional conversion between Node.js and Web streams
 * - Backpressure handling
 * - Error propagation
 * - Utility functions for common stream operations
 *
 * ## Quick Start
 *
 * ```typescript
 * import { createStreamAdapter } from 'parquedb/streams'
 * import { createReadStream } from 'node:fs'
 *
 * // Convert Node.js stream to Web stream
 * const nodeStream = createReadStream('data.parquet')
 * const webStream = createStreamAdapter(nodeStream).toWebReadable()
 *
 * // Use with fetch Response
 * const response = new Response(webStream)
 * ```
 *
 * @module streams
 */

// =============================================================================
// Node.js Stream Adapter
// =============================================================================

export {
  // Main factory function
  createStreamAdapter,

  // Individual adapter factories
  createNodeReadableAdapter,
  createNodeWritableAdapter,
  createWebReadableAdapter,
  createWebWritableAdapter,

  // Type guards
  isWebReadableStream,
  isWebWritableStream,
  isNodeReadable,
  isNodeWritable,

  // Utility functions
  createTee,
  createTransform,
  collectStream,
  fromAsyncIterator,
  webReadableFromAsyncIterator,

  // Types
  type NodeStreamAdapterOptions,
  type ReadableAdapterResult,
  type WritableAdapterResult,
  type WebReadableAdapterResult,
  type WebWritableAdapterResult,
  type AsyncWriter,
  type StreamInput,
  type StreamAdapterResult,
} from './node-adapter'
