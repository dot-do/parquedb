/**
 * GeneratedContent Materialized View Tests
 *
 * Tests for the GeneratedContentMV that captures and analyzes
 * AI-generated content (text and structured objects).
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  GeneratedContentMV,
  createGeneratedContentMV,
  createGeneratedContentMVHandler,
  type GeneratedContentRecord,
  type RecordContentInput,
  GENERATED_CONTENT_SCHEMA,
  detectContentType,
  detectCodeLanguage,
  estimateTokenCount,
} from '../../../src/streaming/generated-content'
import type { StorageBackend } from '../../../src/types/storage'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock storage backend
 */
function createMockStorage(): StorageBackend & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>()

  return {
    files,
    async read(path: string): Promise<Uint8Array> {
      const data = files.get(path)
      if (!data) throw new Error(`File not found: ${path}`)
      return data
    },
    async write(path: string, data: Uint8Array): Promise<{ etag: string; size: number }> {
      files.set(path, data)
      return { etag: 'mock-etag', size: data.length }
    },
    async writeAtomic(path: string, data: Uint8Array): Promise<{ etag: string; size: number }> {
      files.set(path, data)
      return { etag: 'mock-etag', size: data.length }
    },
    async exists(path: string): Promise<boolean> {
      return files.has(path)
    },
    async delete(path: string): Promise<void> {
      files.delete(path)
    },
    async list(prefix: string): Promise<string[]> {
      return Array.from(files.keys()).filter(k => k.startsWith(prefix))
    },
  } as StorageBackend & { files: Map<string, Uint8Array> }
}

/**
 * Create a sample content input
 */
function createContentInput(overrides: Partial<RecordContentInput> = {}): RecordContentInput {
  return {
    requestId: 'req_test123',
    modelId: 'gpt-4',
    providerId: 'openai',
    contentType: 'text',
    content: 'This is a test response from the AI model.',
    tokenCount: 10,
    finishReason: 'stop',
    latencyMs: 150,
    ...overrides,
  }
}

/**
 * Create a deterministic ID generator for testing
 */
function createIdGenerator(): () => string {
  let counter = 0
  return () => `test-id-${++counter}`
}

// =============================================================================
// GeneratedContentMV Tests
// =============================================================================

describe('GeneratedContentMV', () => {
  let storage: StorageBackend & { files: Map<string, Uint8Array> }
  let mv: GeneratedContentMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createGeneratedContentMV({
      storage,
      datasetPath: 'ai/generated',
      flushThreshold: 100, // Low threshold for testing
      flushIntervalMs: 60000, // High interval so we control flushing
      generateId: createIdGenerator(),
    })
  })

  afterEach(async () => {
    if (mv.isRunning()) {
      await mv.stop()
    }
  })

  describe('constructor', () => {
    it('creates an instance with default config', () => {
      const defaultMv = createGeneratedContentMV({
        storage,
        datasetPath: 'ai/test',
      })
      expect(defaultMv).toBeInstanceOf(GeneratedContentMV)
    })

    it('normalizes dataset path by removing trailing slash', () => {
      const mvWithSlash = createGeneratedContentMV({
        storage,
        datasetPath: 'ai/test/',
        generateId: createIdGenerator(),
      })
      mvWithSlash.start()
      expect(mvWithSlash.isRunning()).toBe(true)
      mvWithSlash.stop()
    })
  })

  describe('start/stop', () => {
    it('starts and stops the MV', () => {
      expect(mv.isRunning()).toBe(false)

      mv.start()
      expect(mv.isRunning()).toBe(true)

      mv.start() // Should be idempotent
      expect(mv.isRunning()).toBe(true)
    })

    it('stop flushes remaining records', async () => {
      mv.start()

      // Ingest some records
      await mv.ingestContent(createContentInput())
      expect(mv.getBuffer().length).toBeGreaterThan(0)

      // Stop should flush
      await mv.stop()
      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)
    })

    it('stop is idempotent', async () => {
      mv.start()
      await mv.stop()
      await mv.stop() // Should not throw
      expect(mv.isRunning()).toBe(false)
    })
  })

  describe('ingestContent', () => {
    it('ingests simple text content', async () => {
      mv.start()

      await mv.ingestContent(createContentInput())

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(1)
      expect(buffer[0].modelId).toBe('gpt-4')
      expect(buffer[0].contentType).toBe('text')
      expect(buffer[0].content).toBe('This is a test response from the AI model.')
    })

    it('ingests JSON object content', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({
        contentType: 'json',
        content: { key: 'value', nested: { foo: 'bar' } },
      }))

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(1)
      expect(buffer[0].contentType).toBe('json')
      expect(buffer[0].content).toBe('{"key":"value","nested":{"foo":"bar"}}')
    })

    it('calculates content length correctly', async () => {
      mv.start()

      const testContent = 'Hello, world!'
      await mv.ingestContent(createContentInput({
        content: testContent,
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].contentLength).toBe(testContent.length)
    })

    it('generates content hash', async () => {
      mv.start()

      await mv.ingestContent(createContentInput())

      const buffer = mv.getBuffer()
      expect(buffer[0].contentHash).toBeDefined()
      expect(buffer[0].contentHash).not.toBeNull()
    })

    it('records all optional fields', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({
        promptTokenCount: 50,
        totalTokenCount: 60,
        isStreaming: true,
        isCached: false,
        classification: 'safe',
        toolName: 'get_weather',
        toolCallId: 'call_123',
        language: 'en',
        sessionId: 'session_abc',
        userId: 'user_xyz',
        source: 'chat-app',
        metadata: { customField: 'value' },
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].promptTokenCount).toBe(50)
      expect(buffer[0].totalTokenCount).toBe(60)
      expect(buffer[0].isStreaming).toBe(true)
      expect(buffer[0].isCached).toBe(false)
      expect(buffer[0].classification).toBe('safe')
      expect(buffer[0].toolName).toBe('get_weather')
      expect(buffer[0].toolCallId).toBe('call_123')
      expect(buffer[0].language).toBe('en')
      expect(buffer[0].sessionId).toBe('session_abc')
      expect(buffer[0].userId).toBe('user_xyz')
      expect(buffer[0].source).toBe('chat-app')
      expect(buffer[0].metadata).toBe('{"customField":"value"}')
    })

    it('uses default values for optional fields', async () => {
      mv.start()

      await mv.ingestContent({
        requestId: 'req_123',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Test',
      })

      const buffer = mv.getBuffer()
      expect(buffer[0].providerId).toBeNull()
      expect(buffer[0].tokenCount).toBeNull()
      expect(buffer[0].finishReason).toBe('unknown')
      expect(buffer[0].latencyMs).toBeNull()
      expect(buffer[0].isStreaming).toBe(false)
      expect(buffer[0].isCached).toBe(false)
      expect(buffer[0].classification).toBe('unclassified')
    })
  })

  describe('ingestContents', () => {
    it('ingests multiple content records at once', async () => {
      mv.start()

      await mv.ingestContents([
        createContentInput({ requestId: 'req_1', content: 'Content 1' }),
        createContentInput({ requestId: 'req_2', content: 'Content 2' }),
        createContentInput({ requestId: 'req_3', content: 'Content 3' }),
      ])

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(3)
      expect(buffer.map(r => r.requestId)).toEqual(['req_1', 'req_2', 'req_3'])
    })
  })

  describe('ingestRecords', () => {
    it('ingests raw GeneratedContentRecords', async () => {
      mv.start()

      const records: GeneratedContentRecord[] = [
        {
          id: 'rec-1',
          requestId: 'req_test',
          timestamp: Date.now(),
          modelId: 'claude-3',
          providerId: 'anthropic',
          contentType: 'text',
          content: 'Test content',
          contentLength: 12,
          tokenCount: 5,
          promptTokenCount: null,
          totalTokenCount: null,
          finishReason: 'stop',
          latencyMs: 100,
          isStreaming: false,
          isCached: false,
          classification: 'safe',
          toolName: null,
          toolCallId: null,
          language: 'en',
          contentHash: 'abc123',
          sessionId: null,
          userId: null,
          source: null,
          metadata: null,
        },
      ]

      await mv.ingestRecords(records)

      expect(mv.getBuffer().length).toBe(1)
      expect(mv.getStats().recordsIngested).toBe(1)
    })
  })

  describe('flush', () => {
    it('flushes buffer to Parquet file', async () => {
      mv.start()

      await mv.ingestContent(createContentInput())
      expect(mv.getBuffer().length).toBe(1)

      await mv.flush()

      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)

      // Verify file path has timestamp partitioning
      const [path] = Array.from(storage.files.keys())
      expect(path).toMatch(/ai\/generated\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/content-\d+\.parquet/)
    })

    it('handles empty buffer gracefully', async () => {
      mv.start()

      await mv.flush()

      expect(storage.files.size).toBe(0)
    })

    it('auto-flushes when threshold is reached', async () => {
      // Create MV with low threshold
      mv = createGeneratedContentMV({
        storage,
        datasetPath: 'ai/generated',
        flushThreshold: 3,
        generateId: createIdGenerator(),
      })
      mv.start()

      // Ingest records below threshold
      await mv.ingestContent(createContentInput({ requestId: 'req_1' }))
      await mv.ingestContent(createContentInput({ requestId: 'req_2' }))
      expect(storage.files.size).toBe(0)

      // This should trigger auto-flush
      await mv.ingestContent(createContentInput({ requestId: 'req_3' }))
      expect(storage.files.size).toBe(1)
    })

    it('puts records back in buffer on flush failure', async () => {
      // Create storage that fails on write
      const failingStorage = createMockStorage()
      failingStorage.writeAtomic = async () => {
        throw new Error('Storage write failed')
      }

      const failingMv = createGeneratedContentMV({
        storage: failingStorage,
        datasetPath: 'ai/generated',
        generateId: createIdGenerator(),
      })

      await failingMv.ingestContent(createContentInput())
      expect(failingMv.getBuffer().length).toBe(1)

      await expect(failingMv.flush()).rejects.toThrow('Storage write failed')
      expect(failingMv.getBuffer().length).toBe(1) // Records restored
    })
  })

  describe('statistics', () => {
    it('tracks ingested records', async () => {
      mv.start()

      await mv.ingestContent(createContentInput())

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(1)
      expect(stats.bufferSize).toBe(1)
    })

    it('tracks records by content type', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({ contentType: 'text' }))
      await mv.ingestContent(createContentInput({ contentType: 'code' }))
      await mv.ingestContent(createContentInput({ contentType: 'json' }))
      await mv.ingestContent(createContentInput({ contentType: 'json' }))

      const stats = mv.getStats()
      expect(stats.byContentType.text).toBe(1)
      expect(stats.byContentType.code).toBe(1)
      expect(stats.byContentType.json).toBe(2)
    })

    it('tracks records by model', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({ modelId: 'gpt-4' }))
      await mv.ingestContent(createContentInput({ modelId: 'gpt-4' }))
      await mv.ingestContent(createContentInput({ modelId: 'claude-3' }))

      const stats = mv.getStats()
      expect(stats.byModel['gpt-4']).toBe(2)
      expect(stats.byModel['claude-3']).toBe(1)
    })

    it('tracks records by finish reason', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({ finishReason: 'stop' }))
      await mv.ingestContent(createContentInput({ finishReason: 'length' }))
      await mv.ingestContent(createContentInput({ finishReason: 'tool_calls' }))

      const stats = mv.getStats()
      expect(stats.byFinishReason.stop).toBe(1)
      expect(stats.byFinishReason.length).toBe(1)
      expect(stats.byFinishReason.tool_calls).toBe(1)
    })

    it('tracks records by classification', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({ classification: 'safe' }))
      await mv.ingestContent(createContentInput({ classification: 'sensitive' }))
      await mv.ingestContent(createContentInput({ classification: 'pii' }))

      const stats = mv.getStats()
      expect(stats.byClassification.safe).toBe(1)
      expect(stats.byClassification.sensitive).toBe(1)
      expect(stats.byClassification.pii).toBe(1)
    })

    it('tracks total tokens', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({ tokenCount: 100 }))
      await mv.ingestContent(createContentInput({ tokenCount: 200 }))
      await mv.ingestContent(createContentInput({ tokenCount: 50 }))

      const stats = mv.getStats()
      expect(stats.totalTokens).toBe(350)
    })

    it('tracks total characters', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({ content: 'Hello' })) // 5 chars
      await mv.ingestContent(createContentInput({ content: 'World!' })) // 6 chars

      const stats = mv.getStats()
      expect(stats.totalCharacters).toBe(11)
    })

    it('tracks written records and files after flush', async () => {
      mv.start()

      await mv.ingestContent(createContentInput())
      await mv.ingestContent(createContentInput())
      await mv.flush()

      const stats = mv.getStats()
      expect(stats.recordsWritten).toBe(2)
      expect(stats.filesCreated).toBe(1)
      expect(stats.bytesWritten).toBeGreaterThan(0)
      expect(stats.flushCount).toBe(1)
      expect(stats.lastFlushAt).not.toBeNull()
    })

    it('resets statistics', async () => {
      mv.start()

      await mv.ingestContent(createContentInput())
      expect(mv.getStats().recordsIngested).toBe(1)

      mv.resetStats()

      const stats = mv.getStats()
      expect(stats.recordsIngested).toBe(0)
      expect(stats.bufferSize).toBe(1) // Buffer is not cleared
    })
  })

  describe('content type variations', () => {
    it('handles tool_call content type', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({
        contentType: 'tool_call',
        content: { name: 'get_weather', arguments: { city: 'London' } },
        toolName: 'get_weather',
        toolCallId: 'call_abc123',
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].contentType).toBe('tool_call')
      expect(buffer[0].toolName).toBe('get_weather')
      expect(buffer[0].toolCallId).toBe('call_abc123')
    })

    it('handles code content type', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({
        contentType: 'code',
        content: 'function hello() {\n  return "world";\n}',
        language: 'code:javascript',
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].contentType).toBe('code')
      expect(buffer[0].language).toBe('code:javascript')
    })

    it('handles markdown content type', async () => {
      mv.start()

      await mv.ingestContent(createContentInput({
        contentType: 'markdown',
        content: '# Heading\n\n- Item 1\n- Item 2',
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].contentType).toBe('markdown')
    })

    it('handles embedding content type', async () => {
      mv.start()

      const embedding = [0.1, 0.2, 0.3, 0.4, 0.5]
      await mv.ingestContent(createContentInput({
        contentType: 'embedding',
        content: embedding,
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].contentType).toBe('embedding')
      expect(buffer[0].content).toBe(JSON.stringify(embedding))
    })
  })
})

// =============================================================================
// Schema Tests
// =============================================================================

describe('GENERATED_CONTENT_SCHEMA', () => {
  it('defines all required columns', () => {
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('id')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('requestId')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('timestamp')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('modelId')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('providerId')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('contentType')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('content')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('contentLength')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('tokenCount')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('finishReason')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('latencyMs')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('isStreaming')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('isCached')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('classification')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('toolName')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('language')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('contentHash')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('sessionId')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('userId')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('source')
    expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('metadata')
  })

  it('marks required columns as non-optional', () => {
    expect(GENERATED_CONTENT_SCHEMA.id.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.requestId.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.timestamp.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.modelId.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.contentType.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.content.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.finishReason.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.isStreaming.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.isCached.optional).toBe(false)
    expect(GENERATED_CONTENT_SCHEMA.classification.optional).toBe(false)
  })

  it('marks nullable columns as optional', () => {
    expect(GENERATED_CONTENT_SCHEMA.providerId.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.tokenCount.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.latencyMs.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.toolName.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.toolCallId.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.language.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.contentHash.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.sessionId.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.userId.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.source.optional).toBe(true)
    expect(GENERATED_CONTENT_SCHEMA.metadata.optional).toBe(true)
  })
})

// =============================================================================
// Handler Integration Tests
// =============================================================================

describe('createGeneratedContentMVHandler', () => {
  let storage: StorageBackend & { files: Map<string, Uint8Array> }
  let mv: GeneratedContentMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createGeneratedContentMV({
      storage,
      datasetPath: 'ai/generated',
      generateId: createIdGenerator(),
    })
    mv.start()
  })

  afterEach(async () => {
    await mv.stop()
  })

  it('creates an MV handler for StreamingRefreshEngine', () => {
    const handler = createGeneratedContentMVHandler(mv)

    expect(handler.name).toBe('GeneratedContent')
    expect(handler.sourceNamespaces).toEqual(['generated_content'])
    expect(typeof handler.process).toBe('function')
  })

  it('uses custom source namespace', () => {
    const handler = createGeneratedContentMVHandler(mv, 'custom_content')

    expect(handler.sourceNamespaces).toEqual(['custom_content'])
  })

  it('processes CDC events into GeneratedContentRecords', async () => {
    const handler = createGeneratedContentMVHandler(mv)

    const events = [
      {
        id: 'evt-1',
        ts: Date.now(),
        op: 'CREATE' as const,
        target: 'generated_content:content-1',
        after: {
          requestId: 'req_test',
          modelId: 'gpt-4',
          contentType: 'text' as const,
          content: 'Generated content',
          tokenCount: 10,
        },
      },
    ]

    await handler.process(events)

    const buffer = mv.getBuffer()
    expect(buffer.length).toBe(1)
    expect(buffer[0].requestId).toBe('req_test')
    expect(buffer[0].modelId).toBe('gpt-4')
  })

  it('ignores events without required fields', async () => {
    const handler = createGeneratedContentMVHandler(mv)

    const events = [
      {
        id: 'evt-1',
        ts: Date.now(),
        op: 'CREATE' as const,
        target: 'generated_content:content-1',
        after: {
          // Missing requestId, modelId, contentType, content
          tokenCount: 10,
        },
      },
    ]

    await handler.process(events)

    expect(mv.getBuffer().length).toBe(0)
  })

  it('ignores UPDATE and DELETE events', async () => {
    const handler = createGeneratedContentMVHandler(mv)

    const events = [
      {
        id: 'evt-1',
        ts: Date.now(),
        op: 'UPDATE' as const,
        target: 'generated_content:content-1',
        before: { content: 'old' },
        after: {
          requestId: 'req_test',
          modelId: 'gpt-4',
          contentType: 'text' as const,
          content: 'new',
        },
      },
      {
        id: 'evt-2',
        ts: Date.now(),
        op: 'DELETE' as const,
        target: 'generated_content:content-2',
        before: {
          requestId: 'req_test',
          modelId: 'gpt-4',
          contentType: 'text' as const,
          content: 'deleted',
        },
      },
    ]

    await handler.process(events)

    expect(mv.getBuffer().length).toBe(0)
  })
})

// =============================================================================
// Utility Function Tests
// =============================================================================

describe('detectContentType', () => {
  it('detects JSON objects', () => {
    expect(detectContentType('{"key": "value"}')).toBe('json')
    expect(detectContentType('{"nested": {"foo": "bar"}}')).toBe('json')
    expect(detectContentType('  { "spaced": true }  ')).toBe('json')
  })

  it('detects JSON arrays', () => {
    expect(detectContentType('[1, 2, 3]')).toBe('json')
    expect(detectContentType('[{"a": 1}, {"b": 2}]')).toBe('json')
  })

  it('detects HTML', () => {
    expect(detectContentType('<!DOCTYPE html>')).toBe('html')
    expect(detectContentType('<html><body>Hello</body></html>')).toBe('html')
    expect(detectContentType('<div class="test">Content</div>')).toBe('html')
  })

  it('detects Markdown', () => {
    expect(detectContentType('# Heading')).toBe('markdown')
    expect(detectContentType('## Subheading')).toBe('markdown')
    expect(detectContentType('[Link](https://example.com)')).toBe('markdown')
    expect(detectContentType('```js\ncode\n```')).toBe('markdown')
    expect(detectContentType('---')).toBe('markdown')
    expect(detectContentType('***')).toBe('markdown')
  })

  it('detects code', () => {
    expect(detectContentType('import React from "react"')).toBe('code')
    expect(detectContentType('const x = 5;\nconst y = 10;')).toBe('code')
    expect(detectContentType('function hello() {\n  return "world";\n}')).toBe('code')
    expect(detectContentType('export default class MyClass')).toBe('code')
    expect(detectContentType('def my_function():\n    pass')).toBe('code')
  })

  it('defaults to text for plain text', () => {
    expect(detectContentType('Hello, world!')).toBe('text')
    expect(detectContentType('This is a simple sentence.')).toBe('text')
    expect(detectContentType('Just some text without any special formatting')).toBe('text')
  })

  it('handles invalid JSON as text', () => {
    expect(detectContentType('{invalid json}')).toBe('text')
    expect(detectContentType('[missing, bracket')).toBe('text')
  })
})

describe('detectCodeLanguage', () => {
  it('detects JavaScript imports', () => {
    expect(detectCodeLanguage('import React from "react"')).toBe('code:javascript')
    expect(detectCodeLanguage("import { useState } from 'react'")).toBe('code:javascript')
  })

  it('detects Python imports', () => {
    expect(detectCodeLanguage('from flask import Flask')).toBe('code:python')
    expect(detectCodeLanguage('from typing import List')).toBe('code:python')
  })

  it('detects Go', () => {
    expect(detectCodeLanguage('package main')).toBe('code:go')
    expect(detectCodeLanguage('func main() {')).toBe('code:go')
  })

  it('detects Rust', () => {
    expect(detectCodeLanguage('use std::io')).toBe('code:rust')
    expect(detectCodeLanguage('fn main() {')).toBe('code:rust')
  })

  it('detects C++', () => {
    expect(detectCodeLanguage('#include <iostream>')).toBe('code:cpp')
  })

  it('detects Java', () => {
    expect(detectCodeLanguage('public class MyClass')).toBe('code:java')
  })

  it('detects TypeScript', () => {
    expect(detectCodeLanguage('const x: number = 5')).toBe('code:typescript')
    expect(detectCodeLanguage('interface User {')).toBe('code:typescript')
    expect(detectCodeLanguage('type Props =')).toBe('code:typescript')
  })

  it('detects Python functions', () => {
    expect(detectCodeLanguage('def my_function():')).toBe('code:python')
  })

  it('returns null for unknown patterns', () => {
    expect(detectCodeLanguage('Hello world')).toBeNull()
    expect(detectCodeLanguage('Some text without code')).toBeNull()
  })
})

describe('estimateTokenCount', () => {
  it('estimates tokens based on character count', () => {
    // ~4 characters per token
    expect(estimateTokenCount('Hello')).toBe(2) // 5 chars -> 2 tokens
    expect(estimateTokenCount('Hello, world!')).toBe(4) // 13 chars -> 4 tokens
    expect(estimateTokenCount('a'.repeat(100))).toBe(25) // 100 chars -> 25 tokens
  })

  it('handles empty string', () => {
    expect(estimateTokenCount('')).toBe(0)
  })

  it('rounds up for partial tokens', () => {
    expect(estimateTokenCount('abc')).toBe(1) // 3 chars -> 1 token (rounded up)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  let storage: StorageBackend & { files: Map<string, Uint8Array> }
  let mv: GeneratedContentMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createGeneratedContentMV({
      storage,
      datasetPath: 'ai/generated',
      generateId: createIdGenerator(),
    })
    mv.start()
  })

  afterEach(async () => {
    await mv.stop()
  })

  it('handles empty content', async () => {
    await mv.ingestContent(createContentInput({
      content: '',
    }))

    const buffer = mv.getBuffer()
    expect(buffer.length).toBe(1)
    expect(buffer[0].content).toBe('')
    expect(buffer[0].contentLength).toBe(0)
  })

  it('handles very long content', async () => {
    const longContent = 'x'.repeat(100000)
    await mv.ingestContent(createContentInput({
      content: longContent,
    }))

    const buffer = mv.getBuffer()
    expect(buffer.length).toBe(1)
    expect(buffer[0].contentLength).toBe(100000)
  })

  it('handles special characters in content', async () => {
    const specialContent = 'Test with special chars: \n\t\r "quotes" and \'apostrophes\''
    await mv.ingestContent(createContentInput({
      content: specialContent,
    }))

    const buffer = mv.getBuffer()
    expect(buffer[0].content).toBe(specialContent)
  })

  it('handles unicode content', async () => {
    const unicodeContent = 'Unicode: \u{1F600} \u{1F64B} \u4E2D\u6587'
    await mv.ingestContent(createContentInput({
      content: unicodeContent,
    }))

    const buffer = mv.getBuffer()
    expect(buffer[0].content).toBe(unicodeContent)
  })

  it('handles deeply nested JSON objects', async () => {
    const nestedObj = {
      level1: {
        level2: {
          level3: {
            level4: {
              value: 'deep',
            },
          },
        },
      },
    }
    await mv.ingestContent(createContentInput({
      contentType: 'json',
      content: nestedObj,
    }))

    const buffer = mv.getBuffer()
    expect(buffer[0].content).toBe(JSON.stringify(nestedObj))
  })

  it('generates unique IDs for each record', async () => {
    mv = createGeneratedContentMV({
      storage,
      datasetPath: 'ai/generated',
      // Use default ULID generator
    })
    mv.start()

    await mv.ingestContent(createContentInput())
    await mv.ingestContent(createContentInput())

    const buffer = mv.getBuffer()
    expect(buffer[0].id).not.toBe(buffer[1].id)

    await mv.stop()
  })

  it('same content produces same hash', async () => {
    const content = 'Identical content'
    await mv.ingestContent(createContentInput({ content, requestId: 'req_1' }))
    await mv.ingestContent(createContentInput({ content, requestId: 'req_2' }))

    const buffer = mv.getBuffer()
    expect(buffer[0].contentHash).toBe(buffer[1].contentHash)
  })

  it('different content produces different hash', async () => {
    await mv.ingestContent(createContentInput({ content: 'Content A', requestId: 'req_1' }))
    await mv.ingestContent(createContentInput({ content: 'Content B', requestId: 'req_2' }))

    const buffer = mv.getBuffer()
    expect(buffer[0].contentHash).not.toBe(buffer[1].contentHash)
  })

  it('handles custom timestamp', async () => {
    const customTimestamp = 1700000000000
    await mv.ingestContent(createContentInput({
      timestamp: customTimestamp,
    }))

    const buffer = mv.getBuffer()
    expect(buffer[0].timestamp).toBe(customTimestamp)
  })

  it('handles custom ID', async () => {
    await mv.ingestContent(createContentInput({
      id: 'custom-content-id-123',
    }))

    const buffer = mv.getBuffer()
    expect(buffer[0].id).toBe('custom-content-id-123')
  })

  it('handles all content types', async () => {
    const contentTypes = [
      'text', 'code', 'json', 'markdown', 'html',
      'tool_call', 'tool_result', 'image_description', 'embedding', 'other',
    ] as const

    for (const contentType of contentTypes) {
      await mv.ingestContent(createContentInput({ contentType }))
    }

    const stats = mv.getStats()
    expect(stats.recordsIngested).toBe(contentTypes.length)

    for (const contentType of contentTypes) {
      expect(stats.byContentType[contentType]).toBe(1)
    }
  })

  it('handles all finish reasons', async () => {
    const finishReasons = [
      'stop', 'length', 'tool_calls', 'content_filter', 'error', 'unknown',
    ] as const

    for (const finishReason of finishReasons) {
      await mv.ingestContent(createContentInput({ finishReason }))
    }

    const stats = mv.getStats()
    for (const reason of finishReasons) {
      expect(stats.byFinishReason[reason]).toBe(1)
    }
  })

  it('handles all classifications', async () => {
    const classifications = [
      'safe', 'sensitive', 'pii', 'flagged', 'unclassified',
    ] as const

    for (const classification of classifications) {
      await mv.ingestContent(createContentInput({ classification }))
    }

    const stats = mv.getStats()
    for (const classification of classifications) {
      expect(stats.byClassification[classification]).toBe(1)
    }
  })
})

// =============================================================================
// Content Versioning Tests
// =============================================================================

describe('Content Versioning', () => {
  let storage: StorageBackend & { files: Map<string, Uint8Array> }
  let mv: GeneratedContentMV

  beforeEach(() => {
    storage = createMockStorage()
    mv = createGeneratedContentMV({
      storage,
      datasetPath: 'ai/generated',
      generateId: createIdGenerator(),
    })
    mv.start()
  })

  afterEach(async () => {
    await mv.stop()
  })

  describe('version field defaults', () => {
    it('sets version to 1 for new content without parent', async () => {
      await mv.ingestContent(createContentInput())

      const buffer = mv.getBuffer()
      expect(buffer[0].version).toBe(1)
      expect(buffer[0].parentContentId).toBeNull()
    })

    it('sets rootContentId to own id for first version', async () => {
      await mv.ingestContent(createContentInput({ id: 'content-1' }))

      const buffer = mv.getBuffer()
      expect(buffer[0].rootContentId).toBe('content-1')
    })

    it('sets version to 2 when parentContentId is provided without explicit version', async () => {
      await mv.ingestContent(createContentInput({
        parentContentId: 'parent-content-1',
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].version).toBe(2)
      expect(buffer[0].parentContentId).toBe('parent-content-1')
    })

    it('uses explicit version when provided', async () => {
      await mv.ingestContent(createContentInput({
        version: 5,
        parentContentId: 'parent-content-1',
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].version).toBe(5)
    })

    it('preserves rootContentId when provided', async () => {
      await mv.ingestContent(createContentInput({
        version: 3,
        parentContentId: 'parent-2',
        rootContentId: 'original-root',
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].rootContentId).toBe('original-root')
    })

    it('records versionReason when provided', async () => {
      await mv.ingestContent(createContentInput({
        parentContentId: 'parent-1',
        versionReason: 'user_edit',
      }))

      const buffer = mv.getBuffer()
      expect(buffer[0].versionReason).toBe('user_edit')
    })

    it('sets versionReason to null when not provided', async () => {
      await mv.ingestContent(createContentInput())

      const buffer = mv.getBuffer()
      expect(buffer[0].versionReason).toBeNull()
    })
  })

  describe('createVersion', () => {
    it('creates a new version linked to parent', async () => {
      // Create initial content
      await mv.ingestContent(createContentInput({
        id: 'content-v1',
        content: 'Initial draft',
      }))

      const parentRecord = mv.getBuffer()[0]
      expect(parentRecord.version).toBe(1)
      expect(parentRecord.rootContentId).toBe('content-v1')

      // Create new version
      const newId = await mv.createVersion(parentRecord, {
        requestId: 'req_v2',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Revised draft',
      })

      const buffer = mv.getBuffer()
      expect(buffer.length).toBe(2)

      const newVersion = buffer.find(r => r.id === newId)
      expect(newVersion).toBeDefined()
      expect(newVersion!.version).toBe(2)
      expect(newVersion!.parentContentId).toBe('content-v1')
      expect(newVersion!.rootContentId).toBe('content-v1')
    })

    it('increments version number correctly', async () => {
      // Create initial content
      await mv.ingestContent(createContentInput({
        id: 'content-v1',
        content: 'Version 1',
      }))

      const v1 = mv.getBuffer()[0]

      // Create version 2
      await mv.createVersion(v1, {
        requestId: 'req_v2',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Version 2',
      })

      const v2 = mv.getBuffer().find(r => r.version === 2)!

      // Create version 3
      await mv.createVersion(v2, {
        requestId: 'req_v3',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Version 3',
      })

      const v3 = mv.getBuffer().find(r => r.version === 3)
      expect(v3).toBeDefined()
      expect(v3!.version).toBe(3)
      expect(v3!.parentContentId).toBe(v2.id)
      expect(v3!.rootContentId).toBe('content-v1')
    })

    it('preserves rootContentId through version chain', async () => {
      await mv.ingestContent(createContentInput({
        id: 'root-content',
        content: 'Original',
      }))

      let parent = mv.getBuffer()[0]

      // Create 3 more versions
      for (let i = 2; i <= 4; i++) {
        await mv.createVersion(parent, {
          requestId: `req_v${i}`,
          modelId: 'gpt-4',
          contentType: 'text',
          content: `Version ${i}`,
        })
        parent = mv.getBuffer().find(r => r.version === i)!
      }

      // All versions should have the same rootContentId
      const allRecords = mv.getBuffer()
      expect(allRecords.length).toBe(4)
      for (const record of allRecords) {
        expect(record.rootContentId).toBe('root-content')
      }
    })

    it('allows custom versionReason', async () => {
      await mv.ingestContent(createContentInput({
        id: 'content-1',
        content: 'Original',
      }))

      const parent = mv.getBuffer()[0]

      await mv.createVersion(parent, {
        requestId: 'req_2',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Revised',
        versionReason: 'grammar_correction',
      })

      const newVersion = mv.getBuffer().find(r => r.version === 2)
      expect(newVersion!.versionReason).toBe('grammar_correction')
    })

    it('allows custom id for new version', async () => {
      await mv.ingestContent(createContentInput({
        id: 'content-1',
        content: 'Original',
      }))

      const parent = mv.getBuffer()[0]

      const newId = await mv.createVersion(parent, {
        id: 'my-custom-id',
        requestId: 'req_2',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Revised',
      })

      expect(newId).toBe('my-custom-id')
      const newVersion = mv.getBuffer().find(r => r.id === 'my-custom-id')
      expect(newVersion).toBeDefined()
    })
  })

  describe('getVersionHistory', () => {
    beforeEach(async () => {
      // Create a version chain: v1 -> v2 -> v3
      await mv.ingestContent(createContentInput({
        id: 'content-v1',
        content: 'Version 1',
      }))

      const v1 = mv.getBuffer()[0]
      await mv.createVersion(v1, {
        requestId: 'req_v2',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Version 2',
      })

      const v2 = mv.getBuffer().find(r => r.version === 2)!
      await mv.createVersion(v2, {
        requestId: 'req_v3',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Version 3',
      })
    })

    it('returns version history for root content', () => {
      const history = mv.getVersionHistory('content-v1')

      expect(history.length).toBe(3)
      expect(history[0].version).toBe(1)
      expect(history[1].version).toBe(2)
      expect(history[2].version).toBe(3)
    })

    it('returns version history for any version in chain', () => {
      const v2 = mv.getBuffer().find(r => r.version === 2)!
      const history = mv.getVersionHistory(v2.id)

      expect(history.length).toBe(3)
      expect(history[0].version).toBe(1)
      expect(history[1].version).toBe(2)
      expect(history[2].version).toBe(3)
    })

    it('returns sorted history (oldest first)', () => {
      const history = mv.getVersionHistory('content-v1')

      for (let i = 0; i < history.length - 1; i++) {
        expect(history[i].version).toBeLessThan(history[i + 1].version)
      }
    })

    it('returns empty array for non-existent content', () => {
      const history = mv.getVersionHistory('non-existent')
      expect(history).toEqual([])
    })

    it('returns single record for content without versions', async () => {
      await mv.ingestContent(createContentInput({
        id: 'standalone-content',
        content: 'No versions',
      }))

      const history = mv.getVersionHistory('standalone-content')
      expect(history.length).toBe(1)
      expect(history[0].id).toBe('standalone-content')
    })
  })

  describe('getLatestVersion', () => {
    beforeEach(async () => {
      // Create version chain
      await mv.ingestContent(createContentInput({
        id: 'content-v1',
        content: 'Version 1',
      }))

      const v1 = mv.getBuffer()[0]
      await mv.createVersion(v1, {
        requestId: 'req_v2',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Version 2',
      })

      const v2 = mv.getBuffer().find(r => r.version === 2)!
      await mv.createVersion(v2, {
        requestId: 'req_v3',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Version 3',
      })
    })

    it('returns the latest version from root content id', () => {
      const latest = mv.getLatestVersion('content-v1')

      expect(latest).toBeDefined()
      expect(latest!.version).toBe(3)
      expect(latest!.content).toBe('Version 3')
    })

    it('returns the latest version from any version in chain', () => {
      const v2 = mv.getBuffer().find(r => r.version === 2)!
      const latest = mv.getLatestVersion(v2.id)

      expect(latest).toBeDefined()
      expect(latest!.version).toBe(3)
    })

    it('returns undefined for non-existent content', () => {
      const latest = mv.getLatestVersion('non-existent')
      expect(latest).toBeUndefined()
    })

    it('returns the same record for content without versions', async () => {
      await mv.ingestContent(createContentInput({
        id: 'standalone-content',
        content: 'No versions',
      }))

      const latest = mv.getLatestVersion('standalone-content')
      expect(latest).toBeDefined()
      expect(latest!.id).toBe('standalone-content')
      expect(latest!.version).toBe(1)
    })
  })

  describe('versioning with flush', () => {
    it('includes version fields in Parquet schema', () => {
      expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('version')
      expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('parentContentId')
      expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('rootContentId')
      expect(GENERATED_CONTENT_SCHEMA).toHaveProperty('versionReason')
    })

    it('version field is non-optional in schema', () => {
      expect(GENERATED_CONTENT_SCHEMA.version.optional).toBe(false)
    })

    it('versioning reference fields are optional in schema', () => {
      expect(GENERATED_CONTENT_SCHEMA.parentContentId.optional).toBe(true)
      expect(GENERATED_CONTENT_SCHEMA.rootContentId.optional).toBe(true)
      expect(GENERATED_CONTENT_SCHEMA.versionReason.optional).toBe(true)
    })

    it('flushes versioned content correctly', async () => {
      // Create version chain
      await mv.ingestContent(createContentInput({
        id: 'content-v1',
        content: 'Version 1',
      }))

      const v1 = mv.getBuffer()[0]
      await mv.createVersion(v1, {
        requestId: 'req_v2',
        modelId: 'gpt-4',
        contentType: 'text',
        content: 'Version 2',
        versionReason: 'improvement',
      })

      // Flush to storage
      await mv.flush()

      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)
      expect(mv.getStats().recordsWritten).toBe(2)
    })
  })
})
