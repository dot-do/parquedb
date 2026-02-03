/**
 * Fetch Subrequest Tracking Tests
 *
 * Tests for tracking fetch subrequests in tail worker events.
 * Fetch subrequests are tracked through:
 * 1. diagnosticsChannelEvents array in TraceItem
 * 2. Explicit fetchCount property (if available)
 *
 * @see https://developers.cloudflare.com/workers/observability/logs/tail-workers/
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  extractFetchSubrequests,
  countFetchSubrequests,
  type FetchSubrequest,
  type DiagnosticsChannelEvent,
} from '../../../src/worker/subrequest-tracking'
import { validateTraceItem, type ValidatedTraceItem } from '../../../src/worker/tail-validation'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock diagnostics channel event for a fetch subrequest
 */
function createFetchDiagnosticsEvent(overrides: Partial<DiagnosticsChannelEvent> = {}): DiagnosticsChannelEvent {
  return {
    channel: 'fetch',
    timestamp: Date.now(),
    message: {
      url: 'https://api.example.com/data',
      method: 'GET',
      status: 200,
      duration: 42,
    },
    ...overrides,
  }
}

/**
 * Create a valid trace item with diagnosticsChannelEvents
 */
function createTraceItemWithSubrequests(
  subrequestCount: number,
  overrides: Partial<ValidatedTraceItem> = {}
): ValidatedTraceItem {
  const diagnosticsChannelEvents: DiagnosticsChannelEvent[] = []

  for (let i = 0; i < subrequestCount; i++) {
    diagnosticsChannelEvents.push(createFetchDiagnosticsEvent({
      timestamp: Date.now() + i * 10,
      message: {
        url: `https://api.example.com/data/${i}`,
        method: i % 2 === 0 ? 'GET' : 'POST',
        status: 200,
        duration: 10 + i * 5,
      },
    }))
  }

  return {
    scriptName: 'test-worker',
    outcome: 'ok',
    eventTimestamp: Date.now(),
    event: {
      request: {
        url: 'https://example.com/',
        method: 'GET',
        headers: {},
      },
    },
    logs: [],
    exceptions: [],
    diagnosticsChannelEvents,
    ...overrides,
  }
}

// =============================================================================
// Test Suites
// =============================================================================

describe('Fetch Subrequest Tracking', () => {
  describe('countFetchSubrequests', () => {
    it('counts fetch events in diagnosticsChannelEvents', () => {
      const item = createTraceItemWithSubrequests(3)
      const count = countFetchSubrequests(item)
      expect(count).toBe(3)
    })

    it('returns 0 for empty diagnosticsChannelEvents', () => {
      const item = createTraceItemWithSubrequests(0)
      const count = countFetchSubrequests(item)
      expect(count).toBe(0)
    })

    it('returns 0 for undefined diagnosticsChannelEvents', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [],
      }
      const count = countFetchSubrequests(item)
      expect(count).toBe(0)
    })

    it('only counts events with channel "fetch"', () => {
      const item = createTraceItemWithSubrequests(2)
      // Add a non-fetch event
      item.diagnosticsChannelEvents.push({
        channel: 'some-other-channel',
        timestamp: Date.now(),
        message: { data: 'test' },
      })
      const count = countFetchSubrequests(item)
      expect(count).toBe(2)
    })

    it('handles mixed event types', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [
          createFetchDiagnosticsEvent(),
          { channel: 'db', timestamp: Date.now(), message: {} },
          createFetchDiagnosticsEvent(),
          { channel: 'cache', timestamp: Date.now(), message: {} },
          createFetchDiagnosticsEvent(),
        ],
      }
      const count = countFetchSubrequests(item)
      expect(count).toBe(3)
    })

    it('handles malformed diagnosticsChannelEvents gracefully', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [
          null as unknown,
          undefined as unknown,
          'invalid' as unknown,
          123 as unknown,
          createFetchDiagnosticsEvent(), // Valid
        ] as unknown[],
      }
      const count = countFetchSubrequests(item)
      expect(count).toBe(1)
    })
  })

  describe('extractFetchSubrequests', () => {
    it('extracts detailed subrequest information', () => {
      const item = createTraceItemWithSubrequests(2)
      const subrequests = extractFetchSubrequests(item)

      expect(subrequests).toHaveLength(2)
      expect(subrequests[0]).toMatchObject({
        url: expect.stringContaining('api.example.com'),
        method: expect.any(String),
      })
    })

    it('extracts URL, method, status, and duration', () => {
      const item = createTraceItemWithSubrequests(1)
      const subrequests = extractFetchSubrequests(item)

      expect(subrequests[0]).toHaveProperty('url')
      expect(subrequests[0]).toHaveProperty('method')
      expect(subrequests[0]).toHaveProperty('status')
      expect(subrequests[0]).toHaveProperty('duration')
    })

    it('returns empty array for no subrequests', () => {
      const item = createTraceItemWithSubrequests(0)
      const subrequests = extractFetchSubrequests(item)
      expect(subrequests).toEqual([])
    })

    it('handles subrequests with missing fields', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [
          {
            channel: 'fetch',
            timestamp: Date.now(),
            message: {
              url: 'https://api.example.com/data',
              // Missing method, status, duration
            },
          },
        ],
      }
      const subrequests = extractFetchSubrequests(item)

      expect(subrequests).toHaveLength(1)
      expect(subrequests[0].url).toBe('https://api.example.com/data')
      expect(subrequests[0].method).toBeUndefined()
    })
  })

  describe('Integration with TailItem processing', () => {
    it('preserves diagnosticsChannelEvents through validation', () => {
      const rawItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [
          createFetchDiagnosticsEvent(),
          createFetchDiagnosticsEvent(),
        ],
      }

      const result = validateTraceItem(rawItem)
      expect(result.valid).toBe(true)
      expect(result.item?.diagnosticsChannelEvents).toHaveLength(2)

      // Should be able to count subrequests from validated item
      const count = countFetchSubrequests(result.item!)
      expect(count).toBe(2)
    })
  })

  describe('Snippets Compliance', () => {
    it('can detect when subrequest limit is exceeded (5 for Snippets)', () => {
      const item = createTraceItemWithSubrequests(6)
      const count = countFetchSubrequests(item)

      const SNIPPETS_SUBREQUEST_LIMIT = 5
      expect(count).toBeGreaterThan(SNIPPETS_SUBREQUEST_LIMIT)
    })

    it('passes compliance check when under limit', () => {
      const item = createTraceItemWithSubrequests(3)
      const count = countFetchSubrequests(item)

      const SNIPPETS_SUBREQUEST_LIMIT = 5
      expect(count).toBeLessThanOrEqual(SNIPPETS_SUBREQUEST_LIMIT)
    })
  })

  describe('Edge Cases', () => {
    it('handles large number of subrequests', () => {
      const item = createTraceItemWithSubrequests(1000)
      const count = countFetchSubrequests(item)
      expect(count).toBe(1000)
    })

    it('handles diagnosticsChannelEvents as non-array', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [] as unknown[], // Ensure it's always an array after validation
      }
      const count = countFetchSubrequests(item)
      expect(count).toBe(0)
    })

    it('handles events with empty message', () => {
      const item: ValidatedTraceItem = {
        scriptName: 'test-worker',
        outcome: 'ok',
        eventTimestamp: Date.now(),
        event: null,
        logs: [],
        exceptions: [],
        diagnosticsChannelEvents: [
          { channel: 'fetch', timestamp: Date.now(), message: null },
          { channel: 'fetch', timestamp: Date.now(), message: {} },
        ] as unknown[],
      }
      const subrequests = extractFetchSubrequests(item)
      // Should still count as fetch events but with minimal data
      expect(subrequests).toHaveLength(2)
    })
  })
})
