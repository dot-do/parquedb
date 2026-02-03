/**
 * Stream Views API Tests
 *
 * Tests for defineStreamView() and related utilities
 */

import { describe, it, expect } from 'vitest'
import {
  defineStreamView,
  defineStreamViews,
  validateStreamViewDefinition,
  durationToMs,
  streamViewName,
  isTumblingWindow,
  isSlidingWindow,
  isSessionWindow,
  isGlobalWindow,
  isCollectionSink,
  isWebhookSink,
  isQueueSink,
  isConsoleSink,
  type StreamViewDefinition,
  type WindowConfig,
  type OutputSink,
} from '../../../src/streaming'

describe('defineStreamView', () => {
  describe('basic definition', () => {
    it('should create a simple stream view with minimal config', () => {
      const view = defineStreamView({
        name: 'active_users',
        source: {
          collection: 'users',
        },
      })

      expect(view.name).toBe('active_users')
      expect(view.source.collection).toBe('users')
      expect(view.enabled).toBe(true)
    })

    it('should apply default window config (global)', () => {
      const view = defineStreamView({
        name: 'test_view',
        source: { collection: 'events' },
      })

      expect(view.window).toEqual({ type: 'global' })
    })

    it('should apply default event types', () => {
      const view = defineStreamView({
        name: 'test_view',
        source: { collection: 'events' },
      })

      expect(view.source.eventTypes).toEqual(['CREATE', 'UPDATE', 'DELETE'])
    })

    it('should apply default start position', () => {
      const view = defineStreamView({
        name: 'test_view',
        source: { collection: 'events' },
      })

      expect(view.source.startPosition).toBe('latest')
    })

    it('should apply default output sink', () => {
      const view = defineStreamView({
        name: 'test_view',
        source: { collection: 'events' },
      })

      expect(view.output).toEqual({
        sink: {
          type: 'collection',
          collection: 'test_view',
          mode: 'upsert',
        },
      })
    })

    it('should apply default watermark config', () => {
      const view = defineStreamView({
        name: 'test_view',
        source: { collection: 'events' },
      })

      expect(view.watermark).toEqual({
        lateEventPolicy: 'drop',
      })
    })
  })

  describe('source configuration', () => {
    it('should accept filter in source config', () => {
      const view = defineStreamView({
        name: 'active_users',
        source: {
          collection: 'users',
          filter: { status: 'active' },
        },
      })

      expect(view.source.filter).toEqual({ status: 'active' })
    })

    it('should accept custom event types', () => {
      const view = defineStreamView({
        name: 'creates_only',
        source: {
          collection: 'events',
          eventTypes: ['CREATE'],
        },
      })

      expect(view.source.eventTypes).toEqual(['CREATE'])
    })

    it('should accept earliest start position', () => {
      const view = defineStreamView({
        name: 'replay_view',
        source: {
          collection: 'events',
          startPosition: 'earliest',
        },
      })

      expect(view.source.startPosition).toBe('earliest')
    })

    it('should accept timestamp start position', () => {
      const timestamp = Date.now() - 3600000 // 1 hour ago
      const view = defineStreamView({
        name: 'from_timestamp',
        source: {
          collection: 'events',
          startPosition: timestamp,
        },
      })

      expect(view.source.startPosition).toBe(timestamp)
    })
  })

  describe('window configuration', () => {
    it('should accept tumbling window', () => {
      const view = defineStreamView({
        name: 'minute_counts',
        source: { collection: 'events' },
        window: {
          type: 'tumbling',
          size: { minutes: 1 },
        },
      })

      expect(view.window?.type).toBe('tumbling')
      if (view.window?.type === 'tumbling') {
        expect(view.window.size).toEqual({ minutes: 1 })
      }
    })

    it('should accept sliding window', () => {
      const view = defineStreamView({
        name: 'sliding_avg',
        source: { collection: 'metrics' },
        window: {
          type: 'sliding',
          size: { minutes: 5 },
          slide: { minutes: 1 },
        },
      })

      expect(view.window?.type).toBe('sliding')
      if (view.window?.type === 'sliding') {
        expect(view.window.size).toEqual({ minutes: 5 })
        expect(view.window.slide).toEqual({ minutes: 1 })
      }
    })

    it('should accept session window', () => {
      const view = defineStreamView({
        name: 'user_sessions',
        source: { collection: 'page_views' },
        window: {
          type: 'session',
          gap: { minutes: 30 },
        },
      })

      expect(view.window?.type).toBe('session')
      if (view.window?.type === 'session') {
        expect(view.window.gap).toEqual({ minutes: 30 })
      }
    })

    it('should accept session window with max duration', () => {
      const view = defineStreamView({
        name: 'bounded_sessions',
        source: { collection: 'page_views' },
        window: {
          type: 'session',
          gap: { minutes: 30 },
          maxDuration: { hours: 4 },
        },
      })

      if (view.window?.type === 'session') {
        expect(view.window.maxDuration).toEqual({ hours: 4 })
      }
    })

    it('should accept global window explicitly', () => {
      const view = defineStreamView({
        name: 'all_time',
        source: { collection: 'events' },
        window: { type: 'global' },
      })

      expect(view.window?.type).toBe('global')
    })
  })

  describe('transform configuration', () => {
    it('should accept aggregation pipeline', () => {
      const view = defineStreamView({
        name: 'projected_users',
        source: { collection: 'users' },
        transform: {
          pipeline: [
            { $project: { name: 1, email: 1 } },
          ],
        },
      })

      expect(view.transform?.pipeline).toHaveLength(1)
    })

    it('should accept builtin count transform', () => {
      const view = defineStreamView({
        name: 'event_counts',
        source: { collection: 'events' },
        transform: {
          builtin: 'count',
        },
      })

      expect(view.transform?.builtin).toBe('count')
    })

    it('should accept builtin sum transform with field', () => {
      const view = defineStreamView({
        name: 'total_amount',
        source: { collection: 'orders' },
        transform: {
          builtin: 'sum',
          field: 'amount',
        },
      })

      expect(view.transform?.builtin).toBe('sum')
      expect(view.transform?.field).toBe('amount')
    })

    it('should accept groupBy in transform', () => {
      const view = defineStreamView({
        name: 'counts_by_type',
        source: { collection: 'events' },
        transform: {
          builtin: 'count',
          groupBy: 'type',
        },
      })

      expect(view.transform?.groupBy).toBe('type')
    })

    it('should accept multiple groupBy fields', () => {
      const view = defineStreamView({
        name: 'multi_group',
        source: { collection: 'events' },
        transform: {
          builtin: 'count',
          groupBy: ['type', 'category'],
        },
      })

      expect(view.transform?.groupBy).toEqual(['type', 'category'])
    })
  })

  describe('output configuration', () => {
    it('should accept collection sink', () => {
      const view = defineStreamView({
        name: 'test_view',
        source: { collection: 'events' },
        output: {
          sink: {
            type: 'collection',
            collection: 'materialized_events',
            mode: 'append',
          },
        },
      })

      expect(view.output?.sink?.type).toBe('collection')
    })

    it('should accept webhook sink', () => {
      const view = defineStreamView({
        name: 'webhook_view',
        source: { collection: 'events' },
        output: {
          sink: {
            type: 'webhook',
            url: 'https://example.com/webhook',
            method: 'POST',
          },
        },
      })

      expect(view.output?.sink?.type).toBe('webhook')
    })

    it('should accept queue sink', () => {
      const view = defineStreamView({
        name: 'queue_view',
        source: { collection: 'events' },
        output: {
          sink: {
            type: 'queue',
            queue: 'my-queue',
          },
        },
      })

      expect(view.output?.sink?.type).toBe('queue')
    })

    it('should accept console sink for debugging', () => {
      const view = defineStreamView({
        name: 'debug_view',
        source: { collection: 'events' },
        output: {
          sink: {
            type: 'console',
            level: 'debug',
          },
        },
      })

      expect(view.output?.sink?.type).toBe('console')
    })

    it('should accept batch configuration', () => {
      const view = defineStreamView({
        name: 'batched_view',
        source: { collection: 'events' },
        output: {
          batchSize: 100,
          batchTimeoutMs: 5000,
        },
      })

      expect(view.output?.batchSize).toBe(100)
      expect(view.output?.batchTimeoutMs).toBe(5000)
    })
  })

  describe('watermark configuration', () => {
    it('should accept max lateness', () => {
      const view = defineStreamView({
        name: 'late_tolerant',
        source: { collection: 'events' },
        watermark: {
          maxLateness: { minutes: 5 },
        },
      })

      expect(view.watermark?.maxLateness).toEqual({ minutes: 5 })
    })

    it('should accept update policy for late events', () => {
      const view = defineStreamView({
        name: 'updating_view',
        source: { collection: 'events' },
        watermark: {
          maxLateness: { minutes: 5 },
          lateEventPolicy: 'update',
        },
      })

      expect(view.watermark?.lateEventPolicy).toBe('update')
    })

    it('should accept side output policy with collection', () => {
      const view = defineStreamView({
        name: 'side_output_view',
        source: { collection: 'events' },
        watermark: {
          maxLateness: { minutes: 5 },
          lateEventPolicy: 'sideOutput',
          sideOutputCollection: 'late_events',
        },
      })

      expect(view.watermark?.lateEventPolicy).toBe('sideOutput')
      expect(view.watermark?.sideOutputCollection).toBe('late_events')
    })
  })

  describe('metadata', () => {
    it('should accept description', () => {
      const view = defineStreamView({
        name: 'documented_view',
        source: { collection: 'events' },
        description: 'A view that processes events',
      })

      expect(view.description).toBe('A view that processes events')
    })

    it('should accept tags', () => {
      const view = defineStreamView({
        name: 'tagged_view',
        source: { collection: 'events' },
        tags: ['production', 'critical'],
      })

      expect(view.tags).toEqual(['production', 'critical'])
    })

    it('should accept custom metadata', () => {
      const view = defineStreamView({
        name: 'metadata_view',
        source: { collection: 'events' },
        metadata: {
          owner: 'team-a',
          version: '1.0.0',
        },
      })

      expect(view.metadata).toEqual({
        owner: 'team-a',
        version: '1.0.0',
      })
    })

    it('should accept enabled flag', () => {
      const view = defineStreamView({
        name: 'disabled_view',
        source: { collection: 'events' },
        enabled: false,
      })

      expect(view.enabled).toBe(false)
    })
  })

  describe('validation', () => {
    it('should throw on missing name', () => {
      expect(() => {
        defineStreamView({
          name: '',
          source: { collection: 'events' },
        })
      }).toThrow('Invalid stream view definition')
    })

    it('should throw on invalid name format', () => {
      expect(() => {
        defineStreamView({
          name: '123-invalid',
          source: { collection: 'events' },
        })
      }).toThrow('must be a valid identifier')
    })

    it('should throw on missing source collection', () => {
      expect(() => {
        defineStreamView({
          name: 'test',
          source: { collection: '' },
        })
      }).toThrow('Source collection is required')
    })

    it('should throw on tumbling window without size', () => {
      expect(() => {
        defineStreamView({
          name: 'test',
          source: { collection: 'events' },
          window: {
            type: 'tumbling',
            size: {},
          },
        })
      }).toThrow('requires a positive size')
    })

    it('should throw on sliding window without slide', () => {
      expect(() => {
        defineStreamView({
          name: 'test',
          source: { collection: 'events' },
          window: {
            type: 'sliding',
            size: { minutes: 5 },
            slide: {},
          },
        })
      }).toThrow('requires a positive slide')
    })

    it('should throw on sum transform without field', () => {
      expect(() => {
        defineStreamView({
          name: 'test',
          source: { collection: 'events' },
          transform: {
            builtin: 'sum',
          },
        })
      }).toThrow("Transform 'sum' requires a field")
    })

    it('should throw on sideOutput policy without collection', () => {
      expect(() => {
        defineStreamView({
          name: 'test',
          source: { collection: 'events' },
          watermark: {
            lateEventPolicy: 'sideOutput',
          },
        })
      }).toThrow('requires sideOutputCollection')
    })
  })
})

describe('defineStreamViews', () => {
  it('should create multiple views at once', () => {
    const views = defineStreamViews({
      activeUsers: {
        source: { collection: 'users', filter: { active: true } },
      },
      recentPosts: {
        source: { collection: 'posts' },
        window: { type: 'tumbling', size: { hours: 1 } },
      },
    })

    expect(views.activeUsers.name).toBe('activeUsers')
    expect(views.activeUsers.source.filter).toEqual({ active: true })
    expect(views.recentPosts.name).toBe('recentPosts')
    expect(views.recentPosts.window?.type).toBe('tumbling')
  })

  it('should apply defaults to all views', () => {
    const views = defineStreamViews({
      view1: { source: { collection: 'c1' } },
      view2: { source: { collection: 'c2' } },
    })

    expect(views.view1.enabled).toBe(true)
    expect(views.view2.enabled).toBe(true)
    expect(views.view1.window).toEqual({ type: 'global' })
    expect(views.view2.window).toEqual({ type: 'global' })
  })
})

describe('validateStreamViewDefinition', () => {
  it('should return valid for correct definition', () => {
    const result = validateStreamViewDefinition({
      name: 'test_view',
      source: { collection: 'events' },
    })

    expect(result.valid).toBe(true)
    expect(result.errors).toHaveLength(0)
  })

  it('should return errors for invalid definition', () => {
    const result = validateStreamViewDefinition({
      name: '',
      source: { collection: '' },
    } as StreamViewDefinition)

    expect(result.valid).toBe(false)
    expect(result.errors.length).toBeGreaterThan(0)
  })

  it('should collect multiple errors', () => {
    const result = validateStreamViewDefinition({
      name: '123invalid',
      source: { collection: '' },
      window: { type: 'tumbling', size: {} },
      watermark: { lateEventPolicy: 'sideOutput' },
    } as StreamViewDefinition)

    expect(result.valid).toBe(false)
    expect(result.errors.length).toBeGreaterThanOrEqual(3)
  })
})

describe('durationToMs', () => {
  it('should convert milliseconds', () => {
    expect(durationToMs({ ms: 500 })).toBe(500)
  })

  it('should convert seconds', () => {
    expect(durationToMs({ seconds: 30 })).toBe(30000)
  })

  it('should convert minutes', () => {
    expect(durationToMs({ minutes: 5 })).toBe(300000)
  })

  it('should convert hours', () => {
    expect(durationToMs({ hours: 2 })).toBe(7200000)
  })

  it('should convert days', () => {
    expect(durationToMs({ days: 1 })).toBe(86400000)
  })

  it('should combine multiple units', () => {
    expect(durationToMs({
      hours: 1,
      minutes: 30,
      seconds: 45,
    })).toBe(3600000 + 1800000 + 45000)
  })

  it('should return 0 for empty duration', () => {
    expect(durationToMs({})).toBe(0)
  })
})

describe('streamViewName', () => {
  it('should create a branded stream view name', () => {
    const name = streamViewName('my_view')
    expect(name).toBe('my_view')
  })
})

describe('Window Type Guards', () => {
  describe('isTumblingWindow', () => {
    it('should return true for tumbling window', () => {
      const window: WindowConfig = { type: 'tumbling', size: { minutes: 5 } }
      expect(isTumblingWindow(window)).toBe(true)
    })

    it('should return false for other window types', () => {
      const window: WindowConfig = { type: 'global' }
      expect(isTumblingWindow(window)).toBe(false)
    })
  })

  describe('isSlidingWindow', () => {
    it('should return true for sliding window', () => {
      const window: WindowConfig = { type: 'sliding', size: { minutes: 5 }, slide: { minutes: 1 } }
      expect(isSlidingWindow(window)).toBe(true)
    })

    it('should return false for other window types', () => {
      const window: WindowConfig = { type: 'tumbling', size: { minutes: 5 } }
      expect(isSlidingWindow(window)).toBe(false)
    })
  })

  describe('isSessionWindow', () => {
    it('should return true for session window', () => {
      const window: WindowConfig = { type: 'session', gap: { minutes: 30 } }
      expect(isSessionWindow(window)).toBe(true)
    })

    it('should return false for other window types', () => {
      const window: WindowConfig = { type: 'global' }
      expect(isSessionWindow(window)).toBe(false)
    })
  })

  describe('isGlobalWindow', () => {
    it('should return true for global window', () => {
      const window: WindowConfig = { type: 'global' }
      expect(isGlobalWindow(window)).toBe(true)
    })

    it('should return false for other window types', () => {
      const window: WindowConfig = { type: 'tumbling', size: { minutes: 5 } }
      expect(isGlobalWindow(window)).toBe(false)
    })
  })
})

describe('Sink Type Guards', () => {
  describe('isCollectionSink', () => {
    it('should return true for collection sink', () => {
      const sink: OutputSink = { type: 'collection', collection: 'test' }
      expect(isCollectionSink(sink)).toBe(true)
    })

    it('should return false for other sink types', () => {
      const sink: OutputSink = { type: 'console' }
      expect(isCollectionSink(sink)).toBe(false)
    })
  })

  describe('isWebhookSink', () => {
    it('should return true for webhook sink', () => {
      const sink: OutputSink = { type: 'webhook', url: 'https://example.com' }
      expect(isWebhookSink(sink)).toBe(true)
    })

    it('should return false for other sink types', () => {
      const sink: OutputSink = { type: 'collection', collection: 'test' }
      expect(isWebhookSink(sink)).toBe(false)
    })
  })

  describe('isQueueSink', () => {
    it('should return true for queue sink', () => {
      const sink: OutputSink = { type: 'queue', queue: 'my-queue' }
      expect(isQueueSink(sink)).toBe(true)
    })

    it('should return false for other sink types', () => {
      const sink: OutputSink = { type: 'collection', collection: 'test' }
      expect(isQueueSink(sink)).toBe(false)
    })
  })

  describe('isConsoleSink', () => {
    it('should return true for console sink', () => {
      const sink: OutputSink = { type: 'console' }
      expect(isConsoleSink(sink)).toBe(true)
    })

    it('should return false for other sink types', () => {
      const sink: OutputSink = { type: 'collection', collection: 'test' }
      expect(isConsoleSink(sink)).toBe(false)
    })
  })
})

describe('Complex Examples', () => {
  it('should handle real-world analytics view', () => {
    const view = defineStreamView({
      name: 'hourly_page_views',
      description: 'Aggregated page view counts per URL per hour',
      source: {
        collection: 'page_views',
        filter: { bot: { $ne: true } },
        eventTypes: ['CREATE'],
        startPosition: 'earliest',
      },
      transform: {
        groupBy: 'url',
        pipeline: [
          { $group: {
            _id: '$url',
            views: { $sum: 1 },
            uniqueUsers: { $addToSet: '$userId' },
          }},
        ],
      },
      window: {
        type: 'tumbling',
        size: { hours: 1 },
      },
      output: {
        sink: {
          type: 'collection',
          collection: 'page_view_stats',
          mode: 'append',
        },
      },
      watermark: {
        maxLateness: { minutes: 5 },
        lateEventPolicy: 'update',
      },
      tags: ['analytics', 'production'],
    })

    expect(view.name).toBe('hourly_page_views')
    expect(view.source.filter).toEqual({ bot: { $ne: true } })
    expect(view.window?.type).toBe('tumbling')
    expect(view.transform?.groupBy).toBe('url')
  })

  it('should handle user session aggregation', () => {
    const view = defineStreamView({
      name: 'user_sessions',
      source: {
        collection: 'user_events',
      },
      transform: {
        groupBy: 'userId',
        pipeline: [
          { $group: {
            _id: null,
            eventCount: { $sum: 1 },
            events: { $push: '$$ROOT' },
            firstEvent: { $first: '$timestamp' },
            lastEvent: { $last: '$timestamp' },
          }},
        ],
      },
      window: {
        type: 'session',
        gap: { minutes: 30 },
        maxDuration: { hours: 8 },
      },
      output: {
        sink: {
          type: 'collection',
          collection: 'sessions',
          mode: 'upsert',
          keyFields: ['userId', 'sessionStart'],
        },
      },
    })

    expect(view.window?.type).toBe('session')
    if (view.window?.type === 'session') {
      expect(view.window.maxDuration).toEqual({ hours: 8 })
    }
  })
})

// =============================================================================
// Stream Collections ($ingest directive) Tests
// =============================================================================

import {
  isStreamCollection,
  AIRequests,
  Generations,
  TailEvents,
  EvalRuns,
  EvalScoresCollection,
  type StreamCollectionSchema,
  type IngestSource,
} from '../../../src/streaming'

describe('Stream Collections ($ingest directive)', () => {
  describe('isStreamCollection', () => {
    it('should return true for objects with $ingest directive', () => {
      const schema = {
        $type: 'TestEvent',
        $ingest: 'custom:test-handler' as IngestSource,
        name: 'string!',
      }
      expect(isStreamCollection(schema)).toBe(true)
    })

    it('should return false for objects without $ingest directive', () => {
      const schema = {
        $type: 'RegularEntity',
        name: 'string!',
      }
      expect(isStreamCollection(schema)).toBe(false)
    })

    it('should return false for non-objects', () => {
      expect(isStreamCollection(null)).toBe(false)
      expect(isStreamCollection(undefined)).toBe(false)
      expect(isStreamCollection('string')).toBe(false)
      expect(isStreamCollection(123)).toBe(false)
    })

    it('should return false for objects with non-string $ingest', () => {
      const schema = {
        $type: 'TestEvent',
        $ingest: 123,
      }
      expect(isStreamCollection(schema)).toBe(false)
    })
  })

  describe('Pre-built Stream Collections', () => {
    describe('AIRequests', () => {
      it('should be a valid stream collection', () => {
        expect(isStreamCollection(AIRequests)).toBe(true)
      })

      it('should have correct $type', () => {
        expect(AIRequests.$type).toBe('AIRequest')
      })

      it('should have $ingest set to ai-sdk', () => {
        expect(AIRequests.$ingest).toBe('ai-sdk')
      })

      it('should have required AI request fields', () => {
        expect(AIRequests.modelId).toBe('string!')
        expect(AIRequests.providerId).toBe('string!')
        expect(AIRequests.requestType).toBe('string!')
        expect(AIRequests.latencyMs).toBe('int!')
        expect(AIRequests.cached).toBe('boolean!')
        expect(AIRequests.timestamp).toBe('timestamp!')
      })
    })

    describe('Generations', () => {
      it('should be a valid stream collection', () => {
        expect(isStreamCollection(Generations)).toBe(true)
      })

      it('should have correct $type', () => {
        expect(Generations.$type).toBe('Generation')
      })

      it('should have $ingest set to ai-sdk', () => {
        expect(Generations.$ingest).toBe('ai-sdk')
      })

      it('should have required generation fields', () => {
        expect(Generations.modelId).toBe('string!')
        expect(Generations.contentType).toBe('string!')
        expect(Generations.content).toBe('variant!')
        expect(Generations.timestamp).toBe('timestamp!')
      })
    })

    describe('TailEvents', () => {
      it('should be a valid stream collection', () => {
        expect(isStreamCollection(TailEvents)).toBe(true)
      })

      it('should have correct $type', () => {
        expect(TailEvents.$type).toBe('TailEvent')
      })

      it('should have $ingest set to tail', () => {
        expect(TailEvents.$ingest).toBe('tail')
      })

      it('should have required tail event fields', () => {
        expect(TailEvents.scriptName).toBe('string!')
        expect(TailEvents.outcome).toBe('string!')
        expect(TailEvents.eventTimestamp).toBe('timestamp!')
        expect(TailEvents.logs).toBe('variant[]')
        expect(TailEvents.exceptions).toBe('variant[]')
      })
    })

    describe('EvalRuns', () => {
      it('should be a valid stream collection', () => {
        expect(isStreamCollection(EvalRuns)).toBe(true)
      })

      it('should have correct $type', () => {
        expect(EvalRuns.$type).toBe('EvalRun')
      })

      it('should have $ingest set to evalite', () => {
        expect(EvalRuns.$ingest).toBe('evalite')
      })

      it('should have required eval run fields', () => {
        expect(EvalRuns.runId).toBe('int!')
        expect(EvalRuns.runType).toBe('string!')
        expect(EvalRuns.timestamp).toBe('timestamp!')
      })
    })

    describe('EvalScoresCollection', () => {
      it('should be a valid stream collection', () => {
        expect(isStreamCollection(EvalScoresCollection)).toBe(true)
      })

      it('should have correct $type', () => {
        expect(EvalScoresCollection.$type).toBe('EvalScore')
      })

      it('should have $ingest set to evalite', () => {
        expect(EvalScoresCollection.$ingest).toBe('evalite')
      })

      it('should have required eval score fields', () => {
        expect(EvalScoresCollection.runId).toBe('int!')
        expect(EvalScoresCollection.suiteName).toBe('string!')
        expect(EvalScoresCollection.scorerName).toBe('string!')
        expect(EvalScoresCollection.score).toBe('decimal(5,4)!')
        expect(EvalScoresCollection.timestamp).toBe('timestamp!')
      })
    })
  })

  describe('Custom Stream Collection', () => {
    it('should allow creating custom stream collections', () => {
      const CustomEvents: StreamCollectionSchema = {
        $type: 'CustomEvent',
        $ingest: 'custom:my-handler',
        eventType: 'string!',
        payload: 'variant?',
        timestamp: 'timestamp!',
      }

      expect(isStreamCollection(CustomEvents)).toBe(true)
      expect(CustomEvents.$type).toBe('CustomEvent')
      expect(CustomEvents.$ingest).toBe('custom:my-handler')
    })
  })
})
