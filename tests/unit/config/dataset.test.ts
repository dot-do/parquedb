/**
 * Dataset Configuration Test Suite
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  DatasetConfigManager,
  createDatasetConfigManager,
  isEventsEnabled,
  validateTimeTravelSupport,
  type ConfigStorage,
  type DatasetConfigFile,
} from '@/config/dataset'

// =============================================================================
// Mock Storage Implementation
// =============================================================================

class MockStorage implements ConfigStorage {
  private files: Map<string, Uint8Array> = new Map()

  async put(key: string, data: Uint8Array | ArrayBuffer): Promise<void> {
    const uint8 = data instanceof Uint8Array ? data : new Uint8Array(data)
    this.files.set(key, uint8)
  }

  async get(key: string): Promise<Uint8Array | null> {
    return this.files.get(key) ?? null
  }

  async head(key: string): Promise<boolean> {
    return this.files.has(key)
  }

  clear(): void {
    this.files.clear()
  }

  // Helper to set config directly
  setConfig(dataset: string, config: Partial<DatasetConfigFile>): void {
    const full: DatasetConfigFile = {
      version: 1,
      dataset,
      events: false,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      ...config,
    }
    const json = JSON.stringify(full)
    this.files.set(`${dataset}/_config.json`, new TextEncoder().encode(json))
  }
}

// =============================================================================
// DatasetConfigManager Tests
// =============================================================================

describe('DatasetConfigManager', () => {
  let storage: MockStorage
  let manager: DatasetConfigManager

  beforeEach(() => {
    storage = new MockStorage()
    manager = new DatasetConfigManager({ dataset: 'test-app', storage })
  })

  describe('load and save', () => {
    it('creates default config if none exists', async () => {
      const config = await manager.load()

      expect(config.version).toBe(1)
      expect(config.dataset).toBe('test-app')
      expect(config.events).toBe(false)
      expect(config.createdAt).toBeGreaterThan(0)
      expect(config.updatedAt).toBeGreaterThan(0)
    })

    it('loads existing config', async () => {
      storage.setConfig('test-app', {
        events: true,
        compaction: { interval: '1h', retention: '30d' },
      })

      const config = await manager.load()

      expect(config.events).toBe(true)
      expect(config.compaction).toEqual({ interval: '1h', retention: '30d' })
    })

    it('saves config to storage', async () => {
      await manager.load()
      await manager.setEventsEnabled(true)
      await manager.save()

      // Clear cache and reload
      manager.clearCache()
      const config = await manager.load()

      expect(config.events).toBe(true)
    })

    it('tracks dirty state', async () => {
      await manager.load()
      expect(manager.isDirty()).toBe(false)

      await manager.setEventsEnabled(true)
      expect(manager.isDirty()).toBe(true)

      await manager.save()
      expect(manager.isDirty()).toBe(false)
    })

    it('saveIfDirty only saves when dirty', async () => {
      await manager.load()
      await manager.saveIfDirty() // Should not throw

      await manager.setEventsEnabled(true)
      await manager.saveIfDirty()
      expect(manager.isDirty()).toBe(false)
    })

    it('returns correct config path', () => {
      expect(manager.getConfigPath()).toBe('test-app/_config.json')
    })
  })

  describe('events configuration', () => {
    it('isEventsEnabled returns false by default', async () => {
      await manager.load()
      expect(manager.isEventsEnabled()).toBe(false)
    })

    it('isEventsEnabled returns true when enabled', async () => {
      storage.setConfig('test-app', { events: true })
      await manager.load()
      expect(manager.isEventsEnabled()).toBe(true)
    })

    it('setEventsEnabled enables events', async () => {
      await manager.load()
      await manager.setEventsEnabled(true)

      expect(manager.isEventsEnabled()).toBe(true)
      expect(manager.isDirty()).toBe(true)
    })

    it('setEventsEnabled disables events', async () => {
      storage.setConfig('test-app', { events: true })
      await manager.load()

      await manager.setEventsEnabled(false)

      expect(manager.isEventsEnabled()).toBe(false)
    })
  })

  describe('compaction configuration', () => {
    it('getCompactionConfig returns undefined by default', async () => {
      await manager.load()
      expect(manager.getCompactionConfig()).toBeUndefined()
    })

    it('getCompactionConfig returns config when set', async () => {
      storage.setConfig('test-app', {
        events: true,
        compaction: { interval: '2h', minEvents: 5000 },
      })
      await manager.load()

      const compaction = manager.getCompactionConfig()
      expect(compaction).toEqual({ interval: '2h', minEvents: 5000 })
    })

    it('setCompactionConfig updates config', async () => {
      await manager.load()
      await manager.setCompactionConfig({ interval: '4h', retention: '7d' })

      expect(manager.getCompactionConfig()).toEqual({ interval: '4h', retention: '7d' })
      expect(manager.isDirty()).toBe(true)
    })
  })

  describe('time-travel support', () => {
    it('isTimeTravelQuery returns false for undefined', () => {
      expect(manager.isTimeTravelQuery(undefined)).toBe(false)
    })

    it('isTimeTravelQuery returns false for empty options', () => {
      expect(manager.isTimeTravelQuery({})).toBe(false)
    })

    it('isTimeTravelQuery returns true when at is specified', () => {
      expect(manager.isTimeTravelQuery({ at: 1000 })).toBe(true)
    })

    it('validateTimeTravelQuery passes for non-time-travel query', async () => {
      await manager.load()
      expect(() => manager.validateTimeTravelQuery({})).not.toThrow()
    })

    it('validateTimeTravelQuery throws for time-travel without events', async () => {
      await manager.load()
      expect(() => manager.validateTimeTravelQuery({ at: 1000 }))
        .toThrow('Time-travel queries require events: true')
    })

    it('validateTimeTravelQuery passes when events enabled', async () => {
      storage.setConfig('test-app', { events: true })
      await manager.load()
      expect(() => manager.validateTimeTravelQuery({ at: 1000 })).not.toThrow()
    })
  })

  describe('toDatasetConfig', () => {
    it('returns DatasetConfig from loaded config', async () => {
      storage.setConfig('test-app', {
        events: true,
        compaction: { interval: '1h' },
      })
      await manager.load()

      const datasetConfig = manager.toDatasetConfig()
      expect(datasetConfig).toEqual({
        events: true,
        compaction: { interval: '1h' },
      })
    })

    it('returns defaults when not loaded', () => {
      const datasetConfig = manager.toDatasetConfig()
      expect(datasetConfig).toEqual({
        events: false,
        compaction: undefined,
      })
    })
  })

  describe('cache management', () => {
    it('clearCache clears loaded config', async () => {
      await manager.load()
      expect(manager.getLoadedConfig()).not.toBeNull()

      manager.clearCache()

      expect(manager.getLoadedConfig()).toBeNull()
      expect(manager.isDirty()).toBe(false)
    })
  })
})

// =============================================================================
// Helper Function Tests
// =============================================================================

describe('isEventsEnabled', () => {
  let storage: MockStorage

  beforeEach(() => {
    storage = new MockStorage()
  })

  it('returns false when no config exists', async () => {
    const enabled = await isEventsEnabled(storage, 'my-app')
    expect(enabled).toBe(false)
  })

  it('returns true when events enabled in config', async () => {
    storage.setConfig('my-app', { events: true })
    const enabled = await isEventsEnabled(storage, 'my-app')
    expect(enabled).toBe(true)
  })

  it('returns false when events disabled in config', async () => {
    storage.setConfig('my-app', { events: false })
    const enabled = await isEventsEnabled(storage, 'my-app')
    expect(enabled).toBe(false)
  })

  it('returns false for invalid JSON', async () => {
    await storage.put('broken-app/_config.json', new TextEncoder().encode('not json'))
    const enabled = await isEventsEnabled(storage, 'broken-app')
    expect(enabled).toBe(false)
  })
})

describe('validateTimeTravelSupport', () => {
  let storage: MockStorage

  beforeEach(() => {
    storage = new MockStorage()
  })

  it('passes for non-time-travel query', async () => {
    await expect(validateTimeTravelSupport(storage, 'my-app', {}))
      .resolves.not.toThrow()
  })

  it('passes for undefined options', async () => {
    await expect(validateTimeTravelSupport(storage, 'my-app', undefined))
      .resolves.not.toThrow()
  })

  it('throws for time-travel without events', async () => {
    await expect(validateTimeTravelSupport(storage, 'my-app', { at: 1000 }))
      .rejects.toThrow('Time-travel queries require events: true')
  })

  it('passes for time-travel with events enabled', async () => {
    storage.setConfig('my-app', { events: true })
    await expect(validateTimeTravelSupport(storage, 'my-app', { at: 1000 }))
      .resolves.not.toThrow()
  })

  it('error message includes dataset name', async () => {
    await expect(validateTimeTravelSupport(storage, 'custom-dataset', { at: 1000 }))
      .rejects.toThrow('custom-dataset')
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createDatasetConfigManager', () => {
  it('creates a manager', () => {
    const storage = new MockStorage()
    const manager = createDatasetConfigManager({ dataset: 'my-app', storage })
    expect(manager).toBeInstanceOf(DatasetConfigManager)
  })
})
