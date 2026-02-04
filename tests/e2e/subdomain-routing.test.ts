/**
 * E2E Tests for Subdomain Routing
 *
 * Tests that custom domains route correctly to their respective endpoints.
 * These tests run against the deployed worker.
 */

import { describe, it, expect } from 'vitest'

const DOMAINS = {
  api: 'https://api.parquedb.com',
  benchmarks: 'https://benchmarks.parquedb.com',
  onet: 'https://onet.parquedb.com',
  imdb: 'https://imdb.parquedb.com',
  unspsc: 'https://unspsc.parquedb.com',
}

describe('Subdomain Routing E2E', () => {
  describe('Dataset Aliases', () => {
    it('should support onet alias at /datasets/onet', async () => {
      const res = await fetch(`${DOMAINS.benchmarks}/datasets/onet`)
      expect(res.status).toBe(200)
      const data = await res.json() as { api: { id: string }; links: { occupations: string } }
      expect(data.api.id).toBe('onet')
      expect(data.links.occupations).toContain('/datasets/onet/occupations')
    })

    it('should return full URLs in error responses', async () => {
      const res = await fetch(`${DOMAINS.benchmarks}/datasets/nonexistent`)
      expect(res.status).toBe(404)
      const data = await res.json() as { links: { home: string; datasets: string } }
      expect(data.links.home).toBe('https://benchmarks.parquedb.com')
      expect(data.links.datasets).toBe('https://benchmarks.parquedb.com/datasets')
    })
  })

  describe('api.parquedb.com', () => {
    it('should return healthy status on /health', async () => {
      const res = await fetch(`${DOMAINS.api}/health`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.api.status).toBe('healthy')
    })

    it('should return deep health check on /health?deep=true', async () => {
      const res = await fetch(`${DOMAINS.api}/health?deep=true`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.api.mode).toBe('deep')
      expect(data.checks).toBeDefined()
      expect(data.checks.r2).toBeDefined()
      expect(data.checks.read).toBeDefined()
      expect(data.checks.write).toBeDefined()
    })

    it('should list datasets on /datasets', async () => {
      const res = await fetch(`${DOMAINS.api}/datasets`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.items).toBeDefined()
      expect(Array.isArray(data.items)).toBe(true)
    })
  })

  describe('benchmarks.parquedb.com', () => {
    it('should return benchmark suite info on /benchmark/e2e', async () => {
      const res = await fetch(`${DOMAINS.benchmarks}/benchmark/e2e`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.benchmark).toBe('E2E Benchmark Suite')
      expect(data.endpoints).toBeDefined()
    })

    it('should return health check on /benchmark/e2e/health', async () => {
      const res = await fetch(`${DOMAINS.benchmarks}/benchmark/e2e/health`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.status).toBeDefined()
      expect(data.checks).toBeDefined()
    })

    it('should run CRUD benchmark on /benchmark/e2e/crud/create', async () => {
      const res = await fetch(`${DOMAINS.benchmarks}/benchmark/e2e/crud/create?iterations=1&warmup=0`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.operation).toBe('create')
      expect(data.latencyMs).toBeDefined()
      expect(data.throughput).toBeDefined()
    })
  })

  describe('onet.parquedb.com', () => {
    it('should return ONET dataset info on /', async () => {
      const res = await fetch(`${DOMAINS.onet}/`)
      expect(res.status).toBe(200)
      const data = await res.json() as { api: { id: string }; data: { collections: unknown } }
      expect(data.api.id).toBe('onet-graph')
      expect(data.data.collections).toBeDefined()
    })

    it('should list occupations on /occupations', async () => {
      const res = await fetch(`${DOMAINS.onet}/occupations`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.items).toBeDefined()
      expect(Array.isArray(data.items)).toBe(true)
    })

    it('should return occupation details on /occupations/:id', async () => {
      const res = await fetch(`${DOMAINS.onet}/occupations/11-1011.00`)
      expect(res.status).toBe(200)
      const data = await res.json() as { api: { id: string }; data: { $id: string } }
      expect(data.api.id).toBe('11-1011.00')
      expect(data.data.$id).toBeDefined()
    })
  })

  describe('imdb.parquedb.com', () => {
    it('should return IMDB dataset info on /', async () => {
      const res = await fetch(`${DOMAINS.imdb}/`)
      expect(res.status).toBe(200)
      const data = await res.json() as { api: { id: string }; data: { collections: unknown } }
      expect(data.api.id).toBe('imdb')
      expect(data.data.collections).toBeDefined()
    })

    it('should list titles on /titles', async () => {
      const res = await fetch(`${DOMAINS.imdb}/titles`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.items).toBeDefined()
      expect(Array.isArray(data.items)).toBe(true)
    })

    it('should filter titles by type on /titles?filter={"titleType":"movie"}', async () => {
      const filter = encodeURIComponent(JSON.stringify({ titleType: 'movie' }))
      const res = await fetch(`${DOMAINS.imdb}/titles?filter=${filter}&limit=10`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.items).toBeDefined()
      expect(data.items.every((item: any) => item.titleType === 'movie')).toBe(true)
    })
  })

  describe('unspsc.parquedb.com', () => {
    it('should return UNSPSC dataset info on /', async () => {
      const res = await fetch(`${DOMAINS.unspsc}/`)
      expect(res.status).toBe(200)
      const data = await res.json() as { api: { id: string }; data: { collections: unknown } }
      expect(data.api.id).toBe('unspsc')
      expect(data.data.collections).toBeDefined()
    })

    it('should list segments on /segments', async () => {
      const res = await fetch(`${DOMAINS.unspsc}/segments`)
      expect(res.status).toBe(200)
      const data = await res.json()
      expect(data.items).toBeDefined()
      expect(Array.isArray(data.items)).toBe(true)
    })
  })
})
