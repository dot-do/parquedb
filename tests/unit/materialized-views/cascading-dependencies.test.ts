/**
 * Cascading Dependency Tests for Materialized Views
 *
 * Tests MV dependency chains and refresh propagation:
 * - Cascading dependencies (A -> B -> C)
 * - Refresh propagation through dependency chains
 * - Circular dependency detection
 * - Dependency ordering (topological sort)
 * - Diamond dependencies (A -> B, A -> C, B -> D, C -> D)
 *
 * Issue: parquedb-lilj [P1] - Add: MV cascading dependency tests
 *
 * Run with: pnpm test tests/materialized-views/cascading-dependencies.test.ts
 */

import { describe, test, expect, beforeEach, afterEach, vi } from 'vitest'
import type { Event, EventOp } from '../../src/types/entity'

// =============================================================================
// Types
// =============================================================================

/**
 * Dependency graph node representing an MV
 */
interface MVNode {
  /** MV name */
  name: string
  /** Source namespaces (base collections this MV depends on) */
  sourceNamespaces: string[]
  /** MVs this MV depends on (derived from other MVs) */
  dependsOn: string[]
  /** MVs that depend on this MV */
  dependents: string[]
}

/**
 * Dependency graph for managing MV relationships
 */
interface DependencyGraph {
  /** All MV nodes indexed by name */
  nodes: Map<string, MVNode>
  /** Add an MV to the graph */
  addMV(name: string, sourceNamespaces: string[], dependsOn?: string[]): void
  /** Remove an MV from the graph */
  removeMV(name: string): void
  /** Get dependencies for an MV */
  getDependencies(name: string): string[]
  /** Get dependents of an MV */
  getDependents(name: string): string[]
  /** Get topological order for refresh */
  getRefreshOrder(startingFrom?: string): string[]
  /** Detect circular dependencies */
  detectCircularDependencies(): string[][] | null
  /** Get all MVs that need refresh when source changes */
  getAffectedMVs(sourceNamespace: string): string[]
}

/**
 * Result of a cascading refresh operation
 */
interface CascadingRefreshResult {
  /** MVs refreshed in order */
  refreshedMVs: string[]
  /** Total refresh time */
  totalDurationMs: number
  /** Individual refresh times */
  refreshTimes: Map<string, number>
  /** Any errors encountered */
  errors: Map<string, Error>
}

/**
 * Handler that tracks refresh calls for testing
 */
interface TestMVHandler {
  name: string
  sourceNamespaces: string[]
  dependsOn: string[]
  refreshCount: number
  lastRefreshTime: number
  refreshHistory: number[]
  process(events: Event[]): Promise<void>
  refresh(): Promise<void>
}

// =============================================================================
// Dependency Graph Implementation (for testing)
// =============================================================================

function createDependencyGraph(): DependencyGraph {
  const nodes = new Map<string, MVNode>()

  return {
    nodes,

    addMV(name: string, sourceNamespaces: string[], dependsOn: string[] = []): void {
      // Create node
      const node: MVNode = {
        name,
        sourceNamespaces,
        dependsOn: [...dependsOn],
        dependents: [],
      }
      nodes.set(name, node)

      // Update dependents of dependencies
      for (const dep of dependsOn) {
        const depNode = nodes.get(dep)
        if (depNode) {
          depNode.dependents.push(name)
        }
      }
    },

    removeMV(name: string): void {
      const node = nodes.get(name)
      if (!node) return

      // Remove from dependents of dependencies
      for (const dep of node.dependsOn) {
        const depNode = nodes.get(dep)
        if (depNode) {
          depNode.dependents = depNode.dependents.filter((d) => d !== name)
        }
      }

      // Remove from dependencies of dependents
      for (const dependent of node.dependents) {
        const depNode = nodes.get(dependent)
        if (depNode) {
          depNode.dependsOn = depNode.dependsOn.filter((d) => d !== name)
        }
      }

      nodes.delete(name)
    },

    getDependencies(name: string): string[] {
      const node = nodes.get(name)
      if (!node) return []

      // Recursive collection of all dependencies
      const visited = new Set<string>()
      const result: string[] = []

      function collect(mvName: string): void {
        const mvNode = nodes.get(mvName)
        if (!mvNode) return

        for (const dep of mvNode.dependsOn) {
          if (!visited.has(dep)) {
            visited.add(dep)
            collect(dep)
            result.push(dep)
          }
        }
      }

      collect(name)
      return result
    },

    getDependents(name: string): string[] {
      const node = nodes.get(name)
      if (!node) return []

      // Recursive collection of all dependents
      const visited = new Set<string>()
      const result: string[] = []

      function collect(mvName: string): void {
        const mvNode = nodes.get(mvName)
        if (!mvNode) return

        for (const dep of mvNode.dependents) {
          if (!visited.has(dep)) {
            visited.add(dep)
            result.push(dep)
            collect(dep)
          }
        }
      }

      collect(name)
      return result
    },

    getRefreshOrder(startingFrom?: string): string[] {
      // Topological sort using Kahn's algorithm
      const inDegree = new Map<string, number>()
      const queue: string[] = []
      const result: string[] = []

      // Calculate in-degrees
      for (const [name, node] of nodes) {
        inDegree.set(name, node.dependsOn.length)
        if (node.dependsOn.length === 0) {
          queue.push(name)
        }
      }

      // Process queue
      while (queue.length > 0) {
        const current = queue.shift()!
        result.push(current)

        const node = nodes.get(current)!
        for (const dependent of node.dependents) {
          const degree = (inDegree.get(dependent) ?? 1) - 1
          inDegree.set(dependent, degree)
          if (degree === 0) {
            queue.push(dependent)
          }
        }
      }

      // If starting from a specific MV, filter to only relevant downstream MVs
      if (startingFrom) {
        const affected = new Set([startingFrom, ...this.getDependents(startingFrom)])
        return result.filter((mv) => affected.has(mv))
      }

      return result
    },

    detectCircularDependencies(): string[][] | null {
      const cycles: string[][] = []
      const visited = new Set<string>()
      const recursionStack = new Set<string>()
      const path: string[] = []

      function dfs(name: string): boolean {
        visited.add(name)
        recursionStack.add(name)
        path.push(name)

        const node = nodes.get(name)
        if (node) {
          for (const dep of node.dependsOn) {
            if (!visited.has(dep)) {
              if (dfs(dep)) {
                return true
              }
            } else if (recursionStack.has(dep)) {
              // Found cycle
              const cycleStart = path.indexOf(dep)
              cycles.push([...path.slice(cycleStart), dep])
              return true
            }
          }
        }

        path.pop()
        recursionStack.delete(name)
        return false
      }

      for (const name of nodes.keys()) {
        if (!visited.has(name)) {
          dfs(name)
        }
      }

      return cycles.length > 0 ? cycles : null
    },

    getAffectedMVs(sourceNamespace: string): string[] {
      const affected = new Set<string>()

      // Find all MVs that directly depend on this source
      for (const [name, node] of nodes) {
        if (node.sourceNamespaces.includes(sourceNamespace)) {
          affected.add(name)
          // Add all transitive dependents
          for (const dependent of this.getDependents(name)) {
            affected.add(dependent)
          }
        }
      }

      // Return in topological order
      const order = this.getRefreshOrder()
      return order.filter((mv) => affected.has(mv))
    },
  }
}

// =============================================================================
// Cascading Refresh Engine (for testing)
// =============================================================================

interface CascadingRefreshEngine {
  graph: DependencyGraph
  handlers: Map<string, TestMVHandler>
  registerMV(handler: TestMVHandler): void
  unregisterMV(name: string): void
  refreshMV(name: string): Promise<CascadingRefreshResult>
  refreshAll(): Promise<CascadingRefreshResult>
  onSourceChange(sourceNamespace: string): Promise<CascadingRefreshResult>
}

function createCascadingRefreshEngine(): CascadingRefreshEngine {
  const graph = createDependencyGraph()
  const handlers = new Map<string, TestMVHandler>()

  return {
    graph,
    handlers,

    registerMV(handler: TestMVHandler): void {
      handlers.set(handler.name, handler)
      graph.addMV(handler.name, handler.sourceNamespaces, handler.dependsOn)
    },

    unregisterMV(name: string): void {
      handlers.delete(name)
      graph.removeMV(name)
    },

    async refreshMV(name: string): Promise<CascadingRefreshResult> {
      const result: CascadingRefreshResult = {
        refreshedMVs: [],
        totalDurationMs: 0,
        refreshTimes: new Map(),
        errors: new Map(),
      }

      const startTime = Date.now()

      // Get refresh order starting from this MV
      const order = graph.getRefreshOrder(name)

      // Refresh in order
      for (const mvName of order) {
        const handler = handlers.get(mvName)
        if (!handler) continue

        const mvStartTime = Date.now()
        try {
          await handler.refresh()
          const duration = Date.now() - mvStartTime
          result.refreshedMVs.push(mvName)
          result.refreshTimes.set(mvName, duration)
        } catch (error) {
          result.errors.set(mvName, error instanceof Error ? error : new Error(String(error)))
        }
      }

      result.totalDurationMs = Date.now() - startTime
      return result
    },

    async refreshAll(): Promise<CascadingRefreshResult> {
      const result: CascadingRefreshResult = {
        refreshedMVs: [],
        totalDurationMs: 0,
        refreshTimes: new Map(),
        errors: new Map(),
      }

      const startTime = Date.now()

      // Get full topological order
      const order = graph.getRefreshOrder()

      // Refresh in order
      for (const mvName of order) {
        const handler = handlers.get(mvName)
        if (!handler) continue

        const mvStartTime = Date.now()
        try {
          await handler.refresh()
          const duration = Date.now() - mvStartTime
          result.refreshedMVs.push(mvName)
          result.refreshTimes.set(mvName, duration)
        } catch (error) {
          result.errors.set(mvName, error instanceof Error ? error : new Error(String(error)))
        }
      }

      result.totalDurationMs = Date.now() - startTime
      return result
    },

    async onSourceChange(sourceNamespace: string): Promise<CascadingRefreshResult> {
      const result: CascadingRefreshResult = {
        refreshedMVs: [],
        totalDurationMs: 0,
        refreshTimes: new Map(),
        errors: new Map(),
      }

      const startTime = Date.now()

      // Get affected MVs in topological order
      const affected = graph.getAffectedMVs(sourceNamespace)

      // Refresh in order
      for (const mvName of affected) {
        const handler = handlers.get(mvName)
        if (!handler) continue

        const mvStartTime = Date.now()
        try {
          await handler.refresh()
          const duration = Date.now() - mvStartTime
          result.refreshedMVs.push(mvName)
          result.refreshTimes.set(mvName, duration)
        } catch (error) {
          result.errors.set(mvName, error instanceof Error ? error : new Error(String(error)))
        }
      }

      result.totalDurationMs = Date.now() - startTime
      return result
    },
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(target: string, op: EventOp, after?: Record<string, unknown>): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op,
    target,
    after,
    actor: 'test:user',
  }
}

function createTestHandler(
  name: string,
  sourceNamespaces: string[],
  dependsOn: string[] = [],
  options?: { delayMs?: number; shouldFail?: boolean }
): TestMVHandler {
  return {
    name,
    sourceNamespaces,
    dependsOn,
    refreshCount: 0,
    lastRefreshTime: 0,
    refreshHistory: [],

    async process(_events: Event[]): Promise<void> {
      await this.refresh()
    },

    async refresh(): Promise<void> {
      if (options?.delayMs) {
        await new Promise((resolve) => setTimeout(resolve, options.delayMs))
      }

      if (options?.shouldFail) {
        throw new Error(`Refresh failed for ${name}`)
      }

      this.refreshCount++
      this.lastRefreshTime = Date.now()
      this.refreshHistory.push(this.lastRefreshTime)
    },
  }
}

// =============================================================================
// Test Suite: Dependency Graph
// =============================================================================

describe('MV Dependency Graph', () => {
  let graph: DependencyGraph

  beforeEach(() => {
    graph = createDependencyGraph()
  })

  describe('Basic Operations', () => {
    test('should add MVs to the graph', () => {
      graph.addMV('Orders', ['orders'], [])
      graph.addMV('OrderTotals', ['orders'], ['Orders'])

      expect(graph.nodes.size).toBe(2)
      expect(graph.nodes.get('Orders')).toBeDefined()
      expect(graph.nodes.get('OrderTotals')).toBeDefined()
    })

    test('should track dependencies correctly', () => {
      graph.addMV('Orders', ['orders'], [])
      graph.addMV('OrderTotals', ['orders'], ['Orders'])
      graph.addMV('DailySales', ['orders'], ['OrderTotals'])

      const orderTotalsNode = graph.nodes.get('OrderTotals')!
      expect(orderTotalsNode.dependsOn).toContain('Orders')

      const dailySalesNode = graph.nodes.get('DailySales')!
      expect(dailySalesNode.dependsOn).toContain('OrderTotals')
    })

    test('should track dependents correctly', () => {
      graph.addMV('Orders', ['orders'], [])
      graph.addMV('OrderTotals', ['orders'], ['Orders'])
      graph.addMV('DailySales', ['orders'], ['OrderTotals'])

      const ordersNode = graph.nodes.get('Orders')!
      expect(ordersNode.dependents).toContain('OrderTotals')

      const orderTotalsNode = graph.nodes.get('OrderTotals')!
      expect(orderTotalsNode.dependents).toContain('DailySales')
    })

    test('should remove MVs and update relationships', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['B'])

      graph.removeMV('B')

      expect(graph.nodes.has('B')).toBe(false)

      const aNode = graph.nodes.get('A')!
      expect(aNode.dependents).not.toContain('B')

      const cNode = graph.nodes.get('C')!
      expect(cNode.dependsOn).not.toContain('B')
    })
  })

  describe('Dependency Traversal', () => {
    test('should get all dependencies recursively', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['B'])
      graph.addMV('D', ['source'], ['C'])

      const deps = graph.getDependencies('D')

      expect(deps).toContain('A')
      expect(deps).toContain('B')
      expect(deps).toContain('C')
      expect(deps.length).toBe(3)
    })

    test('should get all dependents recursively', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['B'])
      graph.addMV('D', ['source'], ['C'])

      const dependents = graph.getDependents('A')

      expect(dependents).toContain('B')
      expect(dependents).toContain('C')
      expect(dependents).toContain('D')
      expect(dependents.length).toBe(3)
    })

    test('should handle diamond dependencies', () => {
      // A -> B, A -> C, B -> D, C -> D
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['A'])
      graph.addMV('D', ['source'], ['B', 'C'])

      const depsOfD = graph.getDependencies('D')
      expect(depsOfD).toContain('A')
      expect(depsOfD).toContain('B')
      expect(depsOfD).toContain('C')

      const dependentsOfA = graph.getDependents('A')
      expect(dependentsOfA).toContain('B')
      expect(dependentsOfA).toContain('C')
      expect(dependentsOfA).toContain('D')
    })
  })

  describe('Topological Sort', () => {
    test('should return correct refresh order for simple chain', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['B'])

      const order = graph.getRefreshOrder()

      const aIndex = order.indexOf('A')
      const bIndex = order.indexOf('B')
      const cIndex = order.indexOf('C')

      expect(aIndex).toBeLessThan(bIndex)
      expect(bIndex).toBeLessThan(cIndex)
    })

    test('should return correct refresh order for diamond', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['A'])
      graph.addMV('D', ['source'], ['B', 'C'])

      const order = graph.getRefreshOrder()

      const aIndex = order.indexOf('A')
      const bIndex = order.indexOf('B')
      const cIndex = order.indexOf('C')
      const dIndex = order.indexOf('D')

      // A must come before B, C, and D
      expect(aIndex).toBeLessThan(bIndex)
      expect(aIndex).toBeLessThan(cIndex)
      expect(aIndex).toBeLessThan(dIndex)

      // B and C must come before D
      expect(bIndex).toBeLessThan(dIndex)
      expect(cIndex).toBeLessThan(dIndex)
    })

    test('should return order starting from specific MV', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['B'])
      graph.addMV('X', ['other'], []) // Unrelated MV

      const order = graph.getRefreshOrder('B')

      expect(order).toContain('B')
      expect(order).toContain('C')
      expect(order).not.toContain('A') // A is a dependency, not a dependent
      expect(order).not.toContain('X') // X is unrelated
    })

    test('should handle multiple independent chains', () => {
      graph.addMV('A1', ['source1'], [])
      graph.addMV('A2', ['source1'], ['A1'])

      graph.addMV('B1', ['source2'], [])
      graph.addMV('B2', ['source2'], ['B1'])

      const order = graph.getRefreshOrder()

      // A1 before A2
      expect(order.indexOf('A1')).toBeLessThan(order.indexOf('A2'))
      // B1 before B2
      expect(order.indexOf('B1')).toBeLessThan(order.indexOf('B2'))

      expect(order.length).toBe(4)
    })
  })

  describe('Circular Dependency Detection', () => {
    test('should detect direct circular dependency (A -> B -> A)', () => {
      // Create circular dependency manually (bypassing normal validation)
      const nodeA: MVNode = {
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: ['B'],
        dependents: ['B'],
      }
      const nodeB: MVNode = {
        name: 'B',
        sourceNamespaces: ['source'],
        dependsOn: ['A'],
        dependents: ['A'],
      }
      graph.nodes.set('A', nodeA)
      graph.nodes.set('B', nodeB)

      const cycles = graph.detectCircularDependencies()

      expect(cycles).not.toBeNull()
      expect(cycles!.length).toBeGreaterThan(0)
    })

    test('should detect indirect circular dependency (A -> B -> C -> A)', () => {
      const nodeA: MVNode = {
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: ['C'],
        dependents: ['B'],
      }
      const nodeB: MVNode = {
        name: 'B',
        sourceNamespaces: ['source'],
        dependsOn: ['A'],
        dependents: ['C'],
      }
      const nodeC: MVNode = {
        name: 'C',
        sourceNamespaces: ['source'],
        dependsOn: ['B'],
        dependents: ['A'],
      }
      graph.nodes.set('A', nodeA)
      graph.nodes.set('B', nodeB)
      graph.nodes.set('C', nodeC)

      const cycles = graph.detectCircularDependencies()

      expect(cycles).not.toBeNull()
      expect(cycles!.length).toBeGreaterThan(0)
    })

    test('should return null when no circular dependencies exist', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['B'])

      const cycles = graph.detectCircularDependencies()

      expect(cycles).toBeNull()
    })

    test('should return null for diamond dependency (not circular)', () => {
      graph.addMV('A', ['source'], [])
      graph.addMV('B', ['source'], ['A'])
      graph.addMV('C', ['source'], ['A'])
      graph.addMV('D', ['source'], ['B', 'C'])

      const cycles = graph.detectCircularDependencies()

      expect(cycles).toBeNull()
    })
  })

  describe('Source Change Propagation', () => {
    test('should find all affected MVs when source changes', () => {
      graph.addMV('OrdersRaw', ['orders'], [])
      graph.addMV('OrderTotals', ['orders'], ['OrdersRaw'])
      graph.addMV('DailySales', ['orders'], ['OrderTotals'])
      graph.addMV('ProductsMV', ['products'], []) // Different source

      const affected = graph.getAffectedMVs('orders')

      expect(affected).toContain('OrdersRaw')
      expect(affected).toContain('OrderTotals')
      expect(affected).toContain('DailySales')
      expect(affected).not.toContain('ProductsMV')
    })

    test('should return affected MVs in topological order', () => {
      graph.addMV('A', ['orders'], [])
      graph.addMV('B', ['orders'], ['A'])
      graph.addMV('C', ['orders'], ['B'])

      const affected = graph.getAffectedMVs('orders')

      const aIndex = affected.indexOf('A')
      const bIndex = affected.indexOf('B')
      const cIndex = affected.indexOf('C')

      expect(aIndex).toBeLessThan(bIndex)
      expect(bIndex).toBeLessThan(cIndex)
    })

    test('should handle multiple sources', () => {
      graph.addMV('OrdersJoin', ['orders', 'products'], [])
      graph.addMV('OrderAnalytics', ['orders'], ['OrdersJoin'])

      const ordersAffected = graph.getAffectedMVs('orders')
      expect(ordersAffected).toContain('OrdersJoin')
      expect(ordersAffected).toContain('OrderAnalytics')

      const productsAffected = graph.getAffectedMVs('products')
      expect(productsAffected).toContain('OrdersJoin')
      expect(productsAffected).toContain('OrderAnalytics')
    })
  })
})

// =============================================================================
// Test Suite: Cascading Refresh Engine
// =============================================================================

describe('Cascading Refresh Engine', () => {
  let engine: CascadingRefreshEngine

  beforeEach(() => {
    engine = createCascadingRefreshEngine()
  })

  describe('Registration', () => {
    test('should register MVs with dependencies', () => {
      const handlerA = createTestHandler('A', ['source'])
      const handlerB = createTestHandler('B', ['source'], ['A'])

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)

      expect(engine.handlers.size).toBe(2)
      expect(engine.graph.nodes.size).toBe(2)
    })

    test('should unregister MVs', () => {
      const handlerA = createTestHandler('A', ['source'])
      const handlerB = createTestHandler('B', ['source'], ['A'])

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)
      engine.unregisterMV('A')

      expect(engine.handlers.has('A')).toBe(false)
      expect(engine.graph.nodes.has('A')).toBe(false)
    })
  })

  describe('Cascading Refresh', () => {
    test('should refresh MV and all dependents in order', async () => {
      const handlerA = createTestHandler('A', ['source'])
      const handlerB = createTestHandler('B', ['source'], ['A'])
      const handlerC = createTestHandler('C', ['source'], ['B'])

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)
      engine.registerMV(handlerC)

      const result = await engine.refreshMV('A')

      expect(result.refreshedMVs).toEqual(['A', 'B', 'C'])
      expect(handlerA.refreshCount).toBe(1)
      expect(handlerB.refreshCount).toBe(1)
      expect(handlerC.refreshCount).toBe(1)
    })

    test('should refresh only downstream MVs', async () => {
      const handlerA = createTestHandler('A', ['source'])
      const handlerB = createTestHandler('B', ['source'], ['A'])
      const handlerC = createTestHandler('C', ['source'], ['B'])

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)
      engine.registerMV(handlerC)

      const result = await engine.refreshMV('B')

      expect(result.refreshedMVs).toEqual(['B', 'C'])
      expect(handlerA.refreshCount).toBe(0) // A should not be refreshed
      expect(handlerB.refreshCount).toBe(1)
      expect(handlerC.refreshCount).toBe(1)
    })

    test('should handle diamond dependencies correctly', async () => {
      const handlerA = createTestHandler('A', ['source'])
      const handlerB = createTestHandler('B', ['source'], ['A'])
      const handlerC = createTestHandler('C', ['source'], ['A'])
      const handlerD = createTestHandler('D', ['source'], ['B', 'C'])

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)
      engine.registerMV(handlerC)
      engine.registerMV(handlerD)

      const result = await engine.refreshMV('A')

      // All should be refreshed exactly once
      expect(handlerA.refreshCount).toBe(1)
      expect(handlerB.refreshCount).toBe(1)
      expect(handlerC.refreshCount).toBe(1)
      expect(handlerD.refreshCount).toBe(1)

      // D should be refreshed after or at the same time as B and C
      // (when execution is fast, timestamps may be equal within same millisecond)
      expect(handlerD.lastRefreshTime).toBeGreaterThanOrEqual(handlerB.lastRefreshTime)
      expect(handlerD.lastRefreshTime).toBeGreaterThanOrEqual(handlerC.lastRefreshTime)

      // Verify ordering via the result array (more reliable than timestamps)
      const bIndex = result.refreshedMVs.indexOf('B')
      const cIndex = result.refreshedMVs.indexOf('C')
      const dIndex = result.refreshedMVs.indexOf('D')
      expect(dIndex).toBeGreaterThan(bIndex)
      expect(dIndex).toBeGreaterThan(cIndex)
    })

    test('should record refresh times for each MV', async () => {
      const handlerA = createTestHandler('A', ['source'], [], { delayMs: 10 })
      const handlerB = createTestHandler('B', ['source'], ['A'], { delayMs: 10 })

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)

      const result = await engine.refreshAll()

      expect(result.refreshTimes.has('A')).toBe(true)
      expect(result.refreshTimes.has('B')).toBe(true)
      expect(result.refreshTimes.get('A')!).toBeGreaterThanOrEqual(10)
      expect(result.refreshTimes.get('B')!).toBeGreaterThanOrEqual(10)
    })
  })

  describe('Error Handling', () => {
    test('should continue refreshing other MVs after failure', async () => {
      const handlerA = createTestHandler('A', ['source'])
      const handlerB = createTestHandler('B', ['source'], [], { shouldFail: true })
      const handlerC = createTestHandler('C', ['source'])

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)
      engine.registerMV(handlerC)

      const result = await engine.refreshAll()

      expect(result.refreshedMVs).toContain('A')
      expect(result.refreshedMVs).toContain('C')
      expect(result.refreshedMVs).not.toContain('B')
      expect(result.errors.has('B')).toBe(true)
    })

    test('should track errors for each failed MV', async () => {
      const handlerA = createTestHandler('A', ['source'], [], { shouldFail: true })

      engine.registerMV(handlerA)

      const result = await engine.refreshAll()

      expect(result.errors.size).toBe(1)
      expect(result.errors.get('A')?.message).toContain('Refresh failed')
    })
  })

  describe('Source Change Propagation', () => {
    test('should refresh affected MVs when source changes', async () => {
      const handlerOrders = createTestHandler('OrdersMV', ['orders'])
      const handlerTotals = createTestHandler('TotalsMV', ['orders'], ['OrdersMV'])
      const handlerProducts = createTestHandler('ProductsMV', ['products'])

      engine.registerMV(handlerOrders)
      engine.registerMV(handlerTotals)
      engine.registerMV(handlerProducts)

      const result = await engine.onSourceChange('orders')

      expect(result.refreshedMVs).toContain('OrdersMV')
      expect(result.refreshedMVs).toContain('TotalsMV')
      expect(result.refreshedMVs).not.toContain('ProductsMV')
    })

    test('should refresh MVs in correct order after source change', async () => {
      const handlerA = createTestHandler('A', ['source'])
      const handlerB = createTestHandler('B', ['source'], ['A'])
      const handlerC = createTestHandler('C', ['source'], ['B'])

      engine.registerMV(handlerA)
      engine.registerMV(handlerB)
      engine.registerMV(handlerC)

      const result = await engine.onSourceChange('source')

      const aIndex = result.refreshedMVs.indexOf('A')
      const bIndex = result.refreshedMVs.indexOf('B')
      const cIndex = result.refreshedMVs.indexOf('C')

      expect(aIndex).toBeLessThan(bIndex)
      expect(bIndex).toBeLessThan(cIndex)
    })
  })
})

// =============================================================================
// Test Suite: Complex Dependency Scenarios
// =============================================================================

describe('Complex Dependency Scenarios', () => {
  let engine: CascadingRefreshEngine

  beforeEach(() => {
    engine = createCascadingRefreshEngine()
  })

  describe('Deep Dependency Chains', () => {
    test('should handle chain of 10 MVs', async () => {
      const handlers: TestMVHandler[] = []

      for (let i = 0; i < 10; i++) {
        const deps = i > 0 ? [`MV${i - 1}`] : []
        const handler = createTestHandler(`MV${i}`, ['source'], deps)
        handlers.push(handler)
        engine.registerMV(handler)
      }

      const result = await engine.refreshMV('MV0')

      expect(result.refreshedMVs.length).toBe(10)

      // Verify order
      for (let i = 0; i < 9; i++) {
        const currentIndex = result.refreshedMVs.indexOf(`MV${i}`)
        const nextIndex = result.refreshedMVs.indexOf(`MV${i + 1}`)
        expect(currentIndex).toBeLessThan(nextIndex)
      }
    })

    test('should handle wide dependency tree', async () => {
      // One root with 10 direct children
      const root = createTestHandler('Root', ['source'])
      engine.registerMV(root)

      for (let i = 0; i < 10; i++) {
        const handler = createTestHandler(`Child${i}`, ['source'], ['Root'])
        engine.registerMV(handler)
      }

      const result = await engine.refreshMV('Root')

      expect(result.refreshedMVs.length).toBe(11)
      expect(result.refreshedMVs[0]).toBe('Root')
    })
  })

  describe('Multi-Level Diamond Dependencies', () => {
    test('should handle nested diamond pattern', async () => {
      //       A
      //      / \
      //     B   C
      //      \ /
      //       D
      //      / \
      //     E   F
      //      \ /
      //       G

      const handlers: Record<string, TestMVHandler> = {
        A: createTestHandler('A', ['source']),
        B: createTestHandler('B', ['source'], ['A']),
        C: createTestHandler('C', ['source'], ['A']),
        D: createTestHandler('D', ['source'], ['B', 'C']),
        E: createTestHandler('E', ['source'], ['D']),
        F: createTestHandler('F', ['source'], ['D']),
        G: createTestHandler('G', ['source'], ['E', 'F']),
      }

      for (const handler of Object.values(handlers)) {
        engine.registerMV(handler)
      }

      const result = await engine.refreshMV('A')

      expect(result.refreshedMVs.length).toBe(7)

      // Verify ordering constraints
      const indexOf = (name: string) => result.refreshedMVs.indexOf(name)

      // A before B, C
      expect(indexOf('A')).toBeLessThan(indexOf('B'))
      expect(indexOf('A')).toBeLessThan(indexOf('C'))

      // B, C before D
      expect(indexOf('B')).toBeLessThan(indexOf('D'))
      expect(indexOf('C')).toBeLessThan(indexOf('D'))

      // D before E, F
      expect(indexOf('D')).toBeLessThan(indexOf('E'))
      expect(indexOf('D')).toBeLessThan(indexOf('F'))

      // E, F before G
      expect(indexOf('E')).toBeLessThan(indexOf('G'))
      expect(indexOf('F')).toBeLessThan(indexOf('G'))
    })
  })

  describe('Mixed Source Dependencies', () => {
    test('should correctly propagate changes through mixed sources', async () => {
      // Orders -> OrderTotals
      // Products -> ProductStats
      // OrderTotals + ProductStats -> SalesReport

      const ordersHandler = createTestHandler('OrderTotals', ['orders'])
      const productsHandler = createTestHandler('ProductStats', ['products'])
      const salesHandler = createTestHandler('SalesReport', [], ['OrderTotals', 'ProductStats'])

      engine.registerMV(ordersHandler)
      engine.registerMV(productsHandler)
      engine.registerMV(salesHandler)

      // Change to orders should refresh OrderTotals and SalesReport
      const ordersResult = await engine.onSourceChange('orders')
      expect(ordersResult.refreshedMVs).toContain('OrderTotals')
      expect(ordersResult.refreshedMVs).toContain('SalesReport')
      expect(ordersResult.refreshedMVs).not.toContain('ProductStats')

      // Reset
      ordersHandler.refreshCount = 0
      productsHandler.refreshCount = 0
      salesHandler.refreshCount = 0

      // Change to products should refresh ProductStats and SalesReport
      const productsResult = await engine.onSourceChange('products')
      expect(productsResult.refreshedMVs).toContain('ProductStats')
      expect(productsResult.refreshedMVs).toContain('SalesReport')
      expect(productsResult.refreshedMVs).not.toContain('OrderTotals')
    })
  })
})

// =============================================================================
// Test Suite: Dependency Ordering Verification
// =============================================================================

describe('Dependency Ordering Verification', () => {
  let engine: CascadingRefreshEngine

  beforeEach(() => {
    engine = createCascadingRefreshEngine()
  })

  test('should ensure parent is refreshed before child using timestamps', async () => {
    const handlers: TestMVHandler[] = []

    for (let i = 0; i < 5; i++) {
      const deps = i > 0 ? [`MV${i - 1}`] : []
      const handler = createTestHandler(`MV${i}`, ['source'], deps, { delayMs: 5 })
      handlers.push(handler)
      engine.registerMV(handler)
    }

    await engine.refreshAll()

    // Verify each handler was refreshed after its dependency
    for (let i = 1; i < handlers.length; i++) {
      const current = handlers[i]!
      const parent = handlers[i - 1]!

      expect(current.lastRefreshTime).toBeGreaterThanOrEqual(parent.lastRefreshTime)
    }
  })

  test('should track refresh history correctly', async () => {
    const handlerA = createTestHandler('A', ['source'], [], { delayMs: 5 })
    const handlerB = createTestHandler('B', ['source'], ['A'], { delayMs: 5 })

    engine.registerMV(handlerA)
    engine.registerMV(handlerB)

    // Refresh multiple times
    await engine.refreshAll()
    await engine.refreshAll()
    await engine.refreshAll()

    expect(handlerA.refreshHistory.length).toBe(3)
    expect(handlerB.refreshHistory.length).toBe(3)

    // Each refresh of B should be after A
    for (let i = 0; i < 3; i++) {
      expect(handlerB.refreshHistory[i]).toBeGreaterThanOrEqual(handlerA.refreshHistory[i]!)
    }
  })
})

// =============================================================================
// Test Suite: Performance and Timing
// =============================================================================

describe('Cascading Refresh Performance', () => {
  test('should measure total refresh time for dependency chain', async () => {
    const engine = createCascadingRefreshEngine()

    // Create chain with known delays
    const handlers = [
      createTestHandler('A', ['source'], [], { delayMs: 10 }),
      createTestHandler('B', ['source'], ['A'], { delayMs: 10 }),
      createTestHandler('C', ['source'], ['B'], { delayMs: 10 }),
    ]

    for (const h of handlers) {
      engine.registerMV(h)
    }

    const result = await engine.refreshAll()

    // Total time should be at least 30ms (sequential refresh)
    expect(result.totalDurationMs).toBeGreaterThanOrEqual(30)

    console.log('\n--- Cascading Refresh Performance ---')
    console.log(`  Total duration: ${result.totalDurationMs}ms`)
    for (const [name, time] of result.refreshTimes) {
      console.log(`  ${name}: ${time}ms`)
    }
  })

  test('should benchmark dependency graph operations', () => {
    const graph = createDependencyGraph()

    // Create a moderately complex graph
    const numNodes = 50
    const startTime = performance.now()

    // Create nodes with random dependencies (DAG structure)
    for (let i = 0; i < numNodes; i++) {
      const deps: string[] = []
      // Each node depends on 0-3 earlier nodes
      const numDeps = Math.min(i, Math.floor(Math.random() * 4))
      for (let j = 0; j < numDeps; j++) {
        const depIndex = Math.floor(Math.random() * i)
        const depName = `MV${depIndex}`
        if (!deps.includes(depName)) {
          deps.push(depName)
        }
      }
      graph.addMV(`MV${i}`, ['source'], deps)
    }

    const createTime = performance.now() - startTime

    // Benchmark topological sort
    const sortStart = performance.now()
    const order = graph.getRefreshOrder()
    const sortTime = performance.now() - sortStart

    // Benchmark circular dependency detection
    const cycleStart = performance.now()
    graph.detectCircularDependencies()
    const cycleTime = performance.now() - cycleStart

    console.log('\n--- Dependency Graph Benchmark ---')
    console.log(`  Nodes: ${numNodes}`)
    console.log(`  Graph creation: ${createTime.toFixed(2)}ms`)
    console.log(`  Topological sort: ${sortTime.toFixed(2)}ms`)
    console.log(`  Cycle detection: ${cycleTime.toFixed(2)}ms`)
    console.log(`  Refresh order length: ${order.length}`)

    expect(order.length).toBe(numNodes)
  })
})

// =============================================================================
// Test Suite: Staleness Propagation
// =============================================================================

/**
 * Staleness state enum matching StalenessDetector output
 */
type StalenessState = 'fresh' | 'stale' | 'invalid'

/**
 * MV with staleness tracking for cascading tests
 */
interface StalenessAwareMV {
  name: string
  sourceNamespaces: string[]
  dependsOn: string[]
  state: StalenessState
  lastRefreshVersion: number
  definitionVersion: number
}

/**
 * Staleness manager that tracks MV states and propagates staleness
 */
interface StalenessManager {
  mvs: Map<string, StalenessAwareMV>
  versions: Map<string, number> // namespace -> version
  registerMV(mv: Omit<StalenessAwareMV, 'state' | 'lastRefreshVersion'>): void
  unregisterMV(name: string): void
  incrementVersion(namespace: string): void
  refreshMV(name: string): void
  getState(name: string): StalenessState
  getStaleDownstream(name: string): string[]
  markStale(name: string): void
  propagateStaleness(startingFrom: string): string[]
  checkStaleness(name: string): StalenessState
}

function createStalenessManager(): StalenessManager {
  const mvs = new Map<string, StalenessAwareMV>()
  const versions = new Map<string, number>()

  return {
    mvs,
    versions,

    registerMV(mv: Omit<StalenessAwareMV, 'state' | 'lastRefreshVersion'>): void {
      mvs.set(mv.name, {
        ...mv,
        state: 'stale', // New MVs start stale (need initial refresh)
        lastRefreshVersion: 0,
      })

      // Initialize source versions if not present
      for (const ns of mv.sourceNamespaces) {
        if (!versions.has(ns)) {
          versions.set(ns, 1)
        }
      }

      // Initialize MV-as-source versions
      versions.set(mv.name, 1)
    },

    unregisterMV(name: string): void {
      mvs.delete(name)
    },

    incrementVersion(namespace: string): void {
      const current = versions.get(namespace) ?? 0
      versions.set(namespace, current + 1)
    },

    refreshMV(name: string): void {
      const mv = mvs.get(name)
      if (!mv) return

      // Capture current versions of all dependencies
      let maxVersion = 0
      for (const ns of mv.sourceNamespaces) {
        maxVersion = Math.max(maxVersion, versions.get(ns) ?? 0)
      }
      for (const dep of mv.dependsOn) {
        maxVersion = Math.max(maxVersion, versions.get(dep) ?? 0)
      }

      mv.lastRefreshVersion = maxVersion
      mv.state = 'fresh'

      // Increment this MV's version (it has new data)
      this.incrementVersion(name)
    },

    getState(name: string): StalenessState {
      const mv = mvs.get(name)
      if (!mv) return 'invalid'
      return mv.state
    },

    getStaleDownstream(name: string): string[] {
      const stale: string[] = []

      for (const [mvName, mv] of mvs) {
        if (mv.dependsOn.includes(name) && mv.state === 'stale') {
          stale.push(mvName)
        }
      }

      return stale
    },

    markStale(name: string): void {
      const mv = mvs.get(name)
      if (mv) {
        mv.state = 'stale'
      }
    },

    propagateStaleness(startingFrom: string): string[] {
      const marked: string[] = []
      const queue = [startingFrom]
      const visited = new Set<string>()

      while (queue.length > 0) {
        const current = queue.shift()!
        if (visited.has(current)) continue
        visited.add(current)

        // Find all MVs that depend on current
        for (const [mvName, mv] of mvs) {
          if (mv.dependsOn.includes(current) && mv.state !== 'stale') {
            mv.state = 'stale'
            marked.push(mvName)
            queue.push(mvName) // Propagate further
          }
        }
      }

      return marked
    },

    checkStaleness(name: string): StalenessState {
      const mv = mvs.get(name)
      if (!mv) return 'invalid'

      // Check if definition changed
      if (mv.definitionVersion !== mv.definitionVersion) {
        return 'invalid'
      }

      // Check if any source namespace has newer version
      for (const ns of mv.sourceNamespaces) {
        const currentVersion = versions.get(ns) ?? 0
        if (currentVersion > mv.lastRefreshVersion) {
          return 'stale'
        }
      }

      // Check if any dependent MV has newer version
      for (const dep of mv.dependsOn) {
        const currentVersion = versions.get(dep) ?? 0
        if (currentVersion > mv.lastRefreshVersion) {
          return 'stale'
        }
      }

      return 'fresh'
    },
  }
}

describe('MV Staleness Propagation', () => {
  let manager: StalenessManager

  beforeEach(() => {
    manager = createStalenessManager()
  })

  describe('Basic Staleness Detection', () => {
    test('new MVs should start as stale', () => {
      manager.registerMV({
        name: 'OrderTotals',
        sourceNamespaces: ['orders'],
        dependsOn: [],
        definitionVersion: 1,
      })

      expect(manager.getState('OrderTotals')).toBe('stale')
    })

    test('MV becomes fresh after refresh', () => {
      manager.registerMV({
        name: 'OrderTotals',
        sourceNamespaces: ['orders'],
        dependsOn: [],
        definitionVersion: 1,
      })

      manager.refreshMV('OrderTotals')

      expect(manager.getState('OrderTotals')).toBe('fresh')
    })

    test('MV becomes stale when source changes', () => {
      manager.registerMV({
        name: 'OrderTotals',
        sourceNamespaces: ['orders'],
        dependsOn: [],
        definitionVersion: 1,
      })

      manager.refreshMV('OrderTotals')
      expect(manager.getState('OrderTotals')).toBe('fresh')

      manager.incrementVersion('orders')
      manager.markStale('OrderTotals')

      expect(manager.getState('OrderTotals')).toBe('stale')
    })
  })

  describe('Cascading Staleness: MV-A -> MV-B', () => {
    test('when MV-A updates, dependent MV-B is marked stale', () => {
      // Setup: MV-A depends on orders, MV-B depends on MV-A
      manager.registerMV({
        name: 'MV-A',
        sourceNamespaces: ['orders'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'MV-B',
        sourceNamespaces: [],
        dependsOn: ['MV-A'],
        definitionVersion: 1,
      })

      // Initial refresh both
      manager.refreshMV('MV-A')
      manager.refreshMV('MV-B')
      expect(manager.getState('MV-A')).toBe('fresh')
      expect(manager.getState('MV-B')).toBe('fresh')

      // Source changes - refresh MV-A
      manager.incrementVersion('orders')
      manager.markStale('MV-A')
      manager.refreshMV('MV-A')

      // MV-A is now fresh, but MV-B should be marked stale
      expect(manager.getState('MV-A')).toBe('fresh')

      // Propagate staleness from MV-A
      const staleDownstream = manager.propagateStaleness('MV-A')
      expect(staleDownstream).toContain('MV-B')
      expect(manager.getState('MV-B')).toBe('stale')
    })

    test('refreshing MV-A triggers staleness check in MV-B', () => {
      manager.registerMV({
        name: 'MV-A',
        sourceNamespaces: ['orders'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'MV-B',
        sourceNamespaces: [],
        dependsOn: ['MV-A'],
        definitionVersion: 1,
      })

      // Initial refresh
      manager.refreshMV('MV-A')
      manager.refreshMV('MV-B')

      // Refresh MV-A (new data)
      manager.incrementVersion('orders')
      manager.markStale('MV-A')
      manager.refreshMV('MV-A')

      // Check staleness of MV-B based on version tracking
      const staleness = manager.checkStaleness('MV-B')
      expect(staleness).toBe('stale')
    })
  })

  describe('Cascading Staleness: Multi-level (A -> B -> C)', () => {
    test('multi-level staleness propagation works correctly', () => {
      manager.registerMV({
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'B',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'C',
        sourceNamespaces: [],
        dependsOn: ['B'],
        definitionVersion: 1,
      })

      // Initial refresh all
      manager.refreshMV('A')
      manager.refreshMV('B')
      manager.refreshMV('C')
      expect(manager.getState('A')).toBe('fresh')
      expect(manager.getState('B')).toBe('fresh')
      expect(manager.getState('C')).toBe('fresh')

      // Source changes
      manager.incrementVersion('source')
      manager.markStale('A')

      // Propagate staleness from A
      const marked = manager.propagateStaleness('A')

      // Both B and C should be marked stale
      expect(marked).toContain('B')
      expect(marked).toContain('C')
      expect(manager.getState('A')).toBe('stale')
      expect(manager.getState('B')).toBe('stale')
      expect(manager.getState('C')).toBe('stale')
    })

    test('cascading refresh order is correct (source before dependent)', () => {
      const refreshOrder: string[] = []

      manager.registerMV({
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'B',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'C',
        sourceNamespaces: [],
        dependsOn: ['B'],
        definitionVersion: 1,
      })

      // Track refresh order
      const orderedRefresh = (name: string) => {
        refreshOrder.push(name)
        manager.refreshMV(name)
      }

      // Cascade refresh in correct order: A -> B -> C
      orderedRefresh('A')
      orderedRefresh('B')
      orderedRefresh('C')

      expect(refreshOrder).toEqual(['A', 'B', 'C'])

      // Verify all are fresh
      expect(manager.getState('A')).toBe('fresh')
      expect(manager.getState('B')).toBe('fresh')
      expect(manager.getState('C')).toBe('fresh')
    })

    test('refreshing in wrong order leaves downstream stale', () => {
      manager.registerMV({
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'B',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'C',
        sourceNamespaces: [],
        dependsOn: ['B'],
        definitionVersion: 1,
      })

      // Initial refresh
      manager.refreshMV('A')
      manager.refreshMV('B')
      manager.refreshMV('C')

      // Source changes
      manager.incrementVersion('source')
      manager.markStale('A')
      manager.propagateStaleness('A')

      // Refresh C first (wrong order) - it will capture stale B's version
      manager.refreshMV('C')

      // Then refresh B
      manager.refreshMV('B')

      // Finally refresh A
      manager.refreshMV('A')

      // C was refreshed before B and A had new data, so C is stale
      expect(manager.checkStaleness('C')).toBe('stale')
    })
  })

  describe('Diamond Dependency Staleness (A -> B, A -> C, B -> D, C -> D)', () => {
    test('staleness propagates through diamond pattern', () => {
      manager.registerMV({
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'B',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'C',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'D',
        sourceNamespaces: [],
        dependsOn: ['B', 'C'],
        definitionVersion: 1,
      })

      // Initial refresh in order
      manager.refreshMV('A')
      manager.refreshMV('B')
      manager.refreshMV('C')
      manager.refreshMV('D')

      // All fresh
      expect(manager.getState('A')).toBe('fresh')
      expect(manager.getState('B')).toBe('fresh')
      expect(manager.getState('C')).toBe('fresh')
      expect(manager.getState('D')).toBe('fresh')

      // Source changes
      manager.incrementVersion('source')
      manager.markStale('A')
      const marked = manager.propagateStaleness('A')

      // B, C, and D should all be stale
      expect(marked).toContain('B')
      expect(marked).toContain('C')
      expect(marked).toContain('D')
    })

    test('D only becomes fresh when both B and C are fresh', () => {
      manager.registerMV({
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'B',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'C',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'D',
        sourceNamespaces: [],
        dependsOn: ['B', 'C'],
        definitionVersion: 1,
      })

      // Initial refresh
      manager.refreshMV('A')
      manager.refreshMV('B')
      manager.refreshMV('C')
      manager.refreshMV('D')

      // Source changes
      manager.incrementVersion('source')
      manager.markStale('A')
      manager.propagateStaleness('A')

      // Refresh A and B only
      manager.refreshMV('A')
      manager.refreshMV('B')

      // D is still stale because C is stale
      expect(manager.checkStaleness('D')).toBe('stale')

      // Now refresh C
      manager.refreshMV('C')

      // D can now be refreshed and become fresh
      manager.refreshMV('D')
      expect(manager.getState('D')).toBe('fresh')
      expect(manager.checkStaleness('D')).toBe('fresh')
    })
  })

  describe('Partial Chain Staleness', () => {
    test('only downstream MVs are marked stale, not upstream', () => {
      manager.registerMV({
        name: 'A',
        sourceNamespaces: ['source'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'B',
        sourceNamespaces: [],
        dependsOn: ['A'],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'C',
        sourceNamespaces: [],
        dependsOn: ['B'],
        definitionVersion: 1,
      })

      // Initial refresh
      manager.refreshMV('A')
      manager.refreshMV('B')
      manager.refreshMV('C')

      // Mark B as stale (not from source change)
      manager.markStale('B')
      const marked = manager.propagateStaleness('B')

      // Only C should be marked, A should remain fresh
      expect(marked).toContain('C')
      expect(marked).not.toContain('A')
      expect(manager.getState('A')).toBe('fresh')
      expect(manager.getState('B')).toBe('stale')
      expect(manager.getState('C')).toBe('stale')
    })
  })

  describe('Staleness with Multiple Sources', () => {
    test('MV becomes stale when any source changes', () => {
      manager.registerMV({
        name: 'JoinedView',
        sourceNamespaces: ['orders', 'products'],
        dependsOn: [],
        definitionVersion: 1,
      })

      manager.refreshMV('JoinedView')
      expect(manager.getState('JoinedView')).toBe('fresh')

      // Only orders changes
      manager.incrementVersion('orders')
      manager.markStale('JoinedView')

      expect(manager.getState('JoinedView')).toBe('stale')
    })

    test('staleness propagates correctly with mixed source and MV dependencies', () => {
      manager.registerMV({
        name: 'OrdersMV',
        sourceNamespaces: ['orders'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'ProductsMV',
        sourceNamespaces: ['products'],
        dependsOn: [],
        definitionVersion: 1,
      })
      manager.registerMV({
        name: 'SalesReport',
        sourceNamespaces: [],
        dependsOn: ['OrdersMV', 'ProductsMV'],
        definitionVersion: 1,
      })

      // Initial refresh
      manager.refreshMV('OrdersMV')
      manager.refreshMV('ProductsMV')
      manager.refreshMV('SalesReport')

      // Only orders changes
      manager.incrementVersion('orders')
      manager.markStale('OrdersMV')
      manager.refreshMV('OrdersMV')

      // Propagate staleness
      const marked = manager.propagateStaleness('OrdersMV')

      // SalesReport should be stale (depends on OrdersMV)
      expect(marked).toContain('SalesReport')
      expect(manager.getState('SalesReport')).toBe('stale')

      // ProductsMV should still be fresh
      expect(manager.getState('ProductsMV')).toBe('fresh')
    })
  })
})
