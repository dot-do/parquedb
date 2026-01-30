/**
 * Query Benchmarks for ParqueDB
 *
 * Measures performance of query operations:
 * - Full scan vs indexed lookup
 * - Cursor pagination vs offset pagination
 * - Sort performance (single vs multi-field)
 * - Aggregation pipeline stages
 */

import { describe, bench, beforeAll, beforeEach } from 'vitest'
import { Collection, type AggregationStage } from '../../src/Collection'
import type { Entity, EntityId, Filter } from '../../src/types'
import {
  generateTestData,
  randomElement,
  randomInt,
} from './setup'

// =============================================================================
// Test Types
// =============================================================================

interface Product {
  sku: string
  price: number
  category: string
  stock: number
  active: boolean
  rating: number
  reviewCount: number
}

interface Order {
  orderNumber: string
  status: string
  total: number
  itemCount: number
  createdAt: Date
}

// =============================================================================
// Query Benchmarks
// =============================================================================

describe('Query Benchmarks', () => {
  // ===========================================================================
  // Full Scan vs Indexed Lookup
  // ===========================================================================

  describe('Full Scan vs Indexed Lookup', () => {
    let products: Collection<Product>
    let productNamespace: string
    let productIds: string[] = []
    let skus: string[] = []

    beforeAll(async () => {
      productNamespace = `products-${Date.now()}`
      products = new Collection<Product>(productNamespace)

      // Seed 5000 products
      const testData = generateTestData(5000, 'Product')
      for (const data of testData) {
        const entity = await products.create(
          data as unknown as Partial<Product> & { $type: string; name: string }
        )
        productIds.push(entity.$id as string)
        skus.push((data as any).sku)
      }
    })

    bench('full scan - find all', async () => {
      await products.find()
    })

    bench('full scan - find with unindexed field', async () => {
      // Rating is not indexed
      await products.find({ rating: { $gte: 4.0 } })
    })

    bench('indexed lookup - find by unique indexed field (sku)', async () => {
      const sku = randomElement(skus)
      await products.find({ sku })
    })

    bench('indexed lookup - find by indexed field (category)', async () => {
      await products.find({ category: 'tech' })
    })

    bench('indexed lookup - find by indexed field (active)', async () => {
      await products.find({ active: true })
    })

    bench('indexed + filter - indexed field + unindexed filter', async () => {
      await products.find({
        category: 'tech',
        rating: { $gte: 4.0 },
      })
    })

    bench('compound filter - multiple indexed fields', async () => {
      await products.find({
        active: true,
        category: 'science',
      })
    })

    bench('range scan - indexed field with range', async () => {
      await products.find({
        price: { $gte: 50, $lte: 100 },
      })
    })

    bench('get by ID vs find by unique field', async () => {
      const id = randomElement(productIds)
      await products.get(id)
    })
  })

  // ===========================================================================
  // Pagination Benchmarks
  // ===========================================================================

  describe('Pagination Performance', () => {
    let orders: Collection<Order>
    let orderNamespace: string
    let totalOrders: number

    beforeAll(async () => {
      orderNamespace = `orders-${Date.now()}`
      orders = new Collection<Order>(orderNamespace)

      // Seed 10000 orders for pagination tests
      const testData = generateTestData(10000, 'Order')
      for (const data of testData) {
        await orders.create(
          data as unknown as Partial<Order> & { $type: string; name: string }
        )
      }
      totalOrders = 10000
    })

    bench('offset pagination - page 1 (limit 20)', async () => {
      await orders.find({}, { limit: 20, skip: 0 })
    })

    bench('offset pagination - page 10 (limit 20)', async () => {
      await orders.find({}, { limit: 20, skip: 180 })
    })

    bench('offset pagination - page 100 (limit 20)', async () => {
      await orders.find({}, { limit: 20, skip: 1980 })
    })

    bench('offset pagination - deep page 500 (limit 20)', async () => {
      await orders.find({}, { limit: 20, skip: 9980 })
    })

    bench('cursor pagination - first page', async () => {
      const result = await orders.find({}, { limit: 20 })
      // Result should include cursor for next page
    })

    bench('cursor pagination - with cursor (simulated)', async () => {
      // Get first page to get a cursor
      const first = await orders.find({}, { limit: 20, sort: { orderNumber: 1 } })
      if (first.length > 0) {
        const lastItem = first[first.length - 1]
        // Use the last orderNumber as cursor point
        await orders.find(
          { orderNumber: { $gt: lastItem.orderNumber } },
          { limit: 20, sort: { orderNumber: 1 } }
        )
      }
    })

    bench('pagination with filter - offset', async () => {
      await orders.find(
        { status: 'delivered' },
        { limit: 20, skip: 100 }
      )
    })

    bench('pagination with filter + sort', async () => {
      await orders.find(
        { status: 'delivered' },
        { limit: 20, sort: { total: -1 } }
      )
    })

    bench('large page size (100)', async () => {
      await orders.find({}, { limit: 100 })
    })

    bench('large page size (500)', async () => {
      await orders.find({}, { limit: 500 })
    })

    bench('large page size (1000)', async () => {
      await orders.find({}, { limit: 1000 })
    })
  })

  // ===========================================================================
  // Sort Performance
  // ===========================================================================

  describe('Sort Performance', () => {
    let sortProducts: Collection<Product>
    let sortNamespace: string

    beforeAll(async () => {
      sortNamespace = `sort-products-${Date.now()}`
      sortProducts = new Collection<Product>(sortNamespace)

      // Seed 3000 products
      const testData = generateTestData(3000, 'Product')
      for (const data of testData) {
        await sortProducts.create(
          data as unknown as Partial<Product> & { $type: string; name: string }
        )
      }
    })

    bench('no sort', async () => {
      await sortProducts.find({}, { limit: 100 })
    })

    bench('sort by single field (numeric) - ascending', async () => {
      await sortProducts.find({}, { sort: { price: 1 }, limit: 100 })
    })

    bench('sort by single field (numeric) - descending', async () => {
      await sortProducts.find({}, { sort: { price: -1 }, limit: 100 })
    })

    bench('sort by single field (string) - ascending', async () => {
      await sortProducts.find({}, { sort: { category: 1 }, limit: 100 })
    })

    bench('sort by single field (string) - descending', async () => {
      await sortProducts.find({}, { sort: { category: -1 }, limit: 100 })
    })

    bench('sort by indexed field', async () => {
      await sortProducts.find({}, { sort: { sku: 1 }, limit: 100 })
    })

    bench('sort by two fields', async () => {
      await sortProducts.find({}, { sort: { category: 1, price: -1 }, limit: 100 })
    })

    bench('sort by three fields', async () => {
      await sortProducts.find({}, { sort: { category: 1, active: -1, price: 1 }, limit: 100 })
    })

    bench('sort with filter', async () => {
      await sortProducts.find(
        { active: true },
        { sort: { price: -1 }, limit: 100 }
      )
    })

    bench('sort with complex filter', async () => {
      await sortProducts.find(
        {
          $and: [
            { active: true },
            { price: { $gte: 50, $lte: 500 } },
          ],
        },
        { sort: { rating: -1, price: 1 }, limit: 100 }
      )
    })

    bench('sort all results (no limit)', async () => {
      await sortProducts.find({}, { sort: { price: -1 } })
    })

    bench('sort + skip + limit (pagination)', async () => {
      await sortProducts.find({}, { sort: { price: -1 }, skip: 500, limit: 100 })
    })
  })

  // ===========================================================================
  // Aggregation Pipeline Benchmarks
  // ===========================================================================

  describe('Aggregation Pipeline', () => {
    let aggProducts: Collection<Product>
    let aggNamespace: string

    beforeAll(async () => {
      aggNamespace = `agg-products-${Date.now()}`
      aggProducts = new Collection<Product>(aggNamespace)

      // Seed 2000 products
      const testData = generateTestData(2000, 'Product')
      for (const data of testData) {
        await aggProducts.create(
          data as unknown as Partial<Product> & { $type: string; name: string }
        )
      }
    })

    bench('aggregate - single $match stage', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
      ])
    })

    bench('aggregate - $match + $sort', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
        { $sort: { price: -1 } },
      ])
    })

    bench('aggregate - $match + $sort + $limit', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
        { $sort: { price: -1 } },
        { $limit: 50 },
      ])
    })

    bench('aggregate - $match + $project', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
        { $project: { sku: 1, price: 1, category: 1 } },
      ])
    })

    bench('aggregate - $group by category', async () => {
      await aggProducts.aggregate([
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
            avgPrice: { $avg: '$price' },
            totalStock: { $sum: '$stock' },
          },
        },
      ])
    })

    bench('aggregate - $group + $sort', async () => {
      await aggProducts.aggregate([
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
            avgPrice: { $avg: '$price' },
          },
        },
        { $sort: { count: -1 } },
      ])
    })

    bench('aggregate - $match + $group', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
            avgRating: { $avg: '$rating' },
          },
        },
      ])
    })

    bench('aggregate - $count stage', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
        { $count: 'totalActive' },
      ])
    })

    bench('aggregate - $skip + $limit (pagination)', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
        { $sort: { price: -1 } },
        { $skip: 100 },
        { $limit: 50 },
      ])
    })

    bench('aggregate - $addFields', async () => {
      await aggProducts.aggregate([
        { $match: { active: true } },
        {
          $addFields: {
            discountedPrice: { $multiply: ['$price', 0.9] },
            isExpensive: { $gt: ['$price', 100] },
          },
        },
        { $limit: 100 },
      ])
    })

    bench('aggregate - full pipeline (complex)', async () => {
      await aggProducts.aggregate([
        { $match: { active: true, price: { $gte: 10 } } },
        {
          $addFields: {
            priceCategory: {
              $cond: {
                if: { $gt: ['$price', 100] },
                then: 'premium',
                else: 'standard',
              },
            },
          },
        },
        {
          $group: {
            _id: { category: '$category', priceCategory: '$priceCategory' },
            count: { $sum: 1 },
            avgPrice: { $avg: '$price' },
            avgRating: { $avg: '$rating' },
          },
        },
        { $sort: { avgPrice: -1 } },
        { $limit: 20 },
      ])
    })

    bench('aggregate - $unwind array field', async () => {
      // First need to add products with array field
      await aggProducts.aggregate([
        { $match: { active: true } },
        { $limit: 100 },
        // Note: This assumes there's an array field to unwind
        // In actual usage, this would unwind a real array field
      ])
    })
  })

  // ===========================================================================
  // Distinct and Special Queries
  // ===========================================================================

  describe('Distinct and Special Queries', () => {
    let specialProducts: Collection<Product>
    let specialNamespace: string

    beforeAll(async () => {
      specialNamespace = `special-products-${Date.now()}`
      specialProducts = new Collection<Product>(specialNamespace)

      // Seed 2000 products
      const testData = generateTestData(2000, 'Product')
      for (const data of testData) {
        await specialProducts.create(
          data as unknown as Partial<Product> & { $type: string; name: string }
        )
      }
    })

    bench('distinct values - category (via aggregation)', async () => {
      await specialProducts.aggregate([
        { $group: { _id: '$category' } },
      ])
    })

    bench('distinct values - with filter (via aggregation)', async () => {
      await specialProducts.aggregate([
        { $match: { active: true } },
        { $group: { _id: '$category' } },
      ])
    })

    bench('exists check - field exists', async () => {
      await specialProducts.find({ rating: { $exists: true } }, { limit: 100 })
    })

    bench('exists check - field does not exist', async () => {
      await specialProducts.find({ nonExistentField: { $exists: false } }, { limit: 100 })
    })

    bench('type check - $type operator', async () => {
      await specialProducts.find({ price: { $type: 'number' } }, { limit: 100 })
    })

    bench('null check - field is null', async () => {
      await specialProducts.find({ rating: null }, { limit: 100 })
    })

    bench('$nin - not in array', async () => {
      await specialProducts.find({
        category: { $nin: ['tech', 'science'] },
      }, { limit: 100 })
    })

    bench('$all - array contains all', async () => {
      // This would work on products with array fields
      await specialProducts.find({
        tags: { $all: ['featured', 'trending'] },
      }, { limit: 100 })
    })

    bench('$size - array length', async () => {
      await specialProducts.find({
        tags: { $size: 3 },
      }, { limit: 100 })
    })

    bench('text search simulation (regex)', async () => {
      await specialProducts.find({
        name: { $regex: 'product', $options: 'i' },
      }, { limit: 100 })
    })
  })

  // ===========================================================================
  // Query Optimization Comparisons
  // ===========================================================================

  describe('Query Optimization Comparisons', () => {
    let optProducts: Collection<Product>
    let optNamespace: string

    beforeAll(async () => {
      optNamespace = `opt-products-${Date.now()}`
      optProducts = new Collection<Product>(optNamespace)

      // Seed 5000 products
      const testData = generateTestData(5000, 'Product')
      for (const data of testData) {
        await optProducts.create(
          data as unknown as Partial<Product> & { $type: string; name: string }
        )
      }
    })

    bench('query: selective filter first', async () => {
      // More selective filter (status) first, then less selective
      await optProducts.find({
        $and: [
          { category: 'tech' }, // More selective
          { active: true },      // Less selective
        ],
      }, { limit: 100 })
    })

    bench('query: less selective filter first', async () => {
      await optProducts.find({
        $and: [
          { active: true },      // Less selective
          { category: 'tech' }, // More selective
        ],
      }, { limit: 100 })
    })

    bench('query: $or with 2 conditions', async () => {
      await optProducts.find({
        $or: [
          { category: 'tech' },
          { category: 'science' },
        ],
      }, { limit: 100 })
    })

    bench('query: equivalent $in', async () => {
      await optProducts.find({
        category: { $in: ['tech', 'science'] },
      }, { limit: 100 })
    })

    bench('query: nested $and in $or', async () => {
      await optProducts.find({
        $or: [
          { $and: [{ category: 'tech' }, { price: { $gt: 100 } }] },
          { $and: [{ category: 'science' }, { rating: { $gte: 4 } }] },
        ],
      }, { limit: 100 })
    })

    bench('query: projection reduces data size', async () => {
      await optProducts.find(
        { active: true },
        { project: { sku: 1, price: 1 }, limit: 100 }
      )
    })

    bench('query: no projection (all fields)', async () => {
      await optProducts.find(
        { active: true },
        { limit: 100 }
      )
    })

    bench('query: early limit (top N)', async () => {
      await optProducts.find(
        { active: true },
        { sort: { price: -1 }, limit: 10 }
      )
    })

    bench('query: late limit (sort all then limit)', async () => {
      // This simulates sorting all then taking top 10
      const all = await optProducts.find({ active: true }, { sort: { price: -1 } })
      // In practice, we'd just use limit, but this shows the difference
    })
  })
})
