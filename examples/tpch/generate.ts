/**
 * TPC-H Data Generator for ParqueDB
 *
 * Generates synthetic TPC-H benchmark data at various scale factors.
 * Based on the official TPC-H specification.
 */

import { DB, type DBInstance } from '../../src/db'
import { FsBackend } from '../../src/storage'
import { tpchSchema } from './schema'
import type {
  Region,
  Nation,
  Supplier,
  Part,
  PartSupp,
  Customer,
  Order,
  LineItem,
} from './schema'

import {
  TPCH_REGIONS,
  TPCH_NATIONS,
  TPCH_SEGMENTS,
  TPCH_PRIORITIES,
  TPCH_SHIP_MODES,
  TPCH_SHIP_INSTRUCTIONS,
  TPCH_CONTAINERS,
  TPCH_TYPE_SYLLABLES,
  TPCH_NAME_SYLLABLES,
  TPCH_DATE_RANGE,
} from './schema'

// =============================================================================
// Extended DB Interface
// =============================================================================

interface TPCHDatabase extends DBInstance {
  Region: { create: (data: Region) => Promise<Region & { $id: string }> }
  Nation: { create: (data: Nation) => Promise<Nation & { $id: string }> }
  Supplier: { create: (data: Supplier) => Promise<Supplier & { $id: string }> }
  Part: { create: (data: Part) => Promise<Part & { $id: string }> }
  PartSupp: { create: (data: PartSupp) => Promise<PartSupp & { $id: string }> }
  Customer: { create: (data: Customer) => Promise<Customer & { $id: string }> }
  Order: { create: (data: Order) => Promise<Order & { $id: string }> }
  LineItem: { create: (data: LineItem) => Promise<LineItem & { $id: string }> }
}

// =============================================================================
// Types
// =============================================================================

export interface GenerateTPCHOptions {
  /** Scale factor (0.01, 0.1, 1, 10) */
  scaleFactor?: number
  /** Output directory for generated data */
  outputDir?: string
  /** Batch size for bulk inserts */
  batchSize?: number
  /** Progress callback */
  onProgress?: (info: ProgressInfo) => void
  /** Seed for reproducible generation */
  seed?: number
}

export interface ProgressInfo {
  table: string
  current: number
  total: number
  percentage: number
}

export interface GenerateStats {
  scaleFactor: number
  tables: {
    region: number
    nation: number
    supplier: number
    part: number
    partsupp: number
    customer: number
    orders: number
    lineitem: number
  }
  durationMs: number
}

// =============================================================================
// Seeded Random Number Generator
// =============================================================================

/**
 * Simple seeded pseudo-random number generator (Mulberry32)
 */
class SeededRandom {
  private state: number

  constructor(seed: number) {
    this.state = seed
  }

  /** Returns a random number between 0 and 1 */
  random(): number {
    let t = (this.state += 0x6d2b79f5)
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }

  /** Returns a random integer between min and max (inclusive) */
  int(min: number, max: number): number {
    return Math.floor(this.random() * (max - min + 1)) + min
  }

  /** Returns a random float between min and max with given precision */
  float(min: number, max: number, precision: number = 2): number {
    const value = min + this.random() * (max - min)
    const multiplier = Math.pow(10, precision)
    return Math.round(value * multiplier) / multiplier
  }

  /** Returns a random element from an array */
  pick<T>(arr: readonly T[]): T {
    return arr[this.int(0, arr.length - 1)]
  }

  /** Returns a random string of digits */
  digits(length: number): string {
    let result = ''
    for (let i = 0; i < length; i++) {
      result += this.int(0, 9).toString()
    }
    return result
  }

  /** Returns a random alphanumeric string */
  alphanumeric(length: number): string {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    let result = ''
    for (let i = 0; i < length; i++) {
      result += chars[this.int(0, chars.length - 1)]
    }
    return result
  }
}

// =============================================================================
// Date Utilities
// =============================================================================

/**
 * Generate a random date within TPC-H range
 */
function randomDate(rng: SeededRandom): string {
  const start = new Date(TPCH_DATE_RANGE.startDate).getTime()
  const end = new Date(TPCH_DATE_RANGE.endDate).getTime()
  const date = new Date(start + rng.random() * (end - start))
  return date.toISOString().split('T')[0]
}

/**
 * Add days to a date string
 */
function addDays(dateStr: string, days: number): string {
  const date = new Date(dateStr)
  date.setDate(date.getDate() + days)
  return date.toISOString().split('T')[0]
}

// =============================================================================
// Comment Generation
// =============================================================================

const TEXT_POOL = [
  'the quick brown fox jumps over the lazy dog',
  'lorem ipsum dolor sit amet consectetur adipiscing elit',
  'regular packages shall wake silently',
  'furiously ironic ideas about the',
  'blithely bold excuses detect furiously regular packages',
  'carefully final deposits cajole quickly across the',
  'slyly express asymptotes may nag after the special',
  'pending deposits boost regular ideas blithely',
  'even foxes wake pending courts',
  'accounts sleep furiously final pinto beans',
]

function generateComment(rng: SeededRandom, minLength: number = 10, maxLength: number = 43): string {
  const length = rng.int(minLength, maxLength)
  let comment = ''
  while (comment.length < length) {
    comment += rng.pick(TEXT_POOL) + ' '
  }
  return comment.substring(0, length).trim()
}

// =============================================================================
// Region Generator
// =============================================================================

export function generateRegions(): Region[] {
  return TPCH_REGIONS.map(r => ({
    r_regionkey: r.key,
    r_name: r.name,
    r_comment: `${r.name} region comment`,
  }))
}

// =============================================================================
// Nation Generator
// =============================================================================

export function generateNations(rng: SeededRandom): Nation[] {
  return TPCH_NATIONS.map(n => ({
    n_nationkey: n.key,
    n_name: n.name,
    n_regionkey: n.regionkey,
    n_comment: generateComment(rng, 30, 114),
  }))
}

// =============================================================================
// Supplier Generator
// =============================================================================

export function generateSuppliers(scaleFactor: number, rng: SeededRandom): Supplier[] {
  const count = Math.floor(10_000 * scaleFactor)
  const suppliers: Supplier[] = []

  for (let i = 1; i <= count; i++) {
    const nationkey = rng.int(0, 24)
    suppliers.push({
      s_suppkey: i,
      s_name: `Supplier#${i.toString().padStart(9, '0')}`,
      s_address: `${rng.int(1, 999)} ${rng.pick(['Industrial', 'Commerce', 'Trade', 'Market', 'Supply'])} Ave`,
      s_nationkey: nationkey,
      s_phone: `${(nationkey + 10).toString()}-${rng.digits(3)}-${rng.digits(3)}-${rng.digits(4)}`,
      s_acctbal: rng.float(-999.99, 9999.99, 2),
      s_comment: generateComment(rng, 25, 100),
    })
  }

  return suppliers
}

// =============================================================================
// Part Generator
// =============================================================================

export function generateParts(scaleFactor: number, rng: SeededRandom): Part[] {
  const count = Math.floor(200_000 * scaleFactor)
  const parts: Part[] = []

  for (let i = 1; i <= count; i++) {
    // Generate part name from 5 random syllables
    const name = [
      rng.pick(TPCH_NAME_SYLLABLES),
      rng.pick(TPCH_NAME_SYLLABLES),
      rng.pick(TPCH_NAME_SYLLABLES),
      rng.pick(TPCH_NAME_SYLLABLES),
      rng.pick(TPCH_NAME_SYLLABLES),
    ].join(' ')

    // Manufacturer: Manufacturer#X where X is 1-5
    const mfgrNum = rng.int(1, 5)
    const mfgr = `Manufacturer#${mfgrNum}`

    // Brand: Brand#XY where X is mfgr number and Y is 1-5
    const brandNum = rng.int(1, 5)
    const brand = `Brand#${mfgrNum}${brandNum}`

    // Type: 3 syllables
    const type = `${rng.pick(TPCH_TYPE_SYLLABLES.syllable1)} ${rng.pick(TPCH_TYPE_SYLLABLES.syllable2)} ${rng.pick(TPCH_TYPE_SYLLABLES.syllable3)}`

    // Size: 1-50
    const size = rng.int(1, 50)

    // Container
    const container = rng.pick(TPCH_CONTAINERS)

    // Retail price: based on partkey (deterministic formula from TPC-H spec)
    const retailprice = Math.round((90000 + ((i / 10) % 20001) + 100 * (i % 1000)) / 100 * 100) / 100

    parts.push({
      p_partkey: i,
      p_name: name,
      p_mfgr: mfgr,
      p_brand: brand,
      p_type: type,
      p_size: size,
      p_container: container,
      p_retailprice: retailprice,
      p_comment: generateComment(rng, 5, 22),
    })
  }

  return parts
}

// =============================================================================
// PartSupp Generator
// =============================================================================

export function generatePartSupps(
  scaleFactor: number,
  partCount: number,
  supplierCount: number,
  rng: SeededRandom
): PartSupp[] {
  const partsupps: PartSupp[] = []

  // Each part has 4 suppliers
  for (let partkey = 1; partkey <= partCount; partkey++) {
    for (let j = 0; j < 4; j++) {
      const suppkey = ((partkey + (j * ((supplierCount / 4) + ((partkey - 1) / supplierCount)))) % supplierCount) + 1

      partsupps.push({
        ps_partkey: partkey,
        ps_suppkey: suppkey,
        ps_availqty: rng.int(1, 9999),
        ps_supplycost: rng.float(1.0, 1000.0, 2),
        ps_comment: generateComment(rng, 49, 198),
      })
    }
  }

  return partsupps
}

// =============================================================================
// Customer Generator
// =============================================================================

export function generateCustomers(scaleFactor: number, rng: SeededRandom): Customer[] {
  const count = Math.floor(150_000 * scaleFactor)
  const customers: Customer[] = []

  for (let i = 1; i <= count; i++) {
    const nationkey = rng.int(0, 24)
    customers.push({
      c_custkey: i,
      c_name: `Customer#${i.toString().padStart(9, '0')}`,
      c_address: `${rng.int(1, 999)} ${rng.pick(['Main', 'Oak', 'Pine', 'Elm', 'Maple', 'Commerce'])} Street`,
      c_nationkey: nationkey,
      c_phone: `${(nationkey + 10).toString()}-${rng.digits(3)}-${rng.digits(3)}-${rng.digits(4)}`,
      c_acctbal: rng.float(-999.99, 9999.99, 2),
      c_mktsegment: rng.pick(TPCH_SEGMENTS),
      c_comment: generateComment(rng, 29, 116),
    })
  }

  return customers
}

// =============================================================================
// Orders Generator
// =============================================================================

export function* generateOrders(
  scaleFactor: number,
  customerCount: number,
  rng: SeededRandom
): Generator<Order[]> {
  // TPC-H uses sparse order keys (only 4 out of every 8 keys are used)
  // Total orders = 1.5M * SF, but keys go up to 6M * SF
  const targetOrders = Math.floor(1_500_000 * scaleFactor)
  const batchSize = 10_000

  let orderCount = 0
  let batch: Order[] = []
  let orderkey = 0

  while (orderCount < targetOrders) {
    orderkey++

    // Skip every other set of 4 (sparse key generation)
    if ((orderkey - 1) % 8 >= 4) continue

    const custkey = rng.int(1, customerCount)
    const orderdate = randomDate(rng)

    // Order status: F if date < 1995-06-17, O otherwise (simplified)
    const orderstatus = orderdate < '1995-06-17' ? 'F' : 'O'

    batch.push({
      o_orderkey: orderkey,
      o_custkey: custkey,
      o_orderstatus: orderstatus,
      o_totalprice: 0,  // Will be calculated from lineitems
      o_orderdate: orderdate,
      o_orderpriority: rng.pick(TPCH_PRIORITIES),
      o_clerk: `Clerk#${rng.int(1, Math.max(1, Math.floor(1000 * scaleFactor))).toString().padStart(9, '0')}`,
      o_shippriority: 0,
      o_comment: generateComment(rng, 19, 78),
    })

    orderCount++

    if (batch.length >= batchSize) {
      yield batch
      batch = []
    }
  }

  if (batch.length > 0) {
    yield batch
  }
}

// =============================================================================
// LineItem Generator
// =============================================================================

export function* generateLineItems(
  orders: Order[],
  partCount: number,
  supplierCount: number,
  rng: SeededRandom
): Generator<{ lineitems: LineItem[]; orderTotals: Map<number, number> }> {
  const batchSize = 50_000
  let batch: LineItem[] = []
  const orderTotals = new Map<number, number>()

  for (const order of orders) {
    // 1-7 line items per order
    const lineItemCount = rng.int(1, 7)

    for (let linenum = 1; linenum <= lineItemCount; linenum++) {
      const partkey = rng.int(1, partCount)

      // Supplier is based on partkey (each part has 4 suppliers)
      const suppkeyOffset = rng.int(0, 3)
      const suppkey = ((partkey + (suppkeyOffset * ((supplierCount / 4) + ((partkey - 1) / supplierCount)))) % supplierCount) + 1

      const quantity = rng.int(1, 50)
      const discount = rng.float(0.0, 0.10, 2)
      const tax = rng.float(0.0, 0.08, 2)

      // Extended price: based on part retail price formula
      const basePrice = Math.round((90000 + ((partkey / 10) % 20001) + 100 * (partkey % 1000)) / 100 * 100) / 100
      const extendedprice = Math.round(quantity * basePrice * 100) / 100

      // Ship date: order date + [1, 121] days
      const shipdate = addDays(order.o_orderdate, rng.int(1, 121))

      // Commit date: order date + [30, 90] days
      const commitdate = addDays(order.o_orderdate, rng.int(30, 90))

      // Receipt date: ship date + [1, 30] days
      const receiptdate = addDays(shipdate, rng.int(1, 30))

      // Return flag: based on receipt date
      let returnflag: string
      if (receiptdate <= order.o_orderdate) {
        returnflag = 'R'
      } else if (receiptdate > '1995-06-17') {
        returnflag = 'N'
      } else {
        returnflag = rng.random() < 0.5 ? 'A' : 'R'
      }

      // Line status: F if ship date < 1995-06-17, O otherwise
      const linestatus = shipdate < '1995-06-17' ? 'F' : 'O'

      batch.push({
        l_orderkey: order.o_orderkey,
        l_partkey: partkey,
        l_suppkey: suppkey,
        l_linenumber: linenum,
        l_quantity: quantity,
        l_extendedprice: extendedprice,
        l_discount: discount,
        l_tax: tax,
        l_returnflag: returnflag,
        l_linestatus: linestatus,
        l_shipdate: shipdate,
        l_commitdate: commitdate,
        l_receiptdate: receiptdate,
        l_shipinstruct: rng.pick(TPCH_SHIP_INSTRUCTIONS),
        l_shipmode: rng.pick(TPCH_SHIP_MODES),
        l_comment: generateComment(rng, 10, 43),
      })

      // Track order totals
      const currentTotal = orderTotals.get(order.o_orderkey) || 0
      orderTotals.set(order.o_orderkey, currentTotal + extendedprice)

      if (batch.length >= batchSize) {
        yield { lineitems: batch, orderTotals: new Map(orderTotals) }
        batch = []
        orderTotals.clear()
      }
    }
  }

  if (batch.length > 0) {
    yield { lineitems: batch, orderTotals }
  }
}

// =============================================================================
// Main Generator Function
// =============================================================================

/**
 * Generate TPC-H dataset and load into ParqueDB
 */
export async function generateTPCH(options: GenerateTPCHOptions = {}): Promise<GenerateStats> {
  const {
    scaleFactor = 0.01,
    outputDir = '.db/tpch',
    batchSize = 10_000,
    onProgress,
    seed = 42,
  } = options

  const startTime = Date.now()
  const rng = new SeededRandom(seed)

  // Create database with TPC-H schema
  const db = DB(tpchSchema, {
    storage: new FsBackend(outputDir),
  }) as TPCHDatabase

  const stats: GenerateStats = {
    scaleFactor,
    tables: {
      region: 0,
      nation: 0,
      supplier: 0,
      part: 0,
      partsupp: 0,
      customer: 0,
      orders: 0,
      lineitem: 0,
    },
    durationMs: 0,
  }

  try {
    // 1. Generate and load REGION (5 rows, fixed)
    console.log('Generating REGION table...')
    const regions = generateRegions()
    stats.tables.region = regions.length
    for (const region of regions) {
      await db.Region.create(region)
    }
    console.log(`  Loaded ${regions.length} regions`)

    // 2. Generate and load NATION (25 rows, fixed)
    console.log('Generating NATION table...')
    const nations = generateNations(rng)
    stats.tables.nation = nations.length
    for (const nation of nations) {
      await db.Nation.create(nation)
    }
    console.log(`  Loaded ${nations.length} nations`)

    // 3. Generate and load SUPPLIER
    console.log('Generating SUPPLIER table...')
    const suppliers = generateSuppliers(scaleFactor, rng)
    stats.tables.supplier = suppliers.length
    for (let i = 0; i < suppliers.length; i += batchSize) {
      const batch = suppliers.slice(i, i + batchSize)
      for (const supplier of batch) {
        await db.Supplier.create(supplier)
      }
      onProgress?.({
        table: 'SUPPLIER',
        current: Math.min(i + batchSize, suppliers.length),
        total: suppliers.length,
        percentage: Math.min(100, Math.round(((i + batchSize) / suppliers.length) * 100)),
      })
    }
    console.log(`  Loaded ${suppliers.length} suppliers`)

    // 4. Generate and load PART
    console.log('Generating PART table...')
    const parts = generateParts(scaleFactor, rng)
    stats.tables.part = parts.length
    for (let i = 0; i < parts.length; i += batchSize) {
      const batch = parts.slice(i, i + batchSize)
      for (const part of batch) {
        await db.Part.create(part)
      }
      onProgress?.({
        table: 'PART',
        current: Math.min(i + batchSize, parts.length),
        total: parts.length,
        percentage: Math.min(100, Math.round(((i + batchSize) / parts.length) * 100)),
      })
    }
    console.log(`  Loaded ${parts.length} parts`)

    // 5. Generate and load PARTSUPP
    console.log('Generating PARTSUPP table...')
    const partsupps = generatePartSupps(scaleFactor, parts.length, suppliers.length, rng)
    stats.tables.partsupp = partsupps.length
    for (let i = 0; i < partsupps.length; i += batchSize) {
      const batch = partsupps.slice(i, i + batchSize)
      for (const partsupp of batch) {
        await db.PartSupp.create(partsupp)
      }
      onProgress?.({
        table: 'PARTSUPP',
        current: Math.min(i + batchSize, partsupps.length),
        total: partsupps.length,
        percentage: Math.min(100, Math.round(((i + batchSize) / partsupps.length) * 100)),
      })
    }
    console.log(`  Loaded ${partsupps.length} part-supplier records`)

    // 6. Generate and load CUSTOMER
    console.log('Generating CUSTOMER table...')
    const customers = generateCustomers(scaleFactor, rng)
    stats.tables.customer = customers.length
    for (let i = 0; i < customers.length; i += batchSize) {
      const batch = customers.slice(i, i + batchSize)
      for (const customer of batch) {
        await db.Customer.create(customer)
      }
      onProgress?.({
        table: 'CUSTOMER',
        current: Math.min(i + batchSize, customers.length),
        total: customers.length,
        percentage: Math.min(100, Math.round(((i + batchSize) / customers.length) * 100)),
      })
    }
    console.log(`  Loaded ${customers.length} customers`)

    // 7. Generate and load ORDERS and LINEITEM together
    console.log('Generating ORDERS and LINEITEM tables...')
    const targetOrders = Math.floor(1_500_000 * scaleFactor)
    const targetLineItems = Math.floor(6_000_000 * scaleFactor)
    let orderCount = 0
    let lineitemCount = 0

    // Collect all orders first for lineitem generation
    const allOrders: Order[] = []
    const orderGen = generateOrders(scaleFactor, customers.length, rng)
    let orderResult = orderGen.next()
    while (!orderResult.done) {
      const orderBatch = orderResult.value
      for (const order of orderBatch) {
        await db.Order.create(order)
        allOrders.push(order)
      }
      orderCount += orderBatch.length
      onProgress?.({
        table: 'ORDERS',
        current: orderCount,
        total: targetOrders,
        percentage: Math.min(100, Math.round((orderCount / targetOrders) * 100)),
      })

      if (orderCount % 100_000 === 0) {
        console.log(`  Loaded ${orderCount.toLocaleString()} orders...`)
      }
      orderResult = orderGen.next()
    }
    stats.tables.orders = orderCount
    console.log(`  Loaded ${orderCount.toLocaleString()} orders`)

    // Generate line items
    const lineitemGen = generateLineItems(allOrders, parts.length, suppliers.length, rng)
    let lineitemResult = lineitemGen.next()
    while (!lineitemResult.done) {
      const { lineitems } = lineitemResult.value
      for (const lineitem of lineitems) {
        await db.LineItem.create(lineitem)
      }
      lineitemCount += lineitems.length
      onProgress?.({
        table: 'LINEITEM',
        current: lineitemCount,
        total: targetLineItems,
        percentage: Math.min(100, Math.round((lineitemCount / targetLineItems) * 100)),
      })

      if (lineitemCount % 500_000 === 0) {
        console.log(`  Loaded ${lineitemCount.toLocaleString()} line items...`)
      }
      lineitemResult = lineitemGen.next()
    }
    stats.tables.lineitem = lineitemCount
    console.log(`  Loaded ${lineitemCount.toLocaleString()} line items`)

    stats.durationMs = Date.now() - startTime
    console.log(`\nGeneration complete in ${(stats.durationMs / 1000).toFixed(1)}s`)

    return stats
  } finally {
    db.dispose()
  }
}

// =============================================================================
// In-Memory Generator for Testing
// =============================================================================

/**
 * Generate TPC-H data in memory (for testing or custom loading)
 */
export function generateTPCHInMemory(scaleFactor: number, seed: number = 42) {
  const rng = new SeededRandom(seed)

  const regions = generateRegions()
  const nations = generateNations(rng)
  const suppliers = generateSuppliers(scaleFactor, rng)
  const parts = generateParts(scaleFactor, rng)
  const partsupps = generatePartSupps(scaleFactor, parts.length, suppliers.length, rng)
  const customers = generateCustomers(scaleFactor, rng)

  return {
    regions,
    nations,
    suppliers,
    parts,
    partsupps,
    customers,
    ordersGenerator: () => generateOrders(scaleFactor, customers.length, rng),
    lineitemsGenerator: (orders: Order[]) =>
      generateLineItems(orders, parts.length, suppliers.length, rng),
  }
}
