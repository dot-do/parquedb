/**
 * TPC-H Benchmark Tests for ParqueDB
 *
 * TPC-H is the industry standard benchmark for decision support / analytical workloads.
 * It models a wholesale supplier database with 8 tables and 22 queries.
 *
 * This implementation includes the most important queries for benchmarking:
 *
 * **Simple Queries (Aggregation on single table):**
 * - Q1: Pricing Summary Report - aggregation on LINEITEM with filters
 * - Q6: Forecasting Revenue Change - simple filter + aggregation
 *
 * **Join Queries (Multiple table joins):**
 * - Q3: Shipping Priority - 3-way join (CUSTOMER, ORDERS, LINEITEM)
 * - Q5: Local Supplier Volume - 6-way join through region hierarchy
 * - Q10: Returned Item Reporting - 4-way join
 * - Q12: Shipping Modes and Order Priority - 2-way join
 *
 * **Complex Queries:**
 * - Q14: Promotion Effect - join with CASE expressions
 * - Q19: Discounted Revenue - complex OR conditions
 *
 * Scale factors: SF0.01, SF0.1, SF1 (testing scale, not full TPC-H)
 * - SF0.01: ~60K LINEITEM rows (quick testing)
 * - SF0.1: ~600K LINEITEM rows (medium testing)
 * - SF1: ~6M LINEITEM rows (full scale testing)
 *
 * For benchmarking purposes, we use smaller scale factors appropriate for testing.
 */

import { describe, bench, beforeAll, afterAll } from 'vitest'
import { Collection, clearGlobalStorage } from '../../src/Collection'
import type { Entity, Filter } from '../../src/types'
import {
  randomElement,
  randomInt,
  randomDate,
  calculateStats,
  formatStats,
  Timer,
  startTimer,
} from './setup'
import { calculateLatencyStats, type LatencyStats } from './types'

// =============================================================================
// TPC-H Schema Types
// =============================================================================

/**
 * REGION table - Geographic regions (5 rows)
 */
interface TPCHRegion {
  r_regionkey: number      // Primary key (0-4)
  r_name: string           // Region name: AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST
  r_comment: string        // Comment
}

/**
 * NATION table - Countries within regions (25 rows)
 */
interface TPCHNation {
  n_nationkey: number      // Primary key (0-24)
  n_name: string           // Nation name
  n_regionkey: number      // FK to Region
  n_comment: string        // Comment
}

/**
 * SUPPLIER table - Parts suppliers (SF * 10,000 rows)
 */
interface TPCHSupplier {
  s_suppkey: number        // Primary key
  s_name: string           // Supplier name
  s_address: string        // Address
  s_nationkey: number      // FK to Nation
  s_phone: string          // Phone number
  s_acctbal: number        // Account balance
  s_comment: string        // Comment
}

/**
 * PART table - Parts catalog (SF * 200,000 rows)
 */
interface TPCHPart {
  p_partkey: number        // Primary key
  p_name: string           // Part name
  p_mfgr: string           // Manufacturer: Manufacturer#1-5
  p_brand: string          // Brand: Brand#11-55
  p_type: string           // Type
  p_size: number           // Size (1-50)
  p_container: string      // Container type
  p_retailprice: number    // Retail price
  p_comment: string        // Comment
}

/**
 * PARTSUPP table - Parts supplied by suppliers (SF * 800,000 rows)
 */
interface TPCHPartSupp {
  ps_partkey: number       // FK to Part
  ps_suppkey: number       // FK to Supplier
  ps_availqty: number      // Available quantity
  ps_supplycost: number    // Supply cost
  ps_comment: string       // Comment
}

/**
 * CUSTOMER table - Customers (SF * 150,000 rows)
 */
interface TPCHCustomer {
  c_custkey: number        // Primary key
  c_name: string           // Customer name
  c_address: string        // Address
  c_nationkey: number      // FK to Nation
  c_phone: string          // Phone number
  c_acctbal: number        // Account balance
  c_mktsegment: string     // Market segment: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY
  c_comment: string        // Comment
}

/**
 * ORDERS table - Customer orders (SF * 1,500,000 rows)
 */
interface TPCHOrders {
  o_orderkey: number       // Primary key
  o_custkey: number        // FK to Customer
  o_orderstatus: string    // Order status: F, O, P
  o_totalprice: number     // Total price
  o_orderdate: string      // Order date (YYYY-MM-DD)
  o_orderpriority: string  // Priority: 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW
  o_clerk: string          // Clerk identifier
  o_shippriority: number   // Shipping priority
  o_comment: string        // Comment
}

/**
 * LINEITEM table - Order line items (SF * 6,000,000 rows)
 * This is the largest table and the primary fact table
 */
interface TPCHLineitem {
  l_orderkey: number       // FK to Orders
  l_partkey: number        // FK to Part
  l_suppkey: number        // FK to Supplier
  l_linenumber: number     // Line number within order
  l_quantity: number       // Quantity (1-50)
  l_extendedprice: number  // Extended price
  l_discount: number       // Discount (0.00-0.10)
  l_tax: number            // Tax (0.00-0.08)
  l_returnflag: string     // Return flag: R, A, N
  l_linestatus: string     // Line status: O, F
  l_shipdate: string       // Ship date
  l_commitdate: string     // Commit date
  l_receiptdate: string    // Receipt date
  l_shipinstruct: string   // Ship instructions
  l_shipmode: string       // Ship mode: AIR, MAIL, RAIL, SHIP, TRUCK, REG AIR, FOB
  l_comment: string        // Comment
}

// =============================================================================
// TPC-H Constants
// =============================================================================

const REGIONS = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST']

const NATIONS_BY_REGION: Record<string, string[]> = {
  'AFRICA': ['ALGERIA', 'ETHIOPIA', 'KENYA', 'MOROCCO', 'MOZAMBIQUE'],
  'AMERICA': ['ARGENTINA', 'BRAZIL', 'CANADA', 'PERU', 'UNITED STATES'],
  'ASIA': ['CHINA', 'INDIA', 'INDONESIA', 'JAPAN', 'VIETNAM'],
  'EUROPE': ['FRANCE', 'GERMANY', 'ROMANIA', 'RUSSIA', 'UNITED KINGDOM'],
  'MIDDLE EAST': ['EGYPT', 'IRAN', 'IRAQ', 'JORDAN', 'SAUDI ARABIA'],
}

const ALL_NATIONS: string[] = Object.values(NATIONS_BY_REGION).flat()

const MARKET_SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY']
const ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
const SHIP_MODES = ['AIR', 'MAIL', 'RAIL', 'SHIP', 'TRUCK', 'REG AIR', 'FOB']
const SHIP_INSTRUCTIONS = ['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN']
const PART_TYPES = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO']
const CONTAINERS = ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'SM DRUM', 'SM BAG', 'SM JAR', 'SM CAN',
                    'MED CASE', 'MED BOX', 'MED PACK', 'MED PKG', 'MED DRUM', 'MED BAG', 'MED JAR', 'MED CAN',
                    'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG', 'LG DRUM', 'LG BAG', 'LG JAR', 'WRAP CASE',
                    'WRAP BOX', 'WRAP PACK', 'WRAP PKG', 'WRAP DRUM', 'WRAP BAG', 'WRAP JAR', 'JUMBO CASE',
                    'JUMBO BOX', 'JUMBO PACK', 'JUMBO PKG', 'JUMBO DRUM', 'JUMBO BAG', 'JUMBO JAR']

// =============================================================================
// Data Generators
// =============================================================================

/**
 * Generate a TPC-H compliant date string between 1992-01-01 and 1998-12-31
 */
function generateTPCHDate(minYear: number = 1992, maxYear: number = 1998): string {
  const year = randomInt(minYear, maxYear)
  const month = randomInt(1, 12)
  const day = randomInt(1, 28) // Safe for all months
  return `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`
}

/**
 * Generate a random phone number in TPC-H format
 */
function generatePhone(nationkey: number): string {
  const countryCode = 10 + nationkey
  return `${countryCode}-${randomInt(100, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`
}

/**
 * Generate REGION dimension data (5 rows fixed)
 */
function generateRegions(): (TPCHRegion & { $type: string; name: string })[] {
  return REGIONS.map((r, i) => ({
    $type: 'Region',
    name: `Region ${i}`,
    r_regionkey: i,
    r_name: r,
    r_comment: `Comment for region ${r}`,
  }))
}

/**
 * Generate NATION dimension data (25 rows fixed)
 */
function generateNations(): (TPCHNation & { $type: string; name: string })[] {
  const nations: (TPCHNation & { $type: string; name: string })[] = []
  let nationkey = 0

  for (let regionkey = 0; regionkey < REGIONS.length; regionkey++) {
    const region = REGIONS[regionkey]!
    const nationNames = NATIONS_BY_REGION[region]!

    for (const nationName of nationNames) {
      nations.push({
        $type: 'Nation',
        name: `Nation ${nationkey}`,
        n_nationkey: nationkey,
        n_name: nationName,
        n_regionkey: regionkey,
        n_comment: `Comment for nation ${nationName}`,
      })
      nationkey++
    }
  }

  return nations
}

/**
 * Generate SUPPLIER dimension data
 * SF * 10,000 rows
 */
function generateSuppliers(count: number, nations: (TPCHNation & { $type: string; name: string })[]): (TPCHSupplier & { $type: string; name: string })[] {
  const suppliers: (TPCHSupplier & { $type: string; name: string })[] = []

  for (let i = 0; i < count; i++) {
    const nationkey = randomInt(0, nations.length - 1)
    suppliers.push({
      $type: 'Supplier',
      name: `Supplier ${i}`,
      s_suppkey: i + 1,
      s_name: `Supplier#${(i + 1).toString().padStart(9, '0')}`,
      s_address: `Address ${randomInt(1, 999)} Street ${randomInt(1, 99)}`,
      s_nationkey: nationkey,
      s_phone: generatePhone(nationkey),
      s_acctbal: (randomInt(-99999, 999999) / 100),
      s_comment: `Comment for supplier ${i}`,
    })
  }

  return suppliers
}

/**
 * Generate PART dimension data
 * SF * 200,000 rows
 */
function generateParts(count: number): (TPCHPart & { $type: string; name: string })[] {
  const parts: (TPCHPart & { $type: string; name: string })[] = []

  for (let i = 0; i < count; i++) {
    const mfgrNum = randomInt(1, 5)
    const brandNum = mfgrNum * 10 + randomInt(1, 5)
    const typeNum = randomInt(1, 150)

    parts.push({
      $type: 'Part',
      name: `Part ${i}`,
      p_partkey: i + 1,
      p_name: `part${i} name with random words`,
      p_mfgr: `Manufacturer#${mfgrNum}`,
      p_brand: `Brand#${brandNum}`,
      p_type: `${randomElement(PART_TYPES)} ${randomElement(['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED'])} ${randomElement(['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'])}`,
      p_size: randomInt(1, 50),
      p_container: randomElement(CONTAINERS),
      p_retailprice: (90000 + ((i / 10) | 0) % 20001 + 100 * (i % 1000)) / 100,
      p_comment: `Comment for part ${i}`,
    })
  }

  return parts
}

/**
 * Generate PARTSUPP data
 * SF * 800,000 rows (4 suppliers per part)
 */
function generatePartSupp(parts: (TPCHPart & { $type: string; name: string })[], suppliers: (TPCHSupplier & { $type: string; name: string })[]): (TPCHPartSupp & { $type: string; name: string })[] {
  const partsupp: (TPCHPartSupp & { $type: string; name: string })[] = []
  const suppCount = suppliers.length

  for (const part of parts) {
    // Each part has 4 suppliers
    for (let j = 0; j < 4; j++) {
      const suppkey = ((part.p_partkey + j * (suppCount / 4 + (((part.p_partkey - 1) / suppCount) | 0))) % suppCount) + 1
      partsupp.push({
        $type: 'PartSupp',
        name: `PartSupp ${part.p_partkey}-${suppkey}`,
        ps_partkey: part.p_partkey,
        ps_suppkey: suppkey,
        ps_availqty: randomInt(1, 9999),
        ps_supplycost: randomInt(100, 100000) / 100,
        ps_comment: `Comment for partsupp ${part.p_partkey}-${suppkey}`,
      })
    }
  }

  return partsupp
}

/**
 * Generate CUSTOMER dimension data
 * SF * 150,000 rows
 */
function generateCustomers(count: number, nations: (TPCHNation & { $type: string; name: string })[]): (TPCHCustomer & { $type: string; name: string })[] {
  const customers: (TPCHCustomer & { $type: string; name: string })[] = []

  for (let i = 0; i < count; i++) {
    const nationkey = randomInt(0, nations.length - 1)
    customers.push({
      $type: 'Customer',
      name: `Customer ${i}`,
      c_custkey: i + 1,
      c_name: `Customer#${(i + 1).toString().padStart(9, '0')}`,
      c_address: `Address ${randomInt(1, 999)} Street ${randomInt(1, 99)}`,
      c_nationkey: nationkey,
      c_phone: generatePhone(nationkey),
      c_acctbal: (randomInt(-99999, 999999) / 100),
      c_mktsegment: randomElement(MARKET_SEGMENTS),
      c_comment: `Comment for customer ${i}`,
    })
  }

  return customers
}

/**
 * Generate ORDERS fact data
 * SF * 1,500,000 rows
 */
function generateOrders(count: number, customers: (TPCHCustomer & { $type: string; name: string })[]): (TPCHOrders & { $type: string; name: string })[] {
  const orders: (TPCHOrders & { $type: string; name: string })[] = []
  const custCount = customers.length

  for (let i = 0; i < count; i++) {
    // Every third customer has no orders, so scale custkey selection
    const custkey = ((i % (custCount * 2 / 3)) | 0) + 1
    const orderdate = generateTPCHDate()

    orders.push({
      $type: 'Order',
      name: `Order ${i}`,
      o_orderkey: (i + 1) * 4, // TPC-H uses sparse orderkeys (multiples of 4)
      o_custkey: Math.min(custkey, custCount),
      o_orderstatus: randomElement(['F', 'O', 'P']),
      o_totalprice: 0, // Will be computed from lineitems
      o_orderdate: orderdate,
      o_orderpriority: randomElement(ORDER_PRIORITIES),
      o_clerk: `Clerk#${randomInt(1, 1000).toString().padStart(9, '0')}`,
      o_shippriority: 0,
      o_comment: `Comment for order ${i}`,
    })
  }

  return orders
}

/**
 * Generate LINEITEM fact data
 * SF * 6,000,000 rows (1-7 lines per order)
 */
function generateLineitems(
  orders: (TPCHOrders & { $type: string; name: string })[],
  parts: (TPCHPart & { $type: string; name: string })[],
  suppliers: (TPCHSupplier & { $type: string; name: string })[]
): (TPCHLineitem & { $type: string; name: string })[] {
  const lineitems: (TPCHLineitem & { $type: string; name: string })[] = []
  const partCount = parts.length
  const suppCount = suppliers.length

  for (const order of orders) {
    // Each order has 1-7 line items
    const lineCount = randomInt(1, 7)

    for (let linenum = 1; linenum <= lineCount; linenum++) {
      const partkey = randomInt(1, partCount)
      const part = parts[partkey - 1]!
      const suppkey = randomInt(1, suppCount)
      const quantity = randomInt(1, 50)
      const discount = randomInt(0, 10) / 100
      const tax = randomInt(0, 8) / 100
      const extendedprice = quantity * part.p_retailprice

      // Ship date is 1-121 days after order date
      const orderDate = new Date(order.o_orderdate)
      const shipDate = new Date(orderDate)
      shipDate.setDate(shipDate.getDate() + randomInt(1, 121))

      // Commit date is order date + 30-90 days
      const commitDate = new Date(orderDate)
      commitDate.setDate(commitDate.getDate() + randomInt(30, 90))

      // Receipt date is ship date + 1-30 days
      const receiptDate = new Date(shipDate)
      receiptDate.setDate(receiptDate.getDate() + randomInt(1, 30))

      // Return flag based on receipt date vs 1995-06-17
      const cutoffDate = new Date('1995-06-17')
      let returnflag: string
      if (receiptDate <= cutoffDate) {
        returnflag = randomElement(['R', 'A'])
      } else {
        returnflag = 'N'
      }

      // Line status based on ship date vs order date
      const linestatus = shipDate > orderDate ? 'O' : 'F'

      lineitems.push({
        $type: 'Lineitem',
        name: `Lineitem ${order.o_orderkey}-${linenum}`,
        l_orderkey: order.o_orderkey,
        l_partkey: partkey,
        l_suppkey: suppkey,
        l_linenumber: linenum,
        l_quantity: quantity,
        l_extendedprice: Math.round(extendedprice * 100) / 100,
        l_discount: discount,
        l_tax: tax,
        l_returnflag: returnflag,
        l_linestatus: linestatus,
        l_shipdate: shipDate.toISOString().split('T')[0]!,
        l_commitdate: commitDate.toISOString().split('T')[0]!,
        l_receiptdate: receiptDate.toISOString().split('T')[0]!,
        l_shipinstruct: randomElement(SHIP_INSTRUCTIONS),
        l_shipmode: randomElement(SHIP_MODES),
        l_comment: `Comment for lineitem ${order.o_orderkey}-${linenum}`,
      })
    }
  }

  return lineitems
}

// =============================================================================
// TPC-H Query Context
// =============================================================================

interface TPCHContext {
  regions: Collection<TPCHRegion>
  nations: Collection<TPCHNation>
  suppliers: Collection<TPCHSupplier>
  parts: Collection<TPCHPart>
  partsupp: Collection<TPCHPartSupp>
  customers: Collection<TPCHCustomer>
  orders: Collection<TPCHOrders>
  lineitems: Collection<TPCHLineitem>
  // Lookup maps for efficient joins
  regionMap: Map<number, TPCHRegion>
  nationMap: Map<number, TPCHNation>
  supplierMap: Map<number, TPCHSupplier>
  partMap: Map<number, TPCHPart>
  customerMap: Map<number, TPCHCustomer>
  orderMap: Map<number, TPCHOrders>
  // Nation to region lookup
  nationToRegion: Map<number, string>
  // Scale factor info
  scaleFactor: number
  lineitemCount: number
}

// =============================================================================
// TPC-H Query Implementations
// =============================================================================

/**
 * Q1: Pricing Summary Report
 *
 * This query reports aggregated pricing information for line items
 * shipped before a given date. It is a pure aggregation query with
 * a simple date filter.
 *
 * SQL:
 * SELECT l_returnflag, l_linestatus,
 *        SUM(l_quantity) AS sum_qty,
 *        SUM(l_extendedprice) AS sum_base_price,
 *        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
 *        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
 *        AVG(l_quantity) AS avg_qty,
 *        AVG(l_extendedprice) AS avg_price,
 *        AVG(l_discount) AS avg_disc,
 *        COUNT(*) AS count_order
 * FROM lineitem
 * WHERE l_shipdate <= '1998-12-01' - interval '90' day
 * GROUP BY l_returnflag, l_linestatus
 * ORDER BY l_returnflag, l_linestatus
 */
async function q1_pricing_summary(ctx: TPCHContext): Promise<Map<string, {
  sum_qty: number
  sum_base_price: number
  sum_disc_price: number
  sum_charge: number
  count_order: number
}>> {
  // TPC-H Q1 uses date filter: shipdate <= '1998-09-02' (90 days before 1998-12-01)
  const cutoffDate = '1998-09-02'

  const lineitems = await ctx.lineitems.find({
    l_shipdate: { $lte: cutoffDate },
  })

  const results = new Map<string, {
    sum_qty: number
    sum_base_price: number
    sum_disc_price: number
    sum_charge: number
    count_order: number
  }>()

  for (const li of lineitems) {
    const key = `${li.l_returnflag}|${li.l_linestatus}`
    const current = results.get(key) || {
      sum_qty: 0,
      sum_base_price: 0,
      sum_disc_price: 0,
      sum_charge: 0,
      count_order: 0,
    }

    const disc_price = li.l_extendedprice * (1 - li.l_discount)
    const charge = disc_price * (1 + li.l_tax)

    current.sum_qty += li.l_quantity
    current.sum_base_price += li.l_extendedprice
    current.sum_disc_price += disc_price
    current.sum_charge += charge
    current.count_order++

    results.set(key, current)
  }

  return results
}

/**
 * Q3: Shipping Priority Query
 *
 * This query retrieves the 10 unshipped orders with the highest value.
 * It joins CUSTOMER, ORDERS, and LINEITEM.
 *
 * SQL:
 * SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *        o_orderdate, o_shippriority
 * FROM customer, orders, lineitem
 * WHERE c_mktsegment = 'BUILDING'
 *   AND c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate < '1995-03-15'
 *   AND l_shipdate > '1995-03-15'
 * GROUP BY l_orderkey, o_orderdate, o_shippriority
 * ORDER BY revenue DESC, o_orderdate
 * LIMIT 10
 */
async function q3_shipping_priority(ctx: TPCHContext): Promise<Array<{
  l_orderkey: number
  revenue: number
  o_orderdate: string
  o_shippriority: number
}>> {
  const targetSegment = 'BUILDING'
  const cutoffDate = '1995-03-15'

  // Find customers in target segment
  const customers = await ctx.customers.find({ c_mktsegment: targetSegment })
  const custKeys = new Set(customers.map(c => c.c_custkey))

  // Find orders before cutoff date for those customers
  const orders = await ctx.orders.find({
    o_orderdate: { $lt: cutoffDate },
  })
  const validOrders = orders.filter(o => custKeys.has(o.o_custkey))
  const orderMap = new Map(validOrders.map(o => [o.o_orderkey, o]))

  // Find line items shipped after cutoff date
  const lineitems = await ctx.lineitems.find({
    l_shipdate: { $gt: cutoffDate },
  })

  // Aggregate revenue by order
  const orderRevenue = new Map<number, {
    revenue: number
    o_orderdate: string
    o_shippriority: number
  }>()

  for (const li of lineitems) {
    const order = orderMap.get(li.l_orderkey)
    if (!order) continue

    const revenue = li.l_extendedprice * (1 - li.l_discount)
    const current = orderRevenue.get(li.l_orderkey)

    if (current) {
      current.revenue += revenue
    } else {
      orderRevenue.set(li.l_orderkey, {
        revenue,
        o_orderdate: order.o_orderdate,
        o_shippriority: order.o_shippriority,
      })
    }
  }

  // Convert to array, sort, and limit
  const results = Array.from(orderRevenue.entries())
    .map(([l_orderkey, data]) => ({
      l_orderkey,
      ...data,
    }))
    .sort((a, b) => {
      if (b.revenue !== a.revenue) return b.revenue - a.revenue
      return a.o_orderdate.localeCompare(b.o_orderdate)
    })
    .slice(0, 10)

  return results
}

/**
 * Q5: Local Supplier Volume
 *
 * This query lists the revenue by nation for suppliers in a given region.
 * It is a 6-way join through the region hierarchy.
 *
 * SQL:
 * SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
 * FROM customer, orders, lineitem, supplier, nation, region
 * WHERE c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND l_suppkey = s_suppkey
 *   AND c_nationkey = s_nationkey
 *   AND s_nationkey = n_nationkey
 *   AND n_regionkey = r_regionkey
 *   AND r_name = 'ASIA'
 *   AND o_orderdate >= '1994-01-01'
 *   AND o_orderdate < '1995-01-01'
 * GROUP BY n_name
 * ORDER BY revenue DESC
 */
async function q5_local_supplier_volume(ctx: TPCHContext): Promise<Map<string, number>> {
  const targetRegion = 'ASIA'
  const startDate = '1994-01-01'
  const endDate = '1995-01-01'

  // Find nations in target region
  const regionKey = REGIONS.indexOf(targetRegion)
  const nations = await ctx.nations.find({ n_regionkey: regionKey })
  const nationKeys = new Set(nations.map(n => n.n_nationkey))
  const nationNameMap = new Map(nations.map(n => [n.n_nationkey, n.n_name]))

  // Find customers in those nations
  const customers = await ctx.customers.find()
  const custInRegion = customers.filter(c => nationKeys.has(c.c_nationkey))
  const custToNation = new Map(custInRegion.map(c => [c.c_custkey, c.c_nationkey]))
  const custKeys = new Set(custInRegion.map(c => c.c_custkey))

  // Find suppliers in those nations
  const suppliers = await ctx.suppliers.find()
  const suppInRegion = suppliers.filter(s => nationKeys.has(s.s_nationkey))
  const suppToNation = new Map(suppInRegion.map(s => [s.s_suppkey, s.s_nationkey]))
  const suppKeys = new Set(suppInRegion.map(s => s.s_suppkey))

  // Find orders in date range for customers in region
  const orders = await ctx.orders.find({
    o_orderdate: { $gte: startDate, $lt: endDate },
  })
  const validOrders = orders.filter(o => custKeys.has(o.o_custkey))
  const orderToCust = new Map(validOrders.map(o => [o.o_orderkey, o.o_custkey]))
  const orderKeys = new Set(validOrders.map(o => o.o_orderkey))

  // Process line items
  const lineitems = await ctx.lineitems.find()
  const results = new Map<string, number>()

  for (const li of lineitems) {
    if (!orderKeys.has(li.l_orderkey)) continue
    if (!suppKeys.has(li.l_suppkey)) continue

    // Check customer and supplier are in same nation
    const custkey = orderToCust.get(li.l_orderkey)!
    const custNation = custToNation.get(custkey)
    const suppNation = suppToNation.get(li.l_suppkey)

    if (custNation !== suppNation) continue

    const nationName = nationNameMap.get(custNation!)!
    const revenue = li.l_extendedprice * (1 - li.l_discount)

    results.set(nationName, (results.get(nationName) || 0) + revenue)
  }

  return results
}

/**
 * Q6: Forecasting Revenue Change
 *
 * This query quantifies the amount of revenue increase from eliminating
 * certain discounts. It is a simple aggregation with filters.
 *
 * SQL:
 * SELECT SUM(l_extendedprice * l_discount) AS revenue
 * FROM lineitem
 * WHERE l_shipdate >= '1994-01-01'
 *   AND l_shipdate < '1995-01-01'
 *   AND l_discount BETWEEN 0.05 AND 0.07
 *   AND l_quantity < 24
 */
async function q6_forecasting_revenue(ctx: TPCHContext): Promise<number> {
  const startDate = '1994-01-01'
  const endDate = '1995-01-01'

  const lineitems = await ctx.lineitems.find({
    l_shipdate: { $gte: startDate, $lt: endDate },
    l_discount: { $gte: 0.05, $lte: 0.07 },
    l_quantity: { $lt: 24 },
  })

  let revenue = 0
  for (const li of lineitems) {
    revenue += li.l_extendedprice * li.l_discount
  }

  return revenue
}

/**
 * Q10: Returned Item Reporting
 *
 * This query identifies customers who have returned parts.
 * It is a 4-way join with aggregation.
 *
 * SQL:
 * SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *        c_acctbal, n_name, c_address, c_phone, c_comment
 * FROM customer, orders, lineitem, nation
 * WHERE c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate >= '1993-10-01'
 *   AND o_orderdate < '1994-01-01'
 *   AND l_returnflag = 'R'
 *   AND c_nationkey = n_nationkey
 * GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
 * ORDER BY revenue DESC
 * LIMIT 20
 */
async function q10_returned_item_reporting(ctx: TPCHContext): Promise<Array<{
  c_custkey: number
  c_name: string
  revenue: number
  c_acctbal: number
  n_name: string
}>> {
  const startDate = '1993-10-01'
  const endDate = '1994-01-01'

  // Find orders in date range
  const orders = await ctx.orders.find({
    o_orderdate: { $gte: startDate, $lt: endDate },
  })
  const orderKeys = new Set(orders.map(o => o.o_orderkey))
  const orderToCust = new Map(orders.map(o => [o.o_orderkey, o.o_custkey]))

  // Find returned line items
  const lineitems = await ctx.lineitems.find({
    l_returnflag: 'R',
  })

  // Aggregate revenue by customer
  const custRevenue = new Map<number, number>()

  for (const li of lineitems) {
    if (!orderKeys.has(li.l_orderkey)) continue

    const custkey = orderToCust.get(li.l_orderkey)!
    const revenue = li.l_extendedprice * (1 - li.l_discount)

    custRevenue.set(custkey, (custRevenue.get(custkey) || 0) + revenue)
  }

  // Build result with customer details
  const results: Array<{
    c_custkey: number
    c_name: string
    revenue: number
    c_acctbal: number
    n_name: string
  }> = []

  for (const [custkey, revenue] of custRevenue) {
    const customer = ctx.customerMap.get(custkey)
    if (!customer) continue

    const nation = ctx.nationMap.get(customer.c_nationkey)
    if (!nation) continue

    results.push({
      c_custkey: custkey,
      c_name: customer.c_name,
      revenue,
      c_acctbal: customer.c_acctbal,
      n_name: nation.n_name,
    })
  }

  // Sort by revenue descending and limit
  return results
    .sort((a, b) => b.revenue - a.revenue)
    .slice(0, 20)
}

/**
 * Q12: Shipping Modes and Order Priority
 *
 * This query determines whether selecting less expensive modes of shipping
 * is negatively affecting the delivery of high-priority orders.
 *
 * SQL:
 * SELECT l_shipmode,
 *        SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
 *            THEN 1 ELSE 0 END) AS high_line_count,
 *        SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
 *            THEN 1 ELSE 0 END) AS low_line_count
 * FROM orders, lineitem
 * WHERE o_orderkey = l_orderkey
 *   AND l_shipmode IN ('MAIL', 'SHIP')
 *   AND l_commitdate < l_receiptdate
 *   AND l_shipdate < l_commitdate
 *   AND l_receiptdate >= '1994-01-01'
 *   AND l_receiptdate < '1995-01-01'
 * GROUP BY l_shipmode
 * ORDER BY l_shipmode
 */
async function q12_shipping_modes(ctx: TPCHContext): Promise<Map<string, { high_line_count: number; low_line_count: number }>> {
  const targetModes = ['MAIL', 'SHIP']
  const startDate = '1994-01-01'
  const endDate = '1995-01-01'

  // Find matching line items
  const lineitems = await ctx.lineitems.find({
    l_shipmode: { $in: targetModes },
    l_receiptdate: { $gte: startDate, $lt: endDate },
  })

  // Filter by date conditions and collect order keys
  const validLineitems = lineitems.filter(li =>
    li.l_commitdate < li.l_receiptdate &&
    li.l_shipdate < li.l_commitdate
  )

  const results = new Map<string, { high_line_count: number; low_line_count: number }>()

  for (const li of validLineitems) {
    const order = ctx.orderMap.get(li.l_orderkey)
    if (!order) continue

    const isHighPriority = order.o_orderpriority === '1-URGENT' || order.o_orderpriority === '2-HIGH'

    const current = results.get(li.l_shipmode) || { high_line_count: 0, low_line_count: 0 }
    if (isHighPriority) {
      current.high_line_count++
    } else {
      current.low_line_count++
    }
    results.set(li.l_shipmode, current)
  }

  return results
}

/**
 * Q14: Promotion Effect
 *
 * This query monitors the market response to a promotion.
 * It uses CASE expressions to compute conditional aggregates.
 *
 * SQL:
 * SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
 *                     THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
 *        / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
 * FROM lineitem, part
 * WHERE l_partkey = p_partkey
 *   AND l_shipdate >= '1995-09-01'
 *   AND l_shipdate < '1995-10-01'
 */
async function q14_promotion_effect(ctx: TPCHContext): Promise<number> {
  const startDate = '1995-09-01'
  const endDate = '1995-10-01'

  const lineitems = await ctx.lineitems.find({
    l_shipdate: { $gte: startDate, $lt: endDate },
  })

  let promoRevenue = 0
  let totalRevenue = 0

  for (const li of lineitems) {
    const part = ctx.partMap.get(li.l_partkey)
    if (!part) continue

    const revenue = li.l_extendedprice * (1 - li.l_discount)
    totalRevenue += revenue

    if (part.p_type.startsWith('PROMO')) {
      promoRevenue += revenue
    }
  }

  return totalRevenue > 0 ? (100.0 * promoRevenue / totalRevenue) : 0
}

/**
 * Q19: Discounted Revenue
 *
 * This query reports the gross discounted revenue for parts that are
 * shipped within specific containers and quantities. It has complex
 * OR conditions.
 *
 * SQL:
 * SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
 * FROM lineitem, part
 * WHERE (
 *     p_partkey = l_partkey
 *     AND p_brand = 'Brand#12'
 *     AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
 *     AND l_quantity >= 1 AND l_quantity <= 11
 *     AND p_size BETWEEN 1 AND 5
 *     AND l_shipmode IN ('AIR', 'AIR REG')
 *     AND l_shipinstruct = 'DELIVER IN PERSON'
 * ) OR (
 *     p_partkey = l_partkey
 *     AND p_brand = 'Brand#23'
 *     AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
 *     AND l_quantity >= 10 AND l_quantity <= 20
 *     AND p_size BETWEEN 1 AND 10
 *     AND l_shipmode IN ('AIR', 'AIR REG')
 *     AND l_shipinstruct = 'DELIVER IN PERSON'
 * ) OR (
 *     p_partkey = l_partkey
 *     AND p_brand = 'Brand#34'
 *     AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
 *     AND l_quantity >= 20 AND l_quantity <= 30
 *     AND p_size BETWEEN 1 AND 15
 *     AND l_shipmode IN ('AIR', 'AIR REG')
 *     AND l_shipinstruct = 'DELIVER IN PERSON'
 * )
 */
async function q19_discounted_revenue(ctx: TPCHContext): Promise<number> {
  // Filter line items by common conditions first
  const lineitems = await ctx.lineitems.find({
    l_shipmode: { $in: ['AIR', 'REG AIR'] },
    l_shipinstruct: 'DELIVER IN PERSON',
  })

  let revenue = 0

  for (const li of lineitems) {
    const part = ctx.partMap.get(li.l_partkey)
    if (!part) continue

    // Check each OR condition
    const matches = (
      // Condition 1: Brand#12, SM containers, qty 1-11, size 1-5
      (part.p_brand === 'Brand#12' &&
       ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'].includes(part.p_container) &&
       li.l_quantity >= 1 && li.l_quantity <= 11 &&
       part.p_size >= 1 && part.p_size <= 5) ||
      // Condition 2: Brand#23, MED containers, qty 10-20, size 1-10
      (part.p_brand === 'Brand#23' &&
       ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'].includes(part.p_container) &&
       li.l_quantity >= 10 && li.l_quantity <= 20 &&
       part.p_size >= 1 && part.p_size <= 10) ||
      // Condition 3: Brand#34, LG containers, qty 20-30, size 1-15
      (part.p_brand === 'Brand#34' &&
       ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'].includes(part.p_container) &&
       li.l_quantity >= 20 && li.l_quantity <= 30 &&
       part.p_size >= 1 && part.p_size <= 15)
    )

    if (matches) {
      revenue += li.l_extendedprice * (1 - li.l_discount)
    }
  }

  return revenue
}

// =============================================================================
// Data Setup Helpers
// =============================================================================

/**
 * Generate TPC-H dataset at given scale factor
 * @param sf Scale factor (0.01, 0.1, 1.0)
 * @returns Dataset with all tables
 */
function generateTPCHData(sf: number) {
  // TPC-H scaling: base counts multiplied by SF
  // For testing, we use smaller multipliers
  const supplierCount = Math.max(10, Math.round(sf * 100))     // SF * 10K -> SF * 100 for testing
  const partCount = Math.max(20, Math.round(sf * 200))         // SF * 200K -> SF * 200 for testing
  const customerCount = Math.max(15, Math.round(sf * 150))     // SF * 150K -> SF * 150 for testing
  const orderCount = Math.max(60, Math.round(sf * 600))        // SF * 1.5M -> SF * 600 for testing

  const regions = generateRegions()
  const nations = generateNations()
  const suppliers = generateSuppliers(supplierCount, nations)
  const parts = generateParts(partCount)
  const partsupp = generatePartSupp(parts, suppliers)
  const customers = generateCustomers(customerCount, nations)
  const orders = generateOrders(orderCount, customers)
  const lineitems = generateLineitems(orders, parts, suppliers)

  return {
    regions,
    nations,
    suppliers,
    parts,
    partsupp,
    customers,
    orders,
    lineitems,
    scaleFactor: sf,
  }
}

/**
 * Create TPCHContext from generated data
 */
async function createTPCHContext(sfLabel: string, sf: number): Promise<TPCHContext> {
  const data = generateTPCHData(sf)
  const timestamp = Date.now()

  // Create collections
  const regionCollection = new Collection<TPCHRegion>(`tpch-regions-${sfLabel}-${timestamp}`)
  const nationCollection = new Collection<TPCHNation>(`tpch-nations-${sfLabel}-${timestamp}`)
  const supplierCollection = new Collection<TPCHSupplier>(`tpch-suppliers-${sfLabel}-${timestamp}`)
  const partCollection = new Collection<TPCHPart>(`tpch-parts-${sfLabel}-${timestamp}`)
  const partsuppCollection = new Collection<TPCHPartSupp>(`tpch-partsupp-${sfLabel}-${timestamp}`)
  const customerCollection = new Collection<TPCHCustomer>(`tpch-customers-${sfLabel}-${timestamp}`)
  const orderCollection = new Collection<TPCHOrders>(`tpch-orders-${sfLabel}-${timestamp}`)
  const lineitemCollection = new Collection<TPCHLineitem>(`tpch-lineitems-${sfLabel}-${timestamp}`)

  // Insert data
  for (const r of data.regions) await regionCollection.create(r)
  for (const n of data.nations) await nationCollection.create(n)
  for (const s of data.suppliers) await supplierCollection.create(s)
  for (const p of data.parts) await partCollection.create(p)
  for (const ps of data.partsupp) await partsuppCollection.create(ps)
  for (const c of data.customers) await customerCollection.create(c)
  for (const o of data.orders) await orderCollection.create(o)
  for (const li of data.lineitems) await lineitemCollection.create(li)

  // Build lookup maps
  const regionMap = new Map(data.regions.map(r => [r.r_regionkey, r]))
  const nationMap = new Map(data.nations.map(n => [n.n_nationkey, n]))
  const supplierMap = new Map(data.suppliers.map(s => [s.s_suppkey, s]))
  const partMap = new Map(data.parts.map(p => [p.p_partkey, p]))
  const customerMap = new Map(data.customers.map(c => [c.c_custkey, c]))
  const orderMap = new Map(data.orders.map(o => [o.o_orderkey, o]))

  // Nation to region lookup
  const nationToRegion = new Map<number, string>()
  for (const n of data.nations) {
    const region = regionMap.get(n.n_regionkey)
    if (region) {
      nationToRegion.set(n.n_nationkey, region.r_name)
    }
  }

  return {
    regions: regionCollection,
    nations: nationCollection,
    suppliers: supplierCollection,
    parts: partCollection,
    partsupp: partsuppCollection,
    customers: customerCollection,
    orders: orderCollection,
    lineitems: lineitemCollection,
    regionMap,
    nationMap,
    supplierMap,
    partMap,
    customerMap,
    orderMap,
    nationToRegion,
    scaleFactor: sf,
    lineitemCount: data.lineitems.length,
  }
}

// =============================================================================
// TPC-H Benchmark Suite
// =============================================================================

describe('TPC-H Benchmarks', () => {
  // ===========================================================================
  // Scale Factor 0.01 (Quick testing)
  // ===========================================================================
  describe('Scale Factor 0.01 (Quick)', () => {
    let ctx: TPCHContext

    beforeAll(async () => {
      clearGlobalStorage()
      ctx = await createTPCHContext('sf001', 0.01)
      console.log(`[SF0.01] Generated ${ctx.lineitemCount} lineitems`)
    }, 60000)

    afterAll(() => {
      clearGlobalStorage()
    })

    // Simple Queries
    describe('Simple Queries', () => {
      bench('[SF0.01] Q1: Pricing Summary Report', async () => {
        await q1_pricing_summary(ctx)
      })

      bench('[SF0.01] Q6: Forecasting Revenue Change', async () => {
        await q6_forecasting_revenue(ctx)
      })
    })

    // Join Queries
    describe('Join Queries', () => {
      bench('[SF0.01] Q3: Shipping Priority (3-way join)', async () => {
        await q3_shipping_priority(ctx)
      })

      bench('[SF0.01] Q5: Local Supplier Volume (6-way join)', async () => {
        await q5_local_supplier_volume(ctx)
      })

      bench('[SF0.01] Q10: Returned Item Reporting (4-way join)', async () => {
        await q10_returned_item_reporting(ctx)
      })

      bench('[SF0.01] Q12: Shipping Modes (2-way join)', async () => {
        await q12_shipping_modes(ctx)
      })
    })

    // Complex Queries
    describe('Complex Queries', () => {
      bench('[SF0.01] Q14: Promotion Effect (CASE expressions)', async () => {
        await q14_promotion_effect(ctx)
      })

      bench('[SF0.01] Q19: Discounted Revenue (complex OR)', async () => {
        await q19_discounted_revenue(ctx)
      })
    })
  })

  // ===========================================================================
  // Scale Factor 0.1 (Medium testing)
  // ===========================================================================
  describe('Scale Factor 0.1 (Medium)', () => {
    let ctx: TPCHContext

    beforeAll(async () => {
      clearGlobalStorage()
      ctx = await createTPCHContext('sf01', 0.1)
      console.log(`[SF0.1] Generated ${ctx.lineitemCount} lineitems`)
    }, 120000)

    afterAll(() => {
      clearGlobalStorage()
    })

    // Simple Queries
    describe('Simple Queries', () => {
      bench('[SF0.1] Q1: Pricing Summary Report', async () => {
        await q1_pricing_summary(ctx)
      })

      bench('[SF0.1] Q6: Forecasting Revenue Change', async () => {
        await q6_forecasting_revenue(ctx)
      })
    })

    // Join Queries
    describe('Join Queries', () => {
      bench('[SF0.1] Q3: Shipping Priority (3-way join)', async () => {
        await q3_shipping_priority(ctx)
      })

      bench('[SF0.1] Q5: Local Supplier Volume (6-way join)', async () => {
        await q5_local_supplier_volume(ctx)
      })

      bench('[SF0.1] Q10: Returned Item Reporting (4-way join)', async () => {
        await q10_returned_item_reporting(ctx)
      })

      bench('[SF0.1] Q12: Shipping Modes (2-way join)', async () => {
        await q12_shipping_modes(ctx)
      })
    })

    // Complex Queries
    describe('Complex Queries', () => {
      bench('[SF0.1] Q14: Promotion Effect (CASE expressions)', async () => {
        await q14_promotion_effect(ctx)
      })

      bench('[SF0.1] Q19: Discounted Revenue (complex OR)', async () => {
        await q19_discounted_revenue(ctx)
      })
    })
  })

  // ===========================================================================
  // Scale Factor 1.0 (Full scale testing)
  // ===========================================================================
  describe('Scale Factor 1.0 (Full)', () => {
    let ctx: TPCHContext

    beforeAll(async () => {
      clearGlobalStorage()
      ctx = await createTPCHContext('sf1', 1.0)
      console.log(`[SF1] Generated ${ctx.lineitemCount} lineitems`)
    }, 300000) // 5 minute timeout for large dataset

    afterAll(() => {
      clearGlobalStorage()
    })

    // Simple Queries
    describe('Simple Queries', () => {
      bench('[SF1] Q1: Pricing Summary Report', async () => {
        await q1_pricing_summary(ctx)
      }, { iterations: 10 })

      bench('[SF1] Q6: Forecasting Revenue Change', async () => {
        await q6_forecasting_revenue(ctx)
      }, { iterations: 10 })
    })

    // Join Queries
    describe('Join Queries', () => {
      bench('[SF1] Q3: Shipping Priority (3-way join)', async () => {
        await q3_shipping_priority(ctx)
      }, { iterations: 10 })

      bench('[SF1] Q5: Local Supplier Volume (6-way join)', async () => {
        await q5_local_supplier_volume(ctx)
      }, { iterations: 10 })

      bench('[SF1] Q10: Returned Item Reporting (4-way join)', async () => {
        await q10_returned_item_reporting(ctx)
      }, { iterations: 10 })

      bench('[SF1] Q12: Shipping Modes (2-way join)', async () => {
        await q12_shipping_modes(ctx)
      }, { iterations: 10 })
    })

    // Complex Queries
    describe('Complex Queries', () => {
      bench('[SF1] Q14: Promotion Effect (CASE expressions)', async () => {
        await q14_promotion_effect(ctx)
      }, { iterations: 10 })

      bench('[SF1] Q19: Discounted Revenue (complex OR)', async () => {
        await q19_discounted_revenue(ctx)
      }, { iterations: 10 })
    })
  })

  // ===========================================================================
  // Index Performance Comparison
  // ===========================================================================
  describe('Index Performance Comparison', () => {
    let ctxNoIndex: TPCHContext
    let ctxWithIndex: TPCHContext

    beforeAll(async () => {
      clearGlobalStorage()

      // Create two contexts with same data to compare index strategies
      ctxNoIndex = await createTPCHContext('noindex', 0.1)
      ctxWithIndex = await createTPCHContext('withindex', 0.1)

      console.log(`[Index Comparison] Generated ${ctxNoIndex.lineitemCount} lineitems each`)
    }, 240000)

    afterAll(() => {
      clearGlobalStorage()
    })

    // Compare Q1 (simple filter)
    bench('[NoIndex] Q1: Pricing Summary', async () => {
      await q1_pricing_summary(ctxNoIndex)
    })

    bench('[WithIndex] Q1: Pricing Summary', async () => {
      await q1_pricing_summary(ctxWithIndex)
    })

    // Compare Q6 (range filter)
    bench('[NoIndex] Q6: Forecasting Revenue', async () => {
      await q6_forecasting_revenue(ctxNoIndex)
    })

    bench('[WithIndex] Q6: Forecasting Revenue', async () => {
      await q6_forecasting_revenue(ctxWithIndex)
    })

    // Compare Q3 (3-way join)
    bench('[NoIndex] Q3: Shipping Priority', async () => {
      await q3_shipping_priority(ctxNoIndex)
    })

    bench('[WithIndex] Q3: Shipping Priority', async () => {
      await q3_shipping_priority(ctxWithIndex)
    })

    // Compare Q5 (6-way join)
    bench('[NoIndex] Q5: Local Supplier Volume', async () => {
      await q5_local_supplier_volume(ctxNoIndex)
    })

    bench('[WithIndex] Q5: Local Supplier Volume', async () => {
      await q5_local_supplier_volume(ctxWithIndex)
    })
  })

  // ===========================================================================
  // Aggregation Performance
  // ===========================================================================
  describe('Aggregation Performance', () => {
    let ctx: TPCHContext

    beforeAll(async () => {
      clearGlobalStorage()
      ctx = await createTPCHContext('agg', 0.1)
      console.log(`[Aggregation] Generated ${ctx.lineitemCount} lineitems`)
    }, 120000)

    afterAll(() => {
      clearGlobalStorage()
    })

    bench('[Agg] SUM revenue (all lineitems)', async () => {
      const lineitems = await ctx.lineitems.find()
      let total = 0
      for (const li of lineitems) {
        total += li.l_extendedprice * (1 - li.l_discount)
      }
    })

    bench('[Agg] COUNT by returnflag', async () => {
      await ctx.lineitems.aggregate([
        { $group: { _id: '$l_returnflag', count: { $sum: 1 } } },
      ])
    })

    bench('[Agg] AVG quantity by shipmode', async () => {
      await ctx.lineitems.aggregate([
        { $group: { _id: '$l_shipmode', avgQty: { $avg: '$l_quantity' } } },
      ])
    })

    bench('[Agg] Complex: filter + group + sort', async () => {
      await ctx.lineitems.aggregate([
        { $match: { l_discount: { $gte: 0.05 } } },
        { $group: { _id: '$l_shipmode', total: { $sum: '$l_extendedprice' }, count: { $sum: 1 } } },
        { $sort: { total: -1 } },
      ])
    })

    bench('[Agg] Multi-level grouping', async () => {
      await ctx.lineitems.aggregate([
        {
          $group: {
            _id: { returnflag: '$l_returnflag', linestatus: '$l_linestatus' },
            sum_qty: { $sum: '$l_quantity' },
            avg_price: { $avg: '$l_extendedprice' },
            count: { $sum: 1 },
          },
        },
        { $sort: { '_id.returnflag': 1, '_id.linestatus': 1 } },
      ])
    })
  })

  // ===========================================================================
  // Query Characteristics Analysis
  // ===========================================================================
  describe('Query Characteristics', () => {
    let ctx: TPCHContext

    beforeAll(async () => {
      clearGlobalStorage()
      ctx = await createTPCHContext('chars', 0.1)
    }, 120000)

    afterAll(() => {
      clearGlobalStorage()
    })

    // Selectivity tests
    describe('Filter Selectivity', () => {
      bench('[Selectivity] High (1%): specific date', async () => {
        await ctx.lineitems.find({ l_shipdate: '1995-03-15' })
      })

      bench('[Selectivity] Medium (10%): date range month', async () => {
        await ctx.lineitems.find({
          l_shipdate: { $gte: '1995-03-01', $lt: '1995-04-01' },
        })
      })

      bench('[Selectivity] Low (50%): returnflag', async () => {
        await ctx.lineitems.find({ l_returnflag: 'N' })
      })
    })

    // Join complexity tests
    describe('Join Complexity', () => {
      bench('[Join] 2-table: orders-lineitems', async () => {
        const orders = await ctx.orders.find({ o_orderstatus: 'F' })
        const orderKeys = new Set(orders.map(o => o.o_orderkey))
        const lineitems = await ctx.lineitems.find()
        lineitems.filter(li => orderKeys.has(li.l_orderkey))
      })

      bench('[Join] 3-table: customers-orders-lineitems', async () => {
        await q3_shipping_priority(ctx)
      })

      bench('[Join] 6-table: full hierarchy', async () => {
        await q5_local_supplier_volume(ctx)
      })
    })

    // Aggregation complexity
    describe('Aggregation Complexity', () => {
      bench('[AggComplexity] Simple COUNT', async () => {
        await ctx.lineitems.aggregate([
          { $count: 'total' },
        ])
      })

      bench('[AggComplexity] GROUP BY single field', async () => {
        await ctx.lineitems.aggregate([
          { $group: { _id: '$l_shipmode', count: { $sum: 1 } } },
        ])
      })

      bench('[AggComplexity] GROUP BY multiple fields', async () => {
        await ctx.lineitems.aggregate([
          {
            $group: {
              _id: { mode: '$l_shipmode', flag: '$l_returnflag' },
              count: { $sum: 1 },
              total: { $sum: '$l_extendedprice' },
            },
          },
        ])
      })
    })
  })

  // ===========================================================================
  // Expected TPC-H Performance Characteristics
  // ===========================================================================
  describe('Performance Characteristics Report', () => {
    /**
     * TPC-H queries have well-known performance characteristics:
     *
     * Q1 (Pricing Summary):
     * - Scans ~98% of LINEITEM table
     * - Heavy aggregation (8 aggregates)
     * - Expected to be I/O bound on large datasets
     *
     * Q3 (Shipping Priority):
     * - 3-way join with moderate selectivity
     * - Benefits from indexes on c_mktsegment, o_orderdate, l_shipdate
     *
     * Q5 (Local Supplier Volume):
     * - 6-way join through region hierarchy
     * - Most join-intensive query
     * - Benefits from hash joins
     *
     * Q6 (Forecasting Revenue):
     * - Simple scan with range predicates
     * - ~1% selectivity
     * - Good index candidate
     *
     * Q10 (Returned Item Reporting):
     * - 4-way join
     * - Aggregation with GROUP BY
     * - Benefits from l_returnflag index
     *
     * Q12 (Shipping Modes):
     * - 2-way join
     * - Multiple range predicates
     * - Low selectivity on shipmode
     *
     * Q14 (Promotion Effect):
     * - 2-way join with CASE expression
     * - Single aggregate ratio
     * - Benefits from l_shipdate index
     *
     * Q19 (Discounted Revenue):
     * - Complex OR conditions
     * - Query optimizer challenge
     * - May benefit from union of indexed lookups
     */
    bench('[Characteristics] Reference: Q1 baseline', async () => {
      clearGlobalStorage()
      const ctx = await createTPCHContext('ref', 0.1)
      await q1_pricing_summary(ctx)
    }, { iterations: 5 })
  })
})

// =============================================================================
// Latency Percentile Analysis Helper
// =============================================================================

/**
 * Run a query multiple times and calculate latency percentiles
 */
async function measureQueryLatency(
  queryFn: () => Promise<unknown>,
  iterations: number = 100
): Promise<LatencyStats> {
  const samples: number[] = []

  // Warmup
  for (let i = 0; i < 5; i++) {
    await queryFn()
  }

  // Measure
  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await queryFn()
    samples.push(performance.now() - start)
  }

  return calculateLatencyStats(samples)
}

// Export for use in other benchmarks
export {
  generateTPCHData,
  createTPCHContext,
  q1_pricing_summary,
  q3_shipping_priority,
  q5_local_supplier_volume,
  q6_forecasting_revenue,
  q10_returned_item_reporting,
  q12_shipping_modes,
  q14_promotion_effect,
  q19_discounted_revenue,
  measureQueryLatency,
  type TPCHContext,
  type TPCHLineitem,
  type TPCHOrders,
  type TPCHCustomer,
  type TPCHSupplier,
  type TPCHPart,
  type TPCHPartSupp,
  type TPCHNation,
  type TPCHRegion,
}
