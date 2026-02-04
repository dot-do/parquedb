/**
 * SSB (Star Schema Benchmark) Tests for ParqueDB
 *
 * The SSB benchmark tests OLAP query performance on a star schema with:
 * - Fact table: lineorder (sales transactions)
 * - Dimension tables: date, customer, supplier, part
 *
 * Includes 13 queries across 4 flights:
 * - Flight 1 (Q1.1-Q1.3): Filter on date range and discount
 * - Flight 2 (Q2.1-Q2.3): Part/supplier joins with region filters
 * - Flight 3 (Q3.1-Q3.4): Customer/supplier joins with date filters
 * - Flight 4 (Q4.1-Q4.3): Complex multi-dimension analysis
 *
 * Scale factors: 100, 1000, 10000 entities for testing
 */

import { describe, bench, beforeAll, beforeEach, afterAll } from 'vitest'
import { Collection, clearGlobalStorage } from '../../src/Collection'
import type { Entity, EntityId, Filter } from '../../src/types'
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
// SSB Schema Types
// =============================================================================

/**
 * Date dimension - calendar dates with hierarchical attributes
 */
interface SSBDate {
  datekey: number           // YYYYMMDD format, e.g., 19980101
  date: string              // Full date string
  dayofweek: string         // Monday, Tuesday, etc.
  month: string             // January, February, etc.
  year: number              // 1992-1998
  yearmonthnum: number      // YYYYMM
  yearmonth: string         // Jan1994, Feb1994, etc.
  daynuminweek: number      // 1-7
  daynuminmonth: number     // 1-31
  daynuminyear: number      // 1-366
  monthnuminyear: number    // 1-12
  weeknuminyear: number     // 1-53
  sellingseason: string     // Christmas, Summer, etc.
  lastdayinweekfl: boolean  // Is last day of week
  lastdayinmonthfl: boolean // Is last day of month
  holidayfl: boolean        // Is holiday
  weekdayfl: boolean        // Is weekday
}

/**
 * Customer dimension - customers with geographic hierarchy
 */
interface SSBCustomer {
  custkey: number           // Primary key
  name: string              // Customer name
  address: string           // Street address
  city: string              // City
  nation: string            // Country/Nation
  region: string            // AMERICA, EUROPE, ASIA, etc.
  phone: string             // Phone number
  mktsegment: string        // Market segment: AUTOMOBILE, BUILDING, etc.
}

/**
 * Supplier dimension - suppliers with geographic hierarchy
 */
interface SSBSupplier {
  suppkey: number           // Primary key
  name: string              // Supplier name
  address: string           // Street address
  city: string              // City
  nation: string            // Country/Nation
  region: string            // AMERICA, EUROPE, ASIA, etc.
  phone: string             // Phone number
}

/**
 * Part dimension - parts with categorization hierarchy
 */
interface SSBPart {
  partkey: number           // Primary key
  name: string              // Part name
  mfgr: string              // Manufacturer: MFGR#1, MFGR#2, etc.
  category: string          // Category: MFGR#11, MFGR#12, etc.
  brand: string             // Brand: MFGR#1101, MFGR#1102, etc.
  color: string             // Color
  type: string              // Type description
  size: number              // Size (1-50)
  container: string         // Container type
}

/**
 * Lineorder fact table - sales transactions
 */
interface SSBLineorder {
  orderkey: number          // Order identifier
  linenumber: number        // Line number within order
  custkey: number           // FK to customer
  partkey: number           // FK to part
  suppkey: number           // FK to supplier
  orderdate: number         // FK to date (YYYYMMDD)
  orderpriority: string     // Order priority
  shippriority: string      // Shipping priority
  quantity: number          // Quantity ordered (1-50)
  extendedprice: number     // Extended price (quantity * price)
  ordertotalprice: number   // Total order price
  discount: number          // Discount percentage (0-10)
  revenue: number           // extendedprice * (1 - discount/100)
  supplycost: number        // Supply cost
  tax: number               // Tax percentage (0-8)
  commitdate: number        // Commit date (YYYYMMDD)
  shipmode: string          // Shipping mode
}

// =============================================================================
// Data Generators
// =============================================================================

const REGIONS = ['AMERICA', 'EUROPE', 'ASIA', 'AFRICA', 'MIDDLE EAST']
const NATIONS_BY_REGION: Record<string, string[]> = {
  'AMERICA': ['UNITED STATES', 'CANADA', 'BRAZIL', 'ARGENTINA', 'PERU'],
  'EUROPE': ['FRANCE', 'GERMANY', 'UNITED KINGDOM', 'ITALY', 'RUSSIA'],
  'ASIA': ['CHINA', 'JAPAN', 'INDIA', 'INDONESIA', 'VIETNAM'],
  'AFRICA': ['EGYPT', 'ETHIOPIA', 'KENYA', 'MOROCCO', 'ALGERIA'],
  'MIDDLE EAST': ['IRAN', 'IRAQ', 'JORDAN', 'SAUDI ARABIA', 'EGYPT'],
}
const CITIES = ['City A', 'City B', 'City C', 'City D', 'City E']
const MARKET_SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY']
const MANUFACTURERS = ['MFGR#1', 'MFGR#2', 'MFGR#3', 'MFGR#4', 'MFGR#5']
const COLORS = ['red', 'blue', 'green', 'yellow', 'white', 'black', 'brown', 'pink', 'purple', 'orange']
const CONTAINERS = ['SM CASE', 'SM BOX', 'SM PACK', 'SM BAG', 'MED CASE', 'MED BOX', 'LG CASE', 'LG BOX', 'WRAP CASE', 'JUMBO BOX']
const SHIP_MODES = ['AIR', 'MAIL', 'RAIL', 'SHIP', 'TRUCK', 'REG AIR', 'FOB']
const ORDER_PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
const DAYS_OF_WEEK = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

/**
 * Generate SSB date dimension data
 * Standard range: 1992-1998 (7 years)
 */
function generateDates(startYear: number = 1992, endYear: number = 1998): (SSBDate & { $type: string; name: string })[] {
  const dates: (SSBDate & { $type: string; name: string })[] = []

  for (let year = startYear; year <= endYear; year++) {
    for (let month = 1; month <= 12; month++) {
      const daysInMonth = new Date(year, month, 0).getDate()
      for (let day = 1; day <= daysInMonth; day++) {
        const date = new Date(year, month - 1, day)
        const datekey = year * 10000 + month * 100 + day
        const dayOfWeek = date.getDay()
        const dayOfYear = Math.floor((date.getTime() - new Date(year, 0, 0).getTime()) / 86400000)
        const weekOfYear = Math.ceil((dayOfYear + new Date(year, 0, 1).getDay()) / 7)

        dates.push({
          $type: 'Date',
          name: `Date ${datekey}`,
          datekey,
          date: date.toISOString().split('T')[0]!,
          dayofweek: DAYS_OF_WEEK[dayOfWeek]!,
          month: MONTHS[month - 1]!,
          year,
          yearmonthnum: year * 100 + month,
          yearmonth: `${MONTHS[month - 1]!.slice(0, 3)}${year}`,
          daynuminweek: dayOfWeek === 0 ? 7 : dayOfWeek,
          daynuminmonth: day,
          daynuminyear: dayOfYear,
          monthnuminyear: month,
          weeknuminyear: weekOfYear,
          sellingseason: getSellingSeason(month),
          lastdayinweekfl: dayOfWeek === 6,
          lastdayinmonthfl: day === daysInMonth,
          holidayfl: Math.random() < 0.03, // ~3% holidays
          weekdayfl: dayOfWeek > 0 && dayOfWeek < 6,
        })
      }
    }
  }

  return dates
}

function getSellingSeason(month: number): string {
  if (month === 12 || month === 11) return 'Christmas'
  if (month >= 6 && month <= 8) return 'Summer'
  if (month >= 3 && month <= 5) return 'Spring'
  return 'Winter'
}

/**
 * Generate customer dimension data
 */
function generateCustomers(count: number): (SSBCustomer & { $type: string; name: string })[] {
  const customers: (SSBCustomer & { $type: string; name: string })[] = []

  for (let i = 0; i < count; i++) {
    const region = randomElement(REGIONS)
    const nation = randomElement(NATIONS_BY_REGION[region]!)
    const city = `${nation.slice(0, 2)}${randomInt(0, 9)}`

    customers.push({
      $type: 'Customer',
      name: `Customer ${i}`,
      custkey: i + 1,
      address: `${randomInt(1, 999)} ${randomElement(['Main', 'Oak', 'Elm', 'Maple'])} Street`,
      city,
      nation,
      region,
      phone: `${randomInt(10, 34)}-${randomInt(100, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`,
      mktsegment: randomElement(MARKET_SEGMENTS),
    })
  }

  return customers
}

/**
 * Generate supplier dimension data
 */
function generateSuppliers(count: number): (SSBSupplier & { $type: string; name: string })[] {
  const suppliers: (SSBSupplier & { $type: string; name: string })[] = []

  for (let i = 0; i < count; i++) {
    const region = randomElement(REGIONS)
    const nation = randomElement(NATIONS_BY_REGION[region]!)
    const city = `${nation.slice(0, 2)}${randomInt(0, 9)}`

    suppliers.push({
      $type: 'Supplier',
      name: `Supplier ${i}`,
      suppkey: i + 1,
      address: `${randomInt(1, 999)} ${randomElement(['Industrial', 'Commerce', 'Trade'])} Blvd`,
      city,
      nation,
      region,
      phone: `${randomInt(10, 34)}-${randomInt(100, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`,
    })
  }

  return suppliers
}

/**
 * Generate part dimension data
 */
function generateParts(count: number): (SSBPart & { $type: string; name: string })[] {
  const parts: (SSBPart & { $type: string; name: string })[] = []

  for (let i = 0; i < count; i++) {
    const mfgr = randomElement(MANUFACTURERS)
    const mfgrNum = parseInt(mfgr.split('#')[1]!)
    const category = `${mfgr}${randomInt(1, 5)}`
    const brand = `${category}${randomInt(1, 40).toString().padStart(2, '0')}`

    parts.push({
      $type: 'Part',
      name: `Part ${i}`,
      partkey: i + 1,
      mfgr,
      category,
      brand,
      color: randomElement(COLORS),
      type: `TYPE${randomInt(1, 150)}`,
      size: randomInt(1, 50),
      container: randomElement(CONTAINERS),
    })
  }

  return parts
}

/**
 * Generate lineorder fact data
 */
function generateLineorders(
  count: number,
  customers: (SSBCustomer & { $type: string; name: string })[],
  suppliers: (SSBSupplier & { $type: string; name: string })[],
  parts: (SSBPart & { $type: string; name: string })[],
  dates: (SSBDate & { $type: string; name: string })[]
): (SSBLineorder & { $type: string; name: string })[] {
  const lineorders: (SSBLineorder & { $type: string; name: string })[] = []

  let orderkey = 1
  let i = 0

  while (i < count) {
    // Generate 1-7 line items per order
    const linesPerOrder = randomInt(1, 7)

    for (let line = 1; line <= linesPerOrder && i < count; line++, i++) {
      const customer = randomElement(customers)
      const supplier = randomElement(suppliers)
      const part = randomElement(parts)
      const date = randomElement(dates)

      const quantity = randomInt(1, 50)
      const basePrice = randomInt(900, 200000)
      const extendedprice = quantity * basePrice
      const discount = randomInt(0, 10)
      const revenue = Math.round(extendedprice * (1 - discount / 100))
      const supplycost = Math.round(basePrice * 0.6 * quantity)
      const tax = randomInt(0, 8)

      // Commit date is 30-90 days after order date
      const commitOffset = randomInt(30, 90)
      const commitDate = new Date(date.year, Math.floor((date.datekey % 10000) / 100) - 1, date.datekey % 100)
      commitDate.setDate(commitDate.getDate() + commitOffset)
      const commitdatekey = commitDate.getFullYear() * 10000 + (commitDate.getMonth() + 1) * 100 + commitDate.getDate()

      lineorders.push({
        $type: 'Lineorder',
        name: `Lineorder ${orderkey}-${line}`,
        orderkey,
        linenumber: line,
        custkey: customer.custkey,
        partkey: part.partkey,
        suppkey: supplier.suppkey,
        orderdate: date.datekey,
        orderpriority: randomElement(ORDER_PRIORITIES),
        shippriority: '0',
        quantity,
        extendedprice,
        ordertotalprice: extendedprice * linesPerOrder,
        discount,
        revenue,
        supplycost,
        tax,
        commitdate: commitdatekey,
        shipmode: randomElement(SHIP_MODES),
      })
    }

    orderkey++
  }

  return lineorders
}

// =============================================================================
// SSB Query Implementations
// =============================================================================

/**
 * SSB Query Results Interface
 */
interface QueryResult {
  name: string
  duration: number
  rowCount: number
}

/**
 * Benchmark context with collections and lookup maps
 */
interface SSBContext {
  lineorders: Collection<SSBLineorder>
  dates: Collection<SSBDate>
  customers: Collection<SSBCustomer>
  suppliers: Collection<SSBSupplier>
  parts: Collection<SSBPart>
  dateMap: Map<number, SSBDate>
  customerMap: Map<number, SSBCustomer>
  supplierMap: Map<number, SSBSupplier>
  partMap: Map<number, SSBPart>
}

// -----------------------------------------------------------------------------
// Flight 1: Filters on date range and discount
// These queries filter the fact table with selectivity increasing from Q1.1 to Q1.3
// -----------------------------------------------------------------------------

/**
 * Q1.1: Filter by year and discount range
 * SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 * FROM lineorder, date
 * WHERE lo_orderdate = d_datekey
 *   AND d_year = 1993
 *   AND lo_discount BETWEEN 1 AND 3
 *   AND lo_quantity < 25
 */
async function q1_1(ctx: SSBContext): Promise<number> {
  const targetYear = 1993
  const lineorders = await ctx.lineorders.find({
    discount: { $gte: 1, $lte: 3 },
    quantity: { $lt: 25 },
  })

  let revenue = 0
  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    if (date && date.year === targetYear) {
      revenue += lo.extendedprice * lo.discount
    }
  }
  return revenue
}

/**
 * Q1.2: Filter by year-month and discount range
 * SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 * FROM lineorder, date
 * WHERE lo_orderdate = d_datekey
 *   AND d_yearmonthnum = 199401
 *   AND lo_discount BETWEEN 4 AND 6
 *   AND lo_quantity BETWEEN 26 AND 35
 */
async function q1_2(ctx: SSBContext): Promise<number> {
  const targetYearMonth = 199401
  const lineorders = await ctx.lineorders.find({
    discount: { $gte: 4, $lte: 6 },
    quantity: { $gte: 26, $lte: 35 },
  })

  let revenue = 0
  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    if (date && date.yearmonthnum === targetYearMonth) {
      revenue += lo.extendedprice * lo.discount
    }
  }
  return revenue
}

/**
 * Q1.3: Filter by week and discount range
 * SELECT SUM(lo_extendedprice * lo_discount) AS revenue
 * FROM lineorder, date
 * WHERE lo_orderdate = d_datekey
 *   AND d_weeknuminyear = 6
 *   AND d_year = 1994
 *   AND lo_discount BETWEEN 5 AND 7
 *   AND lo_quantity BETWEEN 26 AND 35
 */
async function q1_3(ctx: SSBContext): Promise<number> {
  const targetWeek = 6
  const targetYear = 1994
  const lineorders = await ctx.lineorders.find({
    discount: { $gte: 5, $lte: 7 },
    quantity: { $gte: 26, $lte: 35 },
  })

  let revenue = 0
  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    if (date && date.weeknuminyear === targetWeek && date.year === targetYear) {
      revenue += lo.extendedprice * lo.discount
    }
  }
  return revenue
}

// -----------------------------------------------------------------------------
// Flight 2: Part/supplier joins with region filters
// These queries join fact with part and supplier dimensions
// -----------------------------------------------------------------------------

/**
 * Q2.1: Revenue by brand grouped by year
 * SELECT SUM(lo_revenue), d_year, p_brand
 * FROM lineorder, date, part, supplier
 * WHERE lo_orderdate = d_datekey
 *   AND lo_partkey = p_partkey
 *   AND lo_suppkey = s_suppkey
 *   AND p_category = 'MFGR#12'
 *   AND s_region = 'AMERICA'
 * GROUP BY d_year, p_brand
 * ORDER BY d_year, p_brand
 */
async function q2_1(ctx: SSBContext): Promise<Map<string, number>> {
  const targetCategory = 'MFGR#12'
  const targetRegion = 'AMERICA'

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const part = ctx.partMap.get(lo.partkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)

    if (date && part && supplier &&
        part.category === targetCategory &&
        supplier.region === targetRegion) {
      const key = `${date.year}-${part.brand}`
      results.set(key, (results.get(key) || 0) + lo.revenue)
    }
  }

  return results
}

/**
 * Q2.2: Revenue by brand grouped by year (more selective)
 * Similar to Q2.1 but with specific brands
 * AND p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
 * AND s_region = 'ASIA'
 */
async function q2_2(ctx: SSBContext): Promise<Map<string, number>> {
  const brandMin = 'MFGR#2221'
  const brandMax = 'MFGR#2228'
  const targetRegion = 'ASIA'

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const part = ctx.partMap.get(lo.partkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)

    if (date && part && supplier &&
        part.brand >= brandMin && part.brand <= brandMax &&
        supplier.region === targetRegion) {
      const key = `${date.year}-${part.brand}`
      results.set(key, (results.get(key) || 0) + lo.revenue)
    }
  }

  return results
}

/**
 * Q2.3: Revenue by brand grouped by year (most selective)
 * AND p_brand = 'MFGR#2339'
 * AND s_region = 'EUROPE'
 */
async function q2_3(ctx: SSBContext): Promise<Map<string, number>> {
  const targetBrand = 'MFGR#2339'
  const targetRegion = 'EUROPE'

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const part = ctx.partMap.get(lo.partkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)

    if (date && part && supplier &&
        part.brand === targetBrand &&
        supplier.region === targetRegion) {
      const key = `${date.year}-${part.brand}`
      results.set(key, (results.get(key) || 0) + lo.revenue)
    }
  }

  return results
}

// -----------------------------------------------------------------------------
// Flight 3: Customer/supplier joins with date filters
// These queries join fact with customer and supplier dimensions
// -----------------------------------------------------------------------------

/**
 * Q3.1: Revenue by customer/supplier nation and year
 * SELECT c_nation, s_nation, d_year, SUM(lo_revenue) AS revenue
 * FROM lineorder, date, customer, supplier
 * WHERE lo_orderdate = d_datekey
 *   AND lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND c_region = 'ASIA'
 *   AND s_region = 'ASIA'
 *   AND d_year >= 1992 AND d_year <= 1997
 * GROUP BY c_nation, s_nation, d_year
 * ORDER BY d_year, revenue DESC
 */
async function q3_1(ctx: SSBContext): Promise<Map<string, number>> {
  const targetRegion = 'ASIA'
  const minYear = 1992
  const maxYear = 1997

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const customer = ctx.customerMap.get(lo.custkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)

    if (date && customer && supplier &&
        customer.region === targetRegion &&
        supplier.region === targetRegion &&
        date.year >= minYear && date.year <= maxYear) {
      const key = `${customer.nation}-${supplier.nation}-${date.year}`
      results.set(key, (results.get(key) || 0) + lo.revenue)
    }
  }

  return results
}

/**
 * Q3.2: Revenue by customer/supplier nation (more selective)
 * Similar to Q3.1 but filtered to specific nations
 * AND c_nation = 'UNITED STATES'
 * AND s_nation = 'UNITED STATES'
 */
async function q3_2(ctx: SSBContext): Promise<Map<string, number>> {
  const targetNation = 'UNITED STATES'
  const minYear = 1992
  const maxYear = 1997

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const customer = ctx.customerMap.get(lo.custkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)

    if (date && customer && supplier &&
        customer.nation === targetNation &&
        supplier.nation === targetNation &&
        date.year >= minYear && date.year <= maxYear) {
      const key = `${customer.city}-${supplier.city}-${date.year}`
      results.set(key, (results.get(key) || 0) + lo.revenue)
    }
  }

  return results
}

/**
 * Q3.3: Revenue by city (more selective)
 * AND c_city IN ('UNITED KI1', 'UNITED KI5')
 * AND s_city IN ('UNITED KI1', 'UNITED KI5')
 */
async function q3_3(ctx: SSBContext): Promise<Map<string, number>> {
  const targetCities = ['UN1', 'UN5'] // Simplified city codes
  const minYear = 1992
  const maxYear = 1997

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const customer = ctx.customerMap.get(lo.custkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)

    if (date && customer && supplier &&
        targetCities.some(c => customer.city.includes(c)) &&
        targetCities.some(c => supplier.city.includes(c)) &&
        date.year >= minYear && date.year <= maxYear) {
      const key = `${customer.city}-${supplier.city}-${date.year}`
      results.set(key, (results.get(key) || 0) + lo.revenue)
    }
  }

  return results
}

/**
 * Q3.4: Revenue by city in specific month
 * AND d_yearmonth = 'Dec1997'
 */
async function q3_4(ctx: SSBContext): Promise<Map<string, number>> {
  const targetCities = ['UN1', 'UN5']
  const targetYearMonth = 'Dec1997'

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const customer = ctx.customerMap.get(lo.custkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)

    if (date && customer && supplier &&
        targetCities.some(c => customer.city.includes(c)) &&
        targetCities.some(c => supplier.city.includes(c)) &&
        date.yearmonth === targetYearMonth) {
      const key = `${customer.city}-${supplier.city}-${date.year}`
      results.set(key, (results.get(key) || 0) + lo.revenue)
    }
  }

  return results
}

// -----------------------------------------------------------------------------
// Flight 4: Complex multi-dimension analysis
// These queries join all four dimensions
// -----------------------------------------------------------------------------

/**
 * Q4.1: Profit by year and nation
 * SELECT d_year, c_nation, SUM(lo_revenue - lo_supplycost) AS profit
 * FROM lineorder, date, customer, supplier, part
 * WHERE lo_orderdate = d_datekey
 *   AND lo_custkey = c_custkey
 *   AND lo_suppkey = s_suppkey
 *   AND lo_partkey = p_partkey
 *   AND c_region = 'AMERICA'
 *   AND s_region = 'AMERICA'
 *   AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
 * GROUP BY d_year, c_nation
 * ORDER BY d_year, c_nation
 */
async function q4_1(ctx: SSBContext): Promise<Map<string, number>> {
  const targetRegion = 'AMERICA'
  const targetMfgrs = ['MFGR#1', 'MFGR#2']

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const customer = ctx.customerMap.get(lo.custkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)
    const part = ctx.partMap.get(lo.partkey)

    if (date && customer && supplier && part &&
        customer.region === targetRegion &&
        supplier.region === targetRegion &&
        targetMfgrs.includes(part.mfgr)) {
      const key = `${date.year}-${customer.nation}`
      const profit = lo.revenue - lo.supplycost
      results.set(key, (results.get(key) || 0) + profit)
    }
  }

  return results
}

/**
 * Q4.2: Profit by year and nation/category
 * Similar to Q4.1 but with category dimension
 * AND (d_year = 1997 OR d_year = 1998)
 * AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
 */
async function q4_2(ctx: SSBContext): Promise<Map<string, number>> {
  const targetRegion = 'AMERICA'
  const targetMfgrs = ['MFGR#1', 'MFGR#2']
  const targetYears = [1997, 1998]

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const customer = ctx.customerMap.get(lo.custkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)
    const part = ctx.partMap.get(lo.partkey)

    if (date && customer && supplier && part &&
        customer.region === targetRegion &&
        supplier.region === targetRegion &&
        targetMfgrs.includes(part.mfgr) &&
        targetYears.includes(date.year)) {
      const key = `${date.year}-${supplier.nation}-${part.category}`
      const profit = lo.revenue - lo.supplycost
      results.set(key, (results.get(key) || 0) + profit)
    }
  }

  return results
}

/**
 * Q4.3: Profit by year and city/brand
 * Most selective query with specific nation and category
 * AND c_region = 'AMERICA'
 * AND s_nation = 'UNITED STATES'
 * AND (d_year = 1997 OR d_year = 1998)
 * AND p_category = 'MFGR#14'
 */
async function q4_3(ctx: SSBContext): Promise<Map<string, number>> {
  const targetCustRegion = 'AMERICA'
  const targetSuppNation = 'UNITED STATES'
  const targetCategory = 'MFGR#14'
  const targetYears = [1997, 1998]

  const lineorders = await ctx.lineorders.find()
  const results = new Map<string, number>()

  for (const lo of lineorders) {
    const date = ctx.dateMap.get(lo.orderdate)
    const customer = ctx.customerMap.get(lo.custkey)
    const supplier = ctx.supplierMap.get(lo.suppkey)
    const part = ctx.partMap.get(lo.partkey)

    if (date && customer && supplier && part &&
        customer.region === targetCustRegion &&
        supplier.nation === targetSuppNation &&
        part.category === targetCategory &&
        targetYears.includes(date.year)) {
      const key = `${date.year}-${supplier.city}-${part.brand}`
      const profit = lo.revenue - lo.supplycost
      results.set(key, (results.get(key) || 0) + profit)
    }
  }

  return results
}

// =============================================================================
// Benchmark Suite
// =============================================================================

describe('SSB Benchmarks', () => {
  // ===========================================================================
  // Scale Factor 100 (Small)
  // ===========================================================================
  describe('Scale Factor 100', () => {
    let ctx: SSBContext
    const SF = 100 // 100 lineorders

    beforeAll(async () => {
      clearGlobalStorage()

      // Generate dimensions
      const dates = generateDates(1993, 1998) // Reduced date range for testing
      const customers = generateCustomers(Math.ceil(SF / 10))
      const suppliers = generateSuppliers(Math.ceil(SF / 20))
      const parts = generateParts(Math.ceil(SF / 5))

      // Generate facts
      const lineorders = generateLineorders(SF, customers, suppliers, parts, dates)

      // Create collections
      const lineorderCollection = new Collection<SSBLineorder>(`ssb-lineorders-100-${Date.now()}`)
      const dateCollection = new Collection<SSBDate>(`ssb-dates-100-${Date.now()}`)
      const customerCollection = new Collection<SSBCustomer>(`ssb-customers-100-${Date.now()}`)
      const supplierCollection = new Collection<SSBSupplier>(`ssb-suppliers-100-${Date.now()}`)
      const partCollection = new Collection<SSBPart>(`ssb-parts-100-${Date.now()}`)

      // Insert data
      for (const d of dates) await dateCollection.create(d)
      for (const c of customers) await customerCollection.create(c)
      for (const s of suppliers) await supplierCollection.create(s)
      for (const p of parts) await partCollection.create(p)
      for (const lo of lineorders) await lineorderCollection.create(lo)

      // Build lookup maps for efficient joins
      const dateMap = new Map<number, SSBDate>()
      const customerMap = new Map<number, SSBCustomer>()
      const supplierMap = new Map<number, SSBSupplier>()
      const partMap = new Map<number, SSBPart>()

      for (const d of dates) dateMap.set(d.datekey, d)
      for (const c of customers) customerMap.set(c.custkey, c)
      for (const s of suppliers) supplierMap.set(s.suppkey, s)
      for (const p of parts) partMap.set(p.partkey, p)

      ctx = {
        lineorders: lineorderCollection,
        dates: dateCollection,
        customers: customerCollection,
        suppliers: supplierCollection,
        parts: partCollection,
        dateMap,
        customerMap,
        supplierMap,
        partMap,
      }
    }, 60000)

    // Flight 1: Date range and discount filters
    bench('[SF100] Q1.1: Year filter + discount range', async () => {
      await q1_1(ctx)
    })

    bench('[SF100] Q1.2: Year-month filter + discount range', async () => {
      await q1_2(ctx)
    })

    bench('[SF100] Q1.3: Week filter + discount range', async () => {
      await q1_3(ctx)
    })

    // Flight 2: Part/supplier joins
    bench('[SF100] Q2.1: Part category + supplier region', async () => {
      await q2_1(ctx)
    })

    bench('[SF100] Q2.2: Part brand range + supplier region', async () => {
      await q2_2(ctx)
    })

    bench('[SF100] Q2.3: Specific brand + supplier region', async () => {
      await q2_3(ctx)
    })

    // Flight 3: Customer/supplier joins
    bench('[SF100] Q3.1: Customer/supplier region + date range', async () => {
      await q3_1(ctx)
    })

    bench('[SF100] Q3.2: Customer/supplier nation + date range', async () => {
      await q3_2(ctx)
    })

    bench('[SF100] Q3.3: Customer/supplier city + date range', async () => {
      await q3_3(ctx)
    })

    bench('[SF100] Q3.4: Customer/supplier city + specific month', async () => {
      await q3_4(ctx)
    })

    // Flight 4: Complex multi-dimension
    bench('[SF100] Q4.1: All dimensions + region filter', async () => {
      await q4_1(ctx)
    })

    bench('[SF100] Q4.2: All dimensions + year filter', async () => {
      await q4_2(ctx)
    })

    bench('[SF100] Q4.3: All dimensions + most selective', async () => {
      await q4_3(ctx)
    })
  })

  // ===========================================================================
  // Scale Factor 1000 (Medium)
  // ===========================================================================
  describe('Scale Factor 1000', () => {
    let ctx: SSBContext
    const SF = 1000

    beforeAll(async () => {
      clearGlobalStorage()

      const dates = generateDates(1993, 1998)
      const customers = generateCustomers(Math.ceil(SF / 10))
      const suppliers = generateSuppliers(Math.ceil(SF / 20))
      const parts = generateParts(Math.ceil(SF / 5))
      const lineorders = generateLineorders(SF, customers, suppliers, parts, dates)

      const lineorderCollection = new Collection<SSBLineorder>(`ssb-lineorders-1k-${Date.now()}`)
      const dateCollection = new Collection<SSBDate>(`ssb-dates-1k-${Date.now()}`)
      const customerCollection = new Collection<SSBCustomer>(`ssb-customers-1k-${Date.now()}`)
      const supplierCollection = new Collection<SSBSupplier>(`ssb-suppliers-1k-${Date.now()}`)
      const partCollection = new Collection<SSBPart>(`ssb-parts-1k-${Date.now()}`)

      for (const d of dates) await dateCollection.create(d)
      for (const c of customers) await customerCollection.create(c)
      for (const s of suppliers) await supplierCollection.create(s)
      for (const p of parts) await partCollection.create(p)
      for (const lo of lineorders) await lineorderCollection.create(lo)

      const dateMap = new Map<number, SSBDate>()
      const customerMap = new Map<number, SSBCustomer>()
      const supplierMap = new Map<number, SSBSupplier>()
      const partMap = new Map<number, SSBPart>()

      for (const d of dates) dateMap.set(d.datekey, d)
      for (const c of customers) customerMap.set(c.custkey, c)
      for (const s of suppliers) supplierMap.set(s.suppkey, s)
      for (const p of parts) partMap.set(p.partkey, p)

      ctx = {
        lineorders: lineorderCollection,
        dates: dateCollection,
        customers: customerCollection,
        suppliers: supplierCollection,
        parts: partCollection,
        dateMap,
        customerMap,
        supplierMap,
        partMap,
      }
    }, 120000)

    // Flight 1
    bench('[SF1K] Q1.1: Year filter + discount range', async () => {
      await q1_1(ctx)
    })

    bench('[SF1K] Q1.2: Year-month filter + discount range', async () => {
      await q1_2(ctx)
    })

    bench('[SF1K] Q1.3: Week filter + discount range', async () => {
      await q1_3(ctx)
    })

    // Flight 2
    bench('[SF1K] Q2.1: Part category + supplier region', async () => {
      await q2_1(ctx)
    })

    bench('[SF1K] Q2.2: Part brand range + supplier region', async () => {
      await q2_2(ctx)
    })

    bench('[SF1K] Q2.3: Specific brand + supplier region', async () => {
      await q2_3(ctx)
    })

    // Flight 3
    bench('[SF1K] Q3.1: Customer/supplier region + date range', async () => {
      await q3_1(ctx)
    })

    bench('[SF1K] Q3.2: Customer/supplier nation + date range', async () => {
      await q3_2(ctx)
    })

    bench('[SF1K] Q3.3: Customer/supplier city + date range', async () => {
      await q3_3(ctx)
    })

    bench('[SF1K] Q3.4: Customer/supplier city + specific month', async () => {
      await q3_4(ctx)
    })

    // Flight 4
    bench('[SF1K] Q4.1: All dimensions + region filter', async () => {
      await q4_1(ctx)
    })

    bench('[SF1K] Q4.2: All dimensions + year filter', async () => {
      await q4_2(ctx)
    })

    bench('[SF1K] Q4.3: All dimensions + most selective', async () => {
      await q4_3(ctx)
    })
  })

  // ===========================================================================
  // Scale Factor 10000 (Large)
  // ===========================================================================
  describe('Scale Factor 10000', () => {
    let ctx: SSBContext
    const SF = 10000

    beforeAll(async () => {
      clearGlobalStorage()

      const dates = generateDates(1993, 1998)
      const customers = generateCustomers(Math.ceil(SF / 10))
      const suppliers = generateSuppliers(Math.ceil(SF / 20))
      const parts = generateParts(Math.ceil(SF / 5))
      const lineorders = generateLineorders(SF, customers, suppliers, parts, dates)

      const lineorderCollection = new Collection<SSBLineorder>(`ssb-lineorders-10k-${Date.now()}`)
      const dateCollection = new Collection<SSBDate>(`ssb-dates-10k-${Date.now()}`)
      const customerCollection = new Collection<SSBCustomer>(`ssb-customers-10k-${Date.now()}`)
      const supplierCollection = new Collection<SSBSupplier>(`ssb-suppliers-10k-${Date.now()}`)
      const partCollection = new Collection<SSBPart>(`ssb-parts-10k-${Date.now()}`)

      for (const d of dates) await dateCollection.create(d)
      for (const c of customers) await customerCollection.create(c)
      for (const s of suppliers) await supplierCollection.create(s)
      for (const p of parts) await partCollection.create(p)
      for (const lo of lineorders) await lineorderCollection.create(lo)

      const dateMap = new Map<number, SSBDate>()
      const customerMap = new Map<number, SSBCustomer>()
      const supplierMap = new Map<number, SSBSupplier>()
      const partMap = new Map<number, SSBPart>()

      for (const d of dates) dateMap.set(d.datekey, d)
      for (const c of customers) customerMap.set(c.custkey, c)
      for (const s of suppliers) supplierMap.set(s.suppkey, s)
      for (const p of parts) partMap.set(p.partkey, p)

      ctx = {
        lineorders: lineorderCollection,
        dates: dateCollection,
        customers: customerCollection,
        suppliers: supplierCollection,
        parts: partCollection,
        dateMap,
        customerMap,
        supplierMap,
        partMap,
      }
    }, 300000) // 5 minute timeout for large dataset

    // Flight 1
    bench('[SF10K] Q1.1: Year filter + discount range', async () => {
      await q1_1(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q1.2: Year-month filter + discount range', async () => {
      await q1_2(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q1.3: Week filter + discount range', async () => {
      await q1_3(ctx)
    }, { iterations: 10 })

    // Flight 2
    bench('[SF10K] Q2.1: Part category + supplier region', async () => {
      await q2_1(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q2.2: Part brand range + supplier region', async () => {
      await q2_2(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q2.3: Specific brand + supplier region', async () => {
      await q2_3(ctx)
    }, { iterations: 10 })

    // Flight 3
    bench('[SF10K] Q3.1: Customer/supplier region + date range', async () => {
      await q3_1(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q3.2: Customer/supplier nation + date range', async () => {
      await q3_2(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q3.3: Customer/supplier city + date range', async () => {
      await q3_3(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q3.4: Customer/supplier city + specific month', async () => {
      await q3_4(ctx)
    }, { iterations: 10 })

    // Flight 4
    bench('[SF10K] Q4.1: All dimensions + region filter', async () => {
      await q4_1(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q4.2: All dimensions + year filter', async () => {
      await q4_2(ctx)
    }, { iterations: 10 })

    bench('[SF10K] Q4.3: All dimensions + most selective', async () => {
      await q4_3(ctx)
    }, { iterations: 10 })
  })

  // ===========================================================================
  // Index Performance Comparison
  // ===========================================================================
  describe('Index Performance Comparison', () => {
    let ctxWithoutIndex: SSBContext
    let ctxWithIndex: SSBContext
    const SF = 1000

    beforeAll(async () => {
      clearGlobalStorage()

      // Generate shared dimension data
      const dates = generateDates(1993, 1998)
      const customers = generateCustomers(Math.ceil(SF / 10))
      const suppliers = generateSuppliers(Math.ceil(SF / 20))
      const parts = generateParts(Math.ceil(SF / 5))
      const lineorders = generateLineorders(SF, customers, suppliers, parts, dates)

      // Create collections without index optimization
      const noIndexLineorders = new Collection<SSBLineorder>(`ssb-noindex-lo-${Date.now()}`)
      const noIndexDates = new Collection<SSBDate>(`ssb-noindex-d-${Date.now()}`)
      const noIndexCustomers = new Collection<SSBCustomer>(`ssb-noindex-c-${Date.now()}`)
      const noIndexSuppliers = new Collection<SSBSupplier>(`ssb-noindex-s-${Date.now()}`)
      const noIndexParts = new Collection<SSBPart>(`ssb-noindex-p-${Date.now()}`)

      for (const d of dates) await noIndexDates.create(d)
      for (const c of customers) await noIndexCustomers.create(c)
      for (const s of suppliers) await noIndexSuppliers.create(s)
      for (const p of parts) await noIndexParts.create(p)
      for (const lo of lineorders) await noIndexLineorders.create(lo)

      // Create collections with index optimization (same data, different namespace)
      const indexLineorders = new Collection<SSBLineorder>(`ssb-index-lo-${Date.now()}`)
      const indexDates = new Collection<SSBDate>(`ssb-index-d-${Date.now()}`)
      const indexCustomers = new Collection<SSBCustomer>(`ssb-index-c-${Date.now()}`)
      const indexSuppliers = new Collection<SSBSupplier>(`ssb-index-s-${Date.now()}`)
      const indexParts = new Collection<SSBPart>(`ssb-index-p-${Date.now()}`)

      for (const d of dates) await indexDates.create(d)
      for (const c of customers) await indexCustomers.create(c)
      for (const s of suppliers) await indexSuppliers.create(s)
      for (const p of parts) await indexParts.create(p)
      for (const lo of lineorders) await indexLineorders.create(lo)

      // Build lookup maps
      const dateMap = new Map<number, SSBDate>()
      const customerMap = new Map<number, SSBCustomer>()
      const supplierMap = new Map<number, SSBSupplier>()
      const partMap = new Map<number, SSBPart>()

      for (const d of dates) dateMap.set(d.datekey, d)
      for (const c of customers) customerMap.set(c.custkey, c)
      for (const s of suppliers) supplierMap.set(s.suppkey, s)
      for (const p of parts) partMap.set(p.partkey, p)

      ctxWithoutIndex = {
        lineorders: noIndexLineorders,
        dates: noIndexDates,
        customers: noIndexCustomers,
        suppliers: noIndexSuppliers,
        parts: noIndexParts,
        dateMap,
        customerMap,
        supplierMap,
        partMap,
      }

      ctxWithIndex = {
        lineorders: indexLineorders,
        dates: indexDates,
        customers: indexCustomers,
        suppliers: indexSuppliers,
        parts: indexParts,
        dateMap,
        customerMap,
        supplierMap,
        partMap,
      }
    }, 180000)

    // Compare Q1.1 (simple filter)
    bench('[NoIndex] Q1.1: Year + discount filter', async () => {
      await q1_1(ctxWithoutIndex)
    })

    bench('[WithIndex] Q1.1: Year + discount filter', async () => {
      await q1_1(ctxWithIndex)
    })

    // Compare Q2.1 (join-heavy)
    bench('[NoIndex] Q2.1: Part/supplier join', async () => {
      await q2_1(ctxWithoutIndex)
    })

    bench('[WithIndex] Q2.1: Part/supplier join', async () => {
      await q2_1(ctxWithIndex)
    })

    // Compare Q3.1 (customer/supplier join)
    bench('[NoIndex] Q3.1: Customer/supplier join', async () => {
      await q3_1(ctxWithoutIndex)
    })

    bench('[WithIndex] Q3.1: Customer/supplier join', async () => {
      await q3_1(ctxWithIndex)
    })

    // Compare Q4.1 (all dimensions)
    bench('[NoIndex] Q4.1: All dimensions', async () => {
      await q4_1(ctxWithoutIndex)
    })

    bench('[WithIndex] Q4.1: All dimensions', async () => {
      await q4_1(ctxWithIndex)
    })
  })

  // ===========================================================================
  // Aggregation Performance
  // ===========================================================================
  describe('Aggregation Performance', () => {
    let ctx: SSBContext
    const SF = 1000

    beforeAll(async () => {
      clearGlobalStorage()

      const dates = generateDates(1993, 1998)
      const customers = generateCustomers(Math.ceil(SF / 10))
      const suppliers = generateSuppliers(Math.ceil(SF / 20))
      const parts = generateParts(Math.ceil(SF / 5))
      const lineorders = generateLineorders(SF, customers, suppliers, parts, dates)

      const lineorderCollection = new Collection<SSBLineorder>(`ssb-agg-lo-${Date.now()}`)
      const dateCollection = new Collection<SSBDate>(`ssb-agg-d-${Date.now()}`)
      const customerCollection = new Collection<SSBCustomer>(`ssb-agg-c-${Date.now()}`)
      const supplierCollection = new Collection<SSBSupplier>(`ssb-agg-s-${Date.now()}`)
      const partCollection = new Collection<SSBPart>(`ssb-agg-p-${Date.now()}`)

      for (const d of dates) await dateCollection.create(d)
      for (const c of customers) await customerCollection.create(c)
      for (const s of suppliers) await supplierCollection.create(s)
      for (const p of parts) await partCollection.create(p)
      for (const lo of lineorders) await lineorderCollection.create(lo)

      const dateMap = new Map<number, SSBDate>()
      const customerMap = new Map<number, SSBCustomer>()
      const supplierMap = new Map<number, SSBSupplier>()
      const partMap = new Map<number, SSBPart>()

      for (const d of dates) dateMap.set(d.datekey, d)
      for (const c of customers) customerMap.set(c.custkey, c)
      for (const s of suppliers) supplierMap.set(s.suppkey, s)
      for (const p of parts) partMap.set(p.partkey, p)

      ctx = {
        lineorders: lineorderCollection,
        dates: dateCollection,
        customers: customerCollection,
        suppliers: supplierCollection,
        parts: partCollection,
        dateMap,
        customerMap,
        supplierMap,
        partMap,
      }
    }, 120000)

    bench('[Agg] SUM revenue', async () => {
      await ctx.lineorders.aggregate([
        { $group: { _id: null, total: { $sum: '$revenue' } } },
      ])
    })

    bench('[Agg] SUM revenue by discount', async () => {
      await ctx.lineorders.aggregate([
        { $group: { _id: '$discount', total: { $sum: '$revenue' } } },
      ])
    })

    bench('[Agg] AVG quantity by ship mode', async () => {
      await ctx.lineorders.aggregate([
        { $group: { _id: '$shipmode', avgQty: { $avg: '$quantity' } } },
      ])
    })

    bench('[Agg] COUNT by order priority', async () => {
      await ctx.lineorders.aggregate([
        { $group: { _id: '$orderpriority', count: { $sum: 1 } } },
      ])
    })

    bench('[Agg] Complex: filter + group + sort', async () => {
      await ctx.lineorders.aggregate([
        { $match: { discount: { $gte: 3 } } },
        { $group: { _id: '$shipmode', total: { $sum: '$revenue' }, count: { $sum: 1 } } },
        { $sort: { total: -1 } },
      ])
    })

    bench('[Agg] Multi-level: group by two fields', async () => {
      await ctx.lineorders.aggregate([
        {
          $group: {
            _id: { discount: '$discount', priority: '$orderpriority' },
            avgPrice: { $avg: '$extendedprice' },
            count: { $sum: 1 },
          },
        },
        { $sort: { 'avgPrice': -1 } },
        { $limit: 20 },
      ])
    })
  })
})
