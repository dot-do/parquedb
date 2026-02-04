/**
 * SSB Data Generator for ParqueDB
 *
 * Generates synthetic Star Schema Benchmark data at various scale factors.
 * Based on the official SSB specification.
 */

import { DB, type DBInstance } from '../../src/db'
import { FsBackend } from '../../src/storage'
import { ssbSchema } from './schema'
import type {
  LineOrder,
  Customer,
  Supplier,
  Part,
  DateDim,
} from './schema'

// Extended DB interface with typed collections
interface SSBDatabase extends DBInstance {
  DateDim: { create: (data: DateDim) => Promise<DateDim & { $id: string }> }
  Customer: { create: (data: Customer) => Promise<Customer & { $id: string }> }
  Supplier: { create: (data: Supplier) => Promise<Supplier & { $id: string }> }
  Part: { create: (data: Part) => Promise<Part & { $id: string }> }
  LineOrder: { create: (data: LineOrder) => Promise<LineOrder & { $id: string }> }
}
import {
  SSB_REGIONS,
  SSB_NATIONS,
  SSB_SEGMENTS,
  SSB_PRIORITIES,
  SSB_SHIP_MODES,
  SSB_COLORS,
  SSB_CONTAINERS,
  SSB_PART_TYPES,
  SSB_SEASONS,
  DAYS_OF_WEEK,
  MONTH_NAMES,
} from './schema'

// =============================================================================
// Types
// =============================================================================

export interface GenerateSSBOptions {
  /** Scale factor (1, 10, or 100) */
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
    lineorder: number
    customer: number
    supplier: number
    part: number
    date: number
  }
  durationMs: number
}

// =============================================================================
// Random Number Generator (Seeded)
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
}

// =============================================================================
// City Generation
// =============================================================================

/**
 * Generate city names for a nation
 * Format: NATION + digit (e.g., "UNITED STATES0", "UNITED STATES1", ...)
 */
function generateCities(nation: string, count: number): string[] {
  const cities: string[] = []
  for (let i = 0; i < count; i++) {
    cities.push(`${nation}${i}`)
  }
  return cities
}

// =============================================================================
// Date Dimension Generator
// =============================================================================

/**
 * Generate the DATE dimension table
 * Fixed size: covers years 1992-1998 (2,556 days)
 */
export function generateDateDimension(): DateDim[] {
  const dates: DateDim[] = []

  const startDate = new Date(1992, 0, 1)  // Jan 1, 1992
  const endDate = new Date(1998, 11, 31)  // Dec 31, 1998

  let dayNum = 0
  const current = new Date(startDate)

  while (current <= endDate) {
    const year = current.getFullYear()
    const month = current.getMonth()
    const day = current.getDate()
    const dayOfWeek = current.getDay()

    // Calculate week number (ISO 8601 style, simplified)
    const jan1 = new Date(year, 0, 1)
    const dayOfYear = Math.floor((current.getTime() - jan1.getTime()) / 86400000) + 1
    const weekNum = Math.ceil((dayOfYear + jan1.getDay()) / 7)

    // Date key format: YYYYMMDD
    const datekey = year * 10000 + (month + 1) * 100 + day

    // Determine selling season
    let season: string
    if (month >= 10 || month === 0) {
      season = month === 11 ? 'Christmas' : 'Winter'
    } else if (month >= 2 && month <= 4) {
      season = 'Spring'
    } else if (month >= 5 && month <= 7) {
      season = 'Summer'
    } else {
      season = 'Fall'
    }

    // Check for holidays (simplified)
    const isHoliday =
      (month === 0 && day === 1) ||   // New Year
      (month === 11 && day === 25) || // Christmas
      (month === 6 && day === 4) ||   // Independence Day (US)
      (month === 10 && day >= 22 && dayOfWeek === 4) // Thanksgiving (US, approx)

    // Last day in month
    const nextDay = new Date(current)
    nextDay.setDate(nextDay.getDate() + 1)
    const lastDayInMonth = nextDay.getMonth() !== month

    dates.push({
      d_datekey: datekey,
      d_date: `${MONTH_NAMES[month]} ${day}, ${year}`,
      d_dayofweek: DAYS_OF_WEEK[dayOfWeek],
      d_month: MONTH_NAMES[month],
      d_year: year,
      d_yearmonthnum: year * 100 + (month + 1),
      d_yearmonth: `${MONTH_NAMES[month].substring(0, 3)}${year}`,
      d_daynuminweek: dayOfWeek + 1,
      d_daynuminmonth: day,
      d_daynuminyear: dayOfYear,
      d_monthnuminyear: month + 1,
      d_weeknuminyear: weekNum,
      d_sellingseason: season,
      d_lastdayinweekfl: dayOfWeek === 6,
      d_lastdayinmonthfl: lastDayInMonth,
      d_holidayfl: isHoliday,
      d_weekdayfl: dayOfWeek >= 1 && dayOfWeek <= 5,
    })

    dayNum++
    current.setDate(current.getDate() + 1)
  }

  return dates
}

// =============================================================================
// Customer Dimension Generator
// =============================================================================

/**
 * Generate the CUSTOMER dimension table
 */
export function generateCustomers(scaleFactor: number, rng: SeededRandom): Customer[] {
  const count = 30_000 * scaleFactor
  const customers: Customer[] = []

  // Pre-generate cities for each nation
  const nationCities = new Map<string, string[]>()
  for (const region of SSB_REGIONS) {
    for (const nation of SSB_NATIONS[region]) {
      nationCities.set(nation, generateCities(nation, 10))
    }
  }

  for (let i = 1; i <= count; i++) {
    const region = rng.pick(SSB_REGIONS)
    const nation = rng.pick(SSB_NATIONS[region])
    const cities = nationCities.get(nation)!
    const city = rng.pick(cities)

    customers.push({
      c_custkey: i,
      c_name: `Customer#${i.toString().padStart(9, '0')}`,
      c_address: `${rng.int(1, 999)} ${rng.pick(['Main', 'Oak', 'Pine', 'Elm', 'Maple'])} Street`,
      c_city: city,
      c_nation: nation,
      c_region: region,
      c_phone: `${rng.int(10, 34)}-${rng.digits(3)}-${rng.digits(3)}-${rng.digits(4)}`,
      c_mktsegment: rng.pick(SSB_SEGMENTS),
    })
  }

  return customers
}

// =============================================================================
// Supplier Dimension Generator
// =============================================================================

/**
 * Generate the SUPPLIER dimension table
 */
export function generateSuppliers(scaleFactor: number, rng: SeededRandom): Supplier[] {
  const count = Math.floor(2_000 * scaleFactor)
  const suppliers: Supplier[] = []

  // Pre-generate cities for each nation
  const nationCities = new Map<string, string[]>()
  for (const region of SSB_REGIONS) {
    for (const nation of SSB_NATIONS[region]) {
      nationCities.set(nation, generateCities(nation, 10))
    }
  }

  for (let i = 1; i <= count; i++) {
    const region = rng.pick(SSB_REGIONS)
    const nation = rng.pick(SSB_NATIONS[region])
    const cities = nationCities.get(nation)!
    const city = rng.pick(cities)

    suppliers.push({
      s_suppkey: i,
      s_name: `Supplier#${i.toString().padStart(9, '0')}`,
      s_address: `${rng.int(1, 999)} ${rng.pick(['Industrial', 'Commerce', 'Trade', 'Market'])} Ave`,
      s_city: city,
      s_nation: nation,
      s_region: region,
      s_phone: `${rng.int(10, 34)}-${rng.digits(3)}-${rng.digits(3)}-${rng.digits(4)}`,
    })
  }

  return suppliers
}

// =============================================================================
// Part Dimension Generator
// =============================================================================

/**
 * Generate the PART dimension table
 */
export function generateParts(scaleFactor: number, rng: SeededRandom): Part[] {
  // SF1 = 200K, SF10 = 800K, SF100 = 1.4M
  const count = scaleFactor === 1 ? 200_000 :
                scaleFactor === 10 ? 800_000 :
                scaleFactor === 100 ? 1_400_000 :
                Math.floor(200_000 * scaleFactor)

  const parts: Part[] = []

  // Generate part names from syllables (TPC-H style)
  const syllables1 = ['almond', 'antique', 'aquamarine', 'azure', 'beige', 'bisque', 'black', 'blanched', 'blue']
  const syllables2 = ['brown', 'burlywood', 'burnished', 'chartreuse', 'chiffon', 'chocolate', 'coral', 'cream', 'cyan']
  const syllables3 = ['drab', 'firebrick', 'floral', 'forest', 'frosted', 'gainsboro', 'ghost', 'goldenrod', 'green']
  const syllables4 = ['honeydew', 'hot', 'indian', 'ivory', 'khaki', 'lace', 'lavender', 'lawn', 'lemon']
  const syllables5 = ['light', 'lime', 'linen', 'magenta', 'maroon', 'medium', 'metallic', 'midnight', 'mint']

  for (let i = 1; i <= count; i++) {
    // Manufacturer: MFGR#1 to MFGR#5
    const mfgrNum = rng.int(1, 5)
    const mfgr = `MFGR#${mfgrNum}`

    // Category: MFGR#X/Y where Y is 1-5
    const catNum = rng.int(1, 5)
    const category = `MFGR#${mfgrNum}${catNum}`

    // Brand: MFGR#X/Y/Z where Z is 1-40
    const brandNum = rng.int(1, 40)
    const brand = `MFGR#${mfgrNum}${catNum}${brandNum}`

    // Generate part name from syllables
    const name = [
      rng.pick(syllables1),
      rng.pick(syllables2),
      rng.pick(syllables3),
      rng.pick(syllables4),
      rng.pick(syllables5),
    ].join(' ')

    parts.push({
      p_partkey: i,
      p_name: name,
      p_mfgr: mfgr,
      p_category: category,
      p_brand1: brand,
      p_color: rng.pick(SSB_COLORS),
      p_type: `${rng.pick(SSB_PART_TYPES)} ${rng.pick(['ANODIZED', 'BRUSHED', 'BURNISHED', 'PLATED', 'POLISHED'])} ${rng.pick(['BRASS', 'COPPER', 'NICKEL', 'STEEL', 'TIN'])}`,
      p_size: rng.int(1, 50),
      p_container: rng.pick(SSB_CONTAINERS),
    })
  }

  return parts
}

// =============================================================================
// LineOrder Fact Table Generator
// =============================================================================

/**
 * Generate the LINEORDER fact table
 */
export function* generateLineOrders(
  scaleFactor: number,
  dates: DateDim[],
  customerCount: number,
  supplierCount: number,
  partCount: number,
  rng: SeededRandom
): Generator<LineOrder[]> {
  // Approximate row count: SF1 = 6M, SF10 = 60M, SF100 = 600M
  const targetRows = 6_000_000 * scaleFactor
  const ordersPerBatch = 50_000
  const lineItemsPerOrder = 4  // Average

  // Extract date keys for random selection
  const dateKeys = dates.map(d => d.d_datekey)

  let orderKey = 0
  let totalRows = 0
  let batch: LineOrder[] = []

  while (totalRows < targetRows) {
    orderKey++

    // Generate random number of line items per order (1-7)
    const lineItems = rng.int(1, 7)
    const orderDate = rng.pick(dateKeys)

    // Commit date is 1-121 days after order date
    const commitOffset = rng.int(1, 121)
    const orderDateObj = new Date(
      Math.floor(orderDate / 10000),
      Math.floor((orderDate % 10000) / 100) - 1,
      orderDate % 100
    )
    orderDateObj.setDate(orderDateObj.getDate() + commitOffset)
    const commitDate =
      orderDateObj.getFullYear() * 10000 +
      (orderDateObj.getMonth() + 1) * 100 +
      orderDateObj.getDate()

    let orderTotal = 0

    for (let lineNum = 1; lineNum <= lineItems && totalRows < targetRows; lineNum++) {
      const custkey = rng.int(1, customerCount)
      const partkey = rng.int(1, partCount)
      const suppkey = rng.int(1, supplierCount)
      const quantity = rng.int(1, 50)
      const discount = rng.int(0, 10)
      const tax = rng.int(0, 8)

      // Price is based on part key (deterministic for consistency)
      const basePrice = (partkey * 17) % 90000 + 10000  // 100.00 to 1000.00 in cents
      const extendedPrice = quantity * basePrice
      const revenue = Math.floor(extendedPrice * (100 - discount) / 100)
      const supplyCost = Math.floor(basePrice * rng.int(50, 80) / 100)

      orderTotal += extendedPrice

      batch.push({
        lo_orderkey: orderKey,
        lo_linenumber: lineNum,
        lo_custkey: custkey,
        lo_partkey: partkey,
        lo_suppkey: suppkey,
        lo_orderdate: orderDate,
        lo_orderpriority: rng.pick(SSB_PRIORITIES),
        lo_shippriority: 0,
        lo_quantity: quantity,
        lo_extendedprice: extendedPrice,
        lo_ordtotalprice: 0,  // Will be updated after all line items
        lo_discount: discount,
        lo_revenue: revenue,
        lo_supplycost: supplyCost,
        lo_tax: tax,
        lo_commitdate: commitDate,
        lo_shipmode: rng.pick(SSB_SHIP_MODES),
      })

      totalRows++

      if (batch.length >= ordersPerBatch) {
        // Update order totals for this batch
        const orderTotals = new Map<number, number>()
        for (const lo of batch) {
          orderTotals.set(lo.lo_orderkey, (orderTotals.get(lo.lo_orderkey) || 0) + lo.lo_extendedprice)
        }
        for (const lo of batch) {
          lo.lo_ordtotalprice = orderTotals.get(lo.lo_orderkey)!
        }

        yield batch
        batch = []
      }
    }
  }

  // Yield remaining batch
  if (batch.length > 0) {
    const orderTotals = new Map<number, number>()
    for (const lo of batch) {
      orderTotals.set(lo.lo_orderkey, (orderTotals.get(lo.lo_orderkey) || 0) + lo.lo_extendedprice)
    }
    for (const lo of batch) {
      lo.lo_ordtotalprice = orderTotals.get(lo.lo_orderkey)!
    }
    yield batch
  }
}

// =============================================================================
// Main Generator Function
// =============================================================================

/**
 * Generate SSB dataset and load into ParqueDB
 */
export async function generateSSB(options: GenerateSSBOptions = {}): Promise<GenerateStats> {
  const {
    scaleFactor = 1,
    outputDir = '.db/ssb',
    batchSize = 10_000,
    onProgress,
    seed = 42,
  } = options

  const startTime = Date.now()
  const rng = new SeededRandom(seed)

  // Create database with SSB schema
  const db = DB(ssbSchema, {
    storage: new FsBackend(outputDir),
  }) as SSBDatabase

  const stats: GenerateStats = {
    scaleFactor,
    tables: {
      lineorder: 0,
      customer: 0,
      supplier: 0,
      part: 0,
      date: 0,
    },
    durationMs: 0,
  }

  try {
    // 1. Generate and load DATE dimension (fixed size)
    console.log('Generating DATE dimension...')
    const dates = generateDateDimension()
    stats.tables.date = dates.length

    for (let i = 0; i < dates.length; i += batchSize) {
      const batch = dates.slice(i, i + batchSize)
      for (const date of batch) {
        await db.DateDim.create(date)
      }
      onProgress?.({
        table: 'DATE',
        current: Math.min(i + batchSize, dates.length),
        total: dates.length,
        percentage: Math.min(100, Math.round(((i + batchSize) / dates.length) * 100)),
      })
    }
    console.log(`  Loaded ${dates.length} dates`)

    // 2. Generate and load CUSTOMER dimension
    console.log('Generating CUSTOMER dimension...')
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

    // 3. Generate and load SUPPLIER dimension
    console.log('Generating SUPPLIER dimension...')
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

    // 4. Generate and load PART dimension
    console.log('Generating PART dimension...')
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

    // 5. Generate and load LINEORDER fact table (streaming)
    console.log('Generating LINEORDER fact table...')
    const targetLineOrders = 6_000_000 * scaleFactor
    let lineOrderCount = 0

    const lineOrderGen = generateLineOrders(
      scaleFactor,
      dates,
      customers.length,
      suppliers.length,
      parts.length,
      rng
    )

    let batchResult = lineOrderGen.next()
    while (!batchResult.done) {
      const batch = batchResult.value
      for (const lineOrder of batch) {
        await db.LineOrder.create(lineOrder)
      }
      lineOrderCount += batch.length
      onProgress?.({
        table: 'LINEORDER',
        current: lineOrderCount,
        total: targetLineOrders,
        percentage: Math.min(100, Math.round((lineOrderCount / targetLineOrders) * 100)),
      })

      // Log progress every 500K rows
      if (lineOrderCount % 500_000 === 0) {
        console.log(`  Loaded ${lineOrderCount.toLocaleString()} line orders...`)
      }
      batchResult = lineOrderGen.next()
    }

    stats.tables.lineorder = lineOrderCount
    console.log(`  Loaded ${lineOrderCount.toLocaleString()} line orders`)

    stats.durationMs = Date.now() - startTime
    console.log(`\nGeneration complete in ${(stats.durationMs / 1000).toFixed(1)}s`)

    return stats
  } finally {
    db.dispose()
  }
}

// =============================================================================
// Export In-Memory Data Generators
// =============================================================================

/**
 * Generate SSB data in memory (for testing or custom loading)
 */
export function generateSSBInMemory(scaleFactor: number, seed: number = 42) {
  const rng = new SeededRandom(seed)

  const dates = generateDateDimension()
  const customers = generateCustomers(scaleFactor, rng)
  const suppliers = generateSuppliers(scaleFactor, rng)
  const parts = generateParts(scaleFactor, rng)

  return {
    dates,
    customers,
    suppliers,
    parts,
    lineOrderGenerator: () =>
      generateLineOrders(
        scaleFactor,
        dates,
        customers.length,
        suppliers.length,
        parts.length,
        rng
      ),
  }
}
