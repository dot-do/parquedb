# TPC-H Benchmark Example for ParqueDB

This example demonstrates ParqueDB with the TPC-H benchmark, the industry-standard OLAP benchmark for analytical database performance.

## Overview

TPC-H (Transaction Processing Performance Council Benchmark H) is a decision support benchmark consisting of:

- **1 Fact Table**: LINEITEM (line items from orders)
- **7 Dimension Tables**: ORDERS, CUSTOMER, PART, SUPPLIER, PARTSUPP, NATION, REGION

The schema follows a snowflake pattern with NATION and REGION as normalized dimensions.

## Schema

### LINEITEM (Fact Table)

The central fact table containing ~6M rows at Scale Factor 1 (SF1):

| Column | Type | Description |
|--------|------|-------------|
| l_orderkey | int | Order identifier (FK to ORDERS) |
| l_partkey | int | Part key (FK to PART) |
| l_suppkey | int | Supplier key (FK to SUPPLIER) |
| l_linenumber | int | Line item number |
| l_quantity | decimal | Quantity ordered |
| l_extendedprice | decimal | Extended price |
| l_discount | decimal | Discount percentage (0-0.10) |
| l_tax | decimal | Tax percentage (0-0.08) |
| l_returnflag | string | Return flag (R, A, N) |
| l_linestatus | string | Line status (O, F) |
| l_shipdate | date | Ship date |
| l_commitdate | date | Commit date |
| l_receiptdate | date | Receipt date |
| l_shipinstruct | string | Shipping instructions |
| l_shipmode | string | Shipping mode |
| l_comment | string | Comment |

### ORDERS (Dimension)

~1.5M rows at SF1:

| Column | Type | Description |
|--------|------|-------------|
| o_orderkey | int | Order key (PK) |
| o_custkey | int | Customer key (FK to CUSTOMER) |
| o_orderstatus | string | Order status (O, F, P) |
| o_totalprice | decimal | Total price |
| o_orderdate | date | Order date |
| o_orderpriority | string | Order priority |
| o_clerk | string | Clerk identifier |
| o_shippriority | int | Shipping priority |
| o_comment | string | Comment |

### CUSTOMER (Dimension)

~150K rows at SF1:

| Column | Type | Description |
|--------|------|-------------|
| c_custkey | int | Customer key (PK) |
| c_name | string | Customer name |
| c_address | string | Address |
| c_nationkey | int | Nation key (FK to NATION) |
| c_phone | string | Phone number |
| c_acctbal | decimal | Account balance |
| c_mktsegment | string | Market segment |
| c_comment | string | Comment |

### PART (Dimension)

~200K rows at SF1:

| Column | Type | Description |
|--------|------|-------------|
| p_partkey | int | Part key (PK) |
| p_name | string | Part name |
| p_mfgr | string | Manufacturer |
| p_brand | string | Brand |
| p_type | string | Type |
| p_size | int | Size |
| p_container | string | Container type |
| p_retailprice | decimal | Retail price |
| p_comment | string | Comment |

### SUPPLIER (Dimension)

~10K rows at SF1:

| Column | Type | Description |
|--------|------|-------------|
| s_suppkey | int | Supplier key (PK) |
| s_name | string | Supplier name |
| s_address | string | Address |
| s_nationkey | int | Nation key (FK to NATION) |
| s_phone | string | Phone number |
| s_acctbal | decimal | Account balance |
| s_comment | string | Comment |

### PARTSUPP (Bridge Table)

~800K rows at SF1:

| Column | Type | Description |
|--------|------|-------------|
| ps_partkey | int | Part key (FK to PART) |
| ps_suppkey | int | Supplier key (FK to SUPPLIER) |
| ps_availqty | int | Available quantity |
| ps_supplycost | decimal | Supply cost |
| ps_comment | string | Comment |

### NATION (Dimension)

25 rows (fixed):

| Column | Type | Description |
|--------|------|-------------|
| n_nationkey | int | Nation key (PK) |
| n_name | string | Nation name |
| n_regionkey | int | Region key (FK to REGION) |
| n_comment | string | Comment |

### REGION (Dimension)

5 rows (fixed):

| Column | Type | Description |
|--------|------|-------------|
| r_regionkey | int | Region key (PK) |
| r_name | string | Region name |
| r_comment | string | Comment |

## Scale Factors

| Scale Factor | LINEITEM | ORDERS | CUSTOMER | PART | SUPPLIER | PARTSUPP | NATION | REGION |
|--------------|----------|--------|----------|------|----------|----------|--------|--------|
| SF0.01 | ~60K | ~15K | ~1.5K | ~2K | ~100 | ~8K | 25 | 5 |
| SF0.1 | ~600K | ~150K | ~15K | ~20K | ~1K | ~80K | 25 | 5 |
| SF1 | ~6M | ~1.5M | ~150K | ~200K | ~10K | ~800K | 25 | 5 |
| SF10 | ~60M | ~15M | ~1.5M | ~2M | ~100K | ~8M | 25 | 5 |

## ParqueDB Relationships

This example demonstrates ParqueDB's graph-first architecture with bidirectional relationships:

```
REGION (5)
  |
  +-- nations (1:N) --> NATION (25)
        |
        +-- customers (1:N) --> CUSTOMER (150K)
        |     |
        |     +-- orders (1:N) --> ORDERS (1.5M)
        |           |
        |           +-- lineitems (1:N) --> LINEITEM (6M)
        |                 |
        |                 +-- part (N:1) --> PART (200K)
        |                 |
        |                 +-- supplier (N:1) --> SUPPLIER (10K)
        |
        +-- suppliers (1:N) --> SUPPLIER
              |
              +-- partsupps (1:N) --> PARTSUPP (800K)
                    |
                    +-- part (N:1) --> PART
```

## TPC-H Queries Implemented

This example implements 8 key TPC-H queries:

### Q1: Pricing Summary Report
Aggregates line items by return flag and status with date filter.

### Q3: Shipping Priority
Finds unshipped orders by market segment with revenue calculation.

### Q5: Local Supplier Volume
Revenue by nation for a region with multi-table joins.

### Q6: Forecasting Revenue Change
Simple aggregation with range filters on date, discount, and quantity.

### Q10: Returned Item Reporting
Identifies customers with returned items and lost revenue.

### Q12: Shipping Modes and Order Priority
Analyzes shipping mode impact on order priority.

### Q14: Promotion Effect
Measures promotional revenue as percentage of total.

### Q19: Discounted Revenue
Complex filter query with brand, container, and quantity conditions.

## Usage

### Generate Data

```typescript
import { generateTPCH } from './generate'

// Generate SF0.01 dataset (for testing)
await generateTPCH({
  scaleFactor: 0.01,
  outputDir: '.db/tpch'
})

// Generate SF1 dataset
await generateTPCH({
  scaleFactor: 1,
  outputDir: '.db/tpch-sf1'
})
```

### Run Queries

```typescript
import { DB, FsBackend } from 'parquedb'
import { tpchSchema } from './schema'
import { runTPCHQueries, runQ1, runQ3, runQ6 } from './queries'

const db = DB(tpchSchema, {
  storage: new FsBackend('.db/tpch')
})

// Run all queries
const results = await runTPCHQueries(db)

// Or run individual queries
const q1Result = await runQ1(db, { date: '1998-09-02' })
const q3Result = await runQ3(db, { segment: 'BUILDING', date: '1995-03-15' })
const q6Result = await runQ6(db, {
  year: 1994,
  discountLo: 0.05,
  discountHi: 0.07,
  quantity: 24
})
```

### Run Benchmark

```typescript
import { runTPCHBenchmark } from './index'

const results = await runTPCHBenchmark({
  scaleFactor: 0.1,
  iterations: 3,
  warmupIterations: 1
})

console.log(results.summary)
```

## Running the Example

```bash
# Generate data and run benchmark
npx tsx examples/tpch/index.ts

# Generate larger dataset
npx tsx examples/tpch/index.ts --scale-factor=1

# Run only queries (assumes data exists)
npx tsx examples/tpch/index.ts --skip-generate
```

## References

- [TPC-H Benchmark Specification](http://www.tpc.org/tpch/)
- [TPC-H Query Reference](http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf)
