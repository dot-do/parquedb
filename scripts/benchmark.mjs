#!/usr/bin/env node
/**
 * Unified Benchmark Script for ParqueDB
 *
 * Runs comprehensive performance benchmarks and generates reports.
 *
 * Usage:
 *   node scripts/benchmark.mjs [options]
 *
 * Options:
 *   --suite=<name>   Run specific suite: crud, queries, parquet, all (default: all)
 *   --scale=<list>   Entity counts: 100,1000,10000 (default: 100,1000,10000)
 *   --iterations=<n> Iterations per benchmark (default: 10)
 *   --warmup=<n>     Warmup iterations (default: 3)
 *   --output=<fmt>   Output format: table, json, markdown (default: table)
 *   --verbose        Show detailed results
 *   --help           Show this help
 */

// Use dynamic import with fallback for ESM/CJS compatibility
let Collection, MemoryBackend;

try {
  // Try direct import first (for tsx or built dist)
  const mod = await import('../dist/Collection.js');
  Collection = mod.Collection;
  const storageMod = await import('../dist/storage/MemoryBackend.js');
  MemoryBackend = storageMod.MemoryBackend;
} catch (e) {
  // Fallback: Use a simple in-memory mock for benchmarking core operations
  console.log('Note: Using mock Collection for benchmarking (dist not available)');

  // Simple mock implementation for basic benchmarking
  class MockCollection {
    constructor(ns) {
      this.ns = ns;
      this.data = new Map();
      this.idCounter = 0;
    }

    async create(doc) {
      const id = `${this.ns}/${++this.idCounter}`;
      const entity = { $id: id, ...doc, $createdAt: Date.now(), $updatedAt: Date.now() };
      this.data.set(id, entity);
      return entity;
    }

    async get(id) {
      return this.data.get(id) || this.data.get(`${this.ns}/${id}`) || null;
    }

    async find(filter = {}, options = {}) {
      let results = Array.from(this.data.values());

      // Apply basic filter
      if (Object.keys(filter).length > 0) {
        results = results.filter(doc => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === '$and') continue;
            if (key === '$or') continue;
            if (typeof value === 'object' && value !== null) {
              // Handle operators
              if ('$gt' in value && !(doc[key] > value.$gt)) return false;
              if ('$gte' in value && !(doc[key] >= value.$gte)) return false;
              if ('$lt' in value && !(doc[key] < value.$lt)) return false;
              if ('$lte' in value && !(doc[key] <= value.$lte)) return false;
              if ('$in' in value && !value.$in.includes(doc[key])) return false;
            } else {
              if (doc[key] !== value) return false;
            }
          }
          return true;
        });
      }

      // Apply sort
      if (options.sort) {
        const sortEntries = Object.entries(options.sort);
        results.sort((a, b) => {
          for (const [key, order] of sortEntries) {
            const aVal = a[key], bVal = b[key];
            if (aVal < bVal) return order === -1 ? 1 : -1;
            if (aVal > bVal) return order === -1 ? -1 : 1;
          }
          return 0;
        });
      }

      // Apply skip/limit
      if (options.skip) results = results.slice(options.skip);
      if (options.limit) results = results.slice(0, options.limit);

      return results;
    }

    async update(id, ops) {
      const fullId = id.includes('/') ? id : `${this.ns}/${id}`;
      const doc = this.data.get(fullId);
      if (!doc) return null;

      if (ops.$set) Object.assign(doc, ops.$set);
      if (ops.$inc) {
        for (const [key, val] of Object.entries(ops.$inc)) {
          doc[key] = (doc[key] || 0) + val;
        }
      }
      doc.$updatedAt = Date.now();
      return doc;
    }

    async delete(id) {
      const fullId = id.includes('/') ? id : `${this.ns}/${id}`;
      return this.data.delete(fullId);
    }

    async count(filter = {}) {
      return (await this.find(filter)).length;
    }

    async aggregate(pipeline) {
      let results = Array.from(this.data.values());

      for (const stage of pipeline) {
        if (stage.$match) {
          results = results.filter(doc => {
            for (const [key, value] of Object.entries(stage.$match)) {
              if (typeof value === 'object' && value !== null) {
                if ('$gt' in value && !(doc[key] > value.$gt)) return false;
              } else {
                if (doc[key] !== value) return false;
              }
            }
            return true;
          });
        }

        if (stage.$group) {
          const groups = new Map();
          for (const doc of results) {
            const key = stage.$group._id?.startsWith('$')
              ? doc[stage.$group._id.slice(1)]
              : stage.$group._id;
            if (!groups.has(key)) {
              groups.set(key, { _id: key, docs: [] });
            }
            groups.get(key).docs.push(doc);
          }
          results = Array.from(groups.values()).map(g => {
            const result = { _id: g._id };
            for (const [field, op] of Object.entries(stage.$group)) {
              if (field === '_id') continue;
              if (op.$sum === 1) result[field] = g.docs.length;
              if (op.$avg) {
                const fieldName = op.$avg.slice(1);
                result[field] = g.docs.reduce((s, d) => s + (d[fieldName] || 0), 0) / g.docs.length;
              }
            }
            return result;
          });
        }
      }

      return results;
    }
  }

  Collection = MockCollection;
}

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_CONFIG = {
  suite: 'all',
  scale: [100, 1000, 10000],
  iterations: 10,
  warmup: 3,
  output: 'table',
  verbose: false,
};

function parseArgs() {
  const args = process.argv.slice(2);
  const config = { ...DEFAULT_CONFIG };

  for (const arg of args) {
    if (arg === '--help' || arg === '-h') {
      console.log(`
ParqueDB Benchmark Runner

Usage: node scripts/benchmark.mjs [options]

Options:
  --suite=<name>   Run specific suite: crud, queries, parquet, scalability, all
  --scale=<list>   Entity counts comma-separated (default: 100,1000,10000)
  --iterations=<n> Iterations per benchmark (default: 10)
  --warmup=<n>     Warmup iterations (default: 3)
  --output=<fmt>   Output format: table, json, markdown
  --verbose        Show detailed results
  --help           Show this help

Examples:
  node scripts/benchmark.mjs
  node scripts/benchmark.mjs --suite=crud --iterations=20
  node scripts/benchmark.mjs --scale=100,1000 --output=json
`);
      process.exit(0);
    }

    const [key, value] = arg.split('=');
    switch (key) {
      case '--suite':
        config.suite = value;
        break;
      case '--scale':
        config.scale = value.split(',').map(Number);
        break;
      case '--iterations':
        config.iterations = parseInt(value, 10);
        break;
      case '--warmup':
        config.warmup = parseInt(value, 10);
        break;
      case '--output':
        config.output = value;
        break;
      case '--verbose':
        config.verbose = true;
        break;
    }
  }

  return config;
}

// =============================================================================
// Utilities
// =============================================================================

function median(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function percentile(arr, p) {
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function mean(arr) {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function formatBytes(bytes) {
  const units = ['B', 'KB', 'MB', 'GB'];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }
  return `${value.toFixed(2)} ${units[unitIndex]}`;
}

async function benchmark(name, fn, config) {
  const times = [];

  // Warmup
  for (let i = 0; i < config.warmup; i++) {
    await fn();
  }

  // Measure
  for (let i = 0; i < config.iterations; i++) {
    const start = performance.now();
    await fn();
    times.push(performance.now() - start);
  }

  return {
    name,
    iterations: config.iterations,
    mean: mean(times),
    median: median(times),
    min: Math.min(...times),
    max: Math.max(...times),
    p95: percentile(times, 95),
    p99: percentile(times, 99),
    opsPerSec: Math.round((config.iterations / times.reduce((a, b) => a + b, 0)) * 1000),
  };
}

function printTable(results, title) {
  console.log(`\n${'='.repeat(70)}`);
  console.log(title);
  console.log('='.repeat(70));
  console.log(`${'Name'.padEnd(40)} ${'Mean'.padStart(8)} ${'P95'.padStart(8)} ${'Ops/s'.padStart(8)}`);
  console.log('-'.repeat(70));

  for (const r of results) {
    console.log(
      `${r.name.slice(0, 38).padEnd(40)} ${r.mean.toFixed(2).padStart(6)}ms ${r.p95.toFixed(2).padStart(6)}ms ${String(r.opsPerSec).padStart(8)}`
    );
  }
}

function printMarkdown(results, title) {
  console.log(`\n## ${title}\n`);
  console.log('| Operation | Mean (ms) | P95 (ms) | Ops/sec |');
  console.log('|-----------|-----------|----------|---------|');

  for (const r of results) {
    console.log(`| ${r.name} | ${r.mean.toFixed(2)} | ${r.p95.toFixed(2)} | ${r.opsPerSec} |`);
  }
}

// =============================================================================
// Data Generators
// =============================================================================

const statuses = ['draft', 'published', 'archived'];
const categories = ['tech', 'science', 'arts', 'sports', 'business'];
const tags = ['featured', 'trending', 'new', 'popular', 'editor-pick'];

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomElement(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomString(length) {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function generatePost(index) {
  return {
    $type: 'Post',
    name: `Post ${index}`,
    title: `Test Post ${index}: ${randomString(20)}`,
    content: randomString(500),
    status: statuses[index % 3],
    views: randomInt(0, 100000),
    likes: randomInt(0, 5000),
    tags: [randomElement(tags), randomElement(tags)],
  };
}

// =============================================================================
// Benchmark Suites
// =============================================================================

async function runCrudBenchmarks(config) {
  const results = [];
  const ns = `crud-bench-${Date.now()}`;
  const posts = new Collection(ns);

  // Seed some data for read/update/delete tests
  const entityIds = [];
  for (let i = 0; i < 100; i++) {
    const entity = await posts.create(generatePost(i));
    entityIds.push(entity.$id);
  }

  // Create
  results.push(await benchmark('Create single entity', async () => {
    await posts.create(generatePost(Date.now()));
  }, config));

  // Get by ID
  results.push(await benchmark('Get by ID', async () => {
    await posts.get(randomElement(entityIds));
  }, config));

  // Find with filter
  results.push(await benchmark('Find with equality filter', async () => {
    await posts.find({ status: 'published' });
  }, config));

  results.push(await benchmark('Find with range filter', async () => {
    await posts.find({ views: { $gt: 50000 } });
  }, config));

  results.push(await benchmark('Find with $in filter', async () => {
    await posts.find({ status: { $in: ['published', 'archived'] } });
  }, config));

  results.push(await benchmark('Find with complex filter', async () => {
    await posts.find({
      $and: [
        { status: 'published' },
        { views: { $gt: 10000 } },
      ],
    });
  }, config));

  // Update
  results.push(await benchmark('Update ($set)', async () => {
    await posts.update(randomElement(entityIds), {
      $set: { title: `Updated ${Date.now()}` },
    });
  }, config));

  results.push(await benchmark('Update ($inc)', async () => {
    await posts.update(randomElement(entityIds), {
      $inc: { views: 1 },
    });
  }, config));

  // Count
  results.push(await benchmark('Count all', async () => {
    await posts.count();
  }, config));

  results.push(await benchmark('Count with filter', async () => {
    await posts.count({ status: 'published' });
  }, config));

  return results;
}

async function runQueryBenchmarks(config) {
  const results = [];
  const ns = `query-bench-${Date.now()}`;
  const posts = new Collection(ns);

  // Seed data
  console.log('  Seeding 1000 entities for query benchmarks...');
  for (let i = 0; i < 1000; i++) {
    await posts.create(generatePost(i));
  }

  // Query patterns
  results.push(await benchmark('Full scan (no filter)', async () => {
    await posts.find();
  }, config));

  results.push(await benchmark('Find with limit 10', async () => {
    await posts.find({}, { limit: 10 });
  }, config));

  results.push(await benchmark('Find with limit 100', async () => {
    await posts.find({}, { limit: 100 });
  }, config));

  results.push(await benchmark('Find with sort (single field)', async () => {
    await posts.find({}, { sort: { views: -1 }, limit: 100 });
  }, config));

  results.push(await benchmark('Find with sort (multi-field)', async () => {
    await posts.find({}, { sort: { status: 1, views: -1 }, limit: 100 });
  }, config));

  results.push(await benchmark('Find with projection', async () => {
    await posts.find({}, { project: { title: 1, status: 1 }, limit: 100 });
  }, config));

  results.push(await benchmark('Pagination: page 1 (skip 0)', async () => {
    await posts.find({}, { limit: 20, skip: 0 });
  }, config));

  results.push(await benchmark('Pagination: page 10 (skip 180)', async () => {
    await posts.find({}, { limit: 20, skip: 180 });
  }, config));

  results.push(await benchmark('Pagination: page 50 (skip 980)', async () => {
    await posts.find({}, { limit: 20, skip: 980 });
  }, config));

  // Aggregation
  results.push(await benchmark('Aggregate: group by status', async () => {
    await posts.aggregate([
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]);
  }, config));

  results.push(await benchmark('Aggregate: match + group', async () => {
    await posts.aggregate([
      { $match: { views: { $gt: 10000 } } },
      { $group: { _id: '$status', avgViews: { $avg: '$views' } } },
    ]);
  }, config));

  return results;
}

async function runScalabilityBenchmarks(config) {
  const results = [];

  for (const scale of config.scale) {
    console.log(`\n  Testing scale: ${scale.toLocaleString()} entities...`);

    const ns = `scale-${scale}-${Date.now()}`;
    const collection = new Collection(ns);

    // Seed data
    const seedStart = performance.now();
    for (let i = 0; i < scale; i++) {
      await collection.create(generatePost(i));
    }
    const seedTime = performance.now() - seedStart;
    console.log(`    Seeded in ${(seedTime / 1000).toFixed(2)}s (${Math.round(scale / (seedTime / 1000))} entities/sec)`);

    // Benchmarks
    results.push(await benchmark(`[${scale}] Find all`, async () => {
      await collection.find();
    }, { ...config, iterations: Math.min(config.iterations, 5) }));

    results.push(await benchmark(`[${scale}] Find with filter`, async () => {
      await collection.find({ status: 'published' });
    }, config));

    results.push(await benchmark(`[${scale}] Find with sort + limit`, async () => {
      await collection.find({}, { sort: { views: -1 }, limit: 10 });
    }, config));

    results.push(await benchmark(`[${scale}] Count`, async () => {
      await collection.count();
    }, config));

    results.push(await benchmark(`[${scale}] Count with filter`, async () => {
      await collection.count({ status: 'published' });
    }, config));
  }

  return results;
}

async function runBatchBenchmarks(config) {
  const results = [];

  for (const batchSize of [10, 100, 1000]) {
    const ns = `batch-${batchSize}-${Date.now()}`;
    const collection = new Collection(ns);

    results.push(await benchmark(`Batch create ${batchSize}`, async () => {
      for (let i = 0; i < batchSize; i++) {
        await collection.create(generatePost(i));
      }
    }, { ...config, iterations: Math.max(1, Math.floor(config.iterations / (batchSize / 10))) }));
  }

  return results;
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const config = parseArgs();

  console.log('='.repeat(70));
  console.log('ParqueDB Benchmark Suite');
  console.log('='.repeat(70));
  console.log(`Suite: ${config.suite}`);
  console.log(`Scale: ${config.scale.join(', ')}`);
  console.log(`Iterations: ${config.iterations}`);
  console.log(`Warmup: ${config.warmup}`);
  console.log(`Started: ${new Date().toISOString()}`);

  const allResults = {
    config,
    timestamp: new Date().toISOString(),
    suites: {},
  };

  const printFn = config.output === 'markdown' ? printMarkdown : printTable;

  try {
    // CRUD Benchmarks
    if (config.suite === 'all' || config.suite === 'crud') {
      console.log('\nRunning CRUD benchmarks...');
      const crudResults = await runCrudBenchmarks(config);
      allResults.suites.crud = crudResults;
      printFn(crudResults, 'CRUD Operations');
    }

    // Query Benchmarks
    if (config.suite === 'all' || config.suite === 'queries') {
      console.log('\nRunning Query benchmarks...');
      const queryResults = await runQueryBenchmarks(config);
      allResults.suites.queries = queryResults;
      printFn(queryResults, 'Query Operations');
    }

    // Scalability Benchmarks
    if (config.suite === 'all' || config.suite === 'scalability') {
      console.log('\nRunning Scalability benchmarks...');
      const scaleResults = await runScalabilityBenchmarks(config);
      allResults.suites.scalability = scaleResults;
      printFn(scaleResults, 'Scalability Tests');
    }

    // Batch Benchmarks
    if (config.suite === 'all' || config.suite === 'batch') {
      console.log('\nRunning Batch benchmarks...');
      const batchResults = await runBatchBenchmarks(config);
      allResults.suites.batch = batchResults;
      printFn(batchResults, 'Batch Operations');
    }

  } catch (error) {
    console.error('\nBenchmark failed:', error.message);
    if (config.verbose) {
      console.error(error.stack);
    }
    process.exit(1);
  }

  // JSON output
  if (config.output === 'json') {
    console.log('\n' + JSON.stringify(allResults, null, 2));
  }

  // Summary
  console.log('\n' + '='.repeat(70));
  console.log('Summary');
  console.log('='.repeat(70));

  let totalBenchmarks = 0;
  for (const [suite, results] of Object.entries(allResults.suites)) {
    console.log(`  ${suite}: ${results.length} benchmarks`);
    totalBenchmarks += results.length;
  }

  console.log(`\nTotal benchmarks: ${totalBenchmarks}`);
  console.log(`Completed: ${new Date().toISOString()}`);
}

main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
