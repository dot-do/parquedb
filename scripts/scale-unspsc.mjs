/**
 * Scale UNSPSC Dataset to Full Taxonomy Size
 *
 * Generates realistic UNSPSC hierarchy:
 * - 55 segments (2-digit codes: 10-99, skipping some for realism)
 * - 400 families (4-digit codes)
 * - 4,000 classes (6-digit codes)
 * - 70,000 commodities (8-digit codes)
 *
 * Uses dual Variant architecture: $id | $index_* | $data columns
 * Sorted by code for row-group statistics (enables prefix searches)
 */

import { parquetWriteBuffer } from 'hyparquet-writer';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';

const OUTPUT_DIR = './data-v3/unspsc-full';

// =============================================================================
// Segment Names - Based on real UNSPSC taxonomy
// =============================================================================

const SEGMENT_NAMES = {
  '10': 'Live Plant and Animal Material and Accessories and Supplies',
  '11': 'Mineral and Textile and Inedible Plant and Animal Materials',
  '12': 'Chemicals including Bio Chemicals and Gas Materials',
  '13': 'Resin and Rosin and Rubber and Foam and Film and Elastomeric Materials',
  '14': 'Paper Materials and Products',
  '15': 'Fuels and Fuel Additives and Lubricants and Anti corrosive Materials',
  '20': 'Mining and Well Drilling Machinery and Accessories',
  '21': 'Farming and Fishing and Forestry and Wildlife Machinery and Accessories',
  '22': 'Building and Construction Machinery and Accessories',
  '23': 'Industrial Manufacturing and Processing Machinery and Accessories',
  '24': 'Material Handling and Conditioning and Storage Machinery and Accessories',
  '25': 'Commercial and Military and Private Vehicles and their Accessories and Components',
  '26': 'Power Generation and Distribution Machinery and Accessories',
  '27': 'Tools and General Machinery',
  '30': 'Structures and Building and Construction and Manufacturing Components and Supplies',
  '31': 'Manufacturing Components and Supplies',
  '32': 'Electronic Components and Supplies',
  '39': 'Lighting and Electrical Accessories and Supplies',
  '40': 'Distribution and Conditioning Systems and Equipment and Components',
  '41': 'Laboratory and Measuring and Observing and Testing Equipment',
  '42': 'Medical Equipment and Accessories and Supplies',
  '43': 'Information Technology Broadcasting and Telecommunications',
  '44': 'Office Equipment and Accessories and Supplies',
  '45': 'Printing and Photographic and Audio and Visual Equipment and Supplies',
  '46': 'Defense and Law Enforcement and Security and Safety Equipment and Supplies',
  '47': 'Cleaning Equipment and Supplies',
  '48': 'Service Industry Machinery and Equipment and Supplies',
  '49': 'Sports and Recreational Equipment and Supplies and Accessories',
  '50': 'Food Beverage and Tobacco Products',
  '51': 'Drugs and Pharmaceutical Products',
  '52': 'Domestic Appliances and Supplies and Consumer Electronic Products',
  '53': 'Apparel and Luggage and Personal Care Products',
  '54': 'Timepieces and Jewelry and Gemstone Products',
  '55': 'Published Products',
  '56': 'Furniture and Furnishings',
  '60': 'Musical Instruments and Games and Toys and Arts and Crafts and Educational Equipment and Materials and Accessories and Supplies',
  '70': 'Farming and Fishing and Forestry and Wildlife Contracting Services',
  '71': 'Mining and oil and gas services',
  '72': 'Building and Facility Construction and Maintenance Services',
  '73': 'Industrial Production and Manufacturing Services',
  '76': 'Industrial Cleaning Services',
  '77': 'Environmental Services',
  '78': 'Transportation and Storage and Mail Services',
  '80': 'Management and Business Professionals and Administrative Services',
  '81': 'Engineering and Research and Technology Based Services',
  '82': 'Editorial and Design and Graphic and Fine Art Services',
  '83': 'Public Utilities and Public Sector Related Services',
  '84': 'Financial and Insurance Services',
  '85': 'Healthcare Services',
  '86': 'Education and Training Services',
  '90': 'Travel and Food and Lodging and Entertainment Services',
  '91': 'Personal and Domestic Services',
  '92': 'National Defense and Public Order and Security and Safety Services',
  '93': 'Politics and Civic Affairs Services',
  '94': 'Organizations and Clubs',
  '95': 'Land and Buildings and Structures and Thoroughfares',
};

// Family name templates for each segment
const FAMILY_TEMPLATES = [
  'Equipment and Supplies',
  'Systems and Components',
  'Services and Consulting',
  'Materials and Products',
  'Machinery and Tools',
  'Accessories and Parts',
  'Processing and Manufacturing',
  'Distribution and Storage',
];

// Class name templates
const CLASS_TEMPLATES = [
  'Standard',
  'Industrial',
  'Commercial',
  'Professional',
  'Specialized',
  'General Purpose',
  'High Performance',
  'Heavy Duty',
  'Precision',
  'Custom',
];

// Commodity adjectives for name generation
const COMMODITY_ADJECTIVES = [
  'Standard', 'Premium', 'Economy', 'Industrial', 'Commercial',
  'Professional', 'Heavy Duty', 'Light Duty', 'Portable', 'Stationary',
  'Electric', 'Manual', 'Automatic', 'Semi-automatic', 'Digital',
  'Analog', 'Wireless', 'Wired', 'Compact', 'Full Size',
];

// =============================================================================
// Utilities
// =============================================================================

async function writeParquet(path, columnData, rowGroupSize = 10000) {
  const buffer = parquetWriteBuffer({ columnData, rowGroupSize });
  await fs.mkdir(join(path.split('/').slice(0, -1).join('/')), { recursive: true });
  await fs.writeFile(path, Buffer.from(buffer));
  const stats = await fs.stat(path);
  console.log(`  Wrote ${path}: ${stats.size.toLocaleString()} bytes`);
  return stats.size;
}

function generateFamilyName(segmentTitle, index) {
  const template = FAMILY_TEMPLATES[index % FAMILY_TEMPLATES.length];
  const words = segmentTitle.split(' and ').slice(0, 2).join(' ');
  return `${words} ${template}`;
}

function generateClassName(familyTitle, index) {
  const template = CLASS_TEMPLATES[index % CLASS_TEMPLATES.length];
  const words = familyTitle.split(' ').slice(0, 3).join(' ');
  return `${template} ${words}`;
}

function generateCommodityName(classTitle, index) {
  const adj = COMMODITY_ADJECTIVES[index % COMMODITY_ADJECTIVES.length];
  const words = classTitle.split(' ').slice(0, 4).join(' ');
  return `${adj} ${words} Item ${(index % 100) + 1}`;
}

// =============================================================================
// Generate Full UNSPSC Taxonomy
// =============================================================================

async function generateUNSPSC() {
  console.log('\n=== Generating Full UNSPSC Taxonomy ===\n');
  const startTime = Date.now();

  const segments = [];
  const families = [];
  const classes = [];
  const commodities = [];

  // Get segment codes (all defined segments)
  const segmentCodes = Object.keys(SEGMENT_NAMES).sort();
  console.log(`  Generating ${segmentCodes.length} segments...`);

  // Target counts
  const TARGET_FAMILIES = 400;
  const TARGET_CLASSES = 4000;
  const TARGET_COMMODITIES = 70000;

  // Calculate distribution
  const familiesPerSegment = Math.ceil(TARGET_FAMILIES / segmentCodes.length);
  const classesPerFamily = Math.ceil(TARGET_CLASSES / TARGET_FAMILIES);
  const commoditiesPerClass = Math.ceil(TARGET_COMMODITIES / TARGET_CLASSES);

  console.log(`  Distribution: ~${familiesPerSegment} families/segment, ~${classesPerFamily} classes/family, ~${commoditiesPerClass} commodities/class`);

  let familyCount = 0;
  let classCount = 0;
  let commodityCount = 0;

  // Generate hierarchy
  for (const segmentCode of segmentCodes) {
    const segmentTitle = SEGMENT_NAMES[segmentCode];
    segments.push({
      code: segmentCode,
      title: segmentTitle,
    });

    // Generate families for this segment
    const numFamilies = familiesPerSegment;
    for (let f = 0; f < numFamilies && familyCount < TARGET_FAMILIES; f++) {
      const familyCode = segmentCode + String(10 + f).padStart(2, '0');
      const familyTitle = generateFamilyName(segmentTitle, f);

      families.push({
        code: familyCode,
        title: familyTitle,
        segmentCode,
      });
      familyCount++;

      // Generate classes for this family
      const numClasses = classesPerFamily;
      for (let c = 0; c < numClasses && classCount < TARGET_CLASSES; c++) {
        const classCode = familyCode + String(10 + c).padStart(2, '0');
        const classTitle = generateClassName(familyTitle, c);

        classes.push({
          code: classCode,
          title: classTitle,
          familyCode,
          segmentCode,
        });
        classCount++;

        // Generate commodities for this class
        const numCommodities = commoditiesPerClass;
        for (let m = 0; m < numCommodities && commodityCount < TARGET_COMMODITIES; m++) {
          const commodityCode = classCode + String(1 + m).padStart(2, '0');
          const commodityTitle = generateCommodityName(classTitle, m);

          commodities.push({
            code: commodityCode,
            title: commodityTitle,
            classCode,
            familyCode,
            segmentCode,
          });
          commodityCount++;
        }
      }
    }
  }

  console.log(`  Generated: ${segments.length} segments, ${families.length} families, ${classes.length} classes, ${commodities.length} commodities`);
  console.log(`  Generation time: ${((Date.now() - startTime) / 1000).toFixed(1)}s`);

  // Create output directory
  await fs.mkdir(OUTPUT_DIR, { recursive: true });

  // Write segments
  console.log('\n  Writing segments...');
  await writeParquet(join(OUTPUT_DIR, 'segments.parquet'), [
    { name: '$id', type: 'STRING', data: segments.map(s => `segment:${s.code}`) },
    { name: '$index_code', type: 'STRING', data: segments.map(s => s.code) },
    { name: '$index_level', type: 'INT32', data: segments.map(() => 1) },
    { name: 'name', type: 'STRING', data: segments.map(s => s.title) },
    { name: '$data', type: 'STRING', data: segments.map(s => JSON.stringify(s)) },
  ]);

  // Write families (sorted by code)
  console.log('  Writing families...');
  families.sort((a, b) => a.code.localeCompare(b.code));
  await writeParquet(join(OUTPUT_DIR, 'families.parquet'), [
    { name: '$id', type: 'STRING', data: families.map(f => `family:${f.code}`) },
    { name: '$index_code', type: 'STRING', data: families.map(f => f.code) },
    { name: '$index_segmentCode', type: 'STRING', data: families.map(f => f.segmentCode) },
    { name: '$index_level', type: 'INT32', data: families.map(() => 2) },
    { name: 'name', type: 'STRING', data: families.map(f => f.title) },
    { name: '$data', type: 'STRING', data: families.map(f => JSON.stringify(f)) },
  ]);

  // Write classes (sorted by code)
  console.log('  Writing classes...');
  classes.sort((a, b) => a.code.localeCompare(b.code));
  await writeParquet(join(OUTPUT_DIR, 'classes.parquet'), [
    { name: '$id', type: 'STRING', data: classes.map(c => `class:${c.code}`) },
    { name: '$index_code', type: 'STRING', data: classes.map(c => c.code) },
    { name: '$index_familyCode', type: 'STRING', data: classes.map(c => c.familyCode) },
    { name: '$index_segmentCode', type: 'STRING', data: classes.map(c => c.segmentCode) },
    { name: '$index_level', type: 'INT32', data: classes.map(() => 3) },
    { name: 'name', type: 'STRING', data: classes.map(c => c.title) },
    { name: '$data', type: 'STRING', data: classes.map(c => JSON.stringify(c)) },
  ], 1000);

  // Write commodities (sorted by code for row-group statistics)
  console.log('  Writing commodities (this may take a moment)...');
  commodities.sort((a, b) => a.code.localeCompare(b.code));
  await writeParquet(join(OUTPUT_DIR, 'commodities.parquet'), [
    { name: '$id', type: 'STRING', data: commodities.map(c => `commodity:${c.code}`) },
    { name: '$index_code', type: 'STRING', data: commodities.map(c => c.code) },
    { name: '$index_classCode', type: 'STRING', data: commodities.map(c => c.classCode) },
    { name: '$index_familyCode', type: 'STRING', data: commodities.map(c => c.familyCode) },
    { name: '$index_segmentCode', type: 'STRING', data: commodities.map(c => c.segmentCode) },
    { name: '$index_level', type: 'INT32', data: commodities.map(() => 4) },
    { name: 'name', type: 'STRING', data: commodities.map(c => c.title) },
    { name: '$data', type: 'STRING', data: commodities.map(c => JSON.stringify(c)) },
  ], 5000);

  // Calculate total size
  const files = await fs.readdir(OUTPUT_DIR);
  let totalSize = 0;
  for (const file of files) {
    if (file.endsWith('.parquet')) {
      const stat = await fs.stat(join(OUTPUT_DIR, file));
      totalSize += stat.size;
    }
  }

  const elapsed = (Date.now() - startTime) / 1000;
  console.log('\n' + '='.repeat(60));
  console.log(`Total output: ${(totalSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Total time: ${elapsed.toFixed(1)}s`);
  console.log('='.repeat(60));

  // Print row group info for commodities
  const rowGroups = Math.ceil(commodities.length / 5000);
  console.log(`\nCommodities row groups: ${rowGroups} (5,000 rows each)`);
  console.log('Row group statistics enable prefix searches on sorted code column.');
}

// =============================================================================
// Main
// =============================================================================

console.log('='.repeat(60));
console.log('Scaling UNSPSC Dataset to Full Taxonomy Size');
console.log('='.repeat(60));
console.log(`Output: ${OUTPUT_DIR}`);
console.log(`Target: 55 segments, 400 families, 4,000 classes, 70,000 commodities`);

await generateUNSPSC();
