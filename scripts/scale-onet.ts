#!/usr/bin/env bun
/**
 * Scale O*NET Dataset to Realistic Size
 *
 * Generates full-scale O*NET data for benchmarking:
 * - 1,000 occupations with realistic SOC codes (11-xxxx through 53-xxxx)
 * - 35 skills with element IDs (2.A.1.a through 2.B.5.j)
 * - 52 abilities
 * - 33 knowledge areas
 * - ~100,000 occupation-skill ratings
 * - ~50,000 occupation-ability ratings
 * - ~30,000 occupation-knowledge ratings
 *
 * Uses dual Variant architecture: $id | $index_* | $data columns
 */

import { parquetWriteBuffer } from 'hyparquet-writer';
import { promises as fs } from 'node:fs';
import { join } from 'node:path';

const OUTPUT_DIR = './data-v3/onet-full';
const ROW_GROUP_SIZE = 10000;

// =============================================================================
// Types
// =============================================================================

interface ColumnData {
  name: string;
  type: 'STRING' | 'INT32' | 'DOUBLE';
  data: (string | number)[];
}

interface SOCMajorGroup {
  prefix: string;
  name: string;
  count: number;
}

interface Occupation {
  socCode: string;
  title: string;
  majorGroup: string;
  majorGroupName: string;
  jobZone: number;
  description: string;
}

interface Skill {
  elementId: string;
  name: string;
  category: string;
  subcategory: string;
}

interface Ability {
  elementId: string;
  name: string;
  category: string;
  subcategory: string;
}

interface KnowledgeArea {
  elementId: string;
  name: string;
  category: string;
}

interface OccupationSkillRating {
  id: string;
  socCode: string;
  elementId: string;
  skillName: string;
  scaleId: string;
  scaleName: string;
  importance: number;
  level: number;
  dataValue: number;
  n: number;
  standardError: number;
  date: string;
  domainSource: string;
}

interface OccupationAbilityRating {
  id: string;
  socCode: string;
  elementId: string;
  abilityName: string;
  importance: number;
  level: number;
  n: number;
  standardError: number;
  date: string;
  domainSource: string;
}

interface OccupationKnowledgeRating {
  id: string;
  socCode: string;
  elementId: string;
  knowledgeName: string;
  importance: number;
  level: number;
  n: number;
  standardError: number;
  date: string;
  domainSource: string;
}

interface SkillProfile {
  high: string[];
  medium: string[];
}

// =============================================================================
// Utilities
// =============================================================================

async function writeParquet(path: string, columnData: ColumnData[], rowGroupSize: number = ROW_GROUP_SIZE): Promise<number> {
  const buffer = parquetWriteBuffer({ columnData, rowGroupSize });
  const fullPath = join(OUTPUT_DIR, path);
  await fs.mkdir(fullPath.split('/').slice(0, -1).join('/') || OUTPUT_DIR, { recursive: true });
  await fs.writeFile(fullPath, Buffer.from(buffer));
  const rows = columnData[0]?.data?.length || 0;
  console.log(`  Wrote ${path}: ${buffer.byteLength.toLocaleString()} bytes (${rows.toLocaleString()} rows, ${Math.ceil(rows / rowGroupSize)} row groups)`);
  return buffer.byteLength;
}

// Seeded random for reproducibility
function seededRandom(seed: number): () => number {
  return function(): number {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
}

const random = seededRandom(42);

// Generate a random value in range with normal-ish distribution
function randomInRange(min: number, max: number): number {
  // Average of two randoms for slightly normal distribution
  const r = (random() + random()) / 2;
  return min + r * (max - min);
}

// =============================================================================
// SOC Code Generation
// =============================================================================

// Real SOC major groups (11-xxxx through 53-xxxx)
// Total: 1,000 occupations (adjusted counts to match real O*NET proportions)
const SOC_MAJOR_GROUPS: SOCMajorGroup[] = [
  { prefix: '11', name: 'Management', count: 45 },
  { prefix: '13', name: 'Business and Financial Operations', count: 55 },
  { prefix: '15', name: 'Computer and Mathematical', count: 40 },
  { prefix: '17', name: 'Architecture and Engineering', count: 50 },
  { prefix: '19', name: 'Life, Physical, and Social Science', count: 45 },
  { prefix: '21', name: 'Community and Social Service', count: 25 },
  { prefix: '23', name: 'Legal', count: 15 },
  { prefix: '25', name: 'Educational Instruction and Library', count: 40 },
  { prefix: '27', name: 'Arts, Design, Entertainment, Sports, and Media', count: 50 },
  { prefix: '29', name: 'Healthcare Practitioners and Technical', count: 80 },
  { prefix: '31', name: 'Healthcare Support', count: 25 },
  { prefix: '33', name: 'Protective Service', count: 25 },
  { prefix: '35', name: 'Food Preparation and Serving Related', count: 35 },
  { prefix: '37', name: 'Building and Grounds Cleaning and Maintenance', count: 20 },
  { prefix: '39', name: 'Personal Care and Service', count: 40 },
  { prefix: '41', name: 'Sales and Related', count: 50 },
  { prefix: '43', name: 'Office and Administrative Support', count: 75 },
  { prefix: '45', name: 'Farming, Fishing, and Forestry', count: 20 },
  { prefix: '47', name: 'Construction and Extraction', count: 55 },
  { prefix: '49', name: 'Installation, Maintenance, and Repair', count: 55 },
  { prefix: '51', name: 'Production', count: 100 },
  { prefix: '53', name: 'Transportation and Material Moving', count: 55 },
];

// Sample occupation titles by category
const OCCUPATION_TITLES: Record<string, string[]> = {
  '11': ['Chief Executive', 'General Manager', 'Operations Manager', 'Marketing Manager', 'Sales Manager', 'Human Resources Manager', 'Training Manager', 'Financial Manager', 'IT Manager', 'Project Manager'],
  '13': ['Accountant', 'Auditor', 'Budget Analyst', 'Financial Analyst', 'Tax Preparer', 'Cost Estimator', 'Management Analyst', 'Market Research Analyst', 'Buyer', 'Claims Adjuster'],
  '15': ['Software Developer', 'Data Scientist', 'Systems Analyst', 'Database Administrator', 'Network Architect', 'Security Analyst', 'Web Developer', 'DevOps Engineer', 'Machine Learning Engineer', 'QA Engineer'],
  '17': ['Architect', 'Civil Engineer', 'Mechanical Engineer', 'Electrical Engineer', 'Chemical Engineer', 'Industrial Engineer', 'Environmental Engineer', 'Surveyor', 'Drafter', 'Aerospace Engineer'],
  '19': ['Biologist', 'Chemist', 'Physicist', 'Geologist', 'Environmental Scientist', 'Psychologist', 'Sociologist', 'Economist', 'Statistician', 'Research Scientist'],
  '21': ['Social Worker', 'Counselor', 'Probation Officer', 'Community Health Worker', 'Marriage Therapist', 'Rehabilitation Counselor', 'Substance Abuse Counselor', 'Mental Health Counselor', 'Case Manager', 'Youth Worker'],
  '23': ['Lawyer', 'Judge', 'Paralegal', 'Legal Secretary', 'Court Reporter', 'Title Examiner', 'Arbitrator', 'Mediator', 'Law Clerk', 'Compliance Officer'],
  '25': ['Teacher', 'Professor', 'Librarian', 'Archivist', 'Instructional Coordinator', 'Special Education Teacher', 'Tutor', 'Teaching Assistant', 'Curriculum Developer', 'Education Administrator'],
  '27': ['Graphic Designer', 'Writer', 'Editor', 'Photographer', 'Video Producer', 'Animator', 'Musician', 'Actor', 'Director', 'Art Director'],
  '29': ['Physician', 'Surgeon', 'Nurse', 'Pharmacist', 'Dentist', 'Optometrist', 'Physical Therapist', 'Occupational Therapist', 'Speech Pathologist', 'Veterinarian'],
  '31': ['Nursing Assistant', 'Medical Assistant', 'Dental Assistant', 'Pharmacy Technician', 'Physical Therapy Aide', 'Home Health Aide', 'Orderly', 'Psychiatric Aide', 'Massage Therapist', 'Phlebotomist'],
  '33': ['Police Officer', 'Detective', 'Firefighter', 'Security Guard', 'Corrections Officer', 'Lifeguard', 'Fish and Game Warden', 'Parking Enforcement', 'TSA Agent', 'Private Investigator'],
  '35': ['Chef', 'Cook', 'Food Prep Worker', 'Bartender', 'Waiter', 'Host', 'Barista', 'Fast Food Worker', 'Dishwasher', 'Food Service Manager'],
  '37': ['Janitor', 'Maid', 'Groundskeeper', 'Pest Control Worker', 'Tree Trimmer', 'Landscaper', 'Pool Cleaner', 'Window Washer', 'Building Maintenance', 'Cleaning Supervisor'],
  '39': ['Hairdresser', 'Fitness Trainer', 'Recreation Worker', 'Childcare Worker', 'Animal Trainer', 'Tour Guide', 'Concierge', 'Funeral Attendant', 'Personal Care Aide', 'Esthetician'],
  '41': ['Retail Salesperson', 'Cashier', 'Sales Representative', 'Real Estate Agent', 'Insurance Agent', 'Advertising Sales', 'Telemarketer', 'Counter Clerk', 'Parts Salesperson', 'Travel Agent'],
  '43': ['Administrative Assistant', 'Receptionist', 'Data Entry Clerk', 'File Clerk', 'Bookkeeper', 'Customer Service Rep', 'Shipping Clerk', 'Mail Clerk', 'Office Manager', 'Payroll Clerk'],
  '45': ['Farmer', 'Agricultural Worker', 'Fisher', 'Hunter', 'Logger', 'Forest Worker', 'Grader and Sorter', 'Nursery Worker', 'Farm Equipment Operator', 'Animal Breeder'],
  '47': ['Carpenter', 'Electrician', 'Plumber', 'Mason', 'Roofer', 'Ironworker', 'Painter', 'Drywall Installer', 'Heavy Equipment Operator', 'Miner'],
  '49': ['Mechanic', 'HVAC Technician', 'Appliance Repairer', 'Computer Technician', 'Telecommunications Tech', 'Line Installer', 'Industrial Machinery Mechanic', 'Locksmith', 'Elevator Installer', 'Small Engine Mechanic'],
  '51': ['Assembler', 'Machinist', 'Welder', 'Quality Inspector', 'Packaging Operator', 'Printing Press Operator', 'Textile Worker', 'Food Processing Worker', 'Chemical Operator', 'CNC Operator'],
  '53': ['Truck Driver', 'Bus Driver', 'Taxi Driver', 'Delivery Driver', 'Pilot', 'Flight Attendant', 'Ship Captain', 'Material Handler', 'Forklift Operator', 'Railroad Worker'],
};

// =============================================================================
// Skills, Abilities, and Knowledge Definitions
// =============================================================================

// 35 Skills based on O*NET skill categories
const SKILLS: Skill[] = [
  // Basic Skills (Content) - 2.A.1.*
  { elementId: '2.A.1.a', name: 'Reading Comprehension', category: 'Basic Skills', subcategory: 'Content' },
  { elementId: '2.A.1.b', name: 'Active Listening', category: 'Basic Skills', subcategory: 'Content' },
  { elementId: '2.A.1.c', name: 'Writing', category: 'Basic Skills', subcategory: 'Content' },
  { elementId: '2.A.1.d', name: 'Speaking', category: 'Basic Skills', subcategory: 'Content' },
  { elementId: '2.A.1.e', name: 'Mathematics', category: 'Basic Skills', subcategory: 'Content' },
  { elementId: '2.A.1.f', name: 'Science', category: 'Basic Skills', subcategory: 'Content' },

  // Basic Skills (Process) - 2.A.2.*
  { elementId: '2.A.2.a', name: 'Critical Thinking', category: 'Basic Skills', subcategory: 'Process' },
  { elementId: '2.A.2.b', name: 'Active Learning', category: 'Basic Skills', subcategory: 'Process' },
  { elementId: '2.A.2.c', name: 'Learning Strategies', category: 'Basic Skills', subcategory: 'Process' },
  { elementId: '2.A.2.d', name: 'Monitoring', category: 'Basic Skills', subcategory: 'Process' },

  // Social Skills - 2.B.1.*
  { elementId: '2.B.1.a', name: 'Social Perceptiveness', category: 'Social Skills', subcategory: 'Social' },
  { elementId: '2.B.1.b', name: 'Coordination', category: 'Social Skills', subcategory: 'Social' },
  { elementId: '2.B.1.c', name: 'Persuasion', category: 'Social Skills', subcategory: 'Social' },
  { elementId: '2.B.1.d', name: 'Negotiation', category: 'Social Skills', subcategory: 'Social' },
  { elementId: '2.B.1.e', name: 'Instructing', category: 'Social Skills', subcategory: 'Social' },
  { elementId: '2.B.1.f', name: 'Service Orientation', category: 'Social Skills', subcategory: 'Social' },

  // Complex Problem Solving - 2.B.2.*
  { elementId: '2.B.2.i', name: 'Complex Problem Solving', category: 'Complex Problem Solving', subcategory: 'Problem Solving' },

  // Technical Skills - 2.B.3.*
  { elementId: '2.B.3.a', name: 'Operations Analysis', category: 'Technical Skills', subcategory: 'Operations' },
  { elementId: '2.B.3.b', name: 'Technology Design', category: 'Technical Skills', subcategory: 'Design' },
  { elementId: '2.B.3.c', name: 'Equipment Selection', category: 'Technical Skills', subcategory: 'Equipment' },
  { elementId: '2.B.3.d', name: 'Installation', category: 'Technical Skills', subcategory: 'Equipment' },
  { elementId: '2.B.3.e', name: 'Programming', category: 'Technical Skills', subcategory: 'Design' },
  { elementId: '2.B.3.g', name: 'Operation Monitoring', category: 'Technical Skills', subcategory: 'Operations' },
  { elementId: '2.B.3.h', name: 'Operation and Control', category: 'Technical Skills', subcategory: 'Operations' },
  { elementId: '2.B.3.j', name: 'Equipment Maintenance', category: 'Technical Skills', subcategory: 'Equipment' },
  { elementId: '2.B.3.k', name: 'Troubleshooting', category: 'Technical Skills', subcategory: 'Equipment' },
  { elementId: '2.B.3.l', name: 'Repairing', category: 'Technical Skills', subcategory: 'Equipment' },
  { elementId: '2.B.3.m', name: 'Quality Control Analysis', category: 'Technical Skills', subcategory: 'Quality' },

  // Systems Skills - 2.B.4.*
  { elementId: '2.B.4.e', name: 'Judgment and Decision Making', category: 'Systems Skills', subcategory: 'Judgment' },
  { elementId: '2.B.4.g', name: 'Systems Analysis', category: 'Systems Skills', subcategory: 'Analysis' },
  { elementId: '2.B.4.h', name: 'Systems Evaluation', category: 'Systems Skills', subcategory: 'Evaluation' },

  // Resource Management - 2.B.5.*
  { elementId: '2.B.5.a', name: 'Time Management', category: 'Resource Management', subcategory: 'Time' },
  { elementId: '2.B.5.b', name: 'Management of Financial Resources', category: 'Resource Management', subcategory: 'Financial' },
  { elementId: '2.B.5.c', name: 'Management of Material Resources', category: 'Resource Management', subcategory: 'Material' },
  { elementId: '2.B.5.d', name: 'Management of Personnel Resources', category: 'Resource Management', subcategory: 'Personnel' },
];

// 52 Abilities based on O*NET ability categories
const ABILITIES: Ability[] = [
  // Cognitive Abilities - Verbal
  { elementId: '1.A.1.a.1', name: 'Oral Comprehension', category: 'Cognitive', subcategory: 'Verbal' },
  { elementId: '1.A.1.a.2', name: 'Written Comprehension', category: 'Cognitive', subcategory: 'Verbal' },
  { elementId: '1.A.1.a.3', name: 'Oral Expression', category: 'Cognitive', subcategory: 'Verbal' },
  { elementId: '1.A.1.a.4', name: 'Written Expression', category: 'Cognitive', subcategory: 'Verbal' },

  // Cognitive Abilities - Idea Generation and Reasoning
  { elementId: '1.A.1.b.1', name: 'Fluency of Ideas', category: 'Cognitive', subcategory: 'Idea Generation' },
  { elementId: '1.A.1.b.2', name: 'Originality', category: 'Cognitive', subcategory: 'Idea Generation' },
  { elementId: '1.A.1.b.3', name: 'Problem Sensitivity', category: 'Cognitive', subcategory: 'Reasoning' },
  { elementId: '1.A.1.b.4', name: 'Deductive Reasoning', category: 'Cognitive', subcategory: 'Reasoning' },
  { elementId: '1.A.1.b.5', name: 'Inductive Reasoning', category: 'Cognitive', subcategory: 'Reasoning' },
  { elementId: '1.A.1.b.6', name: 'Information Ordering', category: 'Cognitive', subcategory: 'Reasoning' },
  { elementId: '1.A.1.b.7', name: 'Category Flexibility', category: 'Cognitive', subcategory: 'Reasoning' },

  // Cognitive Abilities - Quantitative
  { elementId: '1.A.1.c.1', name: 'Mathematical Reasoning', category: 'Cognitive', subcategory: 'Quantitative' },
  { elementId: '1.A.1.c.2', name: 'Number Facility', category: 'Cognitive', subcategory: 'Quantitative' },

  // Cognitive Abilities - Memory
  { elementId: '1.A.1.d.1', name: 'Memorization', category: 'Cognitive', subcategory: 'Memory' },

  // Cognitive Abilities - Perceptual
  { elementId: '1.A.1.e.1', name: 'Speed of Closure', category: 'Cognitive', subcategory: 'Perceptual' },
  { elementId: '1.A.1.e.2', name: 'Flexibility of Closure', category: 'Cognitive', subcategory: 'Perceptual' },
  { elementId: '1.A.1.e.3', name: 'Perceptual Speed', category: 'Cognitive', subcategory: 'Perceptual' },

  // Cognitive Abilities - Spatial
  { elementId: '1.A.1.f.1', name: 'Spatial Orientation', category: 'Cognitive', subcategory: 'Spatial' },
  { elementId: '1.A.1.f.2', name: 'Visualization', category: 'Cognitive', subcategory: 'Spatial' },

  // Cognitive Abilities - Attentiveness
  { elementId: '1.A.1.g.1', name: 'Selective Attention', category: 'Cognitive', subcategory: 'Attentiveness' },
  { elementId: '1.A.1.g.2', name: 'Time Sharing', category: 'Cognitive', subcategory: 'Attentiveness' },

  // Psychomotor Abilities - Fine Manipulative
  { elementId: '1.A.2.a.1', name: 'Arm-Hand Steadiness', category: 'Psychomotor', subcategory: 'Fine Manipulative' },
  { elementId: '1.A.2.a.2', name: 'Manual Dexterity', category: 'Psychomotor', subcategory: 'Fine Manipulative' },
  { elementId: '1.A.2.a.3', name: 'Finger Dexterity', category: 'Psychomotor', subcategory: 'Fine Manipulative' },

  // Psychomotor Abilities - Control Movement
  { elementId: '1.A.2.b.1', name: 'Control Precision', category: 'Psychomotor', subcategory: 'Control Movement' },
  { elementId: '1.A.2.b.2', name: 'Multilimb Coordination', category: 'Psychomotor', subcategory: 'Control Movement' },
  { elementId: '1.A.2.b.3', name: 'Response Orientation', category: 'Psychomotor', subcategory: 'Control Movement' },
  { elementId: '1.A.2.b.4', name: 'Rate Control', category: 'Psychomotor', subcategory: 'Control Movement' },

  // Psychomotor Abilities - Reaction Time and Speed
  { elementId: '1.A.2.c.1', name: 'Reaction Time', category: 'Psychomotor', subcategory: 'Reaction Time' },
  { elementId: '1.A.2.c.2', name: 'Wrist-Finger Speed', category: 'Psychomotor', subcategory: 'Speed' },
  { elementId: '1.A.2.c.3', name: 'Speed of Limb Movement', category: 'Psychomotor', subcategory: 'Speed' },

  // Physical Abilities - Strength
  { elementId: '1.A.3.a.1', name: 'Static Strength', category: 'Physical', subcategory: 'Strength' },
  { elementId: '1.A.3.a.2', name: 'Explosive Strength', category: 'Physical', subcategory: 'Strength' },
  { elementId: '1.A.3.a.3', name: 'Dynamic Strength', category: 'Physical', subcategory: 'Strength' },
  { elementId: '1.A.3.a.4', name: 'Trunk Strength', category: 'Physical', subcategory: 'Strength' },

  // Physical Abilities - Endurance
  { elementId: '1.A.3.b.1', name: 'Stamina', category: 'Physical', subcategory: 'Endurance' },

  // Physical Abilities - Flexibility, Balance, and Coordination
  { elementId: '1.A.3.c.1', name: 'Extent Flexibility', category: 'Physical', subcategory: 'Flexibility' },
  { elementId: '1.A.3.c.2', name: 'Dynamic Flexibility', category: 'Physical', subcategory: 'Flexibility' },
  { elementId: '1.A.3.c.3', name: 'Gross Body Coordination', category: 'Physical', subcategory: 'Coordination' },
  { elementId: '1.A.3.c.4', name: 'Gross Body Equilibrium', category: 'Physical', subcategory: 'Balance' },

  // Sensory Abilities - Visual
  { elementId: '1.A.4.a.1', name: 'Near Vision', category: 'Sensory', subcategory: 'Visual' },
  { elementId: '1.A.4.a.2', name: 'Far Vision', category: 'Sensory', subcategory: 'Visual' },
  { elementId: '1.A.4.a.3', name: 'Visual Color Discrimination', category: 'Sensory', subcategory: 'Visual' },
  { elementId: '1.A.4.a.4', name: 'Night Vision', category: 'Sensory', subcategory: 'Visual' },
  { elementId: '1.A.4.a.5', name: 'Peripheral Vision', category: 'Sensory', subcategory: 'Visual' },
  { elementId: '1.A.4.a.6', name: 'Depth Perception', category: 'Sensory', subcategory: 'Visual' },
  { elementId: '1.A.4.a.7', name: 'Glare Sensitivity', category: 'Sensory', subcategory: 'Visual' },

  // Sensory Abilities - Auditory and Speech
  { elementId: '1.A.4.b.1', name: 'Hearing Sensitivity', category: 'Sensory', subcategory: 'Auditory' },
  { elementId: '1.A.4.b.2', name: 'Auditory Attention', category: 'Sensory', subcategory: 'Auditory' },
  { elementId: '1.A.4.b.3', name: 'Sound Localization', category: 'Sensory', subcategory: 'Auditory' },
  { elementId: '1.A.4.b.4', name: 'Speech Recognition', category: 'Sensory', subcategory: 'Speech' },
  { elementId: '1.A.4.b.5', name: 'Speech Clarity', category: 'Sensory', subcategory: 'Speech' },
];

// 33 Knowledge areas based on O*NET
const KNOWLEDGE_AREAS: KnowledgeArea[] = [
  // Business and Management
  { elementId: '2.C.1.a', name: 'Administration and Management', category: 'Business and Management' },
  { elementId: '2.C.1.b', name: 'Clerical', category: 'Business and Management' },
  { elementId: '2.C.1.c', name: 'Economics and Accounting', category: 'Business and Management' },
  { elementId: '2.C.1.d', name: 'Sales and Marketing', category: 'Business and Management' },
  { elementId: '2.C.1.e', name: 'Customer and Personal Service', category: 'Business and Management' },
  { elementId: '2.C.1.f', name: 'Personnel and Human Resources', category: 'Business and Management' },

  // Manufacturing and Production
  { elementId: '2.C.2.a', name: 'Production and Processing', category: 'Manufacturing and Production' },
  { elementId: '2.C.2.b', name: 'Food Production', category: 'Manufacturing and Production' },

  // Engineering and Technology
  { elementId: '2.C.3.a', name: 'Computers and Electronics', category: 'Engineering and Technology' },
  { elementId: '2.C.3.b', name: 'Engineering and Technology', category: 'Engineering and Technology' },
  { elementId: '2.C.3.c', name: 'Design', category: 'Engineering and Technology' },
  { elementId: '2.C.3.d', name: 'Building and Construction', category: 'Engineering and Technology' },
  { elementId: '2.C.3.e', name: 'Mechanical', category: 'Engineering and Technology' },

  // Mathematics and Science
  { elementId: '2.C.4.a', name: 'Mathematics', category: 'Mathematics and Science' },
  { elementId: '2.C.4.b', name: 'Physics', category: 'Mathematics and Science' },
  { elementId: '2.C.4.c', name: 'Chemistry', category: 'Mathematics and Science' },
  { elementId: '2.C.4.d', name: 'Biology', category: 'Mathematics and Science' },
  { elementId: '2.C.4.e', name: 'Psychology', category: 'Mathematics and Science' },
  { elementId: '2.C.4.f', name: 'Sociology and Anthropology', category: 'Mathematics and Science' },
  { elementId: '2.C.4.g', name: 'Geography', category: 'Mathematics and Science' },

  // Health Services
  { elementId: '2.C.5.a', name: 'Medicine and Dentistry', category: 'Health Services' },
  { elementId: '2.C.5.b', name: 'Therapy and Counseling', category: 'Health Services' },

  // Education and Training
  { elementId: '2.C.6', name: 'Education and Training', category: 'Education and Training' },

  // Arts and Humanities
  { elementId: '2.C.7.a', name: 'English Language', category: 'Arts and Humanities' },
  { elementId: '2.C.7.b', name: 'Foreign Language', category: 'Arts and Humanities' },
  { elementId: '2.C.7.c', name: 'Fine Arts', category: 'Arts and Humanities' },
  { elementId: '2.C.7.d', name: 'History and Archeology', category: 'Arts and Humanities' },
  { elementId: '2.C.7.e', name: 'Philosophy and Theology', category: 'Arts and Humanities' },

  // Law and Public Safety
  { elementId: '2.C.8.a', name: 'Public Safety and Security', category: 'Law and Public Safety' },
  { elementId: '2.C.8.b', name: 'Law and Government', category: 'Law and Public Safety' },

  // Communications
  { elementId: '2.C.9.a', name: 'Telecommunications', category: 'Communications' },
  { elementId: '2.C.9.b', name: 'Communications and Media', category: 'Communications' },

  // Transportation
  { elementId: '2.C.10', name: 'Transportation', category: 'Transportation' },
];

// =============================================================================
// Skill/Ability Profiles by Occupation Category
// =============================================================================

// Define which skills are important for each SOC major group
const SKILL_PROFILES: Record<string, SkillProfile> = {
  '11': { high: ['2.A.1.c', '2.A.1.d', '2.B.1.d', '2.B.4.e', '2.B.5.d'], medium: ['2.A.2.a', '2.B.1.c', '2.B.5.b'] },
  '13': { high: ['2.A.1.a', '2.A.1.e', '2.A.2.a', '2.B.4.g'], medium: ['2.A.1.c', '2.B.4.e', '2.B.5.a'] },
  '15': { high: ['2.B.3.e', '2.B.2.i', '2.A.2.a', '2.B.4.g'], medium: ['2.A.1.a', '2.B.3.k', '2.A.2.b'] },
  '17': { high: ['2.A.1.e', '2.B.3.b', '2.B.4.g', '2.A.2.a'], medium: ['2.A.1.a', '2.B.3.a', '2.B.2.i'] },
  '19': { high: ['2.A.1.f', '2.A.1.a', '2.A.2.a', '2.A.1.c'], medium: ['2.A.2.b', '2.B.4.g', '2.A.1.e'] },
  '21': { high: ['2.A.1.b', '2.B.1.a', '2.B.1.f', '2.A.1.d'], medium: ['2.A.2.a', '2.B.1.e', '2.A.1.c'] },
  '23': { high: ['2.A.1.a', '2.A.1.c', '2.B.1.c', '2.A.2.a'], medium: ['2.A.1.d', '2.B.1.d', '2.B.4.e'] },
  '25': { high: ['2.B.1.e', '2.A.1.d', '2.A.1.b', '2.A.2.c'], medium: ['2.A.2.b', '2.B.1.a', '2.A.1.a'] },
  '27': { high: ['2.A.1.c', '2.B.3.b', '2.A.2.a'], medium: ['2.B.1.a', '2.A.1.d', '2.B.5.a'] },
  '29': { high: ['2.A.1.a', '2.A.2.a', '2.B.2.i', '2.B.4.e'], medium: ['2.A.1.b', '2.B.1.f', '2.A.1.f'] },
  '31': { high: ['2.B.1.f', '2.A.1.b', '2.B.1.a'], medium: ['2.A.2.d', '2.A.1.d', '2.B.5.a'] },
  '33': { high: ['2.A.1.b', '2.B.1.a', '2.A.2.a', '2.A.1.d'], medium: ['2.B.4.e', '2.B.2.i', '2.B.1.b'] },
  '35': { high: ['2.B.1.f', '2.B.5.a', '2.B.1.b'], medium: ['2.A.1.b', '2.A.2.d', '2.A.1.d'] },
  '37': { high: ['2.B.3.j', '2.B.3.h', '2.B.5.a'], medium: ['2.A.2.d', '2.B.3.k', '2.B.3.c'] },
  '39': { high: ['2.B.1.f', '2.A.1.b', '2.B.1.a'], medium: ['2.A.1.d', '2.B.5.a', '2.B.1.e'] },
  '41': { high: ['2.B.1.c', '2.A.1.d', '2.B.1.f', '2.B.1.d'], medium: ['2.A.1.b', '2.A.2.a', '2.B.1.a'] },
  '43': { high: ['2.A.1.a', '2.A.1.c', '2.A.1.b', '2.B.5.a'], medium: ['2.A.2.d', '2.B.1.f', '2.B.1.b'] },
  '45': { high: ['2.B.3.h', '2.B.3.c', '2.A.2.d'], medium: ['2.B.3.j', '2.B.5.a', '2.B.3.k'] },
  '47': { high: ['2.B.3.d', '2.B.3.l', '2.B.3.j', '2.B.3.k'], medium: ['2.B.3.h', '2.B.3.c', '2.B.5.a'] },
  '49': { high: ['2.B.3.k', '2.B.3.l', '2.B.3.j', '2.B.3.g'], medium: ['2.B.3.c', '2.B.2.i', '2.A.1.a'] },
  '51': { high: ['2.B.3.h', '2.A.2.d', '2.B.3.m', '2.B.3.g'], medium: ['2.B.3.k', '2.B.5.a', '2.B.3.c'] },
  '53': { high: ['2.B.3.h', '2.A.2.d', '2.B.5.a'], medium: ['2.B.3.g', '2.A.1.b', '2.B.1.b'] },
};

// =============================================================================
// Data Generation
// =============================================================================

function generateOccupations(): Occupation[] {
  console.log('  Generating 1,000 occupations...');
  const occupations: Occupation[] = [];

  for (const group of SOC_MAJOR_GROUPS) {
    const titles = OCCUPATION_TITLES[group.prefix] || ['Specialist'];

    for (let i = 0; i < group.count; i++) {
      const minorCode = 1000 + Math.floor(i / 10) * 10 + (i % 10);
      const socCode = `${group.prefix}-${minorCode}.00`;
      const titleBase = titles[i % titles.length];
      const titleSuffix = i >= titles.length ? ` ${Math.floor(i / titles.length) + 1}` : '';

      // Job zone based on occupation type
      let jobZone: number;
      if (['15', '17', '19', '23', '29'].includes(group.prefix)) {
        jobZone = Math.floor(randomInRange(3, 5.99)); // Higher skill jobs
      } else if (['31', '35', '37', '39', '45'].includes(group.prefix)) {
        jobZone = Math.floor(randomInRange(1, 3.99)); // Lower skill jobs
      } else {
        jobZone = Math.floor(randomInRange(2, 4.99)); // Medium skill jobs
      }

      occupations.push({
        socCode,
        title: `${titleBase}${titleSuffix}`,
        majorGroup: group.prefix,
        majorGroupName: group.name,
        jobZone,
        description: `Performs duties related to ${titleBase.toLowerCase()} in the ${group.name.toLowerCase()} field.`,
      });
    }
  }

  // Sort by SOC code for row-group statistics
  occupations.sort((a, b) => a.socCode.localeCompare(b.socCode));
  return occupations;
}

function generateOccupationSkillRatings(occupations: Occupation[]): OccupationSkillRating[] {
  console.log('  Generating occupation-skill ratings (~100K target)...');
  const ratings: OccupationSkillRating[] = [];

  for (const occ of occupations) {
    const profile = SKILL_PROFILES[occ.majorGroup] || { high: [], medium: [] };

    // Each occupation gets ratings for ALL skills (100% coverage)
    // Real O*NET stores importance and level as separate scale records
    // We generate BOTH to reach the ~100K target
    for (const skill of SKILLS) {
      let importanceValue: number;
      let levelValue: number;

      if (profile.high.includes(skill.elementId)) {
        importanceValue = randomInRange(3.5, 5.0);
        levelValue = randomInRange(4.0, 7.0);
      } else if (profile.medium.includes(skill.elementId)) {
        importanceValue = randomInRange(2.5, 4.0);
        levelValue = randomInRange(3.0, 5.5);
      } else {
        importanceValue = randomInRange(1.0, 3.5);
        levelValue = randomInRange(0.5, 4.0);
      }

      // Importance record (scale IM - 1 to 5)
      ratings.push({
        id: `${occ.socCode}:${skill.elementId}:IM`,
        socCode: occ.socCode,
        elementId: skill.elementId,
        skillName: skill.name,
        scaleId: 'IM',
        scaleName: 'Importance',
        importance: Math.round(importanceValue * 100) / 100,
        level: 0,
        dataValue: Math.round(importanceValue * 100) / 100,
        n: Math.floor(randomInRange(8, 30)),
        standardError: Math.round(randomInRange(0.1, 0.5) * 100) / 100,
        date: '07/2024',
        domainSource: 'Analyst',
      });

      // Level record (scale LV - 0 to 7)
      ratings.push({
        id: `${occ.socCode}:${skill.elementId}:LV`,
        socCode: occ.socCode,
        elementId: skill.elementId,
        skillName: skill.name,
        scaleId: 'LV',
        scaleName: 'Level',
        importance: 0,
        level: Math.round(levelValue * 100) / 100,
        dataValue: Math.round(levelValue * 100) / 100,
        n: Math.floor(randomInRange(8, 30)),
        standardError: Math.round(randomInRange(0.1, 0.5) * 100) / 100,
        date: '07/2024',
        domainSource: 'Analyst',
      });

      // Relevance record (scale RL - whether skill is relevant to occupation)
      // This is a binary scale but stored as 0-100 in O*NET
      const relevance = importanceValue >= 2.5 ? randomInRange(70, 100) : randomInRange(20, 70);
      ratings.push({
        id: `${occ.socCode}:${skill.elementId}:RL`,
        socCode: occ.socCode,
        elementId: skill.elementId,
        skillName: skill.name,
        scaleId: 'RL',
        scaleName: 'Relevance',
        importance: 0,
        level: 0,
        dataValue: Math.round(relevance * 100) / 100,
        n: Math.floor(randomInRange(8, 30)),
        standardError: Math.round(randomInRange(0.05, 0.2) * 100) / 100,
        date: '07/2024',
        domainSource: 'Analyst',
      });
    }
  }

  // Sort by socCode for row-group statistics
  ratings.sort((a, b) => {
    const socCmp = a.socCode.localeCompare(b.socCode);
    if (socCmp !== 0) return socCmp;
    const elemCmp = a.elementId.localeCompare(b.elementId);
    if (elemCmp !== 0) return elemCmp;
    return a.scaleId.localeCompare(b.scaleId);
  });
  return ratings;
}

function generateOccupationAbilityRatings(occupations: Occupation[]): OccupationAbilityRating[] {
  console.log('  Generating occupation-ability ratings (~50K target)...');
  const ratings: OccupationAbilityRating[] = [];

  for (const occ of occupations) {
    // Each occupation gets ratings for ALL abilities (100% coverage)
    for (const ability of ABILITIES) {
      // Cognitive abilities more important for knowledge workers
      let importance: number;
      let level: number;
      const isKnowledgeWork = ['11', '13', '15', '17', '19', '23', '25', '29'].includes(occ.majorGroup);
      const isPhysicalWork = ['31', '35', '37', '45', '47', '49', '51', '53'].includes(occ.majorGroup);

      if (ability.category === 'Cognitive' && isKnowledgeWork) {
        importance = randomInRange(3.0, 5.0);
        level = randomInRange(3.5, 7.0);
      } else if ((ability.category === 'Physical' || ability.category === 'Psychomotor') && isPhysicalWork) {
        importance = randomInRange(3.0, 5.0);
        level = randomInRange(3.0, 6.0);
      } else {
        importance = randomInRange(1.0, 3.5);
        level = randomInRange(0.5, 4.0);
      }

      ratings.push({
        id: `${occ.socCode}:${ability.elementId}`,
        socCode: occ.socCode,
        elementId: ability.elementId,
        abilityName: ability.name,
        importance: Math.round(importance * 100) / 100,
        level: Math.round(level * 100) / 100,
        n: Math.floor(randomInRange(8, 30)),
        standardError: Math.round(randomInRange(0.1, 0.5) * 100) / 100,
        date: '07/2024',
        domainSource: 'Analyst',
      });
    }
  }

  // Sort by socCode for row-group statistics
  ratings.sort((a, b) => {
    const socCmp = a.socCode.localeCompare(b.socCode);
    if (socCmp !== 0) return socCmp;
    return a.elementId.localeCompare(b.elementId);
  });
  return ratings;
}

function generateOccupationKnowledgeRatings(occupations: Occupation[]): OccupationKnowledgeRating[] {
  console.log('  Generating occupation-knowledge ratings (~30K target)...');
  const ratings: OccupationKnowledgeRating[] = [];

  // Knowledge relevance by occupation group
  const KNOWLEDGE_PROFILES: Record<string, string[]> = {
    '11': ['2.C.1.a', '2.C.1.f', '2.C.1.c', '2.C.1.d'],
    '13': ['2.C.1.c', '2.C.1.b', '2.C.1.a', '2.C.4.a'],
    '15': ['2.C.3.a', '2.C.4.a', '2.C.3.b', '2.C.7.a'],
    '17': ['2.C.3.b', '2.C.4.a', '2.C.4.b', '2.C.3.c'],
    '19': ['2.C.4.d', '2.C.4.c', '2.C.4.b', '2.C.4.e'],
    '21': ['2.C.4.e', '2.C.5.b', '2.C.4.f', '2.C.6'],
    '23': ['2.C.8.b', '2.C.7.a', '2.C.1.a', '2.C.8.a'],
    '25': ['2.C.6', '2.C.7.a', '2.C.4.e', '2.C.7.d'],
    '27': ['2.C.7.c', '2.C.9.b', '2.C.3.c', '2.C.7.a'],
    '29': ['2.C.5.a', '2.C.4.d', '2.C.4.c', '2.C.5.b'],
    '31': ['2.C.5.a', '2.C.1.e', '2.C.5.b', '2.C.4.e'],
    '33': ['2.C.8.a', '2.C.8.b', '2.C.4.e', '2.C.1.e'],
    '35': ['2.C.2.b', '2.C.1.e', '2.C.2.a', '2.C.8.a'],
    '37': ['2.C.1.e', '2.C.4.c', '2.C.3.e', '2.C.8.a'],
    '39': ['2.C.1.e', '2.C.4.e', '2.C.5.b', '2.C.7.a'],
    '41': ['2.C.1.d', '2.C.1.e', '2.C.1.c', '2.C.7.a'],
    '43': ['2.C.1.b', '2.C.7.a', '2.C.1.e', '2.C.3.a'],
    '45': ['2.C.2.b', '2.C.4.d', '2.C.4.g', '2.C.2.a'],
    '47': ['2.C.3.d', '2.C.3.e', '2.C.4.a', '2.C.8.a'],
    '49': ['2.C.3.e', '2.C.3.a', '2.C.3.b', '2.C.4.a'],
    '51': ['2.C.2.a', '2.C.3.e', '2.C.4.a', '2.C.8.a'],
    '53': ['2.C.10', '2.C.8.a', '2.C.3.e', '2.C.4.g'],
  };

  for (const occ of occupations) {
    const relevantKnowledge = KNOWLEDGE_PROFILES[occ.majorGroup] || [];

    // Each occupation gets ratings for ALL knowledge areas (100% coverage)
    for (const knowledge of KNOWLEDGE_AREAS) {
      let importance: number;
      let level: number;

      if (relevantKnowledge.includes(knowledge.elementId)) {
        importance = randomInRange(3.5, 5.0);
        level = randomInRange(4.0, 7.0);
      } else {
        importance = randomInRange(1.0, 3.0);
        level = randomInRange(0.5, 3.5);
      }

      ratings.push({
        id: `${occ.socCode}:${knowledge.elementId}`,
        socCode: occ.socCode,
        elementId: knowledge.elementId,
        knowledgeName: knowledge.name,
        importance: Math.round(importance * 100) / 100,
        level: Math.round(level * 100) / 100,
        n: Math.floor(randomInRange(8, 30)),
        standardError: Math.round(randomInRange(0.1, 0.5) * 100) / 100,
        date: '07/2024',
        domainSource: 'Analyst',
      });
    }
  }

  // Sort by socCode for row-group statistics
  ratings.sort((a, b) => {
    const socCmp = a.socCode.localeCompare(b.socCode);
    if (socCmp !== 0) return socCmp;
    return a.elementId.localeCompare(b.elementId);
  });
  return ratings;
}

// =============================================================================
// Main
// =============================================================================

console.log('='.repeat(60));
console.log('Scaling O*NET Dataset to Realistic Size');
console.log('='.repeat(60));
console.log(`Output: ${OUTPUT_DIR}`);
console.log(`Row Group Size: ${ROW_GROUP_SIZE.toLocaleString()}`);
console.log();

await fs.mkdir(OUTPUT_DIR, { recursive: true });

// Generate all data
const occupations = generateOccupations();
const occupationSkills = generateOccupationSkillRatings(occupations);
const occupationAbilities = generateOccupationAbilityRatings(occupations);
const occupationKnowledge = generateOccupationKnowledgeRatings(occupations);

console.log();
console.log('Writing Parquet files...');
console.log();

// Write occupations
await writeParquet('occupations.parquet', [
  { name: '$id', type: 'STRING', data: occupations.map(o => `occupation:${o.socCode}`) },
  { name: '$index_socCode', type: 'STRING', data: occupations.map(o => o.socCode) },
  { name: '$index_majorGroup', type: 'STRING', data: occupations.map(o => o.majorGroup) },
  { name: '$index_jobZone', type: 'INT32', data: occupations.map(o => o.jobZone) },
  { name: 'name', type: 'STRING', data: occupations.map(o => o.title) },
  { name: '$data', type: 'STRING', data: occupations.map(o => JSON.stringify(o)) },
], 100);

// Write skills
await writeParquet('skills.parquet', [
  { name: '$id', type: 'STRING', data: SKILLS.map(s => `skill:${s.elementId}`) },
  { name: '$index_elementId', type: 'STRING', data: SKILLS.map(s => s.elementId) },
  { name: '$index_category', type: 'STRING', data: SKILLS.map(s => s.category) },
  { name: '$index_subcategory', type: 'STRING', data: SKILLS.map(s => s.subcategory) },
  { name: 'name', type: 'STRING', data: SKILLS.map(s => s.name) },
  { name: '$data', type: 'STRING', data: SKILLS.map(s => JSON.stringify(s)) },
]);

// Write abilities
await writeParquet('abilities.parquet', [
  { name: '$id', type: 'STRING', data: ABILITIES.map(a => `ability:${a.elementId}`) },
  { name: '$index_elementId', type: 'STRING', data: ABILITIES.map(a => a.elementId) },
  { name: '$index_category', type: 'STRING', data: ABILITIES.map(a => a.category) },
  { name: '$index_subcategory', type: 'STRING', data: ABILITIES.map(a => a.subcategory) },
  { name: 'name', type: 'STRING', data: ABILITIES.map(a => a.name) },
  { name: '$data', type: 'STRING', data: ABILITIES.map(a => JSON.stringify(a)) },
]);

// Write knowledge areas
await writeParquet('knowledge.parquet', [
  { name: '$id', type: 'STRING', data: KNOWLEDGE_AREAS.map(k => `knowledge:${k.elementId}`) },
  { name: '$index_elementId', type: 'STRING', data: KNOWLEDGE_AREAS.map(k => k.elementId) },
  { name: '$index_category', type: 'STRING', data: KNOWLEDGE_AREAS.map(k => k.category) },
  { name: 'name', type: 'STRING', data: KNOWLEDGE_AREAS.map(k => k.name) },
  { name: '$data', type: 'STRING', data: KNOWLEDGE_AREAS.map(k => JSON.stringify(k)) },
]);

// Write occupation-skills (largest file, use full row group size)
// Includes both Importance (IM) and Level (LV) records to match real O*NET structure
await writeParquet('occupation-skills.parquet', [
  { name: '$id', type: 'STRING', data: occupationSkills.map(os => `os:${os.id}`) },
  { name: '$index_socCode', type: 'STRING', data: occupationSkills.map(os => os.socCode) },
  { name: '$index_elementId', type: 'STRING', data: occupationSkills.map(os => os.elementId) },
  { name: '$index_scaleId', type: 'STRING', data: occupationSkills.map(os => os.scaleId) },
  { name: '$index_importance', type: 'DOUBLE', data: occupationSkills.map(os => os.importance) },
  { name: '$index_level', type: 'DOUBLE', data: occupationSkills.map(os => os.level) },
  { name: 'name', type: 'STRING', data: occupationSkills.map(os => os.skillName) },
  { name: '$data', type: 'STRING', data: occupationSkills.map(os => JSON.stringify(os)) },
], ROW_GROUP_SIZE);

// Write occupation-abilities
await writeParquet('occupation-abilities.parquet', [
  { name: '$id', type: 'STRING', data: occupationAbilities.map(oa => `oa:${oa.id}`) },
  { name: '$index_socCode', type: 'STRING', data: occupationAbilities.map(oa => oa.socCode) },
  { name: '$index_elementId', type: 'STRING', data: occupationAbilities.map(oa => oa.elementId) },
  { name: '$index_importance', type: 'DOUBLE', data: occupationAbilities.map(oa => oa.importance) },
  { name: '$index_level', type: 'DOUBLE', data: occupationAbilities.map(oa => oa.level) },
  { name: 'name', type: 'STRING', data: occupationAbilities.map(oa => oa.abilityName) },
  { name: '$data', type: 'STRING', data: occupationAbilities.map(oa => JSON.stringify(oa)) },
], ROW_GROUP_SIZE);

// Write occupation-knowledge
await writeParquet('occupation-knowledge.parquet', [
  { name: '$id', type: 'STRING', data: occupationKnowledge.map(ok => `ok:${ok.id}`) },
  { name: '$index_socCode', type: 'STRING', data: occupationKnowledge.map(ok => ok.socCode) },
  { name: '$index_elementId', type: 'STRING', data: occupationKnowledge.map(ok => ok.elementId) },
  { name: '$index_importance', type: 'DOUBLE', data: occupationKnowledge.map(ok => ok.importance) },
  { name: '$index_level', type: 'DOUBLE', data: occupationKnowledge.map(ok => ok.level) },
  { name: 'name', type: 'STRING', data: occupationKnowledge.map(ok => ok.knowledgeName) },
  { name: '$data', type: 'STRING', data: occupationKnowledge.map(ok => JSON.stringify(ok)) },
], ROW_GROUP_SIZE);

// Summary
console.log();
console.log('='.repeat(60));
console.log('Summary:');
console.log(`  Occupations: ${occupations.length.toLocaleString()}`);
console.log(`  Skills: ${SKILLS.length}`);
console.log(`  Abilities: ${ABILITIES.length}`);
console.log(`  Knowledge Areas: ${KNOWLEDGE_AREAS.length}`);
console.log(`  Occupation-Skill Ratings: ${occupationSkills.length.toLocaleString()}`);
console.log(`  Occupation-Ability Ratings: ${occupationAbilities.length.toLocaleString()}`);
console.log(`  Occupation-Knowledge Ratings: ${occupationKnowledge.length.toLocaleString()}`);
console.log();

// Calculate total size
const files = await fs.readdir(OUTPUT_DIR);
let totalSize = 0;
for (const file of files) {
  if (file.endsWith('.parquet')) {
    const stat = await fs.stat(join(OUTPUT_DIR, file));
    totalSize += stat.size;
  }
}
console.log(`Total output: ${(totalSize / 1024 / 1024).toFixed(2)} MB`);
console.log('='.repeat(60));
