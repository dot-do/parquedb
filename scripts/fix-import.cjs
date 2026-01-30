const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'src', 'ParqueDB.ts');
let content = fs.readFileSync(filePath, 'utf8');

// Fix import to include parseRelation
const oldImport = "import { parseFieldType, isRelationString } from './types/schema'";
const newImport = "import { parseFieldType, isRelationString, parseRelation } from './types/schema'";

if (content.includes(oldImport)) {
  content = content.replace(oldImport, newImport);
  fs.writeFileSync(filePath, content);
  console.log('Successfully added parseRelation import');
} else if (content.includes(newImport)) {
  console.log('parseRelation import already present');
} else {
  console.error('Could not find import statement to update');
  process.exit(1);
}
