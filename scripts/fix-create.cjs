const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'src', 'ParqueDB.ts');
let content = fs.readFileSync(filePath, 'utf8');

// Find and replace the create method's return statement to add reverse relationship setup
const oldCreate = `    // Store in memory
    this.entities.set(fullId, entity as Entity)

    return entity
  }

  /**
   * Update an entity
   */
  async update<T = Record<string, unknown>>(`;

const newCreate = `    // Store in memory
    this.entities.set(fullId, entity as Entity)

    // Set up reverse relationships for inline relationship fields
    const typeName = dataWithDefaults.$type
    const typeDef = this.schema[typeName]
    if (typeDef) {
      for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
        if (fieldName.startsWith('$')) continue
        if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
          const relation = parseRelation(fieldDef)
          if (relation && relation.direction === 'forward' && relation.reverse) {
            const fieldValue = (entity as any)[fieldName]
            if (fieldValue && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
              // This is a relationship field with values
              for (const targetId of Object.values(fieldValue as Record<string, EntityId>)) {
                const targetEntity = this.entities.get(targetId as string)
                if (targetEntity) {
                  // Initialize reverse relationship if needed
                  if (!(targetEntity as any)[relation.reverse] || typeof (targetEntity as any)[relation.reverse] !== 'object') {
                    ;(targetEntity as any)[relation.reverse] = {}
                  }
                  // Add reverse link
                  const sourceDisplayName = entity.name || entity.$id
                  ;(targetEntity as any)[relation.reverse][sourceDisplayName] = entity.$id
                }
              }
            }
          }
        }
      }
    }

    return entity
  }

  /**
   * Update an entity
   */
  async update<T = Record<string, unknown>>(`;

if (content.includes(oldCreate)) {
  content = content.replace(oldCreate, newCreate);
  fs.writeFileSync(filePath, content);
  console.log('Successfully added reverse relationship setup to create method');
} else {
  console.error('Could not find create method pattern to update');
  process.exit(1);
}
