const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'src', 'ParqueDB.ts');
let content = fs.readFileSync(filePath, 'utf8');

// Old $link implementation
const oldLinkUnlink = `    // $link
    if (update.$link) {
      for (const [key, value] of Object.entries(update.$link)) {
        if (!Array.isArray((entity as any)[key])) {
          ;(entity as any)[key] = []
        }
        const values = Array.isArray(value) ? value : [value]
        for (const v of values) {
          if (!(entity as any)[key].includes(v)) {
            ;(entity as any)[key].push(v)
          }
        }
      }
    }

    // $unlink
    if (update.$unlink) {
      for (const [key, value] of Object.entries(update.$unlink)) {
        if (Array.isArray((entity as any)[key])) {
          const values = Array.isArray(value) ? value : [value]
          ;(entity as any)[key] = (entity as any)[key].filter((v: unknown) => !values.includes(v as EntityId))
        }
      }
    }

    // Update metadata`;

// New $link implementation
const newLinkUnlink = `    // $link
    if (update.$link) {
      for (const [key, value] of Object.entries(update.$link)) {
        // Validate relationship is defined in schema
        const typeName = entity.$type
        const typeDef = this.schema[typeName]
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (!fieldDef || (typeof fieldDef === 'string' && !isRelationString(fieldDef))) {
            throw new Error(\`Relationship '\${key}' is not defined in schema for type '\${typeName}'\`)
          }
        }

        const values = Array.isArray(value) ? value : [value]

        // Validate all targets exist and are not deleted
        for (const targetId of values) {
          const targetEntity = this.entities.get(targetId as string)
          if (!targetEntity) {
            throw new Error(\`Target entity '\${targetId}' does not exist\`)
          }
          if (targetEntity.deletedAt) {
            throw new Error(\`Cannot link to deleted entity '\${targetId}'\`)
          }
        }

        // Initialize as Record if not already (or if it's an array from legacy format)
        if (!(entity as any)[key] || typeof (entity as any)[key] !== 'object' || Array.isArray((entity as any)[key])) {
          ;(entity as any)[key] = {}
        }

        // Check if this is a singular relationship (no [] in definition)
        let isSingular = false
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            isSingular = !fieldDef.endsWith('[]')
          }
        }

        // For singular relationships, replace existing links
        if (isSingular) {
          ;(entity as any)[key] = {}
        }

        for (const targetId of values) {
          // Get the target entity's name for the key
          const targetEntity = this.entities.get(targetId as string)
          const displayName = targetEntity?.name || targetId

          // Check for duplicates by value
          const existingValues = Object.values((entity as any)[key] as Record<string, EntityId>)
          if (!existingValues.includes(targetId as EntityId)) {
            ;(entity as any)[key][displayName] = targetId
          }

          // Update reverse relationship
          if (typeDef && targetEntity) {
            const fieldDef = typeDef[key]
            if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
              const relation = parseRelation(fieldDef)
              if (relation && relation.direction === 'forward' && relation.reverse) {
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

    // $unlink
    if (update.$unlink) {
      for (const [key, value] of Object.entries(update.$unlink)) {
        if ((entity as any)[key] && typeof (entity as any)[key] === 'object' && !Array.isArray((entity as any)[key])) {
          const record = (entity as any)[key] as Record<string, EntityId>

          // Handle '$all' special value to remove all links
          if (value === '$all') {
            // Get schema info for reverse relationship handling
            const typeName = entity.$type
            const typeDef = this.schema[typeName]
            if (typeDef) {
              const fieldDef = typeDef[key]
              if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
                const relation = parseRelation(fieldDef)
                if (relation && relation.direction === 'forward' && relation.reverse) {
                  // Remove reverse relationships for all linked entities
                  for (const targetId of Object.values(record)) {
                    const targetEntity = this.entities.get(targetId as string)
                    if (targetEntity && (targetEntity as any)[relation.reverse]) {
                      const reverseRecord = (targetEntity as any)[relation.reverse] as Record<string, EntityId>
                      for (const [rKey, rValue] of Object.entries(reverseRecord)) {
                        if (rValue === entity.$id) {
                          delete reverseRecord[rKey]
                        }
                      }
                    }
                  }
                }
              }
            }
            ;(entity as any)[key] = {}
          } else {
            const values = Array.isArray(value) ? value : [value]

            // Remove entries by value
            for (const targetId of values) {
              for (const [rKey, rValue] of Object.entries(record)) {
                if (rValue === targetId) {
                  delete record[rKey]

                  // Update reverse relationship
                  const typeName = entity.$type
                  const typeDef = this.schema[typeName]
                  if (typeDef) {
                    const fieldDef = typeDef[key]
                    if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
                      const relation = parseRelation(fieldDef)
                      if (relation && relation.direction === 'forward' && relation.reverse) {
                        const targetEntity = this.entities.get(targetId as string)
                        if (targetEntity && (targetEntity as any)[relation.reverse]) {
                          const reverseRecord = (targetEntity as any)[relation.reverse] as Record<string, EntityId>
                          for (const [rrKey, rrValue] of Object.entries(reverseRecord)) {
                            if (rrValue === entity.$id) {
                              delete reverseRecord[rrKey]
                            }
                          }
                        }
                      }
                    }
                  }
                  break
                }
              }
            }
          }
        }
      }
    }

    // Update metadata`;

if (content.includes(oldLinkUnlink)) {
  content = content.replace(oldLinkUnlink, newLinkUnlink);
  fs.writeFileSync(filePath, content);
  console.log('Successfully replaced $link/$unlink implementation');
} else {
  console.error('Could not find old $link/$unlink implementation');
  process.exit(1);
}
