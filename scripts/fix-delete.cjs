const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'src', 'ParqueDB.ts');
let content = fs.readFileSync(filePath, 'utf8');

// Find the first delete method (in ParqueDBImpl)
const oldDelete = `    // Check version for optimistic concurrency (entity exists)
    if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
      throw new Error(\`Version mismatch: expected \${options.expectedVersion}, got \${entity.version}\`)
    }

    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    if (options?.hard) {
      // Hard delete - remove from storage
      this.entities.delete(fullId)
    } else {
      // Soft delete - set deletedAt
      entity.deletedAt = now
      entity.deletedBy = actor
      entity.updatedAt = now
      entity.updatedBy = actor
      entity.version = (entity.version || 1) + 1
      this.entities.set(fullId, entity)
    }

    return { deletedCount: 1 }
  }

  /**
   * Validate data against schema
   */`;

const newDelete = `    // Check version for optimistic concurrency (entity exists)
    if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
      throw new Error(\`Version mismatch: expected \${options.expectedVersion}, got \${entity.version}\`)
    }

    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    // Clean up reverse relationships before deleting
    const typeName = entity.$type
    const typeDef = this.schema[typeName]
    if (typeDef) {
      for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
        if (fieldName.startsWith('$')) continue
        if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
          const relation = parseRelation(fieldDef)
          if (relation && relation.direction === 'forward' && relation.reverse) {
            const fieldValue = (entity as any)[fieldName]
            if (fieldValue && typeof fieldValue === 'object' && !Array.isArray(fieldValue)) {
              // Remove reverse links from all related entities
              for (const targetId of Object.values(fieldValue as Record<string, EntityId>)) {
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
      }
    }

    if (options?.hard) {
      // Hard delete - remove from storage
      this.entities.delete(fullId)
    } else {
      // Soft delete - set deletedAt
      entity.deletedAt = now
      entity.deletedBy = actor
      entity.updatedAt = now
      entity.updatedBy = actor
      entity.version = (entity.version || 1) + 1
      this.entities.set(fullId, entity)
    }

    return { deletedCount: 1 }
  }

  /**
   * Validate data against schema
   */`;

if (content.includes(oldDelete)) {
  content = content.replace(oldDelete, newDelete);
  fs.writeFileSync(filePath, content);
  console.log('Successfully added reverse relationship cleanup to delete method');
} else {
  console.error('Could not find delete method pattern to update');
  process.exit(1);
}
