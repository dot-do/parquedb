/**
 * Tests for type/namespace conversion utilities
 *
 * These utilities properly pluralize/singularize type names and namespaces
 * using the 'pluralize' library for correct English pluralization.
 */

import { describe, it, expect } from 'vitest'
import { typeToNamespace, namespaceToType } from '../../../src/utils/type-utils'

describe('typeToNamespace', () => {
  describe('regular pluralization', () => {
    it('should pluralize simple types', () => {
      expect(typeToNamespace('User')).toBe('users')
      expect(typeToNamespace('Post')).toBe('posts')
      expect(typeToNamespace('Comment')).toBe('comments')
    })

    it('should handle already lowercase input', () => {
      expect(typeToNamespace('user')).toBe('users')
      expect(typeToNamespace('post')).toBe('posts')
    })
  })

  describe('irregular pluralization', () => {
    it('should handle -y to -ies transformation', () => {
      expect(typeToNamespace('Category')).toBe('categories')
      expect(typeToNamespace('Company')).toBe('companies')
      expect(typeToNamespace('Country')).toBe('countries')
    })

    it('should handle -s to -ses transformation', () => {
      // 'Status' should become 'statuses', not 'status'
      expect(typeToNamespace('Status')).toBe('statuses')
      expect(typeToNamespace('Alias')).toBe('aliases')
      expect(typeToNamespace('Class')).toBe('classes')
    })

    it('should handle -x to -xes transformation', () => {
      expect(typeToNamespace('Index')).toBe('indices')
      expect(typeToNamespace('Box')).toBe('boxes')
    })

    it('should handle irregular plurals', () => {
      expect(typeToNamespace('Person')).toBe('people')
      expect(typeToNamespace('Child')).toBe('children')
      expect(typeToNamespace('Mouse')).toBe('mice')
    })
  })

  describe('uncountable nouns', () => {
    it('should not change uncountable nouns', () => {
      expect(typeToNamespace('News')).toBe('news')
      expect(typeToNamespace('Information')).toBe('information')
      expect(typeToNamespace('Equipment')).toBe('equipment')
    })
  })

  describe('edge cases', () => {
    it('should handle already plural input', () => {
      // 'Users' -> 'users' (already plural)
      expect(typeToNamespace('Users')).toBe('users')
    })

    it('should handle PascalCase input', () => {
      expect(typeToNamespace('BlogPost')).toBe('blogposts')
      expect(typeToNamespace('UserProfile')).toBe('userprofiles')
    })
  })
})

describe('namespaceToType', () => {
  describe('regular singularization', () => {
    it('should singularize and capitalize simple namespaces', () => {
      expect(namespaceToType('users')).toBe('User')
      expect(namespaceToType('posts')).toBe('Post')
      expect(namespaceToType('comments')).toBe('Comment')
    })
  })

  describe('irregular singularization', () => {
    it('should handle -ies to -y transformation', () => {
      expect(namespaceToType('categories')).toBe('Category')
      expect(namespaceToType('companies')).toBe('Company')
      expect(namespaceToType('countries')).toBe('Country')
    })

    it('should handle -ses to -s transformation', () => {
      expect(namespaceToType('statuses')).toBe('Status')
      expect(namespaceToType('aliases')).toBe('Alias')
      expect(namespaceToType('classes')).toBe('Class')
    })

    it('should handle irregular singulars', () => {
      expect(namespaceToType('people')).toBe('Person')
      expect(namespaceToType('children')).toBe('Child')
      expect(namespaceToType('mice')).toBe('Mouse')
    })
  })

  describe('uncountable nouns', () => {
    it('should not change uncountable nouns', () => {
      expect(namespaceToType('news')).toBe('News')
      expect(namespaceToType('information')).toBe('Information')
      expect(namespaceToType('equipment')).toBe('Equipment')
    })
  })
})

describe('roundtrip conversion', () => {
  it('should roundtrip common types', () => {
    const types = ['User', 'Post', 'Category', 'Status', 'Person', 'Company']

    for (const type of types) {
      const namespace = typeToNamespace(type)
      const backToType = namespaceToType(namespace)
      expect(backToType).toBe(type)
    }
  })
})
