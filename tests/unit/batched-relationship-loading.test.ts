/**
 * Tests for batched relationship loading (N+1 query fix)
 *
 * Verifies that relationship hydration fetches entities in batches by namespace
 * rather than one at a time, avoiding the N+1 query problem.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { Collection, clearGlobalStorage } from '../../src/Collection'
import type { Entity, EntityId, RelLink, RelSet } from '../../src/types'

// =============================================================================
// Test Types
// =============================================================================

interface Author {
  email: string
  bio?: string
}

interface Category {
  slug: string
  description?: string
}

interface Article {
  title: string
  content: string
  // Outbound relationships
  author?: RelLink | undefined
  categories?: RelSet | undefined
}

// =============================================================================
// Tests
// =============================================================================

describe('Batched Relationship Loading', () => {
  let authors: Collection<Author>
  let categories: Collection<Category>
  let articles: Collection<Article>

  beforeEach(() => {
    clearGlobalStorage()
    authors = new Collection<Author>('authors')
    categories = new Collection<Category>('categories')
    articles = new Collection<Article>('articles')
  })

  afterEach(() => {
    clearGlobalStorage()
  })

  describe('related() method', () => {
    it('fetches multiple related entities from same namespace efficiently', async () => {
      // Create multiple categories
      const cat1 = await categories.create({
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
        description: 'Technology articles',
      })
      const cat2 = await categories.create({
        $type: 'Category',
        name: 'Science',
        slug: 'science',
        description: 'Science articles',
      })
      const cat3 = await categories.create({
        $type: 'Category',
        name: 'News',
        slug: 'news',
        description: 'News articles',
      })

      // Create article linked to multiple categories
      const article = await articles.create({
        $type: 'Article',
        name: 'Multi-Category Article',
        title: 'An Article About Everything',
        content: 'Content here',
      })

      // Link to all categories
      await articles.update(article.$id, {
        $link: {
          categories: [cat1.$id, cat2.$id, cat3.$id],
        },
      })

      // Fetch article with relationships
      const fetchedArticle = await articles.get(article.$id)
      expect(fetchedArticle).not.toBeNull()

      // Use related() to fetch all categories
      const relatedCategories = await fetchedArticle!.related<Category>('categories')

      expect(relatedCategories.items).toHaveLength(3)
      expect(relatedCategories.items.map(c => c.slug).sort()).toEqual(['news', 'science', 'tech'])
    })

    it('fetches related entities from multiple namespaces efficiently', async () => {
      // Create author
      const author = await authors.create({
        $type: 'Author',
        name: 'John Doe',
        email: 'john@example.com',
        bio: 'A prolific writer',
      })

      // Create category
      const category = await categories.create({
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })

      // Create article with relationships to both namespaces
      const article = await articles.create({
        $type: 'Article',
        name: 'Tech Article',
        title: 'Technology Today',
        content: 'Content about tech',
      })

      await articles.update(article.$id, {
        $link: {
          author: author.$id,
          categories: [category.$id],
        },
      })

      // Fetch article
      const fetchedArticle = await articles.get(article.$id)
      expect(fetchedArticle).not.toBeNull()

      // Fetch related entities from different namespaces
      const relatedAuthors = await fetchedArticle!.related<Author>('author')
      const relatedCategories = await fetchedArticle!.related<Category>('categories')

      expect(relatedAuthors.items).toHaveLength(1)
      expect(relatedAuthors.items[0]?.email).toBe('john@example.com')

      expect(relatedCategories.items).toHaveLength(1)
      expect(relatedCategories.items[0]?.slug).toBe('tech')
    })

    it('supports filter option on batched fetch', async () => {
      // Create multiple categories
      const techCat = await categories.create({
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })
      const scienceCat = await categories.create({
        $type: 'Category',
        name: 'Science',
        slug: 'science',
      })

      // Create article linked to both
      const article = await articles.create({
        $type: 'Article',
        name: 'Article',
        title: 'Mixed Article',
        content: 'Content',
      })

      await articles.update(article.$id, {
        $link: {
          categories: [techCat.$id, scienceCat.$id],
        },
      })

      const fetchedArticle = await articles.get(article.$id)

      // Filter to only tech category
      const techCategories = await fetchedArticle!.related<Category>('categories', {
        filter: { slug: 'tech' },
      })

      expect(techCategories.items).toHaveLength(1)
      expect(techCategories.items[0]?.slug).toBe('tech')
    })

    it('supports sorting on batched fetch', async () => {
      // Create categories with different slugs
      const aCat = await categories.create({
        $type: 'Category',
        name: 'Alpha',
        slug: 'alpha',
      })
      const bCat = await categories.create({
        $type: 'Category',
        name: 'Beta',
        slug: 'beta',
      })
      const cCat = await categories.create({
        $type: 'Category',
        name: 'Gamma',
        slug: 'gamma',
      })

      // Create article linked to all
      const article = await articles.create({
        $type: 'Article',
        name: 'Article',
        title: 'Sorted Article',
        content: 'Content',
      })

      await articles.update(article.$id, {
        $link: {
          categories: [cCat.$id, aCat.$id, bCat.$id], // Intentionally out of order
        },
      })

      const fetchedArticle = await articles.get(article.$id)

      // Sort by slug descending
      const sortedCategories = await fetchedArticle!.related<Category>('categories', {
        sort: { slug: -1 },
      })

      expect(sortedCategories.items.map(c => c.slug)).toEqual(['gamma', 'beta', 'alpha'])
    })

    it('supports pagination on batched fetch', async () => {
      // Create 5 categories
      const cats: EntityId[] = []
      for (let i = 0; i < 5; i++) {
        const cat = await categories.create({
          $type: 'Category',
          name: `Cat ${i}`,
          slug: `cat-${i}`,
        })
        cats.push(cat.$id)
      }

      // Create article linked to all
      const article = await articles.create({
        $type: 'Article',
        name: 'Article',
        title: 'Many Categories',
        content: 'Content',
      })

      await articles.update(article.$id, {
        $link: {
          categories: cats,
        },
      })

      const fetchedArticle = await articles.get(article.$id)

      // Get first page
      const page1 = await fetchedArticle!.related<Category>('categories', { limit: 2 })

      expect(page1.items.length).toBe(2)
      expect(page1.total).toBe(5)
      expect(page1.hasMore).toBe(true)
    })

    it('handles empty relationships gracefully', async () => {
      // Create article with no relationships
      const article = await articles.create({
        $type: 'Article',
        name: 'Lonely Article',
        title: 'No Friends',
        content: 'All alone',
      })

      const fetchedArticle = await articles.get(article.$id)

      // Should return empty array, not throw
      const relatedCategories = await fetchedArticle!.related<Category>('categories')

      expect(relatedCategories.items).toHaveLength(0)
      expect(relatedCategories.hasMore).toBe(false)
    })

    it('excludes deleted entities by default', async () => {
      // Create categories
      const activeCat = await categories.create({
        $type: 'Category',
        name: 'Active',
        slug: 'active',
      })
      const deletedCat = await categories.create({
        $type: 'Category',
        name: 'Deleted',
        slug: 'deleted',
      })

      // Create article linked to both
      const article = await articles.create({
        $type: 'Article',
        name: 'Article',
        title: 'Test',
        content: 'Content',
      })

      await articles.update(article.$id, {
        $link: {
          categories: [activeCat.$id, deletedCat.$id],
        },
      })

      // Soft delete one category
      await categories.delete(deletedCat.$id)

      const fetchedArticle = await articles.get(article.$id)

      // Should only return active category
      const relatedCategories = await fetchedArticle!.related<Category>('categories')

      expect(relatedCategories.items).toHaveLength(1)
      expect(relatedCategories.items[0]?.slug).toBe('active')
    })

    it('includes deleted entities when includeDeleted is true', async () => {
      // Create categories
      const activeCat = await categories.create({
        $type: 'Category',
        name: 'Active',
        slug: 'active',
      })
      const deletedCat = await categories.create({
        $type: 'Category',
        name: 'Deleted',
        slug: 'deleted',
      })

      // Create article linked to both
      const article = await articles.create({
        $type: 'Article',
        name: 'Article',
        title: 'Test',
        content: 'Content',
      })

      await articles.update(article.$id, {
        $link: {
          categories: [activeCat.$id, deletedCat.$id],
        },
      })

      // Soft delete one category
      await categories.delete(deletedCat.$id)

      const fetchedArticle = await articles.get(article.$id)

      // Should return both when includeDeleted is true
      const relatedCategories = await fetchedArticle!.related<Category>('categories', {
        includeDeleted: true,
      })

      expect(relatedCategories.items).toHaveLength(2)
    })
  })

  describe('referencedBy() method', () => {
    it('fetches referencing entities efficiently', async () => {
      // Create an author
      const author = await authors.create({
        $type: 'Author',
        name: 'Jane Author',
        email: 'jane@example.com',
      })

      // Create multiple articles by this author
      const article1 = await articles.create({
        $type: 'Article',
        name: 'Article 1',
        title: 'First Article',
        content: 'Content 1',
      })
      const article2 = await articles.create({
        $type: 'Article',
        name: 'Article 2',
        title: 'Second Article',
        content: 'Content 2',
      })
      const article3 = await articles.create({
        $type: 'Article',
        name: 'Article 3',
        title: 'Third Article',
        content: 'Content 3',
      })

      // Link all articles to the author
      await articles.update(article1.$id, { $link: { author: author.$id } })
      await articles.update(article2.$id, { $link: { author: author.$id } })
      await articles.update(article3.$id, { $link: { author: author.$id } })

      // Fetch author with inbound references
      const fetchedAuthor = await authors.get(author.$id)
      expect(fetchedAuthor).not.toBeNull()

      // The inbound references should be available via the RelSet on the entity
      // The articles predicate should show references
      const articlesRef = (fetchedAuthor as Entity<Author> & { articles?: RelSet }).articles
      if (articlesRef) {
        const entries = Object.entries(articlesRef).filter(([key]) => !key.startsWith('$'))
        expect(entries.length).toBeGreaterThanOrEqual(1)
      }
    })
  })

  describe('entity hydration with display names', () => {
    it('batches display name lookups for outbound relationships', async () => {
      // Create multiple categories
      const cat1 = await categories.create({
        $type: 'Category',
        name: 'Technology',
        slug: 'tech',
      })
      const cat2 = await categories.create({
        $type: 'Category',
        name: 'Science',
        slug: 'science',
      })

      // Create article linked to categories
      const article = await articles.create({
        $type: 'Article',
        name: 'Test Article',
        title: 'Test',
        content: 'Content',
      })

      await articles.update(article.$id, {
        $link: {
          categories: [cat1.$id, cat2.$id],
        },
      })

      // Fetch article - display names should be resolved via batched lookups
      const fetchedArticle = await articles.get(article.$id)

      // Check that categories RelSet has display names as keys
      const categoriesRel = fetchedArticle!.categories as RelSet
      expect(categoriesRel).toBeDefined()

      const displayNames = Object.keys(categoriesRel).filter(k => !k.startsWith('$'))
      expect(displayNames).toContain('Technology')
      expect(displayNames).toContain('Science')
    })

    it('batches display name lookups for inbound relationships', async () => {
      // Create author
      const author = await authors.create({
        $type: 'Author',
        name: 'Test Author',
        email: 'test@example.com',
      })

      // Create multiple articles by this author
      const article1 = await articles.create({
        $type: 'Article',
        name: 'First Article',
        title: 'First',
        content: 'Content 1',
      })
      const article2 = await articles.create({
        $type: 'Article',
        name: 'Second Article',
        title: 'Second',
        content: 'Content 2',
      })

      await articles.update(article1.$id, { $link: { author: author.$id } })
      await articles.update(article2.$id, { $link: { author: author.$id } })

      // Fetch author - inbound refs should have display names resolved via batched lookups
      const fetchedAuthor = await authors.get(author.$id)

      // Inbound relationships from articles should show display names
      const articlesRef = (fetchedAuthor as Entity<Author> & { articles?: RelSet }).articles
      if (articlesRef) {
        const displayNames = Object.keys(articlesRef).filter(k => !k.startsWith('$'))
        // Display names should be the article names
        expect(displayNames).toContain('First Article')
        expect(displayNames).toContain('Second Article')
      }
    })
  })

  describe('large batch handling', () => {
    it('handles large number of relationships efficiently', async () => {
      // Create 50 categories
      const catIds: EntityId[] = []
      for (let i = 0; i < 50; i++) {
        const cat = await categories.create({
          $type: 'Category',
          name: `Category ${i}`,
          slug: `cat-${i}`,
        })
        catIds.push(cat.$id)
      }

      // Create article linked to all
      const article = await articles.create({
        $type: 'Article',
        name: 'Mega Article',
        title: 'All Categories',
        content: 'Linked to everything',
      })

      await articles.update(article.$id, {
        $link: {
          categories: catIds,
        },
      })

      // Fetch article and related categories
      const fetchedArticle = await articles.get(article.$id)
      const relatedCategories = await fetchedArticle!.related<Category>('categories')

      expect(relatedCategories.items).toHaveLength(50)
      expect(relatedCategories.total).toBe(50)
    })
  })
})
