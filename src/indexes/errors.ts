/**
 * Index Errors for ParqueDB
 *
 * Custom error types for index-related operations.
 */

/**
 * Error thrown when a unique constraint is violated
 */
export class UniqueConstraintError extends Error {
  override readonly name = 'UniqueConstraintError'

  constructor(
    /** Name of the index */
    public readonly indexName: string,
    /** Value that caused the violation */
    public readonly value: unknown,
    /** Namespace of the index */
    public readonly namespace?: string
  ) {
    const valueStr = typeof value === 'string' ? value : JSON.stringify(value)
    super(`Unique constraint violation on index "${indexName}": duplicate value "${valueStr}"`)
    Object.setPrototypeOf(this, UniqueConstraintError.prototype)
  }
}

/**
 * Error thrown when an index is not found
 */
export class IndexNotFoundError extends Error {
  override readonly name = 'IndexNotFoundError'

  constructor(
    /** Name of the index */
    public readonly indexName: string,
    /** Namespace of the index */
    public readonly namespace: string
  ) {
    super(`Index "${indexName}" not found in namespace "${namespace}"`)
    Object.setPrototypeOf(this, IndexNotFoundError.prototype)
  }
}

/**
 * Error thrown when an index build fails
 */
export class IndexBuildError extends Error {
  override readonly name = 'IndexBuildError'

  constructor(
    /** Name of the index */
    public readonly indexName: string,
    /** Original error that caused the build to fail */
    public override readonly cause: Error
  ) {
    super(`Failed to build index "${indexName}": ${cause.message}`)
    Object.setPrototypeOf(this, IndexBuildError.prototype)
  }
}

/**
 * Error thrown when an index fails to load from storage
 */
export class IndexLoadError extends Error {
  override readonly name = 'IndexLoadError'

  constructor(
    /** Name of the index */
    public readonly indexName: string,
    /** Path where index was expected */
    public readonly path: string,
    /** Original error that caused the load to fail */
    public override readonly cause: Error
  ) {
    super(`Failed to load index "${indexName}" from "${path}": ${cause.message}`)
    Object.setPrototypeOf(this, IndexLoadError.prototype)
  }
}

/**
 * Error thrown when an index catalog fails to parse
 */
export class IndexCatalogError extends Error {
  override readonly name = 'IndexCatalogError'

  constructor(
    /** Path where catalog was expected */
    public readonly path: string,
    /** Original error that caused the parse to fail */
    public override readonly cause: Error
  ) {
    super(`Failed to parse index catalog at "${path}": ${cause.message}`)
    Object.setPrototypeOf(this, IndexCatalogError.prototype)
  }
}
