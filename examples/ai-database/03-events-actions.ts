/**
 * Events and Actions Example with ParqueDB + ai-database
 *
 * This example demonstrates the Events and Actions APIs provided
 * by the ParqueDB adapter for ai-database:
 * - Emitting and subscribing to events
 * - Creating and tracking long-running actions
 * - Progress reporting for async operations
 * - Event replay for audit trails
 *
 * Run with: npx tsx examples/ai-database/03-events-actions.ts
 */

import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import {
  createParqueDBProvider,
  type DBProviderExtended,
  type DBEvent,
  type DBAction,
} from '../../src/integrations/ai-database'

// =============================================================================
// Event-Driven Document Processing Simulation
// =============================================================================

/**
 * Simulates a document processing pipeline with events and actions
 */
class DocumentProcessor {
  private provider: DBProviderExtended
  private eventLog: DBEvent[] = []

  constructor(provider: DBProviderExtended) {
    this.provider = provider

    // Subscribe to all document events
    this.provider.on('document.*', (event) => {
      this.eventLog.push(event)
      console.log(`  [Event] ${event.event}: ${event.object || 'N/A'}`)
    })
  }

  /**
   * Upload a document and emit events
   */
  async uploadDocument(title: string, content: string): Promise<string> {
    // Create the document
    const doc = await this.provider.create('Document', undefined, {
      title,
      content,
      status: 'uploaded',
      size: content.length,
    })

    // Emit upload event
    await this.provider.emit({
      actor: 'system',
      event: 'document.uploaded',
      object: doc.$id as string,
      objectData: { title, size: content.length },
    })

    return doc.$id as string
  }

  /**
   * Process a document with progress tracking
   */
  async processDocument(documentId: string): Promise<DBAction> {
    // Create the processing action
    const action = await this.provider.createAction({
      actor: 'processor',
      action: 'process',
      object: documentId,
      total: 100,
      meta: { startTime: new Date().toISOString() },
    })

    console.log(`  Starting action: ${action.id}`)

    // Start the action
    await this.provider.updateAction(action.id, {
      status: 'active',
      progress: 0,
    })

    await this.provider.emit({
      actor: 'processor',
      event: 'document.processing.started',
      object: documentId,
      meta: { actionId: action.id },
    })

    // Simulate processing steps
    const steps = [
      { name: 'Parsing', progress: 25 },
      { name: 'Analyzing', progress: 50 },
      { name: 'Extracting', progress: 75 },
      { name: 'Finalizing', progress: 100 },
    ]

    for (const step of steps) {
      // Simulate work
      await sleep(100)

      console.log(`  Processing step: ${step.name} (${step.progress}%)`)

      await this.provider.updateAction(action.id, {
        progress: step.progress,
      })

      await this.provider.emit({
        actor: 'processor',
        event: 'document.processing.progress',
        object: documentId,
        objectData: { step: step.name, progress: step.progress },
      })
    }

    // Complete the action
    const completedAction = await this.provider.updateAction(action.id, {
      status: 'completed',
      result: {
        success: true,
        processedAt: new Date().toISOString(),
        stats: {
          wordsAnalyzed: 1500,
          entitiesExtracted: 42,
        },
      },
    })

    // Update document status
    await this.provider.update('Document', documentId, {
      status: 'processed',
      processedAt: new Date().toISOString(),
    })

    await this.provider.emit({
      actor: 'processor',
      event: 'document.processed',
      object: documentId,
      resultData: completedAction.result,
    })

    return completedAction
  }

  /**
   * Get event log
   */
  getEventLog(): DBEvent[] {
    return this.eventLog
  }
}

// =============================================================================
// Batch Processing with Actions
// =============================================================================

/**
 * Demonstrates batch processing with action tracking
 */
async function runBatchProcessing(
  provider: DBProviderExtended,
  items: string[]
): Promise<void> {
  console.log(`\nStarting batch processing of ${items.length} items...`)

  // Create batch action
  const batchAction = await provider.createAction({
    actor: 'batch-processor',
    action: 'batch',
    objectData: { itemCount: items.length },
    total: items.length,
  })

  await provider.updateAction(batchAction.id, { status: 'active' })

  let processed = 0
  let failed = 0

  for (const item of items) {
    try {
      // Simulate processing
      await sleep(50)

      // Randomly fail some items for demonstration
      if (Math.random() < 0.1) {
        throw new Error(`Failed to process: ${item}`)
      }

      processed++
      console.log(`  Processed: ${item}`)
    } catch (error) {
      failed++
      console.log(`  Failed: ${item}`)

      await provider.emit({
        actor: 'batch-processor',
        event: 'batch.item.failed',
        objectData: { item, error: (error as Error).message },
      })
    }

    // Update progress
    await provider.updateAction(batchAction.id, {
      progress: processed + failed,
    })
  }

  // Complete or fail based on results
  if (failed === 0) {
    await provider.updateAction(batchAction.id, {
      status: 'completed',
      result: { processed, failed },
    })
    console.log(`\nBatch completed: ${processed} processed, ${failed} failed`)
  } else if (failed === items.length) {
    await provider.updateAction(batchAction.id, {
      status: 'failed',
      error: 'All items failed',
      result: { processed, failed },
    })
    console.log(`\nBatch failed completely`)
  } else {
    // Partial success - still mark as completed but note failures
    await provider.updateAction(batchAction.id, {
      status: 'completed',
      result: { processed, failed, partial: true },
    })
    console.log(`\nBatch completed with errors: ${processed} processed, ${failed} failed`)
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// =============================================================================
// Main Example
// =============================================================================

async function main() {
  console.log('=== Events and Actions Example ===\n')

  // Initialize ParqueDB
  const parquedb = new ParqueDB({ storage: new MemoryBackend() })
  const provider = createParqueDBProvider(parquedb)

  // Create document processor
  const processor = new DocumentProcessor(provider)

  // ============================================================================
  // Part 1: Document Processing with Events
  // ============================================================================

  console.log('--- Part 1: Document Processing ---\n')

  // Upload documents
  console.log('Uploading documents...')
  const doc1Id = await processor.uploadDocument(
    'Introduction to AI',
    'Artificial intelligence (AI) is the simulation of human intelligence...'
  )
  const doc2Id = await processor.uploadDocument(
    'Machine Learning Basics',
    'Machine learning is a subset of AI that focuses on learning from data...'
  )

  console.log(`\nUploaded ${2} documents`)

  // Process first document
  console.log(`\nProcessing document: ${doc1Id}`)
  const processResult = await processor.processDocument(doc1Id)
  console.log(`\nProcessing complete. Result: ${JSON.stringify(processResult.result, null, 2)}`)

  // ============================================================================
  // Part 2: Batch Processing with Actions
  // ============================================================================

  console.log('\n--- Part 2: Batch Processing ---')

  const batchItems = [
    'item-001',
    'item-002',
    'item-003',
    'item-004',
    'item-005',
  ]

  await runBatchProcessing(provider, batchItems)

  // ============================================================================
  // Part 3: Event Replay and Audit Trail
  // ============================================================================

  console.log('\n--- Part 3: Event Replay ---\n')

  // List all events
  const allEvents = await provider.listEvents({ limit: 20 })
  console.log(`Total events recorded: ${allEvents.length}`)

  // List events by type
  const docEvents = await provider.listEvents({ event: 'document.processed' })
  console.log(`Document processed events: ${docEvents.length}`)

  // Replay events for audit
  console.log('\nReplaying document events...')
  const replayedEvents: DBEvent[] = []
  await provider.replayEvents({
    handler: (event) => {
      if (event.event.startsWith('document.')) {
        replayedEvents.push(event)
      }
    },
  })
  console.log(`Replayed ${replayedEvents.length} document events`)

  // Show event timeline
  console.log('\nEvent Timeline:')
  for (const event of replayedEvents.slice(0, 10)) {
    const time = event.timestamp.toISOString().split('T')[1]?.split('.')[0] || ''
    console.log(`  ${time} - ${event.event}`)
  }

  // ============================================================================
  // Part 4: Action Status Management
  // ============================================================================

  console.log('\n--- Part 4: Action Management ---\n')

  // Create some test actions
  const action1 = await provider.createAction({
    actor: 'user/123',
    action: 'generate',
    object: 'report/quarterly',
    total: 100,
  })

  const action2 = await provider.createAction({
    actor: 'user/456',
    action: 'sync',
    object: 'data/external',
    total: 50,
  })

  // Start and update actions
  await provider.updateAction(action1.id, { status: 'active', progress: 30 })
  await provider.updateAction(action2.id, { status: 'active', progress: 50 })

  // Fail one action
  await provider.updateAction(action2.id, {
    status: 'failed',
    error: 'External service unavailable',
  })

  // List active actions
  const activeActions = await provider.listActions({ status: 'active' })
  console.log(`Active actions: ${activeActions.length}`)

  // List failed actions
  const failedActions = await provider.listActions({ status: 'failed' })
  console.log(`Failed actions: ${failedActions.length}`)

  // Retry failed action
  if (failedActions.length > 0) {
    console.log(`\nRetrying failed action: ${failedActions[0].id}`)
    const retried = await provider.retryAction(failedActions[0].id)
    console.log(`Action status after retry: ${retried.status}`)
  }

  // Cancel pending action
  const pendingActions = await provider.listActions({ status: 'pending' })
  if (pendingActions.length > 0) {
    console.log(`\nCancelling pending action: ${pendingActions[0].id}`)
    await provider.cancelAction(pendingActions[0].id)
    const cancelled = await provider.getAction(pendingActions[0].id)
    console.log(`Action status after cancel: ${cancelled?.status}`)
  }

  // ============================================================================
  // Part 5: Summary Statistics
  // ============================================================================

  console.log('\n--- Summary ---\n')

  // All actions
  const allActions = await provider.listActions()
  console.log(`Total actions created: ${allActions.length}`)

  // Group by status
  const actionsByStatus = allActions.reduce((acc, action) => {
    acc[action.status] = (acc[action.status] || 0) + 1
    return acc
  }, {} as Record<string, number>)

  console.log('Actions by status:')
  for (const [status, count] of Object.entries(actionsByStatus)) {
    console.log(`  ${status}: ${count}`)
  }

  // Event statistics
  const events = await provider.listEvents()
  console.log(`\nTotal events emitted: ${events.length}`)

  // Group by event type
  const eventsByType = events.reduce((acc, event) => {
    const type = event.event.split('.')[0] || 'unknown'
    acc[type] = (acc[type] || 0) + 1
    return acc
  }, {} as Record<string, number>)

  console.log('Events by category:')
  for (const [type, count] of Object.entries(eventsByType)) {
    console.log(`  ${type}.*: ${count}`)
  }

  // Cleanup
  parquedb.dispose()

  console.log('\n=== Events and Actions Example Complete ===')
}

main().catch(console.error)
