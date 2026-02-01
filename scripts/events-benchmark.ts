import { EventWriter, StateCollector, EventReplayer, InMemoryEventSource } from '../src/events'

// Benchmark EventWriter
const flushes: any[] = []
const writer = new EventWriter({
  maxBufferSize: 1000,
  flushIntervalMs: 100000,
})
writer.onFlush(batch => { flushes.push(batch); return Promise.resolve() })

console.log('--- EventWriter Benchmark ---')
const writeStart = performance.now()
const events: any[] = []
for (let i = 0; i < 10000; i++) {
  events.push({
    id: `evt_${i}`,
    ts: Date.now(),
    op: 'CREATE' as const,
    target: `users:user${i}`,
    after: { name: `User ${i}`, email: `user${i}@test.com` },
  })
}
await writer.writeMany(events)
await writer.flush()
const writeEnd = performance.now()
console.log(`Write 10,000 events: ${(writeEnd - writeStart).toFixed(1)}ms`)
console.log(`Events/sec: ${Math.round(10000 / ((writeEnd - writeStart) / 1000)).toLocaleString()}`)

// Benchmark StateCollector
console.log('\n--- StateCollector Benchmark ---')
const collector = new StateCollector()
const collectStart = performance.now()
for (const event of events) {
  collector.processEvent(event)
}
const collectEnd = performance.now()
console.log(`Collect 10,000 events: ${(collectEnd - collectStart).toFixed(1)}ms`)

// Benchmark EventReplayer
console.log('\n--- EventReplayer Benchmark ---')
const source = new InMemoryEventSource()

const replayEvents: any[] = []
for (let entity = 0; entity < 1000; entity++) {
  for (let update = 0; update < 10; update++) {
    replayEvents.push({
      id: `evt_${entity}_${update}`,
      ts: 1000 + update * 100,
      op: update === 0 ? 'CREATE' : 'UPDATE',
      target: `users:user${entity}`,
      after: { name: `User ${entity} v${update}` },
    })
  }
}
source.addEvents(replayEvents)

const replayer = new EventReplayer(source)
const singleStart = performance.now()
for (let i = 0; i < 100; i++) {
  await replayer.replayEntity(`users:user${i}`, { at: 1500 })
}
const singleEnd = performance.now()
console.log(`Replay 100 entities (10 events each): ${(singleEnd - singleStart).toFixed(1)}ms`)
console.log(`Per entity: ${((singleEnd - singleStart) / 100).toFixed(2)}ms`)

console.log('\n--- Impact Summary ---')
console.log('Normal reads: NO IMPACT (events not in query path yet)')
console.log(`Write overhead: ~${((writeEnd - writeStart) / 10000).toFixed(3)}ms per event`)
console.log(`Time-travel: ~${((singleEnd - singleStart) / 100).toFixed(2)}ms per entity replay`)
