/**
 * HNSW Heap Data Structures
 *
 * Priority queue implementations for HNSW search operations.
 */

import type { SearchCandidate } from './hnsw-types'

/**
 * Min-Heap for maintaining candidates during search.
 * Pops elements with smallest distance first.
 */
export class MinHeap {
  private heap: SearchCandidate[] = []

  push(candidate: SearchCandidate): void {
    this.heap.push(candidate)
    this.bubbleUp(this.heap.length - 1)
  }

  pop(): SearchCandidate | undefined {
    if (this.heap.length === 0) return undefined
    const result = this.heap[0]
    const last = this.heap.pop()
    if (this.heap.length > 0 && last) {
      this.heap[0] = last
      this.bubbleDown(0)
    }
    return result
  }

  peek(): SearchCandidate | undefined {
    return this.heap[0]
  }

  get size(): number {
    return this.heap.length
  }

  toArray(): SearchCandidate[] {
    return [...this.heap].sort((a, b) => a.distance - b.distance)
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2)
      if (this.heap[parentIndex]!.distance <= this.heap[index]!.distance) break
      this.swap(parentIndex, index)
      index = parentIndex
    }
  }

  private bubbleDown(index: number): void {
    const length = this.heap.length
    while (true) {
      const leftChild = 2 * index + 1
      const rightChild = 2 * index + 2
      let smallest = index

      if (
        leftChild < length &&
        this.heap[leftChild]!.distance < this.heap[smallest]!.distance
      ) {
        smallest = leftChild
      }

      if (
        rightChild < length &&
        this.heap[rightChild]!.distance < this.heap[smallest]!.distance
      ) {
        smallest = rightChild
      }

      if (smallest === index) break
      this.swap(index, smallest)
      index = smallest
    }
  }

  private swap(i: number, j: number): void {
    const temp = this.heap[i]!
    this.heap[i] = this.heap[j]!
    this.heap[j] = temp
  }
}

/**
 * Max-Heap for maintaining top-k results.
 * Pops elements with largest distance first.
 */
export class MaxHeap {
  private heap: SearchCandidate[] = []

  push(candidate: SearchCandidate): void {
    this.heap.push(candidate)
    this.bubbleUp(this.heap.length - 1)
  }

  pop(): SearchCandidate | undefined {
    if (this.heap.length === 0) return undefined
    const result = this.heap[0]
    const last = this.heap.pop()
    if (this.heap.length > 0 && last) {
      this.heap[0] = last
      this.bubbleDown(0)
    }
    return result
  }

  peek(): SearchCandidate | undefined {
    return this.heap[0]
  }

  get size(): number {
    return this.heap.length
  }

  toArray(): SearchCandidate[] {
    return [...this.heap].sort((a, b) => a.distance - b.distance)
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2)
      if (this.heap[parentIndex]!.distance >= this.heap[index]!.distance) break
      this.swap(parentIndex, index)
      index = parentIndex
    }
  }

  private bubbleDown(index: number): void {
    const length = this.heap.length
    while (true) {
      const leftChild = 2 * index + 1
      const rightChild = 2 * index + 2
      let largest = index

      if (
        leftChild < length &&
        this.heap[leftChild]!.distance > this.heap[largest]!.distance
      ) {
        largest = leftChild
      }

      if (
        rightChild < length &&
        this.heap[rightChild]!.distance > this.heap[largest]!.distance
      ) {
        largest = rightChild
      }

      if (largest === index) break
      this.swap(index, largest)
      index = largest
    }
  }

  private swap(i: number, j: number): void {
    const temp = this.heap[i]!
    this.heap[i] = this.heap[j]!
    this.heap[j] = temp
  }
}
