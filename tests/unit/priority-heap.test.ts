import { describe, it } from 'node:test';
import assert from 'node:assert';
import { PriorityHeap } from '../../src/core/priority-heap.js';

describe('PriorityHeap (Binary Min-Heap)', () => {
  it('should maintain min-heap ordering for numeric values', () => {
    const heap = new PriorityHeap<number>((a, b) => a - b);
    const numbers = [45, 12, 89, 3, 27, 64, 1, 99];

    for (const num of numbers) {
      heap.push(num);
    }

    assert.strictEqual(heap.size, 8);
    assert.strictEqual(heap.peek(), 1);

    const extracted: number[] = [];
    while (!heap.isEmpty()) {
      extracted.push(heap.pop()!);
    }

    assert.deepStrictEqual(extracted, [1, 3, 12, 27, 45, 64, 89, 99]);
  });

  it('should correctly prioritize tasks with priority and FIFO timestamp tie-breaking', () => {
    interface DummyTask {
      id: string;
      priority: number;
      enqueuedAt: number;
    }

    const heap = new PriorityHeap<DummyTask>((a, b) => {
      if (a.priority !== b.priority) return a.priority - b.priority;
      return a.enqueuedAt - b.enqueuedAt;
    });

    heap.push({ id: 'task-low', priority: 3, enqueuedAt: 100 });
    heap.push({ id: 'task-normal-1', priority: 2, enqueuedAt: 200 });
    heap.push({ id: 'task-critical', priority: 0, enqueuedAt: 300 });
    heap.push({ id: 'task-normal-2', priority: 2, enqueuedAt: 150 });
    heap.push({ id: 'task-high', priority: 1, enqueuedAt: 400 });

    const results: string[] = [];
    while (!heap.isEmpty()) {
      results.push(heap.pop()!.id);
    }

    // Expected order:
    // 1. Critical (P0)
    // 2. High (P1)
    // 3. Normal-2 (P2, enqueuedAt: 150 - FIFO)
    // 4. Normal-1 (P2, enqueuedAt: 200)
    // 5. Low (P3)
    assert.deepStrictEqual(results, [
      'task-critical',
      'task-high',
      'task-normal-2',
      'task-normal-1',
      'task-low',
    ]);
  });

  it('should support item removal by predicate', () => {
    const heap = new PriorityHeap<{ id: string; val: number }>((a, b) => a.val - b.val);
    heap.push({ id: 'a', val: 10 });
    heap.push({ id: 'b', val: 5 });
    heap.push({ id: 'c', val: 20 });

    const removed = heap.remove((item) => item.id === 'b');
    assert.strictEqual(removed?.id, 'b');
    assert.strictEqual(heap.size, 2);
    assert.strictEqual(heap.pop()?.id, 'a');
    assert.strictEqual(heap.pop()?.id, 'c');
  });

  it('should return undefined when popping an empty heap', () => {
    const heap = new PriorityHeap<number>((a, b) => a - b);
    assert.strictEqual(heap.pop(), undefined);
    assert.strictEqual(heap.peek(), undefined);
  });
});

