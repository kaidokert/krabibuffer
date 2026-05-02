#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>

/// Lock-free, single-producer/single-consumer ring buffer with a stable
/// C ABI memory layout for cross-language shared memory IPC.
///
/// Memory layout (64-bit):
///
///   Offset  Size  Field
///   0       1     magic
///   8       8     slot_count
///   16      8     stride
///   24      8     head  (atomic)
///   32      8     tail  (atomic)
///   40      N     buffer
///
struct alignas(8) KrabiBuffer
{
    uint8_t magic;
    uint64_t slot_count;
    uint64_t stride;

    std::atomic<uint64_t> head;
    std::atomic<uint64_t> tail;

    // Flexible buffer -- in practice sized by the shared memory region.
    // For standalone use, embed a fixed-size array or allocate dynamically.
    uint8_t buffer[];
};

/// Enqueue `stride` bytes from `data` into the ring buffer.
/// Returns true on success, false if the queue is full.
inline bool krabibuffer_enqueue(KrabiBuffer* kb, const void* data)
{
    const uint64_t tail = kb->tail.load(std::memory_order_relaxed);
    const uint64_t head = kb->head.load(std::memory_order_acquire);
    const uint64_t next_tail = (tail + 1) % kb->slot_count;

    if (next_tail == head) {
        return false;
    }

    const uint64_t offset = tail * kb->stride;
    std::memcpy(kb->buffer + offset, data, kb->stride);

    kb->tail.store(next_tail, std::memory_order_release);
    return true;
}

/// Dequeue `stride` bytes into `out` from the ring buffer.
/// Returns true on success, false if the queue is empty.
inline bool krabibuffer_dequeue(KrabiBuffer* kb, void* out)
{
    const uint64_t head = kb->head.load(std::memory_order_relaxed);
    const uint64_t tail = kb->tail.load(std::memory_order_acquire);

    if (head == tail) {
        return false;
    }

    const uint64_t offset = head * kb->stride;
    std::memcpy(out, kb->buffer + offset, kb->stride);

    const uint64_t next_head = (head + 1) % kb->slot_count;
    kb->head.store(next_head, std::memory_order_release);
    return true;
}
