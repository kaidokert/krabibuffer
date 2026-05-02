#pragma once

#include <atomic>
#include <cstddef>
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

static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t),
              "atomic<uint64_t> must be the same size as uint64_t for ABI stability");
static_assert(offsetof(KrabiBuffer, magic) == 0, "magic must be at offset 0");
static_assert(offsetof(KrabiBuffer, slot_count) == 8, "slot_count must be at offset 8");
static_assert(offsetof(KrabiBuffer, stride) == 16, "stride must be at offset 16");
static_assert(offsetof(KrabiBuffer, head) == 24, "head must be at offset 24");
static_assert(offsetof(KrabiBuffer, tail) == 32, "tail must be at offset 32");

/// Enqueue `stride` bytes from `data` into the ring buffer.
/// Returns true on success, false if the queue is full.
inline bool krabibuffer_enqueue(KrabiBuffer* kb, const void* data)
{
    if (!kb || !data || kb->slot_count == 0) return false;

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
    if (!kb || !out || kb->slot_count == 0) return false;

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
