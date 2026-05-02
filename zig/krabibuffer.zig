const std = @import("std");

/// Lock-free, single-producer/single-consumer ring buffer with a stable
/// C ABI memory layout for cross-language shared memory IPC.
///
/// Memory layout (64-bit, repr(C, align(8))):
///
///   Offset  Size  Field
///   0       1     magic
///   8       8     slot_count
///   16      8     stride
///   24      8     head  (atomic)
///   32      8     tail  (atomic)
///   40      N     buffer
pub const KrabiBuffer = extern struct {
    magic: u8,
    _pad1: u8 = 0,
    _pad2: u8 = 0,
    _pad3: u8 = 0,
    _pad4: u8 = 0,
    _pad5: u8 = 0,
    _pad6: u8 = 0,
    _pad7: u8 = 0,
    slot_count: u64,
    stride: u64,
    head: u64,
    tail: u64,

    const header_size = 40;

    /// Read the head index with acquire ordering.
    pub fn loadHead(self: *const KrabiBuffer) u64 {
        return @atomicLoad(u64, &self.head, .acquire);
    }

    /// Read the tail index with acquire ordering.
    pub fn loadTail(self: *const KrabiBuffer) u64 {
        return @atomicLoad(u64, &self.tail, .acquire);
    }

    /// Usable capacity (slot_count - 1).
    /// Returns 0 if slot_count < 2.
    pub fn capacity(self: *const KrabiBuffer) u64 {
        if (self.slot_count < 2) return 0;
        return self.slot_count - 1;
    }

    /// Current number of occupied slots.
    /// Returns 0 if slot_count < 2.
    pub fn len(self: *const KrabiBuffer) u64 {
        if (self.slot_count < 2) return 0;
        const head = self.loadHead();
        const tail = self.loadTail();
        return (tail -% head +% self.slot_count) % self.slot_count;
    }

    /// Get a pointer to the start of the data buffer (immediately after the header).
    fn bufferPtr(self: *const KrabiBuffer) [*]u8 {
        const base: [*]const u8 = @ptrCast(self);
        return @constCast(base + header_size);
    }

    /// Dequeue one slot into `out`. Returns the slice of bytes read, or null if empty.
    pub fn dequeue(self: *KrabiBuffer, out: []u8) ?[]u8 {
        if (self.slot_count < 2 or self.stride == 0) return null;
        const stride: usize = @intCast(self.stride);
        if (out.len < stride) return null;

        const head = @atomicLoad(u64, &self.head, .acquire);
        const tail = @atomicLoad(u64, &self.tail, .acquire);

        if (head == tail) return null;

        const head_usize: usize = @intCast(head);
        const buf = self.bufferPtr();
        const offset = head_usize * stride;

        @memcpy(out[0..stride], buf[offset .. offset + stride]);

        const next_head = (head + 1) % self.slot_count;
        @atomicStore(u64, &self.head, next_head, .release);

        return out[0..stride];
    }

    /// Enqueue `data` (must be exactly stride bytes). Returns true on success.
    pub fn enqueue(self: *KrabiBuffer, data: []const u8) bool {
        if (self.slot_count < 2 or self.stride == 0) return false;
        const stride: usize = @intCast(self.stride);
        if (data.len != stride) return false;

        const tail = @atomicLoad(u64, &self.tail, .acquire);
        const next_tail = (tail + 1) % self.slot_count;

        if (next_tail == @atomicLoad(u64, &self.head, .acquire)) {
            return false;
        }

        const tail_usize: usize = @intCast(tail);
        const buf = self.bufferPtr();
        const offset = tail_usize * stride;

        @memcpy(buf[offset .. offset + stride], data);

        @atomicStore(u64, &self.tail, next_tail, .release);
        return true;
    }
};
