// Package krabibuffer provides a Go reader for the cross-language
// KrabiBuffer lock-free ring buffer in shared memory.
//
// Memory layout (64-bit, repr(C, align(8))):
//
//	Offset  Size  Field
//	0       1     magic
//	8       8     slot_count
//	16      8     stride
//	24      8     head  (atomic)
//	32      8     tail  (atomic)
//	40      N     buffer
package krabibuffer

import (
	"sync/atomic"
	"unsafe"
)

// Header mirrors the fixed-size prefix of the KrabiBuffer layout.
// The buffer data follows immediately after at offset 40.
type Header struct {
	Magic     uint8
	_         [7]byte // padding to 8-byte alignment
	SlotCount uint64
	Stride    uint64
	Head      uint64 // accessed via atomic operations
	Tail      uint64 // accessed via atomic operations
}

const headerSize = 40

// KrabiBuffer wraps a pointer to a KrabiBuffer instance in shared memory.
type KrabiBuffer struct {
	header *Header
	buffer unsafe.Pointer // points to the start of the ring buffer data
}

// FromPointer creates a KrabiBuffer from a raw shared memory pointer.
func FromPointer(addr unsafe.Pointer) *KrabiBuffer {
	return &KrabiBuffer{
		header: (*Header)(addr),
		buffer: unsafe.Add(addr, headerSize),
	}
}

// Magic returns the architecture marker byte.
func (kb *KrabiBuffer) Magic() uint8 {
	return kb.header.Magic
}

// SlotCount returns the total number of slots.
func (kb *KrabiBuffer) SlotCount() uint64 {
	return kb.header.SlotCount
}

// Stride returns the byte size of each slot.
func (kb *KrabiBuffer) Stride() uint64 {
	return kb.header.Stride
}

// Head returns the current head index (atomic load).
func (kb *KrabiBuffer) Head() uint64 {
	return atomic.LoadUint64(&kb.header.Head)
}

// Tail returns the current tail index (atomic load).
func (kb *KrabiBuffer) Tail() uint64 {
	return atomic.LoadUint64(&kb.header.Tail)
}

// Capacity returns the usable capacity (slot_count - 1).
// Returns 0 if slot_count < 2.
func (kb *KrabiBuffer) Capacity() uint64 {
	sc := kb.header.SlotCount
	if sc < 2 {
		return 0
	}
	return sc - 1
}

// Len returns the current number of occupied slots.
// Returns 0 if slot_count < 2.
func (kb *KrabiBuffer) Len() uint64 {
	sc := kb.header.SlotCount
	if sc < 2 {
		return 0
	}
	head := kb.Head()
	tail := kb.Tail()
	return (tail - head + sc) % sc
}

// Dequeue copies one slot (stride bytes) into out.
// Returns the number of bytes copied, or 0 if empty.
func (kb *KrabiBuffer) Dequeue(out []byte) int {
	stride := kb.header.Stride
	sc := kb.header.SlotCount

	if sc < 2 || stride == 0 {
		return 0
	}

	head := atomic.LoadUint64(&kb.header.Head)
	tail := atomic.LoadUint64(&kb.header.Tail)

	if head == tail {
		return 0
	}

	if uint64(len(out)) < stride {
		return 0
	}

	offset := uintptr(head) * uintptr(stride)
	src := unsafe.Add(kb.buffer, offset)

	copy(out[:stride], unsafe.Slice((*byte)(src), stride))

	nextHead := (head + 1) % sc
	atomic.StoreUint64(&kb.header.Head, nextHead)

	return int(stride)
}

// Enqueue copies stride bytes from data into the ring buffer.
// Returns true on success, false if full.
func (kb *KrabiBuffer) Enqueue(data []byte) bool {
	stride := kb.header.Stride
	sc := kb.header.SlotCount

	if sc < 2 || stride == 0 {
		return false
	}

	if uint64(len(data)) != stride {
		return false
	}

	tail := atomic.LoadUint64(&kb.header.Tail)
	nextTail := (tail + 1) % sc

	if nextTail == atomic.LoadUint64(&kb.header.Head) {
		return false
	}

	offset := uintptr(tail) * uintptr(stride)
	dst := unsafe.Add(kb.buffer, offset)

	copy(unsafe.Slice((*byte)(dst), stride), data)

	atomic.StoreUint64(&kb.header.Tail, nextTail)
	return true
}
