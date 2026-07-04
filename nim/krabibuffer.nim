## KrabiBuffer Nim bindings for the cross-language lock-free ring buffer.
##
## The fixed header intentionally matches the C++ `KrabiBuffer` prefix and the
## Rust `#[repr(C, align(8))]` layout:
##
##   Offset  Size  Field
##   0       1     magic
##   8       8     slot_count
##   16      8     stride
##   24      8     head
##   32      8     tail
##   40      N     buffer
##
## `fromPointer` attaches to a shared-memory region whose header has already
## been initialized by any KrabiBuffer implementation.  The enqueue/dequeue
## operations preserve the single-producer/single-consumer ABI used by the other
## languages in this repository.

import std/[atomics, typetraits]

const
  HeaderSize* = 40'u
  MagicHighNibble* = 0xC0'u8

when sizeof(uint) == 8:
  const PointerWidthMagic* = MagicHighNibble or 3'u8
elif sizeof(uint) == 4:
  const PointerWidthMagic* = MagicHighNibble or 2'u8
elif sizeof(uint) == 2:
  const PointerWidthMagic* = MagicHighNibble or 1'u8
else:
  const PointerWidthMagic* = MagicHighNibble

type
  KrabiBufferHeader* {.bycopy.} = object
    ## Fixed-size C ABI header. The data slots begin immediately after this
    ## object at byte offset 40.
    magic*: uint8
    padding*: array[7, uint8]
    slotCount*: uint64
    stride*: uint64
    head*: uint64
    tail*: uint64

  KrabiBuffer* = object
    ## Non-owning view over a KrabiBuffer shared-memory region.
    header*: ptr KrabiBufferHeader

static:
  doAssert sizeof(KrabiBufferHeader) == int(HeaderSize)
  doAssert alignof(KrabiBufferHeader) == 8
  doAssert offsetof(KrabiBufferHeader, magic) == 0
  doAssert offsetof(KrabiBufferHeader, slotCount) == 8
  doAssert offsetof(KrabiBufferHeader, stride) == 16
  doAssert offsetof(KrabiBufferHeader, head) == 24
  doAssert offsetof(KrabiBufferHeader, tail) == 32

proc fromPointer*(memory: pointer): KrabiBuffer {.inline.} =
  ## Attach to an existing KrabiBuffer region.
  KrabiBuffer(header: cast[ptr KrabiBufferHeader](memory))

proc bufferPtr(kb: KrabiBuffer): ptr UncheckedArray[uint8] {.inline.} =
  cast[ptr UncheckedArray[uint8]](cast[uint](kb.header) + HeaderSize)

proc initHeader*(memory: pointer; slotCount, stride: uint64; magic = PointerWidthMagic): KrabiBuffer =
  ## Initialize a caller-owned memory region as a KrabiBuffer and return a view.
  ## The region must be at least `HeaderSize + slotCount * stride` bytes.
  result = fromPointer(memory)
  result.header.magic = magic
  result.header.padding = default(array[7, uint8])
  result.header.slotCount = slotCount
  result.header.stride = stride
  result.header.head = 0
  result.header.tail = 0

proc magic*(kb: KrabiBuffer): uint8 {.inline.} = kb.header.magic
proc slotCount*(kb: KrabiBuffer): uint64 {.inline.} = kb.header.slotCount
proc stride*(kb: KrabiBuffer): uint64 {.inline.} = kb.header.stride

proc head*(kb: KrabiBuffer): uint64 {.inline.} =
  let p = cast[ptr Atomic[uint64]](addr kb.header.head)
  atomicLoad(p[], moAcquire)

proc tail*(kb: KrabiBuffer): uint64 {.inline.} =
  let p = cast[ptr Atomic[uint64]](addr kb.header.tail)
  atomicLoad(p[], moAcquire)

proc capacity*(kb: KrabiBuffer): uint64 {.inline.} =
  if kb.header.slotCount < 2: 0'u64 else: kb.header.slotCount - 1

proc len*(kb: KrabiBuffer): uint64 {.inline.} =
  let sc = kb.header.slotCount
  if sc < 2: return 0
  (kb.tail - kb.head + sc) mod sc

proc isEmpty*(kb: KrabiBuffer): bool {.inline.} = kb.len == 0

proc enqueue*(kb: KrabiBuffer; data: openArray[uint8]): bool =
  ## Copy exactly `stride` bytes into the next slot. Returns false if full or
  ## if the input length is invalid.
  let sc = kb.header.slotCount
  let stride = kb.header.stride
  if sc < 2 or stride == 0 or uint64(data.len) != stride:
    return false

  let tailPtr = cast[ptr Atomic[uint64]](addr kb.header.tail)
  let headPtr = cast[ptr Atomic[uint64]](addr kb.header.head)
  let currentTail = atomicLoad(tailPtr[], moRelaxed)
  let nextTail = (currentTail + 1) mod sc
  if nextTail == atomicLoad(headPtr[], moAcquire):
    return false

  let offset = int(currentTail * stride)
  copyMem(addr kb.bufferPtr[][offset], unsafeAddr data[0], int(stride))
  atomicStore(tailPtr[], nextTail, moRelease)
  true

proc dequeue*(kb: KrabiBuffer; outBytes: var openArray[uint8]): bool =
  ## Copy one slot into `outBytes`. Returns false if empty or if the output
  ## slice is smaller than `stride`.
  let sc = kb.header.slotCount
  let stride = kb.header.stride
  if sc < 2 or stride == 0 or uint64(outBytes.len) < stride:
    return false

  let headPtr = cast[ptr Atomic[uint64]](addr kb.header.head)
  let tailPtr = cast[ptr Atomic[uint64]](addr kb.header.tail)
  let currentHead = atomicLoad(headPtr[], moRelaxed)
  if currentHead == atomicLoad(tailPtr[], moAcquire):
    return false

  let offset = int(currentHead * stride)
  copyMem(addr outBytes[0], addr kb.bufferPtr[][offset], int(stride))
  atomicStore(headPtr[], (currentHead + 1) mod sc, moRelease)
  true
