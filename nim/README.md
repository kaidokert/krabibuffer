# Nim KrabiBuffer

`krabibuffer.nim` is a non-owning Nim view over the shared KrabiBuffer ABI used
by the C++ and Rust implementations. Attach it to any pointer whose memory is
laid out as:

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 1 | `magic` |
| 8 | 8 | `slotCount` |
| 16 | 8 | `stride` |
| 24 | 8 | `head` |
| 32 | 8 | `tail` |
| 40 | N | ring slot bytes |

```nim
import krabibuffer

var storage = newSeq[uint8](HeaderSize.int + 4 * 8)
let kb = initHeader(addr storage[0], slotCount = 4, stride = 8)
doAssert kb.enqueue([1'u8, 2, 3, 4, 5, 6, 7, 8])
```

The implementation uses acquire/release atomic accesses for `head` and `tail`
and keeps one slot empty, matching the C++ and Rust SPSC protocol.
