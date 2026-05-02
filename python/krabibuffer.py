"""
KrabiBuffer - Python reader for cross-language lock-free ring buffer.

Reads from a KrabiBuffer instance laid out in shared memory with the
following repr(C, align(8)) layout on 64-bit systems:

    Offset  Size  Field
    0       1     magic
    8       8     slot_count
    16      8     stride
    24      8     head  (atomic)
    32      8     tail  (atomic)
    40      N     buffer
"""

import struct
from multiprocessing import shared_memory
from typing import Optional


class KrabiBuffer:
    """
    Attach to a KrabiBuffer ring buffer in shared memory and
    perform enqueue/dequeue operations.

    WARNING: Python cannot perform true atomic loads/stores.
    This implementation uses struct.pack/unpack on the raw memory,
    which is adequate for single-consumer access patterns but not
    safe under concurrent multi-consumer dequeue.
    """

    # Field offsets for 64-bit systems
    _OFFSET_MAGIC = 0
    _OFFSET_SLOT_COUNT = 8
    _OFFSET_STRIDE = 16
    _OFFSET_HEAD = 24
    _OFFSET_TAIL = 32
    _OFFSET_BUFFER = 40

    def __init__(self, link_file_path: str):
        """
        :param link_file_path: Path to the file containing the OS-level
                               shared memory object name.
        """
        with open(link_file_path, "r") as f:
            actual_name = f.read().strip()

        self._shm = shared_memory.SharedMemory(name=actual_name, create=False)
        self._buf = self._shm.buf

        slot_count = struct.unpack_from("=Q", self._buf, self._OFFSET_SLOT_COUNT)[0]
        stride = struct.unpack_from("=Q", self._buf, self._OFFSET_STRIDE)[0]
        if slot_count < 2:
            self._shm.close()
            raise ValueError(f"invalid slot_count={slot_count}, must be >= 2")
        if stride == 0:
            self._shm.close()
            raise ValueError("invalid stride=0, must be > 0")

    def close(self):
        """Detach from the shared memory region."""
        self._shm.close()

    @property
    def magic(self) -> int:
        return struct.unpack_from("=B", self._buf, self._OFFSET_MAGIC)[0]

    @property
    def slot_count(self) -> int:
        return struct.unpack_from("=Q", self._buf, self._OFFSET_SLOT_COUNT)[0]

    @property
    def stride(self) -> int:
        return struct.unpack_from("=Q", self._buf, self._OFFSET_STRIDE)[0]

    @property
    def head(self) -> int:
        return struct.unpack_from("=Q", self._buf, self._OFFSET_HEAD)[0]

    @property
    def tail(self) -> int:
        return struct.unpack_from("=Q", self._buf, self._OFFSET_TAIL)[0]

    @property
    def capacity(self) -> int:
        """Usable capacity (slot_count - 1)."""
        return self.slot_count - 1

    def __len__(self) -> int:
        return (self.tail - self.head + self.slot_count) % self.slot_count

    def dequeue(self) -> Optional[bytes]:
        """
        Dequeue one slot (stride bytes).
        Returns the bytes if successful, or None if empty.
        """
        slot_count = self.slot_count
        stride = self.stride
        head = self.head
        tail = self.tail

        if head == tail:
            return None

        offset = self._OFFSET_BUFFER + head * stride
        data = bytes(self._buf[offset : offset + stride])

        next_head = (head + 1) % slot_count
        struct.pack_into("=Q", self._buf, self._OFFSET_HEAD, next_head)

        return data

    def enqueue(self, data: bytes) -> bool:
        """
        Enqueue data (must be exactly stride bytes).
        Returns True on success, False if full.
        """
        slot_count = self.slot_count
        stride = self.stride

        if len(data) != stride:
            raise ValueError(f"data length {len(data)} != stride {stride}")

        tail = self.tail
        next_tail = (tail + 1) % slot_count

        if next_tail == self.head:
            return False

        offset = self._OFFSET_BUFFER + tail * stride
        self._buf[offset : offset + stride] = data

        struct.pack_into("=Q", self._buf, self._OFFSET_TAIL, next_tail)
        return True


if __name__ == "__main__":
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "my_queue_object"
    buf = KrabiBuffer(path)

    print(f"magic=0x{buf.magic:02x}")
    print(f"slot_count={buf.slot_count}")
    print(f"stride={buf.stride}")
    print(f"head={buf.head}")
    print(f"tail={buf.tail}")
    print(f"len={len(buf)}")

    item = buf.dequeue()
    if item is None:
        print("Queue is empty")
    else:
        print(f"Dequeued: {item}")

    buf.close()
