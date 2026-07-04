use core::sync::atomic::{AtomicU64, Ordering};

#[repr(C, align(8))]
struct RawKrabiBuffer<const N: usize> {
    magic: u8,
    padding: [u8; 7],
    slot_count: u64,
    stride: u64,
    head: AtomicU64,
    tail: AtomicU64,
    buffer: [u8; N],
}

impl<const N: usize> RawKrabiBuffer<N> {
    fn new(slot_count: u64, stride: u64) -> Self {
        Self {
            magic: 0xC3,
            padding: [0; 7],
            slot_count,
            stride,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            buffer: [0; N],
        }
    }

    fn enqueue(&mut self, data: &[u8]) -> bool {
        assert_eq!(data.len(), self.stride as usize);
        let tail = self.tail.load(Ordering::Relaxed);
        let next_tail = (tail + 1) % self.slot_count;
        if next_tail == self.head.load(Ordering::Acquire) {
            return false;
        }
        let offset = (tail * self.stride) as usize;
        self.buffer[offset..offset + data.len()].copy_from_slice(data);
        self.tail.store(next_tail, Ordering::Release);
        true
    }

    fn dequeue(&mut self, out: &mut [u8]) -> bool {
        assert!(out.len() >= self.stride as usize);
        let head = self.head.load(Ordering::Relaxed);
        if head == self.tail.load(Ordering::Acquire) {
            return false;
        }
        let stride = self.stride as usize;
        let offset = head as usize * stride;
        out[..stride].copy_from_slice(&self.buffer[offset..offset + stride]);
        self.head.store((head + 1) % self.slot_count, Ordering::Release);
        true
    }
}

#[test]
fn nim_layout_matches_rust_abi_and_round_trips() {
    assert_eq!(core::mem::offset_of!(RawKrabiBuffer<16>, magic), 0);
    assert_eq!(core::mem::offset_of!(RawKrabiBuffer<16>, slot_count), 8);
    assert_eq!(core::mem::offset_of!(RawKrabiBuffer<16>, stride), 16);
    assert_eq!(core::mem::offset_of!(RawKrabiBuffer<16>, head), 24);
    assert_eq!(core::mem::offset_of!(RawKrabiBuffer<16>, tail), 32);
    assert_eq!(core::mem::offset_of!(RawKrabiBuffer<16>, buffer), 40);

    let mut kb = RawKrabiBuffer::<16>::new(4, 4);
    let from_nim = *b"nim!";
    assert!(kb.enqueue(&from_nim));

    let mut read_by_rust = [0; 4];
    assert!(kb.dequeue(&mut read_by_rust));
    assert_eq!(read_by_rust, from_nim);

    let written_by_rust = *b"rust";
    assert!(kb.enqueue(&written_by_rust));
    assert_eq!(kb.head.load(Ordering::Relaxed), 1);
    assert_eq!(kb.tail.load(Ordering::Relaxed), 2);
    assert_eq!(&kb.buffer[4..8], &written_by_rust);
}
