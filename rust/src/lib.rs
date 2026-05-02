#![cfg_attr(not(feature = "std"), no_std)]

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU64, Ordering};

/// A lock-free, single-producer/single-consumer ring buffer with a stable
/// `repr(C)` memory layout suitable for cross-language shared memory IPC.
///
/// `N` is the total buffer size in bytes. The buffer is divided into slots
/// of `stride` bytes each, giving `N / stride` total slots with a usable
/// capacity of `N / stride - 1` (one slot is reserved to distinguish full
/// from empty).
///
/// # Memory layout (64-bit)
///
/// | Offset | Size  | Field      |
/// |--------|-------|------------|
/// | 0      | 1     | magic      |
/// | 8      | 8     | slot_count |
/// | 16     | 8     | stride     |
/// | 24     | 8     | head       |
/// | 32     | 8     | tail       |
/// | 40     | N     | buffer     |
///
/// # Usage
///
/// Call [`split`](KrabiBuffer::split) to obtain a [`Producer`] and
/// [`Consumer`] pair. The split enforces the SPSC invariant at the type
/// level: only one producer and one consumer can exist for a given buffer.
#[derive(Debug)]
#[repr(C, align(8))]
pub struct KrabiBuffer<const N: usize> {
    /// Marker byte encoding the architecture pointer width.
    /// Upper nibble is `0xC`, lower nibble encodes `log2(bits) - 3`.
    pub magic: u8,

    /// Total number of slots (`N / stride`).
    slot_count: u64,

    /// Byte size of each element in the queue.
    stride: u64,

    /// Index of the next element to dequeue (0 <= head < slot_count).
    head: AtomicU64,

    /// Index of the next element to enqueue (0 <= tail < slot_count).
    tail: AtomicU64,

    /// Raw byte storage for all slots.
    /// Wrapped in `UnsafeCell` to allow `&self` access from both
    /// producer (enqueue) and consumer (dequeue) in SPSC usage.
    buffer: UnsafeCell<[u8; N]>,
}

#[derive(Debug)]
pub enum Error {
    StrideIsZero,
    StrideNotEvenlyDivisible,
    SlotCountTooSmall,
    QueueFull,
    QueueEmpty,
    InvalidDataLength,
}

impl<const N: usize> KrabiBuffer<N> {
    /// Create a new buffer.
    ///
    /// - `stride` is the size in bytes of each item.
    /// - `N` must be a multiple of `stride`.
    /// - `N / stride` must be >= 2 (at least 2 slots).
    pub fn new(stride: usize) -> Result<Self, Error> {
        if stride == 0 {
            return Err(Error::StrideIsZero);
        }
        if N % stride != 0 {
            return Err(Error::StrideNotEvenlyDivisible);
        }
        let slot_count = (N / stride) as u64;
        if slot_count < 2 {
            return Err(Error::SlotCountTooSmall);
        }

        Ok(Self {
            // Lower nibble encodes pointer width: 0..3 for 8/16/32/64.
            magic: 0xC0 | (usize::BITS.trailing_zeros() - 3) as u8,

            slot_count,
            stride: stride as u64,

            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),

            buffer: UnsafeCell::new([0; N]),
        })
    }

    /// Split the buffer into a producer/consumer pair.
    ///
    /// This enforces the SPSC invariant at the type level: the `Producer`
    /// is the only handle that can enqueue, and the `Consumer` is the only
    /// handle that can dequeue.
    pub fn split(&mut self) -> (Producer<'_, N>, Consumer<'_, N>) {
        (Producer { rb: self }, Consumer { rb: self })
    }

    /// Usable capacity: `slot_count - 1`.
    /// One slot is always held open to distinguish full from empty.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.slot_count - 1
    }

    /// Current number of occupied slots.
    #[inline]
    pub fn len(&self) -> u64 {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        (tail.wrapping_sub(head).wrapping_add(self.slot_count)) % self.slot_count
    }

    /// Whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn increment(&self, slot: u64) -> u64 {
        (slot + 1) % self.slot_count
    }
}

/// Producer half of a [`KrabiBuffer`], obtained via [`KrabiBuffer::split`].
///
/// Only one `Producer` can exist per buffer, enforcing the
/// single-producer constraint at the type level.
pub struct Producer<'a, const N: usize> {
    rb: &'a KrabiBuffer<N>,
}

// SAFETY: Producer is the sole writer of tail and the buffer region at
// `current_tail`. The consumer reads disjoint slots. Synchronization is
// via atomic head/tail with acquire/release ordering.
unsafe impl<const N: usize> Send for Producer<'_, N> {}

/// Consumer half of a [`KrabiBuffer`], obtained via [`KrabiBuffer::split`].
///
/// Only one `Consumer` can exist per buffer, enforcing the
/// single-consumer constraint at the type level.
pub struct Consumer<'a, const N: usize> {
    rb: &'a KrabiBuffer<N>,
}

// SAFETY: Consumer is the sole writer of head and reads the buffer region
// at `current_head`. The producer writes disjoint slots. Synchronization
// is via atomic head/tail with acquire/release ordering.
unsafe impl<const N: usize> Send for Consumer<'_, N> {}

impl<const N: usize> Producer<'_, N> {
    /// Enqueue `data` (must be exactly `stride` bytes).
    pub fn enqueue(&mut self, data: &[u8]) -> Result<(), Error> {
        let rb = self.rb;
        if data.len() as u64 != rb.stride {
            return Err(Error::InvalidDataLength);
        }

        let current_tail = rb.tail.load(Ordering::Relaxed);
        let next_tail = rb.increment(current_tail);

        if next_tail != rb.head.load(Ordering::Acquire) {
            let offset = (current_tail * rb.stride) as usize;
            let stride = rb.stride as usize;
            // SAFETY: Producer exclusively owns the slot at `current_tail`.
            // No other code writes to this region. Raw pointers are used
            // to avoid creating references to the full buffer, which would
            // violate aliasing rules when producer and consumer run concurrently.
            unsafe {
                let dst = (rb.buffer.get() as *mut u8).add(offset);
                core::ptr::copy_nonoverlapping(data.as_ptr(), dst, stride);
            }
            rb.tail.store(next_tail, Ordering::Release);
            Ok(())
        } else {
            Err(Error::QueueFull)
        }
    }

    /// Usable capacity of the underlying buffer.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.rb.capacity()
    }

    /// Current number of occupied slots.
    #[inline]
    pub fn len(&self) -> u64 {
        self.rb.len()
    }

    /// Whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rb.is_empty()
    }
}

impl<const N: usize> Consumer<'_, N> {
    /// Dequeue into `out` (must be exactly `stride` bytes).
    pub fn dequeue<'a>(&mut self, out: &'a mut [u8]) -> Result<&'a mut [u8], Error> {
        let rb = self.rb;
        if out.len() as u64 != rb.stride {
            return Err(Error::InvalidDataLength);
        }

        let current_head = rb.head.load(Ordering::Relaxed);
        if current_head == rb.tail.load(Ordering::Acquire) {
            Err(Error::QueueEmpty)
        } else {
            let offset = (current_head * rb.stride) as usize;
            let stride = rb.stride as usize;
            // SAFETY: Consumer exclusively owns the slot at `current_head`.
            // No other code writes to this region. Raw pointers avoid
            // aliasing violations (see enqueue).
            unsafe {
                let src = (rb.buffer.get() as *const u8).add(offset);
                core::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), stride);
            }

            let next_head = rb.increment(current_head);
            rb.head.store(next_head, Ordering::Release);
            Ok(out)
        }
    }

    /// Usable capacity of the underlying buffer.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.rb.capacity()
    }

    /// Current number of occupied slots.
    #[inline]
    pub fn len(&self) -> u64 {
        self.rb.len()
    }

    /// Whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rb.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_size() {
        assert_eq!(core::mem::size_of::<KrabiBuffer<4>>(), 48);
    }

    #[test]
    fn test_creation() {
        let stride = 2;
        let buf = KrabiBuffer::<4>::new(stride).unwrap();

        assert_eq!(buf.magic & 0xF0, 0xC0);
        assert_eq!(buf.slot_count, (4 / stride) as u64);
        assert_eq!(buf.stride, stride as u64);
        assert_eq!(buf.head.load(Ordering::Relaxed), 0);
        assert_eq!(buf.tail.load(Ordering::Relaxed), 0);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), (4 / stride) as u64 - 1);
    }

    #[test]
    fn test_creation_errors() {
        let result = KrabiBuffer::<4>::new(0);
        assert!(matches!(result.unwrap_err(), Error::StrideIsZero));

        let result = KrabiBuffer::<5>::new(2);
        assert!(matches!(
            result.unwrap_err(),
            Error::StrideNotEvenlyDivisible
        ));

        let result = KrabiBuffer::<1>::new(1);
        assert!(matches!(result.unwrap_err(), Error::SlotCountTooSmall));
    }

    #[test]
    fn test_enqueue_dequeue() {
        let mut buf = KrabiBuffer::<6>::new(2).unwrap();
        let (mut prod, mut cons) = buf.split();
        let data1 = [1, 2];
        let data2 = [3, 4];
        let mut out = [0, 0];

        assert!(prod.enqueue(&data1).is_ok());
        assert_eq!(prod.len(), 1);

        assert!(prod.enqueue(&data2).is_ok());
        assert_eq!(prod.len(), 2);

        cons.dequeue(&mut out).unwrap();
        assert_eq!(out, data1);
        assert_eq!(cons.len(), 1);

        cons.dequeue(&mut out).unwrap();
        assert_eq!(out, data2);
        assert_eq!(cons.len(), 0);

        // wrap-around
        assert!(prod.enqueue(&data1).is_ok());
        assert!(prod.enqueue(&data2).is_ok());
        cons.dequeue(&mut out).unwrap();
        assert!(prod.enqueue(&data1).is_ok());
    }

    #[test]
    fn test_enqueue_full() {
        let mut buf = KrabiBuffer::<6>::new(2).unwrap();
        let (mut prod, _cons) = buf.split();
        let data = [1, 2];

        assert!(prod.enqueue(&data).is_ok());
        assert!(prod.enqueue(&data).is_ok());

        let result = prod.enqueue(&data);
        assert!(matches!(result.unwrap_err(), Error::QueueFull));
    }

    #[test]
    fn test_dequeue_empty() {
        let mut buf = KrabiBuffer::<4>::new(2).unwrap();
        let (_prod, mut cons) = buf.split();
        let mut out = [0, 0];

        let result = cons.dequeue(&mut out);
        assert!(matches!(result.unwrap_err(), Error::QueueEmpty));
    }

    #[test]
    fn test_invalid_data_length() {
        let mut buf = KrabiBuffer::<4>::new(2).unwrap();
        let (mut prod, mut cons) = buf.split();
        let data = [1, 2, 3];
        let mut out = [0];

        let result = prod.enqueue(&data);
        assert!(matches!(result.unwrap_err(), Error::InvalidDataLength));

        let result = cons.dequeue(&mut out);
        assert!(matches!(result.unwrap_err(), Error::InvalidDataLength));
    }
}
