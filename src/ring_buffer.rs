use std::mem::MaybeUninit;

pub struct RingBuffer<T, const SZ: usize> {
    buf: [MaybeUninit<T>; SZ],
    r: usize,
    w: usize,
}

impl<T, const SZ: usize> Drop for RingBuffer<T, SZ> {
    fn drop(&mut self) {
        while self.r < self.w {
            let idx = self.r % SZ;
            unsafe {
                let slot = self.buf.get_unchecked_mut(idx);
                slot.as_mut_ptr().drop_in_place()
            };
            self.r += 1;
        }
    }
}

impl<T, const SZ: usize> RingBuffer<T, SZ> {
    /// Returns a `RingBuffer` with a size of 2 ^ `size_power_of_two` argument.
    pub fn new() -> RingBuffer<T, SZ> {
        assert!(SZ.is_power_of_two());
        RingBuffer {
            buf: unsafe { MaybeUninit::zeroed().assume_init() },
            r: 0,
            w: 0,
        }
    }

    /// Try to push an item into the ring buffer, returning the
    /// pushed item argument if the ring buffer is already full.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        todo!()
    }

    /// Try to pop an item from the tail of the ring buffer.
    pub fn pop(&mut self) -> Option<T> {
        if self.w > self.r {
            let idx = self.r % SZ;
            let item: T = unsafe {
                let slot = self.buf.get_unchecked(idx);
                slot.as_ptr().read()
            };
            self.r += 1;
            Some(item)
        } else {
            None
        }
    }
}
