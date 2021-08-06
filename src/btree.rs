use std::{
    cell::UnsafeCell,
    hint::spin_loop,
    mem::size_of,
    ptr::NonNull,
    sync::atomic::{
        AtomicU64,
        Ordering::{Acquire, Release},
    },
};

pub struct BTree {
    root: Page,
}

impl BTree {
    pub fn get(&self, key: Key) -> Option<Value> {
        if let Some((k, v)) = self.root.access(|p| p.get_lte(key)) {
            if k == key {
                Some(v)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn insert(&self, key: Key, value: Value) -> Option<Value> {
        self.root.mutate(|p| p.insert(key, value))
    }
}

impl PageInner {
    fn get_lte(&self, key: Key) -> Option<(Key, Value)> {
        todo!()
    }

    fn insert(&mut self, key: Key, value: Value) -> Option<Value> {
        todo!()
    }
}

type Key = u64;

type Value = u64;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct PageId(u64);

struct SwizzledPointer<T>(*mut T);

unsafe impl<T: Send> Send for SwizzledPointer<T> {}
unsafe impl<T: Send> Sync for SwizzledPointer<T> {}

// 4k is the page size for most flash devices, as well
// as the amount that most DRAM will cache for additional
// cacheline fetches.
const PAGE_SIZE: usize = 4096;

const PAGE_SLOTS: usize = (PAGE_SIZE - size_of::<Header>()) / size_of::<Slot>();

const CHAINED_BUF_MAX_LEN: usize =
    PAGE_SIZE - size_of::<Option<NonNull<ChainedBuf>>>() - size_of::<u16>();

const LOCKED: u64 = 1 << 63;

struct DeferUnlock<'a> {
    version_lock: &'a AtomicU64,
}

impl<'a> Drop for DeferUnlock<'a> {
    fn drop(&mut self) {
        // unlock
        assert_eq!(
            self.version_lock.fetch_xor(LOCKED, Release) & LOCKED,
            LOCKED
        );
    }
}

#[test]
fn assert_correct_size() {
    use std::mem::transmute;

    let buf = [0_u8; PAGE_SIZE];
    let _: Page = unsafe { transmute(buf) };
    let _: ChainedBuf = unsafe { transmute(buf) };
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ChainedBuf {
    next: Option<NonNull<ChainedBuf>>,
    len: u16,
    buf: [u8; CHAINED_BUF_MAX_LEN],
}

#[derive(Clone, Copy, Debug)]
struct ChainedSlice<'a> {
    immediate: &'a [u8],
    next: Option<&'a ChainedBuf>,
}

#[repr(C)]
#[derive(Debug)]
pub struct Page {
    version_lock: AtomicU64,
    inner: UnsafeCell<PageInner>,
}

pub struct PageInner {
    header: Header,
    slots: [Slot; PAGE_SLOTS],
}

impl Page {
    fn mutate<R, F: FnOnce(&mut PageInner) -> R>(&self, f: F) -> R {
        // acquire lock
        while self.version_lock.fetch_or(LOCKED, Acquire) & LOCKED == 0 {
            spin_loop()
        }

        // Use this dropper struct to unlock because it
        // will be dropped even if a panic happens in the
        // passed-in function.
        let unlock_on_drop = DeferUnlock {
            version_lock: &self.version_lock,
        };

        // mutate
        let ret = f(unsafe { &mut *self.inner.get() });

        drop(unlock_on_drop);

        ret
    }

    fn access<R: Copy, F: Fn(&PageInner) -> R>(&self, f: F) -> R {
        let mut vsn = self.version_lock.load(Acquire);

        loop {
            // spin if locked
            while vsn & LOCKED == LOCKED {
                spin_loop();
                vsn = self.version_lock.load(Acquire);
            }

            // access
            let ret = f(unsafe { &*self.inner.get() });

            // vaidate, spin if changed
            let vsn2 = self.version_lock.load(Acquire);
            if vsn == vsn2 {
                return ret;
            } else {
                vsn = vsn2;
            }
        }
    }
}

#[repr(C)]
#[derive(Debug)]
struct Header {
    page_slot: PageId,
    next_cache_slot: PageId,
    prev_cache_slot: PageId,
    children: u16,
    is_index: bool,
    key_prefix: u64,
    _padding: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct Slot {
    key: Key,
    value: u64,
}
