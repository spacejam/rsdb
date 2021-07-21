use std::{
    cell::UnsafeCell,
    hint::spin_loop,
    panic::{catch_unwind, UnwindSafe},
    sync::atomic::{
        AtomicU64,
        Ordering::{Acquire, Release},
    },
};

const LOCKED: u64 = 1 << 63;

#[derive(Default, Debug)]
pub struct OptimisticAccessCell<T: Send + 'static> {
    version_lock: AtomicU64,
    item: UnsafeCell<T>,
}

impl<T: Send + 'static> OptimisticAccessCell<T> {
    pub fn mutate<R, F: UnwindSafe + FnOnce(&mut T) -> R>(&self, f: F) -> R {
        // acquire lock
        while self.version_lock.fetch_or(LOCKED, Acquire) & LOCKED == 0 {
            spin_loop()
        }

        // mutate
        let ret = catch_unwind(move || f(unsafe { self.item.get_mut() }));

        // unlock
        assert_eq!(
            self.version_lock.fetch_xor(LOCKED, Release) & LOCKED,
            LOCKED
        );

        // propagate panic if it happened in the provided function
        ret.expect("mutation function panicked")
    }

    pub fn access<R: Copy, F: Fn(&T) -> R>(&self, f: F) -> R {
        let mut vsn = self.version_lock.load(Acquire);

        loop {
            // spin if locked
            while vsn & LOCKED == LOCKED {
                spin_loop();
                vsn = self.version_lock.load(Acquire);
            }
            // access
            let ret = f(unsafe { self.item.get() });

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
