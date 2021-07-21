use std::{
    cell::UnsafeCell,
    hint::spin_loop,
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

struct DeferUnlock<'a, T: Send + 'static> {
    oac: &'a OptimisticAccessCell<T>,
}

impl<'a, T: Send + 'static> Drop for DeferUnlock<'a, T> {
    fn drop(&mut self) {
        // unlock
        assert_eq!(
            self.oac.version_lock.fetch_xor(LOCKED, Release) & LOCKED,
            LOCKED
        );
    }
}

impl<T: Send + 'static> OptimisticAccessCell<T> {
    pub fn mutate<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        // acquire lock
        while self.version_lock.fetch_or(LOCKED, Acquire) & LOCKED == 0 {
            spin_loop()
        }
        // Use this dropper struct to unlock because it
        // will be dropped even if a panic happens in the
        // passed-in function.
        let unlock_on_drop = DeferUnlock { oac: &self };

        // mutate
        let ret = f(unsafe { &mut *self.item.get() });

        drop(unlock_on_drop);

        ret
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
            let ret = f(unsafe { &*self.item.get() });

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
