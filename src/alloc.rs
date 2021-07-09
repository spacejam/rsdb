#[cfg(feature = "shred_allocations_on_free")]
mod alloc {
    use std::alloc::{GlobalAlloc, Layout, System};

    #[global_allocator]
    static ALLOCATOR: ShredAlloc = ShredAlloc;

    #[derive(Default, Debug, Clone, Copy)]
    struct ShredAlloc;

    unsafe impl GlobalAlloc for ShredAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ret = System.alloc(layout);
            assert_ne!(ret, std::ptr::null_mut());
            std::ptr::write_bytes(ret, 0xa1, layout.size());
            ret
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            std::ptr::write_bytes(ptr, 0xde, layout.size());
            System.dealloc(ptr, layout)
        }
    }
}

#[cfg(feature = "no_allocations_allowed")]
mod alloc {
    use std::alloc::{GlobalAlloc, Layout, System};

    #[global_allocator]
    static ALLOCATOR: NoAlloc = NoAlloc;

    #[derive(Default, Debug, Clone, Copy)]
    struct NoAlloc;

    unsafe impl GlobalAlloc for NoAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            panic!("alloc called while no_allocations_allowed feature was on")
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            panic!("dealloc called while no_allocations_allowed feature was on")
        }
    }
}
