use std::collections::HashMap;

mod alloc;
mod btree;
mod buffer_pool;
mod debug_delay;
pub mod ebr;
pub mod io_uring;
pub mod lazy;
mod log;
pub mod optimistic_access_cell;
//mod pagetable;
mod ring_buffer;
mod sql;
mod stack;

use btree::BTree;
pub use io_uring::IO_URING;
use lazy::Lazy;

#[cfg(test)]
use debug_delay::debug_delay;

#[cfg(not(test))]
const fn debug_delay() {}

#[repr(align(128))]
pub struct CachePadded<T>(T);

pub struct Rsdb {
    system_catalogue: BTree,
    tables: HashMap<String, BTree>,
}
