use std::collections::HashMap;

mod alloc;
mod btree;
mod buffer_pool;
mod ebr;
mod io_uring;
mod lazy;
mod log;
mod sql;

use btree::BTree;
pub use io_uring::IO_URING;
use lazy::Lazy;

pub struct Rsdb {
    system_catalogue: BTree,
    tables: HashMap<String, BTree>,
}
