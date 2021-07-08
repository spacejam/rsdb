mod btree;
mod buffer_pool;
mod ebr;
mod io_uring;
mod log;
mod sql;

use btree::BTree;

pub struct Rsdb {
    system_catalogue: BTree,
    tables: HashMap<String, BTree>,
}
