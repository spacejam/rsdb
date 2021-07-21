use std::{
    mem::MaybeUninit,
    sync::atomic::{
        fence, AtomicPtr,
        Ordering::{Acquire, Relaxed, Release},
    },
};

use crate::ebr::Ebr;

/// A simple lock-free Treiber stack
pub struct Stack<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,
    ebr: Ebr<Box<Node<T>>>,
}

impl<T: Send + 'static> Default for Stack<T> {
    fn default() -> Stack<T> {
        Stack {
            head: Default::default(),
            ebr: Default::default(),
        }
    }
}

impl<T: Send + 'static> Drop for Stack<T> {
    fn drop(&mut self) {
        let mut curr = self.head.load(Acquire);
        while !curr.is_null() {
            unsafe {
                let node: Box<Node<T>> = Box::from_raw(curr);
                curr = (*curr).next.load(Acquire);
                drop(node);
            }
        }
    }
}

#[derive(Debug)]
struct Node<T: Send + 'static> {
    item: MaybeUninit<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T: Send + Sync + 'static> Stack<T> {
    /// Add an item to the stack
    pub fn push(&self, item: T) {
        let node_ptr = Box::into_raw(Box::new(Node {
            item: MaybeUninit::new(item),
            next: AtomicPtr::default(),
        }));

        let mut cur_head_ptr = self.head.load(Acquire);

        loop {
            unsafe {
                (*node_ptr).next.store(cur_head_ptr, Relaxed);
            }
            fence(Acquire);

            let result = self
                .head
                .compare_exchange(cur_head_ptr, node_ptr, Relaxed, Relaxed);

            match result {
                Ok(_) => return,
                Err(current) => cur_head_ptr = current,
            }
        }
    }

    /// Pop an item off of the stack. Note that this
    /// requires a mutable self reference, because it
    /// involves the use of epoch based reclamation to
    /// properly GC deallocated memory once complete.
    pub fn pop(&mut self) -> Option<T> {
        let mut guard = self.ebr.pin();

        let mut cur_head_ptr = self.head.load(Acquire);

        loop {
            if cur_head_ptr.is_null() {
                return None;
            }

            let next = unsafe { (*cur_head_ptr).next.load(Acquire) };
            let result = self
                .head
                .compare_exchange(cur_head_ptr, next, Release, Release);

            if let Err(current) = result {
                cur_head_ptr = current;
            } else {
                unsafe {
                    let node: Box<Node<T>> = Box::from_raw(cur_head_ptr);
                    let item = node.item.as_ptr().read();
                    guard.defer_drop(node);
                    return Some(item);
                }
            }
        }
    }
}
