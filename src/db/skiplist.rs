use rand::Rng;
use std::mem::{self, size_of};
use std::ptr;
use std::rc::Rc;
use std::slice;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::util::arena::Arena;
use crate::util::cmp::Comparator;
use rand::rngs::ThreadRng;
use std::cell::RefCell;
use std::cmp;

// max height for skip list
const MAX_HEIGHT: usize = 12;

const BRANCHING: usize = 4;

#[repr(C)]
pub struct Node<K> {
    pub key: K,
    height: usize,
    next: *mut AtomicPtr<Node<K>>,
}

impl<K> Node<K> {
    pub fn new_node_ptr(key: K, height: usize, arena: Rc<RefCell<Arena>>) -> *mut Node<K> {
        #![cfg_attr(feature = "cargo-clippy", allow(clippy::cast_ptr_alignment))]
        let size = size_of::<Node<K>>() + size_of::<*mut AtomicPtr<*mut Node<K>>>() * height;
        let mem = arena.borrow_mut().allocate_aligned(size);
        let buf = unsafe { slice::from_raw_parts_mut(mem, size) };
        let (node_buf, ptr_buf) = buf.split_at_mut(size_of::<Node<K>>());
        let node_ptr = node_buf.as_mut_ptr() as *mut Node<K>;

        let mut next_vec = unsafe {
            Vec::from_raw_parts(
                ptr_buf.as_mut_ptr() as *mut AtomicPtr<Node<K>>,
                height,
                height,
            )
        };
        for i in 0..next_vec.len() {
            next_vec[i] = AtomicPtr::default();
        }

        unsafe {
            (*node_ptr).key = key;
            (*node_ptr).next = next_vec.as_mut_ptr();
            (*node_ptr).height = height;
        };
        mem::forget(next_vec);

        node_ptr
    }

    #[inline]
    pub fn next(&self, n: usize) -> *mut Node<K> {
        self.do_next(n, Ordering::Acquire)
    }

    #[inline]
    pub fn set_next(&self, n: usize, node: *mut Node<K>) {
        self.do_set_next(n, node, Ordering::Release)
    }

    #[inline]
    pub fn next_no_barrier(&self, n: usize) -> *mut Node<K> {
        self.do_next(n, Ordering::Relaxed)
    }

    #[inline]
    pub fn set_next_no_barrier(&self, n: usize, node: *mut Node<K>) {
        self.do_set_next(n, node, Ordering::Relaxed)
    }

    #[inline]
    fn do_next(&self, n: usize, order: Ordering) -> *mut Node<K> {
        assert!(n < self.height);
        unsafe {
            let p = self.next.add(n);
            (*p).load(order)
        }
    }

    #[inline]
    fn do_set_next(&self, n: usize, node: *mut Node<K>, order: Ordering) {
        assert!(n < self.height);
        unsafe {
            let p = self.next.add(n);
            (*p).store(node, order)
        }
    }
}

pub struct SkipListIterator<'a, K> {
    skip_list: &'a SkipList<K>,
    node: *const Node<K>,
}

impl<'a, K> SkipListIterator<'a, K> {
    pub fn new(skip_list: &'a SkipList<K>) -> Self {
        SkipListIterator {
            skip_list,
            node: ptr::null(),
        }
    }

    #[inline]
    pub fn valid(&self) -> bool {
        !self.node.is_null()
    }

    #[inline]
    pub fn key(&self) -> &K {
        assert!(self.valid());
        unsafe { &(*self.node).key }
    }

    #[inline]
    pub fn next(&mut self) {
        assert!(self.valid());
        self.node = unsafe { (*self.node).next(0) };
    }

    #[inline]
    pub fn prev(&mut self) {
        // Instead of using explicit "prev" links, we just search for the
        // last node that falls before key.
        assert!(self.valid());
        let node = self.skip_list.find_less_than(unsafe { &(*self.node).key });
        if node == self.skip_list.head {
            self.node = ptr::null();
        } else {
            self.node = node;
        }
    }

    #[inline]
    pub fn seek(&mut self, key: &K) {
        self.node = self.skip_list.find_greater_or_equal(key, None)
    }

    #[inline]
    pub fn seek_to_first(&mut self) {
        self.node = unsafe { (*self.skip_list.head).next(0) }
    }

    #[inline]
    pub fn seek_to_last(&mut self) {
        let node = self.skip_list.find_last();
        if node == self.skip_list.head {
            self.node = ptr::null();
        } else {
            self.node = node;
        }
    }
}

pub struct SkipList<K> {
    head: *mut Node<K>,
    comparator: Rc<dyn Comparator<K>>,
    arena: Rc<RefCell<Arena>>,
    max_height: AtomicUsize,
    rand: ThreadRng,
}

impl<K> SkipList<K> {
    pub fn new(comparator: Rc<dyn Comparator<K>>, arena: Rc<RefCell<Arena>>, root: K) -> Self {
        let head = Node::new_node_ptr(root, MAX_HEIGHT, arena.clone());
        let rand = rand::thread_rng();
        SkipList {
            head,
            comparator,
            arena,
            max_height: AtomicUsize::new(1),
            rand,
        }
    }

    pub fn insert(&mut self, key: K) {
        let mut prev = Vec::with_capacity(MAX_HEIGHT);
        prev.resize(MAX_HEIGHT, ptr::null_mut());
        let x = self.find_greater_or_equal(&key, Some(&mut prev));

        assert!(x.is_null() || !self.equal(&key, unsafe { &(*x).key }));
        let height = self.random_height();
        assert!(height <= MAX_HEIGHT);
        let max_height = self.get_max_height();
        if height > max_height {
            for i in max_height..height {
                prev[i] = self.head;
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (nullptr), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since nullptr sorts after all
            // keys.  In the latter case the reader will use the new node.
            self.max_height.store(height, Ordering::Relaxed);
        };

        let new_node = Node::new_node_ptr(key, height, self.arena.clone());
        for (i, node) in prev.iter().take(height).enumerate() {
            unsafe {
                // NoBarrier_SetNext() suffices since we will add a barrier when
                // we publish a pointer to "new_node" in prev[i].
                (*new_node).set_next_no_barrier(i, (**node).next_no_barrier(i));
                (**node).set_next(i, new_node);
            }
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        let x = self.find_greater_or_equal(key, None);
        !x.is_null() && self.equal(key, unsafe { &(*x).key })
    }

    pub fn find_last(&self) -> *mut Node<K> {
        let mut x = self.head;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = unsafe { (*x).next(level) };
            if !next.is_null() {
                x = next
            } else {
                if level == 0 {
                    return x;
                }
                level -= 1;
            }
        }
    }

    fn equal(&self, a: &K, b: &K) -> bool {
        self.comparator.compare(&a, &b) == cmp::Ordering::Equal
    }

    fn key_is_after_node(&self, key: &K, n: *const Node<K>) -> bool {
        if n.is_null() {
            false
        } else {
            let node_key = unsafe { &(*n).key };
            self.comparator.compare(node_key, key) == cmp::Ordering::Less
        }
    }

    fn get_max_height(&self) -> usize {
        self.max_height.load(Ordering::Relaxed)
    }

    fn random_height(&mut self) -> usize {
        let mut height: usize = 1;
        loop {
            if self.rand.gen_range(0, BRANCHING) == 0 && height < MAX_HEIGHT {
                height += 1;
                continue;
            } else {
                break;
            }
        }
        assert!(height <= MAX_HEIGHT);
        height
    }

    fn find_greater_or_equal(
        &self,
        key: &K,
        mut prev: Option<&mut Vec<*mut Node<K>>>,
    ) -> *mut Node<K> {
        let mut x = self.head;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = unsafe { (*x).next(level) };
            if self.key_is_after_node(key, next) {
                x = next;
            } else {
                if let Some(ref mut prev_rout) = prev {
                    assert_eq!(prev_rout.len(), MAX_HEIGHT);
                    prev_rout[level] = x;
                }

                if level == 0 {
                    return next;
                }
                level -= 1;
            }
        }
    }

    fn find_less_than(&self, key: &K) -> *mut Node<K> {
        let mut x = self.head;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = unsafe { (*x).next(level) };
            if !next.is_null()
                && self.comparator.compare(unsafe { &(*next).key }, key) == cmp::Ordering::Less
            {
                x = next;
            } else {
                if level == 0 {
                    return x;
                }
                level -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::thread_rng;
    use std::collections::BTreeSet;

    type Key = u64;

    struct U64Comparator {}

    impl Comparator<Key> for U64Comparator {
        fn compare(&self, left: &Key, right: &Key) -> cmp::Ordering {
            if left < right {
                cmp::Ordering::Less
            } else if left == right {
                cmp::Ordering::Equal
            } else {
                cmp::Ordering::Greater
            }
        }
    }

    fn new_list() -> SkipList<Key> {
        let arena = Rc::new(RefCell::new(Arena::new()));
        let comparator = Rc::new(U64Comparator {});
        SkipList::new(comparator, arena, 0)
    }

    #[test]
    fn test_new_node() {
        let arena = Rc::new(RefCell::new(Arena::new()));
        let node = Node::new_node_ptr(0, MAX_HEIGHT, arena.clone());
        assert_eq!(unsafe { (*node).key }, 0);
        for i in 0..MAX_HEIGHT {
            let p = unsafe { (*node).next(i) };
            assert!(p.is_null())
        }

        for _ in 0..100 {
            let mut rnd = thread_rng();
            let height = rnd.gen_range(0, MAX_HEIGHT + 1);
            let node = Node::new_node_ptr(height, height, arena.clone());
            assert_eq!(unsafe { (*node).key }, height);
            assert_eq!(unsafe { (*node).height }, height)
        }
    }

    #[test]
    fn test_empty() {
        let list = new_list();
        assert!(!list.contains(&10));

        let mut iter = SkipListIterator::new(&list);
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek(&100);
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
    }

    #[test]
    fn test_insert_and_lookup() {
        let (n, r) = (2000 as u64, 5000 as u64);
        let mut rnd = thread_rng();
        let mut keys = BTreeSet::new();
        let mut list = new_list();
        for _ in 0..n {
            let key = rnd.gen_range(0, r);
            if keys.insert(key) {
                list.insert(key)
            }
        }

        for i in 0..r {
            assert_eq!(list.contains(&i), keys.contains(&i))
        }

        // Simple iterator tests
        {
            let mut iter = SkipListIterator::new(&list);
            assert!(!iter.valid());

            iter.seek(&0);
            assert!(iter.valid());
            assert_eq!(keys.iter().next().unwrap(), iter.key());

            iter.seek_to_first();
            assert!(iter.valid());
            assert_eq!(keys.iter().next().unwrap(), iter.key());

            iter.seek_to_last();
            assert!(iter.valid());
            assert_eq!(keys.iter().last().unwrap(), iter.key());
        }

        // Forward iteration test
        for i in 0..r {
            let mut iter = SkipListIterator::new(&list);
            iter.seek(&i);

            let mut model_iter = keys.iter().filter(|&&k| k >= i);
            for _ in 0..3 {
                let model_key = model_iter.next();
                if model_key.is_none() {
                    assert!(!iter.valid());
                    break;
                } else {
                    assert!(iter.valid());
                    assert_eq!(model_key.unwrap(), iter.key());
                }
                iter.next();
            }
        }

        // Backward iteration test
        {
            let mut iter = SkipListIterator::new(&list);
            iter.seek_to_last();

            for key in keys.iter().rev() {
                assert!(iter.valid());
                assert_eq!(key, iter.key());
                iter.prev();
            }
            assert!(!iter.valid());
        }
    }
}
