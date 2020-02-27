use rand::{thread_rng, Rng};
use std::mem::{self, size_of};
use std::ptr;
use std::rc::Rc;
use std::slice;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::util::arena::Arena;
use crate::util::cmp::Comparator;
use std::cell::RefCell;
use std::cmp;

// max height for skip list
const MAX_HEIGHT: usize = 12;
// Increase height with probability 1 in kBranching
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
}

impl<K> SkipList<K> {
    pub fn new(comparator: Rc<dyn Comparator<K>>, arena: Rc<RefCell<Arena>>, root: K) -> Self {
        let head = Node::new_node_ptr(root, MAX_HEIGHT, arena.clone());
        SkipList {
            head,
            comparator,
            arena,
            max_height: AtomicUsize::new(1),
        }
    }

    pub fn insert(&self, key: K) {
        let mut prev = [ptr::null_mut(); MAX_HEIGHT];
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

    fn random_height(&self) -> usize {
        let mut height: usize = 1;
        loop {
            if thread_rng().gen_range(0, BRANCHING) == 0 && height < MAX_HEIGHT {
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
        mut prev: Option<&mut [*mut Node<K>]>,
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
    use rand::rngs::ThreadRng;
    use rand::RngCore;
    use std::collections::hash_map::DefaultHasher;
    use std::collections::BTreeSet;
    use std::hash::Hasher;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;

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
        let list = new_list();
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

    // We want to make sure that with a single writer and multiple
    // concurrent readers (with no synchronization other than when a
    // reader's iterator is created), the reader always observes all the
    // data that was present in the skip list when the iterator was
    // constructed.  Because insertions are happening concurrently, we may
    // also observe new values that were inserted since the iterator was
    // constructed, but we should never miss any values that were present
    // at iterator construction time.
    //
    // We generate multi-part keys:
    //     <key,gen,hash>
    // where:
    //     key is in range [0..K-1]
    //     gen is a generation number for key
    //     hash is hash(key,gen)
    //
    // The insertion code picks a random key, sets gen to be 1 + the last
    // generation number inserted for that key, and sets hash to Hash(key,gen).
    //
    // At the beginning of a read, we snapshot the last inserted
    // generation number for each key.  We then iterate, including random
    // calls to Next() and Seek().  For every key we encounter, we
    // check that it is either expected given the initial snapshot or has
    // been concurrently added since the iterator started.
    const K: u64 = 4;

    struct State {
        generation: Vec<AtomicU64>,
    }

    impl State {
        pub fn new() -> Self {
            let mut generation = Vec::new();
            for _ in 0..K {
                generation.push(AtomicU64::new(0));
            }

            State { generation }
        }

        pub fn get(&self, k: u64) -> u64 {
            self.generation
                .get(k as usize)
                .unwrap()
                .load(Ordering::Acquire)
        }

        pub fn set(&self, k: u64, v: u64) {
            self.generation
                .get(k as usize)
                .unwrap()
                .store(v, Ordering::Release)
        }
    }

    struct ConcurrentTest {
        current: State,
        list: SkipList<Key>,
    }

    unsafe impl Sync for ConcurrentTest {}
    unsafe impl Send for ConcurrentTest {}

    impl ConcurrentTest {
        pub fn new() -> Self {
            let arena = Rc::new(RefCell::new(Arena::new()));
            let comparator = Rc::new(U64Comparator {});
            let list = SkipList::new(comparator, arena, 0);
            let current = State::new();
            ConcurrentTest { current, list }
        }

        pub fn key(key: Key) -> u64 {
            key >> 40
        }

        pub fn gen(key: Key) -> u64 {
            ((key) >> 8) & 0xffffffff
        }

        pub fn hash(key: Key) -> u64 {
            key & 0xff
        }

        pub fn hash_numbers(k: u64, g: u64) -> u64 {
            let mut hash = DefaultHasher::new();
            hash.write_u64(k);
            hash.write_u64(g);
            hash.finish()
        }

        pub fn make_key(k: u64, g: u64) -> Key {
            assert!(k <= K);
            assert!(g <= 0xffffffff);
            ((k << 40) | (g << 8) | (ConcurrentTest::hash_numbers(k, g) & 0xff))
        }

        pub fn is_valid_key(k: Key) -> bool {
            let key = ConcurrentTest::key(k);
            let gen = ConcurrentTest::gen(k);
            ConcurrentTest::hash(k) == ConcurrentTest::hash_numbers(key, gen) & 0xff
        }

        pub fn random_target(rnd: &mut ThreadRng) -> Key {
            match rnd.next_u64() % 10 {
                0 => ConcurrentTest::make_key(0, 0),
                1 => ConcurrentTest::make_key(K, 0),
                _ => ConcurrentTest::make_key(rnd.next_u64() % K, 0),
            }
        }

        // REQUIRES: External synchronization
        pub fn write_step(&self, rnd: &mut ThreadRng) {
            let k = rnd.next_u64() % K;
            let g = self.current.get(k) + 1;
            let key = ConcurrentTest::make_key(k, g);
            self.list.insert(key);
            self.current.set(k, g);
        }

        pub fn read_step(&self, rnd: &mut ThreadRng) {
            let initial_state = State::new();
            for i in 0..K {
                initial_state.set(i, self.current.get(i));
            }

            let mut pos = ConcurrentTest::random_target(rnd);
            let mut iter = SkipListIterator::new(&self.list);
            iter.seek(&pos);
            loop {
                let current = if iter.valid() {
                    let key = *iter.key();
                    assert!(ConcurrentTest::is_valid_key(key));
                    key
                } else {
                    ConcurrentTest::make_key(K, 0)
                };
                assert!(pos <= current);

                // Verify that everything in [pos,current) was not present in
                // initial_state.
                while pos < current {
                    let k = ConcurrentTest::key(pos);
                    let g = ConcurrentTest::gen(pos);
                    assert!(k < K);
                    assert!(g == 0 || g > initial_state.get(k));

                    pos = if k < ConcurrentTest::key(current) {
                        ConcurrentTest::make_key(k + 1, 0)
                    } else {
                        ConcurrentTest::make_key(k, g + 1)
                    }
                }

                if !iter.valid() {
                    break;
                }

                if rnd.next_u64() % 2 > 0 {
                    iter.next();
                    let k = ConcurrentTest::key(pos);
                    let g = ConcurrentTest::gen(pos);
                    pos = ConcurrentTest::make_key(k, g + 1);
                } else {
                    let new_target = ConcurrentTest::random_target(rnd);
                    if new_target > pos {
                        pos = new_target;
                        iter.seek(&new_target);
                    }
                }
            }
        }
    }

    #[test]
    fn test_concurrent_without_threads() {
        let test = ConcurrentTest::new();
        let mut rnd = thread_rng();
        for _ in 0..1000 {
            test.read_step(&mut rnd);
            test.write_step(&mut rnd)
        }
    }

    #[derive(PartialEq, Eq, Copy, Clone)]
    enum ReaderState {
        Starting,
        Running,
        Done,
    }

    struct TestState {
        pub test: ConcurrentTest,
        pub quit_flag: AtomicBool,
        state: (Mutex<ReaderState>, Condvar),
    }

    impl TestState {
        pub fn new() -> Self {
            let test = ConcurrentTest::new();
            let quit_flag = AtomicBool::new(false);
            let state = (Mutex::new(ReaderState::Starting), Condvar::new());

            TestState {
                test,
                quit_flag,
                state,
            }
        }

        pub fn wait(&self, s: ReaderState) {
            let (mu, cond) = &self.state;
            let mut guard = mu.lock().unwrap();
            while *guard != s {
                guard = cond.wait(guard).unwrap();
            }
        }

        pub fn change(&self, s: ReaderState) {
            let (mu, cond) = &self.state;
            let mut guard = mu.lock().unwrap();
            *guard = s;
            cond.notify_one();
        }
    }

    #[test]
    fn test_run_concurrent() {
        let n = 1000;
        let size = 1000;
        let mut handles = Vec::new();
        for _ in 0..n {
            let state_read = Arc::new(TestState::new());
            let state_write = Arc::clone(&state_read);
            let handle = thread::spawn(move || {
                let mut rnd = thread_rng();

                state_read.change(ReaderState::Running);
                while !state_read.quit_flag.load(Ordering::Acquire) {
                    state_read.test.read_step(&mut rnd);
                }
                state_read.change(ReaderState::Done)
            });
            handles.push(handle);

            let mut rnd = thread_rng();
            state_write.wait(ReaderState::Running);
            for _ in 0..size {
                state_write.test.write_step(&mut rnd);
            }
            state_write.quit_flag.store(true, Ordering::Release);
            state_write.wait(ReaderState::Done);
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
