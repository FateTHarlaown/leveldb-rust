use crate::util::hash::hash;
use lru::LruCache;
use std::hash::Hash;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

const NUM_SHARD_BITS: u32 = 4;
const NUM_SHARDS: u32 = 1 << NUM_SHARD_BITS;

// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)
// You can not modify if once it was inserted into the cache.
pub trait Cache<K: Sized, V: Sized> {
    // Insert a mapping from key->value into the cache and assign it
    // the specified charge against the total cache capacity.
    fn insert(&self, key: K, value: V, charge: u64) -> Option<Arc<V>>;

    // If the cache has no mapping for "key", returns None.
    fn look_up(&self, key: &K) -> Option<Arc<V>>;

    // If cache has the key, erase it.
    fn erase(&self, key: &K);

    // Return a new numeric id.  May be used by multiple clients who are
    // sharing the same cache to partition the key space.  Typically the
    // client will allocate a new id at startup and prepend the id to
    // its cache keys.
    fn new_id(&self) -> u64;

    // Return an estimate of the combined charges of all elements stored in the
    // cache.
    fn total_charge(&self) -> u64;
}

pub struct ShardedLruCache<K, V>
where
    K: AsRef<[u8]> + Eq + Hash,
{
    shards: Box<[Arc<Mutex<InnerLruCache<K, V>>>]>,
    last_id: AtomicU64,
}

impl<K, V> ShardedLruCache<K, V>
where
    K: AsRef<[u8]> + Eq + Hash,
{
    pub fn new(capacity: u64) -> Self {
        let per_shard = (capacity + (NUM_SHARDS - 1) as u64) / NUM_SHARDS as u64;
        let mut tmp_vec = Vec::with_capacity(NUM_SHARDS as usize);
        for _ in 0..NUM_SHARDS {
            let shard = Arc::new(Mutex::new(InnerLruCache::new(per_shard)));
            tmp_vec.push(shard)
        }

        ShardedLruCache {
            shards: tmp_vec.into_boxed_slice(),
            last_id: AtomicU64::new(0),
        }
    }

    #[inline]
    fn hash_key(key: &K) -> u32 {
        let data = key.as_ref();
        hash(data, 0)
    }

    #[inline]
    fn shard(h: u32) -> u32 {
        h >> (32 - NUM_SHARD_BITS)
    }
}

impl<K, V> Cache<K, V> for ShardedLruCache<K, V>
where
    K: AsRef<[u8]> + Eq + Hash,
{
    fn insert(&self, key: K, value: V, charge: u64) -> Option<Arc<V>> {
        let hash_val = Self::hash_key(&key);
        let shard = Self::shard(hash_val);
        assert!(shard < NUM_SHARDS);
        let mut lru = self.shards.get(shard as usize).unwrap().lock().unwrap();
        lru.insert(key, value, charge)
    }

    fn look_up(&self, key: &K) -> Option<Arc<V>> {
        let hash_val = Self::hash_key(&key);
        let shard = Self::shard(hash_val);
        assert!(shard < NUM_SHARDS);
        let mut lru = self.shards.get(shard as usize).unwrap().lock().unwrap();
        lru.look_up(key)
    }

    fn erase(&self, key: &K) {
        let hash_val = Self::hash_key(&key);
        let shard = Self::shard(hash_val);
        assert!(shard < NUM_SHARDS);
        let mut lru = self.shards.get(shard as usize).unwrap().lock().unwrap();
        lru.erase(key)
    }

    fn new_id(&self) -> u64 {
        self.last_id.fetch_add(1, Ordering::SeqCst)
    }

    fn total_charge(&self) -> u64 {
        let mut total = 0;
        for mu in self.shards.iter() {
            let shard = mu.lock().unwrap();
            total += shard.total_charge();
        }
        total
    }
}

// a wrapper for LruCache value with it's charge
struct LruValue<V> {
    value: Arc<V>,
    charge: u64,
}

struct InnerLruCache<K: Eq + Hash, V> {
    lru: LruCache<K, LruValue<V>>,
    usage: u64,
    capacity: u64,
}

impl<K: Eq + Hash, V> InnerLruCache<K, V> {
    pub fn new(capacity: u64) -> Self {
        let lru = LruCache::unbounded();
        InnerLruCache {
            lru,
            usage: 0,
            capacity,
        }
    }

    pub fn insert(&mut self, key: K, value: V, charge: u64) -> Option<Arc<V>> {
        if self.capacity == 0 {
            return None;
        }

        self.usage += charge as u64;
        while self.usage > self.capacity && !self.lru.is_empty() {
            let (_, evicted_val) = self.lru.pop_lru().unwrap();
            self.usage -= evicted_val.charge;
        }

        let val = LruValue {
            value: Arc::new(value),
            charge,
        };
        if let Some(handle) = self.lru.put(key, val) {
            Some(handle.value)
        } else {
            None
        }
    }

    pub fn look_up(&mut self, key: &K) -> Option<Arc<V>> {
        if let Some(h) = self.lru.get(key) {
            Some(h.value.clone())
        } else {
            None
        }
    }

    pub fn erase(&mut self, key: &K) {
        if let Some(v) = self.lru.pop(key) {
            self.usage -= v.charge;
        }
    }

    pub fn prune(&mut self) {
        self.lru.clear();
        self.usage = 0;
    }

    pub fn total_charge(&self) -> u64 {
        self.usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::slice::Slice;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::cell::RefCell;
    use std::rc::Rc;

    const CACHE_SIZE: u64 = 1000;

    fn encode_key(k: u32) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.write_u32::<LittleEndian>(k).unwrap();
        vec
    }

    struct TestValue {
        key: u32,
        val: u32,
        deleted_vec: Rc<RefCell<Vec<(u32, u32)>>>,
    }

    impl Drop for TestValue {
        fn drop(&mut self) {
            self.deleted_vec.borrow_mut().push((self.key, self.val));
        }
    }

    struct TestCache {
        cache: ShardedLruCache<Vec<u8>, TestValue>,
        deleted_vec: Rc<RefCell<Vec<(u32, u32)>>>,
    }

    impl TestCache {
        fn new(capacity: u64) -> Self {
            TestCache {
                cache: ShardedLruCache::new(capacity),
                deleted_vec: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn insert(&mut self, key: u32, value: u32, charge: u64) -> Option<u32> {
            let deleted_vec = self.deleted_vec.clone();
            let v = TestValue {
                key,
                val: value,
                deleted_vec,
            };

            if let Some(r) = self.cache.insert(encode_key(key), v, charge) {
                Some(r.val)
            } else {
                None
            }
        }

        fn look_up(&mut self, key: u32) -> Option<u32> {
            if let Some(r) = self.cache.look_up(&encode_key(key)) {
                Some(r.val)
            } else {
                None
            }
        }

        fn erase(&mut self, key: u32) {
            self.cache.erase(&encode_key(key))
        }

        fn check_deleted(&self, index: usize, key: u32, val: u32) {
            let deleted = self.deleted_vec.borrow();
            let (k, v) = deleted.get(index).unwrap();
            assert_eq!(key, *k);
            assert_eq!(val, *v);
        }

        fn check_deleted_num(&self, len: usize) {
            let deleted = self.deleted_vec.borrow();
            assert_eq!(len, deleted.len());
        }
    }

    #[test]
    fn test_zero_size_cache() {
        let mut tester = TestCache::new(0);
        tester.insert(1, 100, 1);
        assert_eq!(None, tester.look_up(1));
    }

    #[test]
    fn test_hit_and_miss() {
        let mut tester = TestCache::new(CACHE_SIZE);
        assert_eq!(None, tester.look_up(100));

        tester.insert(100, 101, 1);
        assert_eq!(101, tester.look_up(100).unwrap());
        assert_eq!(None, tester.look_up(200));
        assert_eq!(None, tester.look_up(300));

        tester.insert(200, 201, 1);
        assert_eq!(101, tester.look_up(100).unwrap());
        assert_eq!(201, tester.look_up(200).unwrap());

        assert_eq!(101, tester.insert(100, 102, 1).unwrap());
        assert_eq!(102, tester.look_up(100).unwrap());
        assert_eq!(201, tester.look_up(200).unwrap());
        assert_eq!(None, tester.look_up(300));

        tester.check_deleted_num(1);
        tester.check_deleted(0, 100, 101);
    }

    #[test]
    fn test_erase() {
        let mut tester = TestCache::new(CACHE_SIZE);
        tester.erase(200);
        tester.check_deleted_num(0);

        tester.insert(100, 101, 1);
        tester.insert(200, 201, 1);

        tester.erase(100);
        assert_eq!(None, tester.look_up(100));
        assert_eq!(201, tester.look_up(200).unwrap());
        tester.check_deleted_num(1);
        tester.check_deleted(0, 100, 101);

        tester.erase(100);
        assert_eq!(None, tester.look_up(100));
        assert_eq!(201, tester.look_up(200).unwrap());
        tester.check_deleted_num(1);
        tester.check_deleted(0, 100, 101);
    }

    #[test]
    fn test_entries_drop() {
        let mut tester = TestCache::new(CACHE_SIZE);
        tester.insert(100, 101, 1);
        let v1 = tester.cache.look_up(&encode_key(100)).unwrap();
        assert_eq!(101, v1.val);
        tester.insert(100, 102, 1);
        let v2 = tester.cache.look_up(&encode_key(100)).unwrap();
        assert_eq!(102, v2.val);
        tester.check_deleted_num(0);

        drop(v1);
        tester.check_deleted_num(1);
        tester.check_deleted(0, 100, 101);

        tester.erase(100);
        assert_eq!(None, tester.look_up(100));
        tester.check_deleted_num(1);

        drop(v2);
        tester.check_deleted_num(2);
        tester.check_deleted(1, 100, 102);
    }

    #[test]
    fn test_eviction_policy() {
        let mut tester = TestCache::new(CACHE_SIZE);
        tester.insert(100, 101, 1);
        tester.insert(200, 201, 1);
        tester.insert(300, 301, 1);

        for i in 0..CACHE_SIZE as u32 + 100 {
            tester.insert(1000 + i, 2000 + i, 1);
            assert_eq!(2000 + i, tester.look_up(1000 + i).unwrap());
            assert_eq!(101, tester.look_up(100).unwrap());
        }
        assert_eq!(None, tester.look_up(200));
        assert_eq!(None, tester.look_up(300));
    }

    #[test]
    fn test_heavy_entries() {
        let mut tester = TestCache::new(CACHE_SIZE);
        let (light, heavy) = (1, 10);

        let mut added = 0;
        let mut index = 0;
        while added < 2 * CACHE_SIZE {
            let weight = if index & 1 == 1 { light } else { heavy };
            tester.insert(index, 1000 + index, weight);
            added += weight;
            index += 1;
        }

        let mut cached_weight = 0;
        for i in 0..index {
            let weight = if i & 1 == 1 { light } else { heavy };
            if let Some(v) = tester.look_up(i) {
                assert_eq!(1000 + i, v);
                cached_weight += weight;
            }
        }
        assert!(cached_weight <= CACHE_SIZE + CACHE_SIZE / 10);
    }

    #[test]
    fn test_new_id() {
        let mut tester = TestCache::new(CACHE_SIZE);
        let a = tester.cache.new_id();
        let b = tester.cache.new_id();
        assert_ne!(a, b);
    }
}
