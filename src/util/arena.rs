use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

const BLOCK_SIZE: usize = 4096;
// a Vec has pointer and two usize field(cap and len)
const POINTER_LENGTH: usize = size_of::<*mut u8>();
const EXTRA_VEC_LEN: usize = POINTER_LENGTH + 2 * size_of::<usize>();

pub struct Arena {
    p: *mut u8,
    remain: usize,
    usage: AtomicUsize,
    blocks: Vec<Vec<u8>>,
}

impl Arena {
    pub fn new() -> Self {
        Arena {
            p: ptr::null_mut(),
            remain: 0,
            usage: AtomicUsize::new(0),
            blocks: Vec::new(),
        }
    }

    #[inline]
    pub fn allocate(&mut self, n: usize) -> *mut u8 {
        assert!(n > 0);
        if n <= self.remain {
            let result = self.p;
            unsafe {
                self.p = self.p.add(n);
            }
            self.remain -= n;
            result
        } else {
            self.allocate_fallback(n)
        }
    }

    pub fn allocate_aligned(&mut self, n: usize) -> *mut u8 {
        let align = if POINTER_LENGTH > 8 {
            POINTER_LENGTH
        } else {
            8
        };

        let current_mod = self.p as usize & (align - 1);
        let slop = if current_mod == 0 {
            0
        } else {
            align - current_mod
        };

        let needed = n + slop;
        if needed <= self.remain {
            let result = unsafe { self.p.add(slop) };
            self.p = unsafe { self.p.add(needed) };
            self.remain -= needed;
            result
        } else {
            self.allocate_fallback(n)
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.usage.load(Ordering::SeqCst)
    }

    fn allocate_fallback(&mut self, n: usize) -> *mut u8 {
        if n > BLOCK_SIZE / 4 {
            self.allocate_new_block(n)
        } else {
            self.p = self.allocate_new_block(BLOCK_SIZE);
            self.remain = BLOCK_SIZE;
            let result = self.p;
            unsafe {
                self.p = self.p.add(n);
            }
            self.remain -= n;
            result
        }
    }

    fn allocate_new_block(&mut self, block_bytes: usize) -> *mut u8 {
        let mut v: Vec<u8> = Vec::with_capacity(block_bytes);
        unsafe {
            v.set_len(block_bytes);
        }
        let r = v.as_mut_ptr();
        self.blocks.push(v);
        self.usage
            .fetch_add(block_bytes + EXTRA_VEC_LEN, Ordering::SeqCst);
        r
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_new() {
        let arena = Arena::new();
        assert_eq!(arena.remain, 0);
        assert_eq!(arena.p.is_null(), true);
        assert_eq!(arena.blocks.len(), 0);
        assert_eq!(arena.memory_usage(), 0);
    }

    #[test]
    fn test_alloc_new_block() {
        let mut arena = Arena::new();
        arena.allocate_new_block(1000);

        assert_eq!(arena.remain, 0);
        assert_eq!(arena.p.is_null(), true);
        assert_eq!(arena.blocks.len(), 1);
        assert_eq!(arena.memory_usage(), 1000 + EXTRA_VEC_LEN);
        assert_eq!(arena.blocks[0].capacity(), 1000);
        assert_eq!(arena.blocks[0].len(), 1000);
    }

    #[test]
    fn test_alloc_aligned() {
        let mut arena = Arena::new();
        let n = 50 * POINTER_LENGTH;
        arena.allocate_fallback(1);
        arena.allocate_aligned(n);

        assert_eq!(arena.p.is_null(), false);
        assert_eq!(arena.remain, BLOCK_SIZE - POINTER_LENGTH - n);
    }

    #[test]
    fn test_alloc_fallback() {
        let mut arena = Arena::new();
        arena.allocate_fallback(1025);
        assert_eq!(arena.remain, 0);
        assert_eq!(arena.p.is_null(), true);
        assert_eq!(arena.blocks.len(), 1);
        assert_eq!(arena.memory_usage(), 1025 + EXTRA_VEC_LEN);

        arena.allocate_fallback(256);
        assert_eq!(arena.remain, BLOCK_SIZE - 256);
        assert_eq!(arena.p.is_null(), false);
        assert_eq!(arena.memory_usage(), 1025 + BLOCK_SIZE + 2 * EXTRA_VEC_LEN);
    }

    #[test]
    fn test_alloc() {
        let mut arena = Arena::new();
        arena.allocate(128);
        assert_eq!(arena.remain, BLOCK_SIZE - 128);
        assert_eq!(arena.p.is_null(), false);
        assert_eq!(arena.blocks.len(), 1);
        assert_eq!(arena.memory_usage(), BLOCK_SIZE + EXTRA_VEC_LEN);

        arena.allocate(1024);
        assert_eq!(arena.remain, BLOCK_SIZE - 128 - 1024);
        assert_eq!(arena.p.is_null(), false);
        assert_eq!(arena.blocks.len(), 1);
        assert_eq!(arena.memory_usage(), BLOCK_SIZE + EXTRA_VEC_LEN);

        arena.allocate(8192);
        assert_eq!(arena.remain, BLOCK_SIZE - 128 - 1024);
        assert_eq!(arena.p.is_null(), false);
        assert_eq!(arena.blocks.len(), 2);
        assert_eq!(arena.memory_usage(), BLOCK_SIZE + 8192 + EXTRA_VEC_LEN * 2);

        arena.allocate(2048);
        assert_eq!(arena.remain, BLOCK_SIZE - 128 - 1024 - 2048);
        assert_eq!(arena.p.is_null(), false);
        assert_eq!(arena.blocks.len(), 2);
        assert_eq!(arena.memory_usage(), BLOCK_SIZE + 8192 + EXTRA_VEC_LEN * 2);

        arena.allocate(1024);
        assert_eq!(arena.remain, BLOCK_SIZE - 1024);
        assert_eq!(arena.p.is_null(), false);
        assert_eq!(arena.blocks.len(), 3);
        assert_eq!(
            arena.memory_usage(),
            BLOCK_SIZE * 2 + 8192 + EXTRA_VEC_LEN * 3
        );
    }

    #[test]
    fn test_alloc_many() {
        let mut arena = Arena::new();
        let mut allocated = vec![];
        let n = 100000;
        let mut bytes = 0;
        let mut rng = rand::thread_rng();

        for i in 0..n {
            let s = if i % (n / 10) == 0 {
                i
            } else {
                if rng.gen::<usize>() < 4000 {
                    rng.gen_range(0, 6000)
                } else {
                    if rng.gen::<usize>() < 10 {
                        rng.gen_range(0, 10)
                    } else {
                        rng.gen_range(0, 20)
                    }
                }
            };
            let s = if s == 0 { 1 } else { s };

            let r = if rng.gen::<usize>() < 10 {
                arena.allocate_aligned(s)
            } else {
                arena.allocate(s)
            };

            for b in 0..s {
                unsafe { *r.add(b) = (i % 256) as u8 };
            }

            bytes += s;
            allocated.push((s, r));
            assert!(arena.memory_usage() >= bytes);
            if i > n / 10 {
                assert!(arena.memory_usage() <= (bytes as f64 * 1.10) as usize);
            }
        }

        let mut i = 0;
        for (n, r) in allocated.iter() {
            for b in 0..*n {
                let data = unsafe { *r.add(b) };
                assert_eq!(data as usize, i % 256);
            }
            i += 1;
        }
    }
}
