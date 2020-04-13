use std::ptr;
use std::slice;

#[derive(Copy, Clone)]
pub struct Slice {
    data: *const u8,
    size: usize,
}

impl Slice {
    pub fn new(data: *const u8, size: usize) -> Self {
        Slice { data, size }
    }

    #[inline]
    pub fn data(&self) -> *const u8 {
        self.data
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn remove_prefix(&mut self, n: usize) {
        if n >= self.size {
            panic!("the slice out bounds ")
        } else {
            unsafe {
                self.data = self.data.add(n);
            }
            self.size -= n;
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_null() || self.size == 0
    }
}

impl<'a> From<&'a [u8]> for Slice {
    fn from(v: &'a [u8]) -> Self {
        Slice::new(v.as_ptr(), v.len())
    }
}

impl Default for Slice {
    fn default() -> Self {
        Slice {
            data: ptr::null(),
            size: 0,
        }
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        if self.data.is_null() {
            panic!("try to convert a empty slice to &[u8]")
        }
        unsafe { slice::from_raw_parts(self.data, self.size) }
    }
}
