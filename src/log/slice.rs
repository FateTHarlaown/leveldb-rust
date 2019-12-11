use crate::util::bit::memcmp;
use crate::util::buffer::BufferReader;
use crate::util::error::{Result, StatusError};

use std::intrinsics::unlikely;
use std::io;

pub type Slice = [u8];

impl<'a> BufferReader for &'a [u8] {
    #[inline]
    fn bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    fn advance(&mut self, count: usize) {
        *self = &self[count..]
    }

    fn read_bytes(&mut self, count: usize) -> Result<&[u8]> {
        if unsafe { unlikely(self.len() < count) } {
            return Err(StatusError::IOError(io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF")));
        }
        let (left, right) = self.split_at(count);
        *self = right;
        Ok(left)
    }
}
