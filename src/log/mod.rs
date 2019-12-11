use crate::log::slice::Slice;
use crate::util::error::Result;

pub mod slice;
pub mod writer;

// define record type in block
pub enum RecordType {
    ZeroType = 0,
    FullType = 1,
    FirstType = 2,
    MiddleType = 3,
    LastType = 4,
}

pub const BLOCK_SIZE: usize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
pub const HEADER_SIZE: usize = 4 + 2 + 1;

pub const MAX_RECORD_TYPE: i32 = 5;

pub trait Writable {
    fn append(&mut self, data: &Slice) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}
