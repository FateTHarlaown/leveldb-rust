use crate::db::error::Result;
use crate::db::slice::Slice;

pub mod error;
pub mod log;
pub mod slice;

// define record type in block
#[derive(Copy, Clone, PartialEq)]
pub enum RecordType {
    ZeroType = 0,
    FullType = 1,
    FirstType = 2,
    MiddleType = 3,
    LastType = 4,
    Eof = 5,
    BadRecord = 6,
}

pub const BLOCK_SIZE: usize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
pub const HEADER_SIZE: usize = 4 + 2 + 1;

impl From<u8> for RecordType {
    fn from(n: u8) -> Self {
        match n {
            0 => RecordType::ZeroType,
            1 => RecordType::FullType,
            2 => RecordType::FirstType,
            3 => RecordType::MiddleType,
            4 => RecordType::LastType,
            5 => RecordType::Eof,
            _ => RecordType::BadRecord,
        }
    }
}

pub trait WritableFile {
    fn append(&mut self, data: &Slice) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

pub trait SequentialFile {
    fn read(&mut self, buf: &mut [u8], n: usize) -> Result<usize>;
    fn skip(&mut self, n: usize) -> Result<()>;
}
