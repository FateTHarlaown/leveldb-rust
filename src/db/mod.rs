use crate::db::error::{Result, StatusError};
use crate::db::option::Options;
use crate::db::slice::Slice;

mod dbformat;
pub mod error;
mod filename;
pub mod log;
mod memtable;
pub mod option;
pub mod skiplist;
pub mod slice;
mod write_batch;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following const var describes which
// compression method (if any) is used to compress a block.
// NOTE: do not change the values of existing entries, as these are
// part of the persistent format on disk.
pub const NO_COMPRESSION: u8 = 0x0;
pub const SNAPPY_COMPRESSION: u8 = 0x1;

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
    UnKnown = 7,
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
            6 => RecordType::BadRecord,
            _ => RecordType::UnKnown,
        }
    }
}

pub trait WritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

pub trait SequentialFile {
    fn read(&mut self, buf: &mut [u8], n: usize) -> Result<usize>;
    fn skip(&mut self, n: usize) -> Result<()>;
}

pub trait RandomAccessFile {
    // Read up to "n" bytes from the file starting at "offset".
    // "scratch[0..n-1]" may be written by this routine.  Return a &[u8]
    // to the data that was read (including if fewer than "n" bytes were
    // successfully read).  May set returned &[u8] to point at data in
    // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
    // "*result" is used.  If an error was encountered, returns a non-OK
    // status.
    //
    // Safe for concurrent use by multiple threads.
    fn read(&self, offset: usize, n: usize, scratch: &mut Vec<u8>) -> Result<Slice>;
}

pub trait Reporter {
    fn corruption(&mut self, n: usize, status: StatusError);
}

pub trait Iterator {
    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    fn valid(&self) -> bool;

    // Position at the first key in the source.  The iterator is Valid()
    // after this call iff the source is not empty.
    fn seek_to_first(&mut self);

    // Position at the last key in the source.  The iterator is
    // Valid() after this call iff the source is not empty.
    fn seek_to_last(&mut self);

    // Position at the first key in the source that is at or past target.
    // The iterator is Valid() after this call iff the source contains
    // an entry that comes at or past target.
    fn seek(&mut self, target: Slice);

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    fn next(&mut self);

    // Moves to the previous entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: Valid()
    fn prev(&mut self);

    // Return the key for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    fn key(&self) -> Slice;

    // Return the value for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    fn value(&self) -> Slice;

    // If an error has occurred, return it.  Else return an Ok(())
    fn status(&mut self) -> Result<()>;
}

#[derive(Clone)]
pub struct ReadOption {
    pub verify_checksum: bool,
    pub fill_cache: bool,
}

impl Default for ReadOption {
    fn default() -> Self {
        ReadOption {
            verify_checksum: false,
            fill_cache: true,
        }
    }
}

pub struct WriteOption {
    // If true, the write will be flushed from the operating system
    // buffer cache (by calling WritableFile::Sync()) before the write
    // is considered complete.  If this flag is true, writes will be
    // slower.
    //
    // If this flag is false, and the machine crashes, some recent
    // writes may be lost.  Note that if it is just the process that
    // crashes (i.e., the machine does not reboot), no writes will be
    // lost even if sync==false.
    //
    // In other words, a DB write with sync==false has similar
    // crash semantics as the "write()" system call.  A DB write
    // with sync==true has similar crash semantics to a "write()"
    // system call followed by "fsync()".
    sync: bool,
}

impl Default for WriteOption {
    fn default() -> Self {
        WriteOption { sync: false }
    }
}

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
pub trait DB {
    // Set the database entry for "key" to "value".  Returns OK on success,
    // Note: consider setting options.sync = true.
    fn put(&mut self, option: &WriteOption, key: Slice, value: Slice) -> Result<()>;

    // Remove the database entry (if any) for "key".  Returns OK on
    // success, and a non-OK status on error.  It is not an error if "key"
    // did not exist in the database.
    // Note: consider setting options.sync = true.
    fn delete(&mut self, option: &WriteOption, key: Slice) -> Result<()>;
}
