use crate::db::error::{Result, StatusError};
use crate::db::option::{NO_COMPRESSION, SNAPPY_COMPRESSION};
use crate::db::slice::Slice;
use crate::db::{RandomAccessFile, ReadOption};
use crate::util::coding::{put_varint64, DecodeVarint};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use snap::read::FrameDecoder;
use std::io;
use std::mem;

// Maximum encoding length of a BlockHandle
pub const BLOCK_HANDLE_MAX_ENCODED_LENGTH: usize = 10 + 10;
// Encoded length of a Footer.  Note that the serialization of a
// Footer will always occupy exactly this many bytes.  It consists
// of two block handles and a magic number.
pub const FOOTER_ENCODED_LENGTH: usize = 2 * BLOCK_HANDLE_MAX_ENCODED_LENGTH + 8;
// TABLE_MAGIC_NUMBER was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
pub const TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57u64;
// 1-byte type + 32-bit crc
pub const BLOCK_TRAILER_SIZE: usize = 5;

#[derive(Copy, Clone)]
pub struct BlockHandle {
    offset: u64,
    size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        BlockHandle { offset, size }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn set_size(&mut self, size: u64) {
        self.size = size
    }

    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        put_varint64(dst, self.offset);
        put_varint64(dst, self.size);
    }

    pub fn decode_from(&mut self, input: &mut &[u8]) -> Result<()> {
        self.offset = input.decode_varint64()?;
        self.size = input.decode_varint64()?;
        Ok(())
    }
}

impl Default for BlockHandle {
    fn default() -> Self {
        BlockHandle { offset: 0, size: 0 }
    }
}

pub struct Footer {
    meta_index_handle: BlockHandle,
    index_handle: BlockHandle,
}

impl Footer {
    pub fn meta_index_handle(&self) -> &BlockHandle {
        &self.meta_index_handle
    }

    pub fn index_handle(&self) -> &BlockHandle {
        &self.index_handle
    }

    pub fn set_meta_index_handle(&mut self, h: &BlockHandle) {
        self.meta_index_handle = *h
    }

    pub fn set_index_handle(&mut self, h: &BlockHandle) {
        self.index_handle = *h
    }

    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        let original_size = dst.len();
        self.meta_index_handle.encode_to(dst);
        self.index_handle.encode_to(dst);
        dst.resize(2 * BLOCK_HANDLE_MAX_ENCODED_LENGTH, 0);
        dst.write_u32::<LittleEndian>((TABLE_MAGIC_NUMBER & 0xffffffffu64) as u32)
            .unwrap();
        dst.write_u32::<LittleEndian>((TABLE_MAGIC_NUMBER >> 32) as u32)
            .unwrap();
        assert_eq!(dst.len(), original_size + FOOTER_ENCODED_LENGTH);
    }

    pub fn decoded_from(&mut self, input: Slice) -> Result<()> {
        let mut buf = input.as_ref();
        let mut magic_buf = buf[FOOTER_ENCODED_LENGTH - 8..].as_ref();
        let magic_lo = magic_buf.read_u32::<LittleEndian>().unwrap();
        let magic_hi = magic_buf.read_u32::<LittleEndian>().unwrap();
        let magic = (magic_hi as u64) << 32 | magic_lo as u64;
        if magic != TABLE_MAGIC_NUMBER {
            return Err(StatusError::Corruption(
                "not an sstable (bad magic number)".to_string(),
            ));
        }
        self.meta_index_handle.decode_from(&mut buf)?;
        self.index_handle.decode_from(&mut buf)?;
        Ok(())
    }
}

impl Default for Footer {
    fn default() -> Self {
        Footer {
            meta_index_handle: Default::default(),
            index_handle: Default::default(),
        }
    }
}

pub struct BlockContent {
    // the data is just for reading, we can't change it
    pub data: Vec<u8>,
    pub cachable: bool,
    pub heap_allocted: bool,
}

impl Drop for BlockContent {
    fn drop(&mut self) {
        // we only need to drop when heap_allocted is true
        if !self.heap_allocted {
            mem::forget(mem::take(&mut self.data));
        }
    }
}

impl BlockContent {
    pub fn read_block_from_file<R: RandomAccessFile>(
        file: &R,
        handle: BlockHandle,
        option: &ReadOption,
    ) -> Result<BlockContent> {
        // Read the block contents as well as the type/crc footer.
        // See table_builder.cc for the code that built this structure.
        let n = handle.size as usize;
        let mut buf = vec![0; n + BLOCK_TRAILER_SIZE];
        let content = file.read(handle.offset() as usize, n + BLOCK_TRAILER_SIZE, &mut buf)?;
        if content.size() != n + BLOCK_TRAILER_SIZE {
            return Err(StatusError::Corruption("truncated block read".to_string()));
        }

        let data = content.as_ref();
        // Check the crc of the type and the block contents
        if option.verify_checksum {
            let checksum = data[n + 1..].as_ref().read_u32::<LittleEndian>().unwrap();
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(data[0..n + 1].as_ref());
            if checksum != hasher.finalize() {
                return Err(StatusError::Corruption(
                    "block checksum mismatch".to_string(),
                ));
            }
        }

        let mut block = BlockContent::default();
        // TODO：decode block from compressed data
        match data[n] {
            NO_COMPRESSION => {
                if buf.is_empty() {
                    // File implementation gave us pointer to some other data.
                    // Use it directly under the assumption that it will be live
                    // while the file is open.
                    unsafe {
                        let p = data.as_ptr() as *mut u8;
                        block.data = Vec::from_raw_parts(p, n, n);
                    }
                    block.heap_allocted = false;
                    block.cachable = false;
                } else {
                    buf.resize(n, 0);
                    block.data = buf;
                    block.heap_allocted = true;
                    block.cachable = true;
                }
            }
            SNAPPY_COMPRESSION => {
                let mut uncompressed_data = Vec::new();
                let mut reader = FrameDecoder::new(&data[0..n]);
                if io::copy(&mut reader, &mut uncompressed_data).is_ok() {
                    block.data = uncompressed_data;
                    block.heap_allocted = true;
                    block.cachable = true;
                } else {
                    return Err(StatusError::Corruption(
                        "corrupted compressed block content".to_string(),
                    ));
                }
            }
            _ => {
                return Err(StatusError::Corruption("bad block type".to_string()));
            }
        }

        Ok(block)
    }
}

impl Default for BlockContent {
    fn default() -> Self {
        BlockContent {
            data: Default::default(),
            cachable: false,
            heap_allocted: false,
        }
    }
}
