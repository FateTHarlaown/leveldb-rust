use crate::db::error::{Result, StatusError};
use crate::db::option::Options;
use crate::db::slice::Slice;
use crate::db::Iterator;
use crate::sstable::format::BlockContent;
use crate::util::buffer::BufferReader;
use crate::util::cmp::Comparator;
use crate::util::coding::{put_varint32, DecodeVarint};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::{self, Ordering};
use std::mem;
use std::rc::Rc;
use std::sync::Arc;

pub struct Block {
    content: Arc<BlockContent>,
    restart_offset: u32,
}

impl Block {
    pub fn from_content(content: BlockContent) -> Result<Self> {
        let n = content.data.len();
        if n < mem::size_of::<u32>() {
            Err(StatusError::Corruption(
                "bad block contents, size smaller than u32".to_string(),
            ))
        } else {
            let mut block = Block {
                content: Arc::new(content),
                restart_offset: 0,
            };
            let max_restart_allowed = (n - mem::size_of::<u32>()) / mem::size_of::<u32>();
            if block.num_restarts() as usize > max_restart_allowed {
                Err(StatusError::Corruption("bad block contents".to_string()))
            } else {
                block.restart_offset =
                    n as u32 - (1 + block.num_restarts()) * mem::size_of::<u32>() as u32;
                Ok(block)
            }
        }
    }

    #[inline]
    pub fn num_restarts(&self) -> u32 {
        assert!(self.content.data.len() > mem::size_of::<u32>());
        let mut buf = self.content.data[self.content.data.len() - mem::size_of::<u32>()..].as_ref();
        buf.read_u32::<LittleEndian>().unwrap()
    }

    pub fn data(&self) -> &[u8] {
        self.content.data.as_slice()
    }

    pub fn new_iterator(&self, comparator: Arc<dyn Comparator<Slice>>) -> BlockIter {
        BlockIter::new(self, comparator)
    }
}

pub struct BlockIter {
    block_content: Arc<BlockContent>, // underlying block contents
    comparator: Arc<dyn Comparator<Slice>>,
    restarts: u32,     // Offset of restart array (list of fixed32)
    num_restarts: u32, // Number of uint32_t entries in restart array
    // current is offset in data of current entry.  >= restarts if !Valid
    current: u32,
    restart_index: u32,
    key: Vec<u8>,
    value: Slice,
    err: Option<StatusError>,
}

impl BlockIter {
    pub fn new(block: &Block, comparator: Arc<dyn Comparator<Slice>>) -> Self {
        BlockIter {
            block_content: block.content.clone(),
            comparator,
            restarts: block.restart_offset,
            num_restarts: block.num_restarts(),
            current: block.restart_offset,
            restart_index: block.num_restarts(),
            key: Vec::new(),
            value: Default::default(),
            err: None,
        }
    }

    fn compare(&self, a: &Slice, b: &Slice) -> Ordering {
        self.comparator.compare(a, b)
    }

    // Return the offset in data_ just past the end of the current entry.
    fn next_entry_offset(&self) -> u32 {
        let offset = unsafe {
            self.value
                .data()
                .offset(self.value.size() as isize)
                .offset_from(self.block_content.data.as_ptr())
        };

        offset as u32
    }

    fn get_restart_point(&self, index: u32) -> u32 {
        assert!(index < self.num_restarts);
        let mut buf = self.block_content.data
            [self.restarts as usize + (mem::size_of::<u32>() * index as usize)..]
            .as_ref();
        buf.read_u32::<LittleEndian>().unwrap()
    }

    fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
        let ptr = unsafe {
            self.block_content
                .data
                .as_ptr()
                .offset(self.current as isize)
        };
        self.value = Slice::new(ptr, 0);
    }

    fn decode_entry(&mut self, offset: u32) -> Result<(u32, u32, u32, u32)> {
        if self.restarts - offset < 3 {
            return Err(StatusError::Corruption("bad entry in block".to_string()));
        }

        let mut data = self.block_content.data[offset as usize..].as_ref();
        let mut step = data.len();
        let (mut shared, mut non_shared, mut value_len) =
            (data[0] as u32, data[1] as u32, data[2] as u32);
        if shared | non_shared | value_len < 128 {
            // Fast path: all three values are encoded in one byte each
            step = 3
        } else {
            shared = data.decode_varint32()?;
            non_shared = data.decode_varint32()?;
            value_len = data.decode_varint32()?;
            step -= data.len();
        }
        let remain = self.restarts - offset - step as u32;
        if remain < non_shared + value_len {
            return Err(StatusError::Corruption("bad entry in block".to_string()));
        }

        Ok((shared, non_shared, value_len, step as u32))
    }

    fn parse_next_key(&mut self) -> bool {
        self.current = self.next_entry_offset();
        if self.current >= self.restarts {
            self.current = self.restarts;
            self.restart_index = self.num_restarts;
            return false;
        }

        if let Ok((shared, non_shared, value_len, step)) = self.decode_entry(self.current) {
            let offset = (self.current + step) as usize;
            let mut buf = self.block_content.data[offset..].as_ref();
            let non_share_key = buf.read_bytes(non_shared as usize).unwrap();
            self.key.truncate(shared as usize);
            self.key.extend_from_slice(non_share_key);
            self.value = buf.read_bytes(value_len as usize).unwrap().into();
            while self.restart_index + 1 < self.num_restarts
                && self.get_restart_point(self.restart_index + 1) < self.current
            {
                self.restart_index += 1;
            }
            true
        } else {
            self.corruption_error();
            false
        }
    }

    fn corruption_error(&mut self) {
        self.err
            .get_or_insert(StatusError::Corruption("bad entry in block".to_string()));
    }
}

impl Iterator for BlockIter {
    fn valid(&self) -> bool {
        self.current < self.restarts
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_next_key();
    }

    fn seek_to_last(&mut self) {
        self.seek_to_restart_point(self.num_restarts - 1);
        loop {
            if !self.parse_next_key() || self.next_entry_offset() >= self.restarts {
                break;
            }
        }
    }

    fn seek(&mut self, target: Slice) {
        // Binary search in restart array to find the last restart point
        // with a key < target
        let (mut left, mut right) = (0, self.num_restarts - 1);
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);
            if let Ok((shared, non_shared, _, step)) = self.decode_entry(region_offset) {
                if shared != 0 {
                    self.corruption_error();
                    return;
                }
                let offset = region_offset + step;
                let mut buf = self.block_content.data[offset as usize..].as_ref();
                let key = buf.read_bytes(non_shared as usize).unwrap();
                if self.compare(&key.into(), &target) == Ordering::Less {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            } else {
                self.corruption_error();
                return;
            }
        }

        // Linear search (within restart block) for first key >= target
        self.seek_to_restart_point(left);
        loop {
            if !self.parse_next_key() {
                return;
            }
            let k = self.key.as_slice().into();
            if self.compare(&k, &target) != Ordering::Less {
                return;
            }
        }
    }

    fn next(&mut self) {
        assert!(self.valid());
        self.parse_next_key();
    }

    fn prev(&mut self) {
        assert!(self.valid());
        let original = self.current;
        while self.get_restart_point(self.restart_index) >= original {
            if self.restart_index == 0 {
                // No more entries
                self.current = self.restarts;
                self.restart_index = self.num_restarts;
                return;
            }
            self.restart_index -= 1;
        }

        self.seek_to_restart_point(self.restart_index);
        loop {
            // Loop until end of current entry hits the start of original entry
            if !self.parse_next_key() || self.next_entry_offset() >= original {
                break;
            }
        }
    }

    fn key(&self) -> Slice {
        assert!(self.valid());
        Slice::new(self.key.as_ptr(), self.key.len())
    }

    fn value(&self) -> Slice {
        assert!(self.valid());
        self.value
    }

    fn status(&mut self) -> Result<()> {
        if self.err.is_some() {
            return Err(self.err.take().unwrap());
        }
        Ok(())
    }
}

pub struct BlockBuilder {
    comparator: Arc<dyn Comparator<Slice>>,
    block_restart_interval: u32,
    buffer: Vec<u8>,    // Destination buffer
    restarts: Vec<u32>, // Restart points
    counter: u32,       // Number of entries emitted since restart
    finished: bool,     // Has finish() been called?
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(comparator: Arc<dyn Comparator<Slice>>, block_restart_interval: u32) -> Self {
        assert!(block_restart_interval >= 1);
        let mut restarts = Vec::new();
        restarts.push(0);
        BlockBuilder {
            comparator,
            block_restart_interval,
            buffer: Vec::new(),
            restarts,
            counter: 0,
            finished: false,
            last_key: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.finished = false;
        self.last_key.clear();
    }

    // REQUIRES: finish() has not been called since the last call to reset().
    // REQUIRES: key is larger than any previously added key
    pub fn add(&mut self, key: Slice, value: Slice) {
        let last_key_piece = self.last_key.as_slice().into();
        assert!(!self.finished);
        assert!(self.counter <= self.block_restart_interval);
        if !self.buffer.is_empty() {
            assert_eq!(
                self.comparator.compare(&key, &last_key_piece),
                Ordering::Greater
            );
        }
        assert!(
            self.buffer.is_empty()
                || self.comparator.compare(&key, &last_key_piece) == Ordering::Greater
        );

        let mut shared = 0;
        if self.counter < self.block_restart_interval {
            let min_len = cmp::min(last_key_piece.size(), key.size());
            // See how much sharing to do with previous string
            while shared < min_len && last_key_piece.at(shared) == key.at(shared) {
                shared += 1;
            }
        } else {
            // Restart compression
            self.counter = 0;
            self.restarts.push(self.buffer.len() as u32);
        }
        // Add "<shared><non_shared><value_size>" to buffer_
        put_varint32(&mut self.buffer, shared as u32);
        put_varint32(&mut self.buffer, (key.size() - shared) as u32);
        put_varint32(&mut self.buffer, value.size() as u32);
        let non_share_key = key.as_ref()[shared..].as_ref();
        self.buffer.extend_from_slice(non_share_key);
        self.buffer.extend_from_slice(value.as_ref());
        // Update state
        self.counter += 1;
        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(non_share_key);
        assert_eq!(key.as_ref(), self.last_key.as_slice());
    }

    pub fn finish(&mut self) -> Slice {
        for restart in self.restarts.iter() {
            self.buffer.write_u32::<LittleEndian>(*restart).unwrap();
        }
        self.buffer
            .write_u32::<LittleEndian>(self.restarts.len() as u32)
            .unwrap();
        self.finished = true;
        self.buffer.as_slice().into()
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + self.restarts.len() * mem::size_of::<u32>() + mem::size_of::<u32>()
    }

    pub fn empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
