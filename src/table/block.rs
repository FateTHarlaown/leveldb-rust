use crate::db::error::{Result, StatusError};
use crate::db::slice::Slice;
use crate::db::Iterator;
use crate::table::format::BlockContent;
use crate::util::buffer::BufferReader;
use crate::util::cmp::Comparator;
use crate::util::coding::DecodeVarint;
use byteorder::{LittleEndian, ReadBytesExt};
use std::cmp::Ordering;
use std::mem;
use std::rc::Rc;

pub struct Block {
    content: BlockContent,
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
                content,
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

    pub fn new_iterator(&self, comparator: Rc<dyn Comparator<Slice>>) -> BlockIter {
       BlockIter::new(self, comparator)
    }
}

pub struct BlockIter<'a> {
    block_data: &'a [u8], // underlying block contents
    comparator: Rc<dyn Comparator<Slice>>,
    restarts: u32,     // Offset of restart array (list of fixed32)
    num_restarts: u32, // Number of uint32_t entries in restart array
    // current is offset in data of current entry.  >= restarts if !Valid
    current: u32,
    restart_index: u32,
    key: Vec<u8>,
    value: Slice,
    err: Option<StatusError>,
}

impl<'a> BlockIter<'a> {
    pub fn new(block: &'a Block, comparator: Rc<dyn Comparator<Slice>>) -> Self {
        BlockIter {
            block_data: block.data(),
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
        self.current + self.value.size() as u32
    }

    fn get_restart_point(&self, index: u32) -> u32 {
        assert!(index < self.num_restarts);
        let mut buf = self.block_data
            [self.restarts as usize + (mem::size_of::<u32>() * index as usize)..]
            .as_ref();
        buf.read_u32::<LittleEndian>().unwrap()
    }

    fn seek_to_restart_point(&mut self, index: u32) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
        self.value = Default::default();
    }

    fn decode_entry(&mut self, offset: u32) -> Result<(u32, u32, u32, u32)> {
        if self.restarts - offset < 3 {
            return Err(StatusError::Corruption("bad entry in block".to_string()));
        }

        let mut data = self.block_data[offset as usize..].as_ref();
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
        }
        step -= data.len();
        let remain = self.restarts - offset - step as u32;
        if remain < shared + non_shared {
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
            let mut buf = self.block_data[offset..].as_ref();
            let non_share_key = buf.read_bytes(non_shared as usize).unwrap();
            self.key.resize(shared as usize, 0);
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
        self.err.get_or_insert(StatusError::Corruption("bad entry in block".to_string()));
    }
}

impl Iterator for BlockIter<'_> {
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
            if let Ok((non_shared, shared, _, step)) = self.decode_entry(region_offset) {
                if shared != 0 {
                    self.corruption_error();
                    return;
                }
                let offset = region_offset + step;
                let mut buf = self.block_data[offset as usize..].as_ref();
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
            if !self.parse_next_key() || self.next_entry_offset() < original {
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
        if let Some(_) = &self.err {
            return Err(self.err.take().unwrap())
        }
        Ok(())
    }
}
