use crate::db::slice::Slice;
use crate::util::filter::FilterPolicy;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use failure::_core::intrinsics::offset;
use std::rc::Rc;

const FILTER_BASE_LG: usize = 11;
const FILTER_BASE: usize = 1 << FILTER_BASE_LG;

pub struct FilterBlockBuilder {
    policy: Rc<dyn FilterPolicy>,
    keys: Vec<u8>,        // Flattened key contents
    start: Vec<usize>,    // Starting index in keys of each key
    result: Vec<u8>,      // Filter data computed so far
    tmp_keys: Vec<Slice>, // policy.create_filter() argument
    filter_offset: Vec<usize>,
}

impl FilterBlockBuilder {
    pub fn new(policy: Rc<dyn FilterPolicy>) -> Self {
        FilterBlockBuilder {
            policy,
            keys: Vec::new(),
            start: Vec::new(),
            result: Vec::new(),
            tmp_keys: Vec::new(),
            filter_offset: Vec::new(),
        }
    }

    pub fn add_key(&mut self, key: Slice) {
        self.start.push(self.keys.len());
        self.keys.extend_from_slice(key.as_ref());
    }

    pub fn start_block(&mut self, block_offset: usize) {
        let filter_index = block_offset / FILTER_BASE;
        assert!(filter_index >= self.filter_offset.len());
        while filter_index > self.filter_offset.len() {
            self.generate_filter();
        }
    }

    pub fn finish(&mut self) -> Slice {
        if !self.start.is_empty() {
            self.generate_filter();
        }
        let array_size = self.result.len();
        for offset in self.filter_offset.iter() {
            self.result.write_u32::<LittleEndian>(*offset as u32);
        }

        self.result.write_u32::<LittleEndian>(array_size as u32);
        self.result.push(FILTER_BASE_LG as u8);
        Slice::new(self.result.as_ptr(), self.result.len())
    }

    fn generate_filter(&mut self) {
        let num_keys = self.start.len();
        if num_keys == 0 {
            self.filter_offset.push(self.result.len());
        }

        // Make list of keys from flattened key structure
        self.start.push(self.keys.len()); // Simplify length computation
        self.tmp_keys.resize(num_keys, Default::default());
        for i in 0..num_keys {
            let (begin, end) = (self.start[i], self.start[i + 1]);
            let data = self.keys[begin..end].as_ptr();
            let length = end - begin;
            self.tmp_keys[i] = Slice::new(data, length);
        }

        // Generate filter for current set of keys and append to result_.
        self.filter_offset.push(self.result.len());
        self.policy.create_filter(&self.tmp_keys, &mut self.result);

        self.keys.clear();
        self.start.clear();
        self.tmp_keys.clear();
    }
}

pub struct FilterBlockReader<'a> {
    policy: Rc<dyn FilterPolicy>,
    data: &'a [u8],
    offset: usize,
    num: usize,
    base_lg: usize,
}

impl<'a> FilterBlockReader<'a> {
    pub fn new(policy: Rc<dyn FilterPolicy>, contents: &'a [u8]) -> Self {
        let mut reader = FilterBlockReader {
            policy,
            data: Default::default(),
            offset: 0,
            num: 0,
            base_lg: 0,
        };
        let n = contents.len();
        if n < 5 {
            return reader;
        }
        reader.base_lg = contents[n - 1] as usize;
        let last_word = contents[n - 5..]
            .as_ref()
            .read_u32::<LittleEndian>()
            .unwrap() as usize;
        if last_word > n - 5 {
            return reader;
        }

        reader.offset = last_word;
        reader.data = contents;
        reader.num = (n - 5 - last_word) / 4;
        reader
    }

    pub fn key_may_match(&self, block_offset: usize, key: Slice) -> bool {
        let index = block_offset >> self.base_lg;
        if index < self.num {
            let start = self.data[(self.offset + index * 4)..]
                .as_ref()
                .read_u32::<LittleEndian>()
                .unwrap();
            let limit = self.data[(self.offset + index * 4 + 4)..]
                .as_ref()
                .read_u32::<LittleEndian>()
                .unwrap();
            if start <= limit && limit <= self.offset as u32 {
                let n = (limit - start) as usize;
                let p = unsafe { self.data.as_ptr().add(n) };
                let filter = Slice::new(p, n);
                return self.policy.key_may_match(key, filter);
            } else if start == limit {
                // Empty filters do not match any keys
                return false;
            }
        }

        true // Errors are treated as potential matches
    }
}
