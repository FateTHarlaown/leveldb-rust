use crate::db::slice::Slice;
use crate::util::filter::FilterPolicy;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
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
            self.result
                .write_u32::<LittleEndian>(*offset as u32)
                .unwrap();
        }

        self.result
            .write_u32::<LittleEndian>(array_size as u32)
            .unwrap();
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
                let p = unsafe { self.data.as_ptr().add(start as usize) };
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

#[cfg(test)]
mod tests {
    use crate::db::slice::Slice;
    use crate::sstable::filter_block::{FilterBlockBuilder, FilterBlockReader, FILTER_BASE_LG};
    use crate::util::filter::FilterPolicy;
    use crate::util::hash::hash;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use std::rc::Rc;

    struct TestHashFilter {}
    impl FilterPolicy for TestHashFilter {
        fn name(&self) -> &'static str {
            "TestHashFilter"
        }

        fn create_filter(&self, keys: &Vec<Slice>, dst: &mut Vec<u8>) {
            for s in keys.iter() {
                let h = hash(s.as_ref(), 1);
                dst.write_u32::<LittleEndian>(h).unwrap();
            }
        }

        fn key_may_match(&self, key: Slice, filter: Slice) -> bool {
            let h = hash(key.as_ref(), 1);
            let mut buf = filter.as_ref();
            while let Ok(f) = buf.read_u32::<LittleEndian>() {
                if f == h {
                    return true;
                }
            }
            false
        }
    }

    #[test]
    fn test_empty_builder() {
        let policy = Rc::new(TestHashFilter {});
        let mut builder = FilterBlockBuilder::new(policy.clone());
        let block = builder.finish();
        assert_eq!(&[0, 0, 0, 0, FILTER_BASE_LG as u8], block.as_ref());

        let reader = FilterBlockReader::new(policy.clone(), block.as_ref());
        assert!(reader.key_may_match(0, "foo".as_bytes().into()));
        assert!(reader.key_may_match(100000, "foo".as_bytes().into()));
    }

    #[test]
    fn test_single_chunk() {
        let policy = Rc::new(TestHashFilter {});
        let mut builder = FilterBlockBuilder::new(policy.clone());
        builder.start_block(100);
        builder.add_key("foo".as_bytes().into());
        builder.add_key("bar".as_bytes().into());
        builder.add_key("box".as_bytes().into());
        builder.start_block(200);
        builder.add_key("box".as_bytes().into());
        builder.start_block(300);
        builder.add_key("hello".as_bytes().into());
        let block = builder.finish();
        let reader = FilterBlockReader::new(policy.clone(), block.as_ref());
        assert!(reader.key_may_match(100, "foo".as_bytes().into()));
        assert!(reader.key_may_match(100, "bar".as_bytes().into()));
        assert!(reader.key_may_match(100, "box".as_bytes().into()));
        assert!(reader.key_may_match(100, "hello".as_bytes().into()));
        assert!(reader.key_may_match(100, "foo".as_bytes().into()));
        assert!(!reader.key_may_match(100, "missing".as_bytes().into()));
        assert!(!reader.key_may_match(100, "other".as_bytes().into()));
    }

    #[test]
    fn test_multi_chunk() {
        let policy = Rc::new(TestHashFilter {});
        let mut builder = FilterBlockBuilder::new(policy.clone());

        // First filter
        builder.start_block(0);
        builder.add_key("foo".as_bytes().into());
        builder.start_block(2000);
        builder.add_key("bar".as_bytes().into());

        // Second filter
        builder.start_block(3100);
        builder.add_key("box".as_bytes().into());

        // Third filter is empty

        // Last filter
        builder.start_block(9000);
        builder.add_key("box".as_bytes().into());
        builder.add_key("hello".as_bytes().into());

        let block = builder.finish();
        let reader = FilterBlockReader::new(policy.clone(), block.as_ref());

        // Check first filter
        assert!(reader.key_may_match(0, "foo".as_bytes().into()));
        assert!(reader.key_may_match(2000, "bar".as_bytes().into()));
        assert!(!reader.key_may_match(0, "box".as_bytes().into()));
        assert!(!reader.key_may_match(0, "hello".as_bytes().into()));

        // Check second filter
        assert!(reader.key_may_match(3100, "box".as_bytes().into()));
        assert!(!reader.key_may_match(3100, "foo".as_bytes().into()));
        assert!(!reader.key_may_match(3100, "bar".as_bytes().into()));
        assert!(!reader.key_may_match(3100, "hello".as_bytes().into()));

        // Check third filter (empty)
        assert!(!reader.key_may_match(4100, "foo".as_bytes().into()));
        assert!(!reader.key_may_match(4100, "bar".as_bytes().into()));
        assert!(!reader.key_may_match(4100, "box".as_bytes().into()));
        assert!(!reader.key_may_match(4100, "hello".as_bytes().into()));

        // Check last filter
        assert!(reader.key_may_match(9000, "box".as_bytes().into()));
        assert!(reader.key_may_match(9000, "hello".as_bytes().into()));
        assert!(!reader.key_may_match(9000, "foo".as_bytes().into()));
        assert!(!reader.key_may_match(9000, "bar".as_bytes().into()));
    }
}
