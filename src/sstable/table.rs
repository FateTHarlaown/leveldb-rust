use crate::db::error::{Result, StatusError};
use crate::db::option::Options;
use crate::db::slice::Slice;
use crate::db::{
    Iterator, RandomAccessFile, ReadOption, WritableFile, NO_COMPRESSION, SNAPPY_COMPRESSION,
};
use crate::sstable::block::{Block, BlockBuilder, BlockIter};
use crate::sstable::filter_block::{FilterBlockBuilder, FilterBlockReader};
use crate::sstable::format::{
    BlockContent, BlockHandle, Footer, BLOCK_TRAILER_SIZE, FOOTER_ENCODED_LENGTH,
};
use crate::sstable::two_level_iterator::{BlockIterBuilder, TwoLevelIterator};
use crate::util::cmp::{BitWiseComparator, Comparator};
use byteorder::{LittleEndian, WriteBytesExt};
use snap::write::FrameEncoder;
use std::cmp::Ordering;
use std::io;
use std::io::Write;
use std::mem;
use std::sync::Arc;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
pub struct Table<R: RandomAccessFile> {
    file: R,
    cache_id: u64,
    options: Arc<Options>,
    metaindex_handle: BlockHandle,
    index_block: Block,
    filter_block_data: Option<BlockContent>,
}

impl<R: RandomAccessFile> Table<R> {
    // Attempt to open the table that is stored in bytes [0..file_size)
    // of "file", and read the metadata entries necessary to allow
    // retrieving data from the table.If successful, returns ok and sets "table" to the newly opened
    // table.
    pub fn open(options: Arc<Options>, mut file: R, size: u64) -> Result<Self> {
        if FOOTER_ENCODED_LENGTH > size as usize {
            return Err(StatusError::Corruption(
                "file is too short to be an sstable".to_string(),
            ));
        }

        let mut scatch = Vec::new();
        let footer_data = file.read(
            size as usize - FOOTER_ENCODED_LENGTH,
            FOOTER_ENCODED_LENGTH,
            &mut scatch,
        )?;
        let mut footer = Footer::default();
        footer.decoded_from(footer_data)?;

        let mut read_option = ReadOption::default();
        read_option.verify_checksum = options.paranoid_checks;
        let index_block_content = BlockContent::read_block_from_file(
            &mut file,
            footer.index_handle().clone(),
            &read_option,
        )?;
        // We've successfully read the footer and the index block: we're
        // ready to serve requests.
        let index_block = Block::from_content(index_block_content)?;
        let cache_id = if let Some(ref cache) = options.block_cache {
            cache.new_id()
        } else {
            0
        };

        let mut table = Table {
            options,
            file,
            metaindex_handle: footer.meta_index_handle().clone(),
            index_block,
            cache_id,
            filter_block_data: None,
        };
        // We've successfully read the footer and the index block: we're
        // ready to serve requests.
        // Do not propagate errors since meta info is not needed for operation
        table.read_meta(&footer);
        Ok(table)
    }

    fn read_meta(&mut self, footer: &Footer) -> Result<()> {
        if let Some(ref policy) = self.options.filter_policy {
            let mut read_option = ReadOption::default();
            read_option.verify_checksum = self.options.paranoid_checks;
            let meta_block_content = BlockContent::read_block_from_file(
                &self.file,
                *footer.meta_index_handle(),
                &read_option,
            )?;
            let meta_block = Block::from_content(meta_block_content)?;
            let mut iter = meta_block.new_iterator(Arc::new(BitWiseComparator {}));
            let mut key = Vec::new();
            key.extend_from_slice("filter".as_bytes());
            key.extend_from_slice(policy.name().as_bytes());
            let cmp = BitWiseComparator {};
            iter.seek(key.as_slice().into());
            if iter.valid() && cmp.compare(&key.as_slice().into(), &iter.key()) == Ordering::Equal {
                let mut handle = BlockHandle::default();
                handle.decode_from(&mut iter.value().as_ref())?;
                let filter_block_content =
                    BlockContent::read_block_from_file(&self.file, handle, &read_option)?;
                self.filter_block_data.get_or_insert(filter_block_content);
            }
        }

        Ok(())
    }

    fn block_iter_from_index(
        &self,
        read_option: &ReadOption,
        index_value: Slice,
    ) -> Result<BlockIter> {
        let mut block_handle: BlockHandle = Default::default();
        block_handle.decode_from(&mut index_value.as_ref())?;

        if let Some(ref cache) = self.options.block_cache {
            let mut cache_key_buffer = Vec::new();
            cache_key_buffer.write_u64::<LittleEndian>(self.cache_id)?;
            cache_key_buffer.write_u64::<LittleEndian>(block_handle.offset())?;
            if let Some(block) = cache.look_up(&cache_key_buffer) {
                Ok(block.new_iterator(self.options.comparator.clone()))
            } else {
                let content =
                    BlockContent::read_block_from_file(&self.file, block_handle, read_option)?;
                let cache_able = content.cachable;
                let block = Block::from_content(content)?;
                let iter = block.new_iterator(self.options.comparator.clone());
                let charge = block.data().len() as u64;
                if cache_able {
                    cache.insert(cache_key_buffer, block, charge);
                }
                Ok(iter)
            }
        } else {
            let content =
                BlockContent::read_block_from_file(&self.file, block_handle, read_option)?;
            let block = Block::from_content(content)?;
            Ok(block.new_iterator(self.options.comparator.clone()))
        }
    }

    pub(crate) fn new_iterator(
        table: Arc<Table<R>>,
        option: &ReadOption,
    ) -> TwoLevelIterator<BlockIter, TableBlockIterBuilder<R>> {
        let index_iter = table
            .index_block
            .new_iterator(table.options.comparator.clone());
        let block_iter_builder = TableBlockIterBuilder { table };
        TwoLevelIterator::new(index_iter, block_iter_builder, option.clone())
    }

    // Calls callback with the entry found after a call
    // to Seek(key).  May not make such a call if filter policy says
    // that key is not present.
    pub fn internal_get<CB>(
        &self,
        read_option: &ReadOption,
        key: Slice,
        mut callback: CB,
    ) -> Result<()>
    where
        CB: FnMut(Slice, Slice),
    {
        let mut index_iter = self
            .index_block
            .new_iterator(self.options.comparator.clone());
        index_iter.seek(key);
        if index_iter.valid() {
            let mut handle = BlockHandle::default();
            handle.decode_from(&mut index_iter.value().as_ref())?;
            if let Some(ref filter_data) = self.filter_block_data {
                // use filter to check if this key exists.
                // if the filter_date if Some, we must have filter policy.
                let policy = self.options.filter_policy.as_ref().unwrap().clone();
                let filter = FilterBlockReader::new(policy, filter_data.data.as_slice());
                if !filter.key_may_match(handle.offset() as usize, key) {
                    // not found
                    return Ok(());
                }
            }

            let mut block_iter = self.block_iter_from_index(read_option, index_iter.value())?;
            block_iter.seek(key);
            if block_iter.valid() {
                callback(block_iter.key(), block_iter.value());
                Ok(())
            } else {
                block_iter.status()
            }
        } else {
            index_iter.status()
        }
    }

    // Given a key, return an approximate byte offset in the file where
    // the data for that key begins (or would begin if the key were
    // present in the file).  The returned value is in terms of file
    // bytes, and so includes effects like compression of the underlying data.
    // E.g., the approximate offset of the last key in the table will
    // be close to the file length.
    pub fn approximate_offset_of(&self, key: Slice) -> u64 {
        let mut index_iter = self
            .index_block
            .new_iterator(self.options.comparator.clone());
        index_iter.seek(key);
        if index_iter.valid() {
            let mut handle = BlockHandle::default();
            if handle.decode_from(&mut index_iter.value().as_ref()).is_ok() {
                handle.offset()
            } else {
                // key is past the last key in the file.  Approximate the offset
                // by returning the offset of the metaindex block (which is
                // right near the end of the file).
                self.metaindex_handle.offset()
            }
        } else {
            // key is past the last key in the file.  Approximate the offset
            // by returning the offset of the metaindex block (which is
            // right near the end of the file).
            self.metaindex_handle.offset()
        }
    }
}

pub(crate) struct TableBlockIterBuilder<R: RandomAccessFile> {
    table: Arc<Table<R>>,
}

impl<R: RandomAccessFile> BlockIterBuilder for TableBlockIterBuilder<R> {
    type Iter = BlockIter;
    fn build(&self, option: &ReadOption, index_val: Slice) -> Result<Self::Iter> {
        self.table.block_iter_from_index(option, index_val)
    }
}

pub struct TableBuilder<W: WritableFile> {
    options: Arc<Options>,
    file: W,
    offset: u64,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    last_key: Vec<u8>,
    num_entries: u64,
    closed: bool, // Either Finish() or Abandon() has been called.
    filter_block: Option<FilterBlockBuilder>,

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    //
    // Invariant: pending_index_entry is true only if data_block is empty.
    pending_index_entry: bool,
    pending_handle: BlockHandle,
    compressed_out: Vec<u8>,
}

impl<W: WritableFile> TableBuilder<W> {
    pub fn new(options: Arc<Options>, file: W) -> Self {
        let data_block =
            BlockBuilder::new(options.comparator.clone(), options.block_restart_interval);
        let index_block = BlockBuilder::new(options.comparator.clone(), 1);
        let filter_block = if let Some(ref filter) = options.filter_policy {
            let mut filter_block_builder = FilterBlockBuilder::new(filter.clone());
            filter_block_builder.start_block(0);
            Some(filter_block_builder)
        } else {
            None
        };

        TableBuilder {
            options,
            file,
            offset: 0,
            data_block,
            index_block,
            last_key: Vec::new(),
            num_entries: 0,
            closed: false,
            filter_block,
            pending_index_entry: false,
            pending_handle: BlockHandle::default(),
            compressed_out: Vec::new(),
        }
    }

    pub fn add(&mut self, key: Slice, value: Slice) -> Result<()> {
        assert!(!self.closed);

        if self.num_entries > 0 {
            assert_eq!(
                self.options
                    .comparator
                    .compare(&key, &self.last_key.as_slice().into()),
                Ordering::Greater
            );
        }

        if self.pending_index_entry {
            assert!(self.data_block.empty());
            self.options
                .comparator
                .find_shortest_separator(&mut self.last_key, key);
            let mut handle_encoding = Vec::new();
            self.pending_handle.encode_to(&mut handle_encoding);
            self.index_block.add(
                self.last_key.as_slice().into(),
                handle_encoding.as_slice().into(),
            );
            self.pending_index_entry = false;
        }

        if let Some(ref mut filter) = self.filter_block {
            filter.add_key(key);
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key.as_ref());
        self.num_entries += 1;
        self.data_block.add(key, value);

        let estimated_block_size = self.data_block.current_size_estimate();
        if estimated_block_size >= self.options.block_size {
            self.flush()?;
        }

        Ok(())
    }

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.  Most clients should not need to use this method.
    // REQUIRES: Finish(), Abandon() have not been called
    pub fn flush(&mut self) -> Result<()> {
        assert!(!self.closed);

        if self.data_block.empty() {
            return Ok(());
        }

        assert!(!self.pending_index_entry);

        write_block(
            &mut self.file,
            &mut self.data_block,
            &mut self.pending_handle,
            self.options.compression_type,
            &mut self.compressed_out,
            &mut self.offset,
        )?;
        self.pending_index_entry = true;
        self.file.flush()?;

        if let Some(ref mut filter) = self.filter_block {
            filter.start_block(self.offset as usize);
        }

        Ok(())
    }

    // Finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: Finish(), Abandon() have not been called
    pub fn finish(&mut self, sync: bool) -> Result<()> {
        self.flush()?;
        assert!(!self.closed);
        self.closed = true;

        let mut filter_block_handle: BlockHandle = Default::default();
        let mut metaindex_block_handle: BlockHandle = Default::default();
        let mut index_block_handle: BlockHandle = Default::default();
        // Write filter block
        if let Some(ref mut filter) = self.filter_block {
            let block_content = filter.finish();
            write_raw_block(
                &mut self.file,
                block_content,
                self.options.compression_type,
                &mut filter_block_handle,
                &mut self.offset,
            )?;
        }

        // Write metaindex block
        let mut meta_index_block = BlockBuilder::new(
            self.options.comparator.clone(),
            self.options.block_restart_interval,
        );
        if self.filter_block.is_some() {
            let mut key = Vec::from("filter".as_bytes());
            if let Some(ref policy) = self.options.filter_policy {
                key.extend_from_slice(policy.name().as_bytes());
                let mut handle_encoding = Vec::new();
                filter_block_handle.encode_to(&mut handle_encoding);
                meta_index_block.add(key.as_slice().into(), handle_encoding.as_slice().into());
            }
        }
        write_block(
            &mut self.file,
            &mut meta_index_block,
            &mut metaindex_block_handle,
            self.options.compression_type,
            &mut self.compressed_out,
            &mut self.offset,
        )?;

        // Write index block
        if self.pending_index_entry {
            self.options
                .comparator
                .find_short_successor(&mut self.last_key);
            let mut handle_encoding = Vec::new();
            self.pending_handle.encode_to(&mut handle_encoding);
            self.index_block.add(
                self.last_key.as_slice().into(),
                handle_encoding.as_slice().into(),
            );
            self.pending_index_entry = false;
        }
        write_block(
            &mut self.file,
            &mut self.index_block,
            &mut index_block_handle,
            self.options.compression_type,
            &mut self.compressed_out,
            &mut self.offset,
        )?;

        // Write footer
        let mut footer = Footer::default();
        footer.set_meta_index_handle(&metaindex_block_handle);
        footer.set_index_handle(&index_block_handle);
        let mut buf = Vec::new();
        footer.encode_to(&mut buf);
        self.file.append(buf.as_slice())?;
        self.offset += buf.len() as u64;

        if sync {
            self.file.sync()?;
            self.file.close()?;
        }

        Ok(())
    }

    pub fn abandon(&mut self) {
        assert!(!self.closed);
        self.closed = true;
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    pub fn file_size(&self) -> u64 {
        self.offset
    }
}

fn write_block<W: WritableFile>(
    file: &mut W,
    block: &mut BlockBuilder,
    handle: &mut BlockHandle,
    compress_type: u8,
    compressed_out: &mut Vec<u8>,
    offset: &mut u64,
) -> Result<()> {
    let raw = block.finish();

    let mut write_compress_type = compress_type;
    let block_content = match write_compress_type {
        NO_COMPRESSION => raw,
        SNAPPY_COMPRESSION => {
            compressed_out.clear();
            {
                let mut encoder = FrameEncoder::new(&mut (*compressed_out));
                io::copy(&mut raw.as_ref(), &mut encoder)?;
            }
            if compressed_out.len() < raw.size() - (raw.size() / 8) {
                compressed_out.as_slice().into()
            } else {
                // Snappy not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                write_compress_type = NO_COMPRESSION;
                raw
            }
        }

        _ => panic!("the compress type invalid"),
    };

    write_raw_block(file, block_content, write_compress_type, handle, offset)?;
    block.reset();
    Ok(())
}

fn write_raw_block<W: WritableFile>(
    file: &mut W,
    block_content: Slice,
    compress_type: u8,
    handle: &mut BlockHandle,
    offset: &mut u64,
) -> Result<()> {
    handle.set_offset(*offset);
    handle.set_size(block_content.size() as u64);
    file.append(block_content.as_ref())?;

    let mut trailer: [u8; BLOCK_TRAILER_SIZE] = [0; BLOCK_TRAILER_SIZE];
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(block_content.as_ref());
    hasher.update(&[compress_type]);
    let checksum = hasher.finalize();
    let mut buf = trailer.as_mut();
    buf.write_u8(compress_type).unwrap();
    buf.write_u32::<LittleEndian>(checksum).unwrap();
    file.append(trailer.as_ref())?;
    *offset += (block_content.size() + BLOCK_TRAILER_SIZE) as u64;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::dbformat::{
        append_internal_key, parse_internal_key, InternalKeyComparator, ParsedInternalKey,
        MAX_SEQUENCE_NUMBER, TYPE_VALUE,
    };
    use crate::db::error::{Result, StatusError};
    use crate::db::memtable::{get_length_prefixed_slice, MemTable};
    use crate::db::option::{Options, NO_COMPRESSION};
    use crate::db::slice::Slice;
    use crate::db::write_batch::WriteBatch;
    use crate::db::Iterator as MyIter;
    use crate::db::{RandomAccessFile, ReadOption, WritableFile};
    use crate::env::Env;
    use crate::sstable::block::{Block, BlockBuilder};
    use crate::sstable::format::BlockContent;
    use crate::sstable::table::{Table, TableBuilder};
    use crate::util::cmp::{BitWiseComparator, Comparator};
    use crate::util::testutil::{compressible_vec_str, random_key, random_vec_str};
    use rand::prelude::ThreadRng;
    use rand::{thread_rng, Rng};
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::rc::Rc;
    use std::slice::Iter;
    use std::sync::Arc;

    fn reverse(key: Slice) -> Vec<u8> {
        let mut ret = Vec::from(key.as_ref());
        ret.reverse();
        ret
    }

    struct ReverseKeyComparator {}

    impl Comparator<Slice> for ReverseKeyComparator {
        fn compare<'a>(&self, left: &'a Slice, right: &'a Slice) -> Ordering {
            BitWiseComparator {}.compare(
                &reverse(*left).as_slice().into(),
                &reverse(*right).as_slice().into(),
            )
        }

        fn name(&self) -> &'static str {
            "leveldb.ReverseBytewiseComparator"
        }

        fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: Slice) {
            let mut s = reverse(start.as_slice().into());
            let l = reverse(limit);
            BitWiseComparator {}.find_shortest_separator(&mut s, l.as_slice().into());
            *start = reverse(s.as_slice().into());
        }

        fn find_short_successor(&self, key: &mut Vec<u8>) {
            let s = reverse(key.as_slice().into());
            BitWiseComparator {}.find_short_successor(key);
            *key = reverse(s.as_slice().into());
        }
    }

    fn increment(cmp: Arc<dyn Comparator<Slice>>, key: &mut Vec<u8>) {
        let bcmp = BitWiseComparator {};
        if cmp.name() == bcmp.name() {
            key.push(0);
        } else {
            assert_eq!(cmp.name(), ReverseKeyComparator {}.name());
            let mut rev = reverse(key.as_slice().into());
            rev.push(0);
            *key = rev;
        }
    }

    #[derive(Clone)]
    struct MemFile {
        content: Rc<RefCell<Vec<u8>>>,
    }

    impl MemFile {
        pub fn new() -> Self {
            MemFile {
                content: Rc::new(RefCell::new(Vec::new())),
            }
        }

        pub fn clear(&self) {
            self.content.borrow_mut().clear();
        }
    }

    impl WritableFile for MemFile {
        fn append(&mut self, data: &[u8]) -> Result<()> {
            self.content.borrow_mut().extend_from_slice(data);
            Ok(())
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn sync(&mut self) -> Result<()> {
            Ok(())
        }
    }

    impl RandomAccessFile for MemFile {
        fn read(&self, offset: usize, n: usize, scratch: &mut Vec<u8>) -> Result<Slice> {
            let content = self.content.borrow();
            scratch.clear();
            if offset >= content.len() {
                return Err(StatusError::InvalidArgument(
                    "invalid read offset".to_string(),
                ));
            }

            let n = if n + offset > content.len() {
                content.len() - offset
            } else {
                n
            };

            let data = content[offset..(offset + n)].as_ref();
            scratch.extend_from_slice(data);
            Ok(scratch.as_slice().into())
        }
    }

    type KVBytes = Vec<u8>;

    trait MaterialBuilder {
        fn finish(&mut self, options: Arc<Options>, kvs: &mut Vec<(KVBytes, KVBytes)>);
        fn new_iterator<'a>(&'a self) -> Box<dyn MyIter + 'a>;
        fn add(&mut self, key: Vec<u8>, val: Vec<u8>);
    }

    struct ConstructorData {
        data: Vec<(KVBytes, KVBytes)>,
        comparator: Arc<dyn Comparator<Slice>>,
    }

    impl ConstructorData {
        pub fn get_sorted_unique_data(&mut self) -> Vec<(KVBytes, KVBytes)> {
            let cmp = self.comparator.clone();
            self.data
                .sort_by(|a, b| cmp.compare(&a.0.as_slice().into(), &b.0.as_slice().into()));
            let mut res: Vec<(KVBytes, KVBytes)> = Vec::new();
            for (k, v) in self.data.iter() {
                if let Some((last_k, _)) = res.last() {
                    if self
                        .comparator
                        .compare(&last_k.as_slice().into(), &k.as_slice().into())
                        == Ordering::Equal
                    {
                        continue;
                    }
                }
                res.push((k.clone(), v.clone()))
            }
            res
        }

        pub fn add(&mut self, key: Vec<u8>, val: Vec<u8>) {
            self.data.push((key, val));
        }
    }

    struct BlockConstructor {
        pub constructor_data: ConstructorData,
        comparator: Arc<dyn Comparator<Slice>>,
        block: Option<Block>,
    }

    impl BlockConstructor {
        pub fn new(comparator: Arc<dyn Comparator<Slice>>) -> Self {
            BlockConstructor {
                comparator: comparator.clone(),
                block: None,
                constructor_data: ConstructorData {
                    data: Vec::new(),
                    comparator,
                },
            }
        }
    }

    impl MaterialBuilder for BlockConstructor {
        fn finish(&mut self, options: Arc<Options>, kvs: &mut Vec<(KVBytes, KVBytes)>) {
            let data = self.constructor_data.get_sorted_unique_data();
            let mut builder =
                BlockBuilder::new(options.comparator.clone(), options.block_restart_interval);
            for (key, val) in data.iter() {
                builder.add(key.as_slice().into(), val.as_slice().into());
            }
            *kvs = data;
            let block_data = builder.finish();
            let mut content = BlockContent::default();
            content.data.extend_from_slice(block_data.as_ref());
            content.cachable = false;
            content.heap_allocted = true;
            let block = Block::from_content(content).unwrap();
            self.block = Some(block);
        }

        fn new_iterator<'a>(&'a self) -> Box<dyn MyIter + 'a> {
            let block = self.block.as_ref().unwrap();
            Box::new(block.new_iterator(self.comparator.clone()))
        }

        fn add(&mut self, key: Vec<u8>, val: Vec<u8>) {
            self.constructor_data.add(key, val);
        }
    }

    struct TableConstructor {
        pub constructor_data: ConstructorData,
        file: MemFile,
        table: Option<Arc<Table<MemFile>>>,
    }

    impl TableConstructor {
        pub fn new(comparator: Arc<dyn Comparator<Slice>>) -> Self {
            TableConstructor {
                file: MemFile::new(),
                table: None,
                constructor_data: ConstructorData {
                    comparator,
                    data: Vec::new(),
                },
            }
        }

        pub fn approximate_offset_of(&self, key: Slice) -> u64 {
            let table = self.table.as_ref().unwrap();
            table.approximate_offset_of(key)
        }
    }

    impl MaterialBuilder for TableConstructor {
        fn finish(&mut self, options: Arc<Options>, kvs: &mut Vec<(KVBytes, KVBytes)>) {
            let data = self.constructor_data.get_sorted_unique_data();
            self.file.clear();
            self.table = None;
            let mut builder = TableBuilder::new(options.clone(), self.file.clone());
            for (key, val) in data.iter() {
                builder
                    .add(key.as_slice().into(), val.as_slice().into())
                    .unwrap();
            }
            *kvs = data;
            builder.finish(true).unwrap();
            assert_eq!(
                self.file.content.borrow().len(),
                builder.file_size() as usize
            );
            let mut table_options = Options::default();
            table_options.comparator = options.comparator.clone();
            let table = Table::open(
                Arc::new(table_options),
                self.file.clone(),
                self.file.content.borrow().len() as u64,
            )
            .unwrap();
            self.table = Some(Arc::new(table));
        }

        fn new_iterator<'a>(&'a self) -> Box<dyn MyIter + 'a> {
            let table = self.table.as_ref().unwrap();
            let read_option = ReadOption::default();
            Box::new(Table::new_iterator(table.clone(), &read_option))
        }

        fn add(&mut self, key: Vec<u8>, val: Vec<u8>) {
            self.constructor_data.add(key, val);
        }
    }

    struct KeyConvertingIterator<'a> {
        inner_iter: Box<dyn MyIter + 'a>,
        status: Option<StatusError>,
    }

    // A helper class that converts internal format keys into user keys
    impl<'a> MyIter for KeyConvertingIterator<'a> {
        fn valid(&self) -> bool {
            self.inner_iter.valid()
        }

        fn seek_to_first(&mut self) {
            self.inner_iter.seek_to_first()
        }

        fn seek_to_last(&mut self) {
            self.inner_iter.seek_to_last()
        }

        fn seek(&mut self, target: Slice) {
            let ikey = ParsedInternalKey {
                user_key: target,
                sequence: MAX_SEQUENCE_NUMBER,
                val_type: TYPE_VALUE,
            };
            let mut encoded = Vec::new();
            append_internal_key(&mut encoded, &&ikey);
            self.inner_iter.seek(encoded.as_slice().into());
        }

        fn next(&mut self) {
            self.inner_iter.next();
        }

        fn prev(&mut self) {
            self.inner_iter.prev();
        }

        fn key(&self) -> Slice {
            let k = self.inner_iter.key();
            let mut internal_key = ParsedInternalKey::default();
            if !parse_internal_key(k, &mut internal_key) {
                b"corrupted key".as_ref().into()
            } else {
                internal_key.user_key
            }
        }

        fn value(&self) -> Slice {
            self.inner_iter.value()
        }

        fn status(&mut self) -> Result<()> {
            if self.status.is_none() {
                self.inner_iter.status()
            } else {
                Err(self.status.take().unwrap())
            }
        }
    }

    struct MemTableConstructor {
        pub constructor_data: ConstructorData,
        memtable: Option<MemTable>,
        internal_key_comparator: InternalKeyComparator,
    }

    impl MemTableConstructor {
        pub fn new(cmp: Arc<dyn Comparator<Slice>>) -> Self {
            MemTableConstructor {
                constructor_data: ConstructorData {
                    data: Vec::new(),
                    comparator: cmp.clone(),
                },
                internal_key_comparator: InternalKeyComparator::new(cmp),
                memtable: None,
            }
        }
    }

    impl MaterialBuilder for MemTableConstructor {
        fn finish(&mut self, options: Arc<Options>, kvs: &mut Vec<(KVBytes, KVBytes)>) {
            let data = self.constructor_data.get_sorted_unique_data();
            self.memtable = None;
            let mem = MemTable::new(self.internal_key_comparator.clone());
            for (i, kv) in data.iter().enumerate() {
                mem.add(
                    (i + 1) as u64,
                    TYPE_VALUE,
                    &kv.0.as_slice().into(),
                    &kv.1.as_slice().into(),
                );
            }
            self.memtable = Some(mem);
            *kvs = data;
        }

        fn new_iterator<'a>(&'a self) -> Box<dyn MyIter + 'a> {
            let mem_iter = self.memtable.as_ref().unwrap().iter();
            let convert_iter = KeyConvertingIterator {
                inner_iter: mem_iter,
                status: None,
            };
            Box::new(convert_iter)
        }

        fn add(&mut self, key: Vec<u8>, val: Vec<u8>) {
            self.constructor_data.add(key, val);
        }
    }

    fn inc_until(mut n: i64, bound: i64) -> i64 {
        n += 1;
        if n > bound {
            bound
        } else {
            n
        }
    }

    // TODO: add db test constructor

    #[derive(Copy, Clone)]
    enum TestType {
        TableTest,
        BlockTest,
        MemTableTest,
        DBTest,
    }

    struct HarnessTester {
        options: Arc<Options>,
        constructor: Option<Box<dyn MaterialBuilder>>,
    }

    impl HarnessTester {
        pub fn new() -> Self {
            HarnessTester {
                options: Arc::new(Options::default()),
                constructor: None,
            }
        }

        pub fn init(&mut self, test_type: TestType, reverse_compare: bool, restart_interval: u32) {
            let mut options = Options::default();
            options.block_restart_interval = restart_interval;
            options.block_size = 256;
            if reverse_compare {
                options.comparator = Arc::new(ReverseKeyComparator {});
            }
            match test_type {
                TestType::BlockTest => {
                    self.constructor =
                        Some(Box::new(BlockConstructor::new(options.comparator.clone())))
                }
                TestType::TableTest => {
                    self.constructor =
                        Some(Box::new(TableConstructor::new(options.comparator.clone())))
                }
                TestType::MemTableTest => {
                    self.constructor = Some(Box::new(MemTableConstructor::new(
                        options.comparator.clone(),
                    )))
                }
                TestType::DBTest => unimplemented!(),
            }
            self.options = Arc::new(options);
        }

        pub fn add(&mut self, key: KVBytes, val: KVBytes) {
            let constructor = self.constructor.as_mut().unwrap();
            constructor.add(key, val);
        }

        pub fn test(&mut self) {
            let mut kvs = Vec::new();
            let constructor = self.constructor.as_mut().unwrap();
            constructor.finish(self.options.clone(), &mut kvs);

            self.test_forward_scan(&kvs);
            self.test_backward_scan(&kvs);
            self.test_random_access(&kvs);
        }

        pub fn test_forward_scan(&self, kvs: &Vec<(KVBytes, KVBytes)>) {
            let constructor = self.constructor.as_ref().unwrap();
            let mut iter = constructor.new_iterator();
            iter.seek_to_first();
            for (key, val) in kvs.iter() {
                assert!(iter.valid());
                assert_eq!(
                    Self::kv_string(key.as_slice(), val.as_slice()),
                    Self::kv_string(iter.key().as_ref(), iter.value().as_ref())
                );
                iter.next();
            }
        }

        pub fn test_backward_scan(&self, kvs: &Vec<(KVBytes, KVBytes)>) {
            let constructor = self.constructor.as_ref().unwrap();
            let mut iter = constructor.new_iterator();
            iter.seek_to_last();
            for (key, val) in kvs.iter().rev() {
                assert!(iter.valid());
                assert_eq!(
                    Self::kv_string(key.as_slice(), val.as_slice()),
                    Self::kv_string(iter.key().as_ref(), iter.value().as_ref())
                );
                iter.prev();
            }
        }

        pub fn test_random_access(&self, kvs: &Vec<(KVBytes, KVBytes)>) {
            let constructor = self.constructor.as_ref().unwrap();
            let mut iter = constructor.new_iterator();
            let mut model_pos: i64 = 0;
            let n = kvs.len() as i64;
            assert!(!iter.valid());
            let mut rand = thread_rng();
            for _ in 0..200 {
                let toss = rand.gen_range(0, 5);
                match toss {
                    0 => {
                        if iter.valid() {
                            iter.next();
                            model_pos = inc_until(model_pos, n);
                            self.check_iter(&iter, &kvs, model_pos);
                        }
                    }

                    1 => {
                        iter.seek_to_first();
                        //assert!(iter.valid());
                        model_pos = 0;
                        self.check_iter(&iter, &kvs, model_pos);
                    }

                    2 => {
                        let key = self.pick_random_key(&mut rand, &kvs);
                        iter.seek(key.as_slice().into());
                        model_pos = -1;
                        for (i, k) in kvs.iter().enumerate() {
                            if self
                                .options
                                .comparator
                                .compare(&k.0.as_slice().into(), &key.as_slice().into())
                                == Ordering::Equal
                                || self
                                    .options
                                    .comparator
                                    .compare(&k.0.as_slice().into(), &key.as_slice().into())
                                    == Ordering::Greater
                            {
                                model_pos = i as i64;
                                break;
                            }
                        }

                        self.check_iter(&iter, &kvs, model_pos);
                    }

                    3 => {
                        if iter.valid() {
                            iter.prev();
                            // Wrap around to invalid value
                            model_pos -= 1;
                            self.check_iter(&iter, &kvs, model_pos);
                        }
                    }

                    4 => {
                        iter.seek_to_last();
                        model_pos = kvs.len() as i64 - 1;
                        self.check_iter(&iter, &kvs, model_pos);
                    }

                    _ => panic!("impossible"),
                }
            }
        }

        fn check_iter<'a>(
            &'a self,
            iter1: &'a Box<dyn MyIter + 'a>,
            kvs: &Vec<(KVBytes, KVBytes)>,
            model_pos: i64,
        ) {
            if !iter1.valid() {
                assert!(model_pos < 0 || model_pos as usize >= kvs.len());
            } else {
                assert!(model_pos >= 0 && model_pos < kvs.len() as i64);
                let (model_key, model_val) = kvs.get(model_pos as usize).unwrap();
                assert_eq!(
                    Self::kv_string(iter1.key().as_ref(), iter1.value().as_ref()),
                    Self::kv_string(model_key.as_slice(), model_val.as_slice())
                );
            }
        }

        fn kv_string(k: &[u8], v: &[u8]) -> String {
            let mut res = String::new();
            let ks = String::from_utf8_lossy(k);
            let vs = String::from_utf8_lossy(v);
            res.push_str(ks.as_ref());
            res.push_str(vs.as_ref());
            res
        }

        fn pick_random_key(&self, rng: &mut ThreadRng, kvs: &Vec<(KVBytes, KVBytes)>) -> Vec<u8> {
            let mut res = Vec::new();
            if kvs.is_empty() {
                res.extend_from_slice(b"foo");
            } else {
                let index = rng.gen_range(0, kvs.len());
                res.extend_from_slice(kvs[index].0.as_slice());
                match rng.gen_range(0, 2) {
                    0 => {}
                    // Return an existing key
                    1 => {
                        // Attempt to return something smaller than an existing key
                        let len = res.len();
                        if !res.is_empty() && res[len - 1] > 0 {
                            res[len - 1] -= 1;
                        }
                    }

                    2 => {
                        increment(self.options.comparator.clone(), &mut res);
                    }

                    _ => panic!("impossible"),
                }
            }

            res
        }
    }

    const TEST_ARGS: &[(TestType, bool, u32)] = &[
        (TestType::TableTest, false, 16),
        (TestType::TableTest, false, 1),
        (TestType::TableTest, false, 1024),
        (TestType::TableTest, true, 16),
        (TestType::TableTest, true, 1),
        (TestType::TableTest, true, 1024),
        (TestType::BlockTest, false, 16),
        (TestType::BlockTest, false, 1),
        (TestType::BlockTest, false, 1024),
        (TestType::BlockTest, true, 16),
        (TestType::BlockTest, true, 1),
        (TestType::BlockTest, true, 1024),
        // Restart interval does not matter for memtables
        (TestType::MemTableTest, false, 16),
        (TestType::MemTableTest, true, 16),
        //TODO: add DB test
    ];

    // Test empty table/block.
    #[test]
    fn test_empty() {
        for (test_type, reverse, restart_interval) in TEST_ARGS {
            let mut tester = HarnessTester::new();
            tester.init(*test_type, *reverse, *restart_interval);
            tester.test();
        }
    }

    // Test the empty key
    #[test]
    fn test_simple_empty_key() {
        for (test_type, reverse, restart_interval) in TEST_ARGS {
            let mut tester = HarnessTester::new();
            tester.init(*test_type, *reverse, *restart_interval);
            tester.add(b"".to_vec(), b"v".to_vec());
            tester.test();
        }
    }

    #[test]
    fn test_simple_single() {
        for (test_type, reverse, restart_interval) in TEST_ARGS {
            let mut tester = HarnessTester::new();
            tester.init(*test_type, *reverse, *restart_interval);
            tester.add(b"abc".to_vec(), b"v".to_vec());
            tester.test();
        }
    }

    #[test]
    fn test_simple_multi() {
        for (test_type, reverse, restart_interval) in TEST_ARGS {
            let mut tester = HarnessTester::new();
            tester.init(*test_type, *reverse, *restart_interval);
            tester.add(b"abc".to_vec(), b"v".to_vec());
            tester.add(b"abcd".to_vec(), b"v".to_vec());
            tester.add(b"ac".to_vec(), b"v2".to_vec());
            tester.test();
        }
    }

    #[test]
    fn test_simple_special_key() {
        for (test_type, reverse, restart_interval) in TEST_ARGS {
            let mut tester = HarnessTester::new();
            tester.init(*test_type, *reverse, *restart_interval);
            tester.add(b"\xff\xff".to_vec(), b"v3".to_vec());
            tester.test();
        }
    }

    #[test]
    fn test_randomize() {
        for (test_type, reverse, restart_interval) in TEST_ARGS {
            let mut tester = HarnessTester::new();
            tester.init(*test_type, *reverse, *restart_interval);

            let mut rng = thread_rng();
            let mut num_entries = 0;
            while num_entries < 2000 {
                for _ in 0..num_entries {
                    let skew = rng.gen_range(0, 5);
                    let key_len = rng.gen_range(0, 1 << skew);
                    let key = random_key(&mut rng, key_len);

                    let skew = rng.gen_range(0, 6);
                    let val_len = rng.gen_range(0, 1 << skew);
                    let val = random_vec_str(&mut rng, val_len);

                    tester.add(key, val);
                }

                num_entries = if num_entries < 50 {
                    num_entries + 1
                } else {
                    num_entries + 50
                }
            }

            tester.test();
        }
    }

    #[test]
    fn test_memtable_simple() {
        let internal_key_comparator = InternalKeyComparator::new(Arc::new(BitWiseComparator {}));
        let memtable = Arc::new(MemTable::new(internal_key_comparator));
        let mut batch = WriteBatch::new();
        batch.set_sequence(100);

        let kvs = vec![
            (b"k1".as_ref(), b"v1".as_ref()),
            (b"k2".as_ref(), b"v2".as_ref()),
            (b"k3".as_ref(), b"v3".as_ref()),
            (b"k4".as_ref(), b"v4".as_ref()),
            (b"k5".as_ref(), b"v5".as_ref()),
            (b"k6".as_ref(), b"v6".as_ref()),
        ];
        for (k, v) in kvs.iter() {
            batch.put((*k).into(), (*v).into());
        }

        batch.insert_into(memtable.clone()).unwrap();

        let mut iter = memtable.iter();
        let mut iter = KeyConvertingIterator {
            inner_iter: iter,
            status: None,
        };

        iter.seek_to_first();
        for (k, v) in kvs.iter() {
            assert!(iter.valid());
            assert_eq!(*k, iter.key().as_ref());
            assert_eq!(*v, iter.value().as_ref());
            iter.next();
        }
        assert!(!iter.valid());
    }

    #[test]
    fn test_table_approximate_offset_of_plain() {
        let mut options = Options::default();
        options.compression_type = NO_COMPRESSION;

        let mut constructor = TableConstructor::new(options.comparator.clone());
        constructor.add(b"k01".as_ref().to_vec(), b"hello".as_ref().to_vec());
        constructor.add(b"k02".as_ref().to_vec(), b"hello2".as_ref().to_vec());

        let mut repeated_val = Vec::with_capacity(10000);
        repeated_val.resize(10000, 'x' as u8);
        constructor.add(b"k03".as_ref().to_vec(), repeated_val);

        let mut repeated_val = Vec::with_capacity(200000);
        repeated_val.resize(200000, 'x' as u8);
        constructor.add(b"k04".as_ref().to_vec(), repeated_val);

        let mut repeated_val = Vec::with_capacity(300000);
        repeated_val.resize(300000, 'x' as u8);
        constructor.add(b"k05".as_ref().to_vec(), repeated_val);

        constructor.add(b"k06".as_ref().to_vec(), b"hello3".as_ref().to_vec());

        let mut repeated_val = Vec::with_capacity(100000);
        repeated_val.resize(100000, 'x' as u8);
        constructor.add(b"k07".as_ref().to_vec(), repeated_val);

        let mut kvs = Vec::new();
        constructor.finish(Arc::new(options), &mut kvs);

        let table = constructor.table.as_ref().unwrap();
        let offset = table.approximate_offset_of(b"abc".as_ref().into());
        assert_eq!(0, offset);
        let offset = table.approximate_offset_of(b"k01".as_ref().into());
        assert_eq!(0, offset);
        let offset = table.approximate_offset_of(b"k01a".as_ref().into());
        assert_eq!(0, offset);
        let offset = table.approximate_offset_of(b"k02".as_ref().into());
        assert_eq!(0, offset);
        let offset = table.approximate_offset_of(b"k03".as_ref().into());
        assert_eq!(0, offset);
        let offset = table.approximate_offset_of(b"k04".as_ref().into());
        assert!(offset >= 10000 && offset <= 11000);
        let offset = table.approximate_offset_of(b"k04a".as_ref().into());
        assert!(offset >= 210000 && offset <= 211000);
        let offset = table.approximate_offset_of(b"k05".as_ref().into());
        assert!(offset >= 210000 && offset <= 211000);
        let offset = table.approximate_offset_of(b"k06".as_ref().into());
        assert!(offset >= 510000 && offset <= 511000);
        let offset = table.approximate_offset_of(b"k07".as_ref().into());
        assert!(offset >= 510000 && offset <= 511000);
        let offset = table.approximate_offset_of(b"xyz".as_ref().into());
        assert!(offset >= 610000 && offset <= 611000);
    }

    #[test]
    fn test_table_approximate_offset_of_compressed() {
        let mut options = Options::default();
        let mut rnd = thread_rng();
        let mut constructor = TableConstructor::new(options.comparator.clone());

        constructor.add(b"k01".as_ref().to_vec(), b"hello".as_ref().to_vec());
        constructor.add(
            b"k02".as_ref().to_vec(),
            compressible_vec_str(&mut rnd, 0.25, 10000),
        );
        constructor.add(b"k03".as_ref().to_vec(), b"hello3".as_ref().to_vec());
        constructor.add(
            b"k04".as_ref().to_vec(),
            compressible_vec_str(&mut rnd, 0.25, 10000),
        );

        let mut kvs = Vec::new();
        constructor.finish(Arc::new(options), &mut kvs);

        // Expected upper and lower bounds of space used by compressible strings.
        let slop = 1000; // Compressor effectiveness varies.
        let expect = 2500; // 10000 * compression ratio (0.25)
        let min_z = expect - slop;
        let max_z = expect + slop;

        let table = constructor.table.as_ref().unwrap();
        let offset = table.approximate_offset_of(b"abc".as_ref().into());
        assert!(offset <= slop);
        let offset = table.approximate_offset_of(b"k01".as_ref().into());
        assert!(offset <= slop);
        let offset = table.approximate_offset_of(b"k02".as_ref().into());
        assert!(offset <= slop);
        let offset = table.approximate_offset_of(b"k03".as_ref().into());
        assert!(offset >= min_z && offset <= max_z);
        let offset = table.approximate_offset_of(b"k04".as_ref().into());
        assert!(offset >= min_z && offset <= max_z);
        let offset = table.approximate_offset_of(b"xyz".as_ref().into());
        assert!(offset >= 2 * min_z && offset <= 2 * max_z);
    }
}
