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
use std::cmp::Ordering;
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
                handle.decode_from(iter.value())?;
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
        block_handle.decode_from(index_value)?;

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

    pub fn new_iterator<'a>(&'a self, option: &ReadOption) -> impl Iterator + 'a {
        let index_iter = self
            .index_block
            .new_iterator(self.options.comparator.clone());
        let block_iter_builder = TableBlockIterBuilder { table: self };
        TwoLevelIterator::new(index_iter, block_iter_builder, option.clone())
    }

    // Calls callback with the entry found after a call
    // to Seek(key).  May not make such a call if filter policy says
    // that key is not present.
    pub fn internal_get(
        &self,
        read_option: &ReadOption,
        key: Slice,
        callback: Box<dyn Fn(Slice, Slice)>,
    ) -> Result<()> {
        let mut index_iter = self
            .index_block
            .new_iterator(self.options.comparator.clone());
        index_iter.seek(key);
        if index_iter.valid() {
            let mut handle = BlockHandle::default();
            handle.decode_from(index_iter.value())?;
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
            if handle.decode_from(index_iter.value()).is_ok() {
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

struct TableBlockIterBuilder<'a, R: RandomAccessFile> {
    table: &'a Table<R>,
}

impl<'a, R: RandomAccessFile> BlockIterBuilder for TableBlockIterBuilder<'a, R> {
    type Iter = BlockIter;
    fn build(&self, option: &ReadOption, index_val: Slice) -> Result<Self::Iter> {
        self.table.block_iter_from_index(option, index_val)
    }
}

struct TableBuilder<W: WritableFile> {
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
    pub fn finish(&mut self) -> Result<()> {
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
            mem::replace(compressed_out, snappy::compress(raw.as_ref()));
            if compressed_out.len() < raw.size() - (raw.size() / 8) {
                raw
            } else {
                // Snappy not supported, or compressed less than 12.5%, so just
                // store uncompressed form
                write_compress_type = NO_COMPRESSION;
                compressed_out.as_slice().into()
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
