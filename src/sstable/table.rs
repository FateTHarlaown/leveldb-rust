use crate::db::error::{Result, StatusError};
use crate::db::option::Options;
use crate::db::slice::Slice;
use crate::db::{Iterator, RandomAccessFile, ReadOption};
use crate::sstable::block::{Block, BlockIter};
use crate::sstable::filter_block::FilterBlockReader;
use crate::sstable::format::{BlockContent, BlockHandle, Footer, FOOTER_ENCODED_LENGTH};
use crate::sstable::two_level_iterator::{BlockIterBuilder, TwoLevelIterator};
use crate::util::cmp::{BitWiseComparator, Comparator};
use byteorder::{LittleEndian, WriteBytesExt};
use std::cmp::Ordering;
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

    pub fn new_iterator<'a>(&'a self, option: &ReadOption) -> Box<dyn Iterator + 'a> {
        let index_iter = self
            .index_block
            .new_iterator(self.options.comparator.clone());
        let block_iter_builder = TableBlockIterBuilder { table: self };
        let iter = TwoLevelIterator::new(index_iter, block_iter_builder, option.clone());
        Box::new(iter)
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
