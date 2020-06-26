use crate::db::error::{Result, StatusError};
use crate::db::slice::Slice;
use crate::db::{Iterator, ReadOption};
use crate::util::cmp::{BitWiseComparator, Comparator};
use std::cmp::Ordering;

pub trait BlockIterBuilder {
    type Iter: Iterator;
    fn build(&self, option: &ReadOption, index_val: Slice) -> Result<Self::Iter>;
}

pub struct TwoLevelIterator<I: Iterator, B: BlockIterBuilder> {
    block_builder: B,
    option: ReadOption,
    index_iter: I,
    // If data_iter_valid is not None, then "data_block_handle" holds the
    // "index_value" passed to block_function to create the data_iter.
    data_iter: Option<B::Iter>,
    data_block_handle: Vec<u8>,
    status: Option<StatusError>,
}

impl<I: Iterator, B: BlockIterBuilder> TwoLevelIterator<I, B> {
    pub fn new(index_iter: I, block_builder: B, option: ReadOption) -> Self {
        TwoLevelIterator {
            block_builder,
            option,
            index_iter,
            data_iter: None,
            data_block_handle: Vec::new(),
            status: None,
        }
    }

    fn init_data_block(&mut self) {
        if !self.index_iter.valid() {
            self.set_data_iterator(None);
        } else {
            let handle = self.index_iter.value();
            if !(self.data_iter.is_some()
                && BitWiseComparator {}.compare(&handle, &self.data_block_handle.as_slice().into())
                    == Ordering::Equal)
            {
                match self.block_builder.build(&self.option, handle) {
                    Ok(iter) => {
                        self.data_block_handle.clear();
                        self.data_block_handle.extend_from_slice(handle.as_ref());
                        self.set_data_iterator(Some(iter))
                    }
                    Err(e) => {
                        self.data_iter = None;
                        self.save_error(e);
                    }
                }
            }
            // data_iter is already constructed with this iterator, so
            // no need to change anything
        }
    }

    fn set_data_iterator(&mut self, data_iter: Option<B::Iter>) {
        if let Some(ref mut iter) = self.data_iter {
            if let Err(e) = iter.status() {
                self.save_error(e)
            }
        }
        self.data_iter = data_iter;
    }

    fn save_error(&mut self, status: StatusError) {
        if self.status.is_none() {
            self.status = Some(status)
        }
    }

    fn skip_empty_data_blocks_forward(&mut self) {
        loop {
            if let Some(ref iter) = self.data_iter {
                if iter.valid() {
                    break;
                }
            }

            if !self.index_iter.valid() {
                self.set_data_iterator(None);
                break;
            }
            self.index_iter.next();
            self.init_data_block();
            if let Some(ref mut iter) = self.data_iter {
                iter.seek_to_first();
            }
        }
    }

    fn skip_empty_data_blocks_backward(&mut self) {
        loop {
            if let Some(ref iter) = self.data_iter {
                if iter.valid() {
                    break;
                }
            }

            if !self.index_iter.valid() {
                self.set_data_iterator(None);
                break;
            }
            self.index_iter.prev();
            self.init_data_block();
            if let Some(ref mut iter) = self.data_iter {
                iter.seek_to_last();
            }
        }
    }
}

impl<I: Iterator, B: BlockIterBuilder> Iterator for TwoLevelIterator<I, B> {
    fn valid(&self) -> bool {
        if let Some(ref iter) = self.data_iter {
            iter.valid()
        } else {
            false
        }
    }
    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        self.init_data_block();
        if let Some(ref mut iter) = self.data_iter {
            iter.seek_to_first();
        }
        self.skip_empty_data_blocks_forward();
    }

    fn seek_to_last(&mut self) {
        self.index_iter.seek_to_last();
        self.init_data_block();
        if let Some(ref mut iter) = self.data_iter {
            iter.seek_to_last();
        }
        self.skip_empty_data_blocks_backward();
    }

    fn seek(&mut self, target: Slice) {
        self.index_iter.seek(target);
        self.init_data_block();
        if let Some(ref mut iter) = self.data_iter {
            iter.seek(target);
        }
        self.skip_empty_data_blocks_forward();
    }

    fn next(&mut self) {
        assert!(self.valid());
        if let Some(ref mut iter) = self.data_iter {
            iter.next();
            self.skip_empty_data_blocks_forward();
        }
    }

    fn prev(&mut self) {
        assert!(self.valid());
        if let Some(ref mut iter) = self.data_iter {
            iter.prev();
            self.skip_empty_data_blocks_backward();
        }
    }

    fn key(&self) -> Slice {
        assert!(self.valid());
        let iter = self.data_iter.as_ref().unwrap();
        iter.key()
    }

    fn value(&self) -> Slice {
        assert!(self.valid());
        let iter = self.data_iter.as_ref().unwrap();
        iter.value()
    }

    fn status(&mut self) -> Result<()> {
        let index_status = self.index_iter.status();
        if index_status.is_err() {
            return index_status;
        }

        if let Some(ref mut data_iter) = self.data_iter {
            let data_status = data_iter.status();
            if data_status.is_err() {
                return data_status;
            }
        }

        if self.status.is_some() {
            return Err(self.status.take().unwrap());
        }

        Ok(())
    }
}
