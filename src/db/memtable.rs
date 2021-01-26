use crate::db::dbformat::{InternalKeyComparator, LookupKey, SequenceNumber, ValueType};
use crate::db::dbformat::{TYPE_DELETION, TYPE_VALUE};
use crate::db::error::{Result, StatusError};
use crate::db::skiplist::{SkipList, SkipListIterator};
use crate::db::slice::Slice;
use crate::db::Iterator;
use crate::util::arena::Arena;
use crate::util::cmp::{BitWiseComparator, Comparator};
use crate::util::coding::{
    put_varint32, varint_length, DecodeVarint, EncodeVarint, VarLengthSliceWriter,
};

use crate::util::buffer::BufferReader;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::io::Write;
use std::mem;
use std::rc::Rc;
use std::slice;
use std::sync::Arc;

pub struct MemTable {
    arena: Rc<RefCell<Arena>>,
    table: SkipList<Slice>,
    comparator: Arc<dyn Comparator<Slice>>,
}

impl MemTable {
    pub fn new(internal_key_comparator: InternalKeyComparator) -> Self {
        let arena = Rc::new(RefCell::new(Arena::new()));
        let comparator = internal_key_comparator.user_comparator();
        let key_comparator = KeyComparator {
            comparator: internal_key_comparator,
        };
        let table = SkipList::new(Rc::new(key_comparator), arena.clone(), Slice::default());
        MemTable {
            arena,
            table,
            comparator,
        }
    }

    pub fn add(&self, s: SequenceNumber, val_type: ValueType, key: &Slice, val: &Slice) {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        let key_size = key.size();
        let val_size = val.size();
        let internal_key_size = key_size + 8;
        let encode_len = varint_length(internal_key_size as u64)
            + internal_key_size
            + varint_length(val_size as u64)
            + val_size;

        let mem_area = self.arena.borrow_mut().allocate_aligned(encode_len);
        let mut buf = unsafe { Vec::from_raw_parts(mem_area, encode_len, encode_len) };
        let mut writer = buf.as_mut_slice();
        // internal_key_size must not overflow
        writer.encode_varint32(internal_key_size as u32).unwrap();
        writer.write_all(key.as_ref()).unwrap();
        writer
            .write_u64::<LittleEndian>((s << 8) | val_type)
            .unwrap();
        writer.encode_varint32(val_size as u32).unwrap();
        if !val.is_empty() {
            writer.write_all(val.as_ref()).unwrap();
        }
        self.table.insert(buf.as_slice().into());
        mem::forget(buf);
    }

    pub fn get(&self, key: &LookupKey) -> Result<Option<Slice>> {
        let mem_key = key.memtable_key();
        let mut iter = SkipListIterator::new(&self.table);
        iter.seek(&mem_key);
        if iter.valid() {
            let mut seek_key_buf = iter.key().as_ref();
            let internal_key_len = seek_key_buf.decode_varint32().unwrap();
            let mut internal_key = seek_key_buf.read_bytes(internal_key_len as usize).unwrap();
            let seek_user_key = internal_key
                .read_bytes(internal_key.len() - 8)
                .unwrap()
                .into();
            if Ordering::Equal == self.comparator.compare(&key.user_key(), &seek_user_key) {
                let record_type = internal_key.read_u64::<LittleEndian>().unwrap() & 0xff;
                if record_type == TYPE_VALUE {
                    let user_value_len = seek_key_buf.decode_varint32().unwrap();
                    let user_value = seek_key_buf.read_bytes(user_value_len as usize).unwrap();
                    return Ok(Some(user_value.into()));
                } else if record_type == TYPE_DELETION {
                    return Ok(None);
                }
            }
        }
        Err(StatusError::NotFound("no key".to_string()))
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.arena.borrow().memory_usage()
    }

    pub fn iter(&'a self) -> Box<dyn Iterator + 'a> {
        Box::new(MemTableIterator::new(SkipListIterator::new(&self.table)))
    }
}

pub struct MemTableIterator<'a> {
    iter: SkipListIterator<'a, Slice>,
    tmp: Vec<u8>,
}

impl<'a> MemTableIterator<'a> {
    pub fn new(iter: SkipListIterator<'a, Slice>) -> Self {
        MemTableIterator {
            iter,
            tmp: Vec::new(),
        }
    }
}

impl<'a> Iterator for MemTableIterator<'a> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first()
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last()
    }

    fn seek(&mut self, target: Slice) {
        self.tmp.clear();
        encode_key(&mut self.tmp, target);
        self.iter.seek(&self.tmp.as_slice().into());
    }

    fn next(&mut self) {
        self.iter.next()
    }

    fn prev(&mut self) {
        self.iter.prev()
    }

    fn key(&self) -> Slice {
        let mut buf = self.iter.key().as_ref();
        get_length_prefixed_slice(&mut buf)
    }

    fn value(&self) -> Slice {
        let mut raw_key = self.iter.key();
        let mut buf = raw_key.as_ref();
        get_length_prefixed_slice(&mut buf);
        get_length_prefixed_slice(&mut buf)
    }

    fn status(&mut self) -> Result<()> {
        Ok(())
    }
}

struct KeyComparator {
    comparator: InternalKeyComparator,
}

impl Comparator<Slice> for KeyComparator {
    fn compare(&self, left: &Slice, right: &Slice) -> Ordering {
        let left_key = get_length_prefixed_slice(&mut left.as_ref());
        let right_key = get_length_prefixed_slice(&mut right.as_ref());
        self.comparator.compare(&left_key, &right_key)
    }

    fn name(&self) -> &'static str {
        "leveldb.KeyComparator"
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: Slice) {
        self.comparator.find_shortest_separator(start, limit)
    }

    fn find_short_successor(&self, key: &mut Vec<u8>) {
        self.comparator.find_short_successor(key)
    }
}

pub fn get_length_prefixed_slice(mut buf: &mut &[u8]) -> Slice {
    let len = buf.decode_varint32().unwrap();
    buf.read_bytes(len as usize).unwrap().into()
}

fn encode_key(scratch: &mut Vec<u8>, target: Slice) -> Slice {
    scratch.clear();
    put_varint32(scratch, target.size() as u32);
    scratch.extend_from_slice(target.as_ref());
    scratch.as_slice().into()
}
