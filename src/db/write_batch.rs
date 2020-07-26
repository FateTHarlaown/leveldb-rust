use crate::db::dbformat::{SequenceNumber, TYPE_DELETION, TYPE_VALUE};
use crate::db::error::{Result, StatusError};
use crate::db::memtable::MemTable;
use crate::db::slice::Slice;
use crate::util::buffer::BufferReader;
use crate::util::coding::{put_varint32, DecodeVarint};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::sync::Arc;

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
const HEADER: usize = 12;

pub struct WriteBatch {
    rep: Vec<u8>,
}

impl WriteBatch {
    pub fn new() -> Self {
        let mut rep = Vec::new();
        rep.resize(HEADER, 0);
        WriteBatch { rep }
    }

    pub fn set_sequence(&mut self, seq: SequenceNumber) {
        self.rep
            .as_mut_slice()
            .write_u64::<LittleEndian>(seq)
            .unwrap()
    }

    // Store the mapping "key->value" in the database.
    pub fn put(&mut self, key: Slice, value: Slice) {
        self.set_count(self.count() + 1);
        self.rep.push(TYPE_VALUE as u8);
        put_varint32(&mut self.rep, key.size() as u32);
        self.rep.extend_from_slice(key.as_ref());
        put_varint32(&mut self.rep, value.size() as u32);
        self.rep.extend_from_slice(value.as_ref());
    }

    // If the database contains a mapping for "key", erase it.  Else do nothing.
    pub fn delete(&mut self, key: Slice) {
        self.set_count(self.count() + 1);
        self.rep.push(TYPE_DELETION as u8);
        put_varint32(&mut self.rep, key.size() as u32);
        self.rep.extend_from_slice(key.as_ref());
    }

    // Clear all updates buffered in this batch.
    pub fn clear(&mut self) {
        self.rep.clear();
        self.rep.resize(HEADER, 0);
    }

    // The size of the database changes caused by this batch.
    //
    // This number is tied to implementation details, and may change across
    // releases. It is intended for LevelDB usage metrics.
    pub fn approximate_size(&self) -> usize {
        self.rep.len()
    }

    // Copies the operations in "source" to this batch.
    //
    // This runs in O(source size) time. However, the constant factor is better
    // than calling Iterate() over the source batch with a Handler that replicates
    // the operations into this batch.
    pub fn append(&mut self, source: &WriteBatch) {
        self.set_count(self.count() + source.count());
        assert!(self.rep.len() >= HEADER);
        let mut buf = source.get_content().as_slice();
        buf.read_bytes(HEADER).unwrap();
        self.rep.extend_from_slice(buf)
    }

    // Support for iterating over the contents of a batch.
    pub fn iterate<H: Handler>(&self, mut handler: H) -> Result<()> {
        let mut buf = self.rep.as_slice();
        if buf.len() < HEADER {
            return Err(StatusError::Corruption(
                "malformed WriteBatch (too small)".to_string(),
            ));
        }

        buf.advance(HEADER);
        let mut found = 0;
        while !buf.is_empty() {
            found += 1;
            let tag: u64 = buf[0] as u64;
            buf.advance(1);
            match tag {
                TYPE_VALUE => {
                    let key_size = buf.decode_varint32()?;
                    let key = buf.read_bytes(key_size as usize)?.into();
                    let value_size = buf.decode_varint32()?;
                    let value = buf.read_bytes(value_size as usize)?.into();
                    handler.put(key, value);
                }
                TYPE_DELETION => {
                    let key_size = buf.decode_varint32()?;
                    let key = buf.read_bytes(key_size as usize)?;
                    handler.delete(key.into());
                }
                _ => {
                    return Err(StatusError::Corruption(
                        "unknown WriteBatch tag".to_string(),
                    ))
                }
            }
        }

        if found != self.count() {
            Err(StatusError::Corruption(
                "WriteBatch has wrong count".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn insert_into(&self, memtable: Arc<MemTable>) -> Result<()> {
        let inserter = MemTableInserter {
            sequence: self.sequence(),
            mem: memtable,
        };

        self.iterate(inserter)
    }

    pub fn get_content(&self) -> &Vec<u8> {
        &self.rep
    }

    pub fn set_count(&mut self, n: u32) {
        let mut buf = self.rep[8..].as_mut();
        buf.write_u32::<LittleEndian>(n).unwrap()
    }

    pub fn count(&self) -> u32 {
        let mut buf = self.rep.as_slice();
        buf.read_bytes(8);
        buf.read_u32::<LittleEndian>().unwrap()
    }

    pub fn sequence(&self) -> SequenceNumber {
        self.rep.as_slice().read_u64::<LittleEndian>().unwrap()
    }
}

trait Handler {
    fn put(&mut self, key: Slice, value: Slice);
    fn delete(&mut self, key: Slice);
}

struct MemTableInserter {
    sequence: SequenceNumber,
    mem: Arc<MemTable>,
}

impl Handler for MemTableInserter {
    fn put(&mut self, key: Slice, value: Slice) {
        self.mem.add(self.sequence, TYPE_VALUE, &key, &value);
        self.sequence += 1;
    }

    fn delete(&mut self, key: Slice) {
        self.mem
            .add(self.sequence, TYPE_DELETION, &key, &Slice::default());
        self.sequence += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::dbformat::{parse_internal_key, InternalKeyComparator, ParsedInternalKey};
    use crate::db::memtable::MemTable;
    use crate::db::write_batch::WriteBatch;
    use crate::util::cmp::BitWiseComparator;
    use std::rc::Rc;

    fn print_content(b: &WriteBatch) -> Vec<u8> {
        let cmp = InternalKeyComparator::new(Rc::new(BitWiseComparator {}));
        let mem = Arc::new(MemTable::new());
        let mut iter = mem.iter();
        let ret = b.insert_into(mem.clone());

        let mut state = Vec::new();
        let mut count = 0;

        iter.seek_to_first();
        while iter.valid() {
            let mut ikey = ParsedInternalKey::default();
            parse_internal_key(iter.key(), &mut ikey);
            match ikey.val_type {
                TYPE_VALUE => {
                    state.extend_from_slice("Put(".as_bytes());
                    state.extend_from_slice(ikey.user_key.as_ref());
                    state.extend_from_slice(", ".as_bytes());
                    let v = iter.value();
                    state.extend_from_slice(v.as_ref());
                    state.extend_from_slice(")".as_bytes());
                    count += 1;
                }

                TYPE_DELETION => {
                    state.extend_from_slice("Delete(".as_bytes());
                    state.extend_from_slice(ikey.user_key.as_ref());
                    state.extend_from_slice(")".as_bytes());
                    count += 1;
                }

                _ => panic!("unknown type"),
            }
            state.extend_from_slice("@".as_bytes());
            let seq = ikey.sequence;
            state.extend_from_slice(ikey.sequence.to_string().as_bytes());
            iter.next();
        }

        if ret.is_err() {
            state.extend_from_slice("ParseError()".as_bytes());
        } else if count != b.count() {
            state.extend_from_slice("CountMismatch()".as_bytes());
        }

        state
    }

    #[test]
    fn test_empty() {
        let batch = WriteBatch::new();
        let state = print_content(&batch);
        assert!(state.is_empty());
        assert_eq!(0, batch.count());
    }

    #[test]
    fn test_multiple() {
        let mut batch = WriteBatch::new();
        batch.put("foo".as_bytes().into(), "bar".as_bytes().into());
        batch.delete("box".as_bytes().into());
        batch.put("baz".as_bytes().into(), "boo".as_bytes().into());
        batch.set_sequence(100);
        assert_eq!(100, batch.sequence());
        assert_eq!(3, batch.count());

        let mut content = Vec::new();
        content.extend_from_slice("Put(baz, boo)@102".as_bytes());
        content.extend_from_slice("Delete(box)@101".as_bytes());
        content.extend_from_slice("Put(foo, bar)@100".as_bytes());

        assert_eq!(content.as_slice(), print_content(&batch).as_slice());
    }

    #[test]
    fn test_corruption() {
        let mut batch = WriteBatch::new();
        batch.put("foo".as_bytes().into(), "bar".as_bytes().into());
        batch.delete("box".as_bytes().into());
        batch.set_sequence(200);
        batch.rep.truncate(batch.rep.len() - 1);

        let mut expect = Vec::new();
        expect.extend_from_slice("Put(foo, bar)@200".as_bytes());
        expect.extend_from_slice("ParseError()".as_bytes());
        assert_eq!(expect, print_content(&batch));
    }

    #[test]
    fn test_append() {
        let (mut batch1, mut batch2) = (WriteBatch::new(), WriteBatch::new());
        batch1.set_sequence(200);
        batch2.set_sequence(300);

        batch1.append(&batch2);
        assert_eq!("".as_bytes(), print_content(&batch1).as_slice());

        batch2.put("a".as_bytes().into(), "va".as_bytes().into());
        batch1.append(&batch2);
        assert_eq!(
            "Put(a, va)@200".as_bytes(),
            print_content(&batch1).as_slice()
        );

        batch2.clear();
        batch2.put("b".as_bytes().into(), "vb".as_bytes().into());
        batch1.append(&batch2);
        assert_eq!(
            "Put(a, va)@200Put(b, vb)@201".as_bytes(),
            print_content(&batch1).as_slice()
        );

        batch2.delete("foo".as_bytes().into());
        batch1.append(&batch2);
        assert_eq!(
            "Put(a, va)@200Put(b, vb)@202Put(b, vb)@201Delete(foo)@203".as_bytes(),
            print_content(&batch1).as_slice()
        );
    }

    #[test]
    fn test_approximate_size() {
        let mut batch = WriteBatch::new();
        let empty_size = batch.approximate_size();

        batch.put("foo".as_bytes().into(), "bar".as_bytes().into());
        let one_key_size = batch.approximate_size();
        assert!(empty_size < one_key_size);

        batch.put("baz".as_bytes().into(), "boo".as_bytes().into());
        let two_keys_size = batch.approximate_size();
        assert!(one_key_size < two_keys_size);

        batch.delete("box".as_bytes().into());
        let post_delete_size = batch.approximate_size();
        assert!(two_keys_size < post_delete_size);
    }
}
