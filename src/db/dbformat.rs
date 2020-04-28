use crate::db::slice::Slice;
use crate::util::buffer::BufferReader;
use crate::util::cmp::Comparator;
use crate::util::coding::{varint_length, EncodeVarint};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::Ordering;
use std::io::Write;
use std::mem;
use std::rc::Rc;

pub type SequenceNumber = u64;
// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
pub const MAX_SEQUENCE_NUMBER: u64 = ((0x1 << 56) - 1);

pub type ValueType = u64;
// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE VALUES: they are embedded in the on-disk
// data structures.
pub const TYPE_DELETION: u64 = 0x0;
pub const TYPE_VALUE: u64 = 0x1;
pub const VALUE_TYPE_FOR_SEEK: u64 = TYPE_VALUE;

pub fn pack_sequence_and_type(seq: u64, t: ValueType) -> u64 {
    assert!(seq <= MAX_SEQUENCE_NUMBER);
    (seq << 8) | t
}

// Append the serialization of "key" to *result.
pub fn append_internal_key(result: &mut Vec<u8>, key: &ParsedInternalKey) {
    result.extend_from_slice(key.user_key.as_ref());
    let mut buf: [u8; mem::size_of::<u64>()] = [0; mem::size_of::<u64>()];
    buf.as_mut()
        .write_u64::<LittleEndian>(pack_sequence_and_type(key.sequence, key.val_type))
        .unwrap();
    result.extend_from_slice(buf.as_ref());
}

// Returns the user key portion of an internal key.
pub fn extract_user_key(internal_key: Slice) -> Slice {
    assert!(internal_key.size() >= 8);
    let buf = internal_key.as_ref();
    buf[0..(buf.len() - 8)].into()
}

pub fn parse_internal_key(internal_key: Slice, result: &mut ParsedInternalKey) -> bool {
    let n = internal_key.size();
    if n >= 8 {
        let mut buf = internal_key.as_ref();
        let user_key = buf.read_bytes(buf.len() - 8).unwrap().into();
        let tag = buf.read_u64::<LittleEndian>().unwrap();
        let seq = tag >> 8;
        let val_type = tag & 0xff;
        result.user_key = user_key;
        result.sequence = seq;
        result.val_type = val_type;
        val_type <= TYPE_VALUE
    } else {
        false
    }
}

pub struct ParsedInternalKey {
    user_key: Slice,
    sequence: SequenceNumber,
    val_type: ValueType,
}

impl Default for ParsedInternalKey {
    fn default() -> Self {
        ParsedInternalKey {
            user_key: Slice::default(),
            sequence: 0,
            val_type: TYPE_DELETION,
        }
    }
}

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
pub struct InternalKey {
    rep: Vec<u8>,
}

impl InternalKey {
    pub fn new_empty_key() -> Self {
        InternalKey { rep: Vec::new() }
    }

    pub fn new(user_key: Slice, s: SequenceNumber, t: ValueType) -> Self {
        let mut rep = Vec::new();
        let parsed_key = ParsedInternalKey {
            user_key,
            sequence: s,
            val_type: t,
        };
        append_internal_key(&mut rep, &parsed_key);

        InternalKey { rep }
    }

    pub fn clear(&mut self) {
        self.rep.clear();
    }

    pub fn user_key(&self) -> Slice {
        extract_user_key(self.rep.as_slice().into())
    }

    pub fn set_from(&mut self, p: &ParsedInternalKey) {
        self.rep.clear();
        append_internal_key(&mut self.rep, p);
    }

    pub fn encode(&self) -> Slice {
        assert!(!self.rep.is_empty());
        self.rep.as_slice().into()
    }

    pub fn decode_from(&mut self, s: Slice) -> bool {
        self.rep = Vec::from(s.as_ref());
        !self.rep.is_empty()
    }
}

pub struct InternalKeyComparator {
    user_comparator: Rc<dyn Comparator<Slice>>,
}

impl InternalKeyComparator {
    pub fn new(user_comparator: Rc<dyn Comparator<Slice>>) -> Self {
        InternalKeyComparator { user_comparator }
    }

    pub fn user_comparator(&self) -> Rc<dyn Comparator<Slice>> {
        self.user_comparator.clone()
    }
}

impl Comparator<Slice> for InternalKeyComparator {
    fn compare(&self, left: &Slice, right: &Slice) -> Ordering {
        // Order by:
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)
        let (left_user_key, right_user_key) =
            (extract_user_key(*left), extract_user_key(*right));
        match self
            .user_comparator
            .compare(&left_user_key, &right_user_key)
        {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let mut left_seq_field = left.as_ref()[left.size()-8..].as_ref();
                let left_seq = left_seq_field.read_u64::<LittleEndian>().unwrap();
                let mut right_seq_filed = right.as_ref()[right.size()-8..].as_ref();
                let right_seq = right_seq_filed.read_u64::<LittleEndian>().unwrap();
                if left_seq > right_seq {
                    Ordering::Less
                } else if left_seq < right_seq {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        }
    }

    fn name(&self) -> &'static str {
        "leveldb.InternalKeyComparator"
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: Slice) {
        let user_start = extract_user_key(start.as_slice().into());
        let user_limit = extract_user_key(limit);
        let mut tmp = Vec::from(user_start.as_ref());
        self.user_comparator
            .find_shortest_separator(&mut tmp, user_limit.into());
        if tmp.len() < user_start.size()
            && self
                .user_comparator
                .compare(&user_start, &tmp.as_slice().into())
                == Ordering::Less
        {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            tmp.write_u64::<LittleEndian>(pack_sequence_and_type(
                MAX_SEQUENCE_NUMBER,
                VALUE_TYPE_FOR_SEEK,
            ))
            .unwrap();
            assert_eq!(
                self.compare(&start.as_slice().into(), &tmp.as_slice().into()),
                Ordering::Less,
            );
            assert_eq!(
                self.compare(&tmp.as_slice().into(), &limit),
                Ordering::Less,
            );
            mem::replace(start, tmp);
        }
    }

    fn find_short_successor(&self, key: &mut Vec<u8>) {
        let user_key = extract_user_key(key.as_slice().into());
        let mut tmp = Vec::from(user_key.as_ref());
        self.user_comparator.find_short_successor(&mut tmp);
        if tmp.len() < user_key.size() {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            tmp.write_u64::<LittleEndian>(pack_sequence_and_type(
                MAX_SEQUENCE_NUMBER,
                VALUE_TYPE_FOR_SEEK,
            )).unwrap();
            assert_eq!(
                self.compare(&key.as_slice().into(), &tmp.as_slice().into()),
                Ordering::Less,
            );
            mem::replace(key, tmp);
        }
    }
}

// A helper class useful for DBImpl::Get()
pub struct LookupKey {
    // We construct a u8 buf of the form:
    //    klength  varint32
    //    userkey  char[klength]          <-- user_key_start
    //    tag      uint64
    //                                    <-- end
    // The array is a suitable MemTable key.
    // The suffix starting with "userkey" can be used as an InternalKey.
    user_key_start: usize,
    end: usize,
    // if long_key_buf is none, that means the key was stored in short_key_buf
    long_key_buf: Option<Vec<u8>>,
    short_key_buf: [u8; 200],
}

impl LookupKey {
    pub fn new(user_key: Slice, s: SequenceNumber) -> Self {
        let mut short_key_buf = [0; 200];
        let mut long_key_buf: Option<Vec<u8>> = None;

        let user_key_size = user_key.size();
        let user_key_start = varint_length(user_key_size as u64);
        let end = user_key_start + user_key_size + 8;
        // A conservative estimate
        let needed = user_key.size() + 13;
        let mut writer_buf = if needed > short_key_buf.len() {
            let mut buf = Vec::with_capacity(needed);
            unsafe {
                buf.set_len(needed);
            }
            long_key_buf.get_or_insert(buf).as_mut_slice()
        } else {
            short_key_buf.as_mut()
        };

        writer_buf.encode_varint32(user_key_size as u32).unwrap();
        writer_buf.write_all(user_key.as_ref()).unwrap();
        writer_buf
            .write_u64::<LittleEndian>(pack_sequence_and_type(s, TYPE_VALUE))
            .unwrap();

        LookupKey {
            user_key_start,
            end,
            long_key_buf,
            short_key_buf,
        }
    }

    pub fn memtable_key(&self) -> Slice {
        let buf = self.get_key_buf();
        buf[0..self.end].into()
    }

    pub fn user_key(&self) -> Slice {
        let buf = self.get_key_buf();
        buf[self.user_key_start..self.end - 8].into()
    }

    pub fn internal_key(&self) -> Slice {
        let buf = self.get_key_buf();
        buf[self.user_key_start..self.end].into()
    }

    fn get_key_buf(&self) -> &[u8] {
        if self.long_key_buf.is_some() {
            self.long_key_buf.as_ref().unwrap().as_slice()
        } else {
            self.short_key_buf.as_ref()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::dbformat::SequenceNumber;
    use crate::util::cmp::BitWiseComparator;

    fn i_key(user_key: Slice, seq: SequenceNumber, vt: ValueType) -> Vec<u8> {
        let mut key = Vec::new();
        let internal = ParsedInternalKey {
            user_key,
            sequence: seq,
            val_type: vt,
        };
        append_internal_key(&mut key, &internal);
        key
    }

    fn test_key(key: Slice, seq: SequenceNumber, vt: ValueType) {
        let encoded = i_key(key, seq, vt);
        let slice_in = encoded.as_slice().into();
        let mut parsed_internal_key = ParsedInternalKey::default();
        assert!(parse_internal_key(slice_in, &mut parsed_internal_key));
        assert_eq!(key.as_ref(), parsed_internal_key.user_key.as_ref());
        assert_eq!(seq, parsed_internal_key.sequence);
        assert_eq!(vt, parsed_internal_key.val_type);

        let mut decoded = ParsedInternalKey::default();
        assert!(!parse_internal_key(Slice::default(), &mut decoded));
    }

    fn shorten(s: &Vec<u8>, l: &Vec<u8>) -> Vec<u8> {
        let mut result = Vec::from(s.as_slice());
        let comparator = InternalKeyComparator::new(Rc::new(BitWiseComparator {}));
        comparator.find_shortest_separator(&mut result, l.as_slice().into());
        result
    }

    fn short_successor(s: &Vec<u8>) -> Vec<u8> {
        let mut result = Vec::from(s.as_slice());
        let comparator = InternalKeyComparator::new(Rc::new(BitWiseComparator {}));
        comparator.find_short_successor(&mut result);
        result
    }

    #[test]
    fn internal_key_encode_decode() {
        let keys = vec![
            "".as_bytes().into(),
            "k".as_bytes().into(),
            "hello".as_bytes().into(),
            "longggggggggggggggggggggg".as_bytes().into(),
        ];
        let seqs = vec![
            1,
            2,
            3,
            (1u64 << 8) - 1,
            1u64 << 8,
            (1u64 << 8) + 1,
            (1u64 << 16) - 1,
            1u64 << 16,
            (1u64 << 16) + 1,
            (1u64 << 32) - 1,
            1u64 << 32,
            (1u64 << 32) + 1,
        ];

        for k in keys.iter() {
            for s in seqs.iter() {
                test_key(*k, *s, TYPE_VALUE);
                test_key("hello".as_bytes().into(), 1, TYPE_DELETION)
            }
        }
    }

    #[test]
    fn internal_key_decode_from_empty() {
        let mut internal_key = InternalKey::new_empty_key();
        assert!(!internal_key.decode_from("".as_bytes().into()))
    }

    #[test]
    fn internal_key_short_separator() {
        // When user keys are same
        assert_eq!(
            i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("foo".as_bytes().into(), 99, TYPE_VALUE)
            )
        );
        assert_eq!(
            i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("foo".as_bytes().into(), 101, TYPE_VALUE)
            )
        );
        assert_eq!(
            i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE)
            )
        );
        assert_eq!(
            i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("foo".as_bytes().into(), 100, TYPE_DELETION)
            )
        );

        // When user keys are misordered
        assert_eq!(
            i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("bar".as_bytes().into(), 99, TYPE_VALUE)
            )
        );

        // When user keys are different, but correctly ordered
        assert_eq!(
            i_key("g".as_bytes().into(), MAX_SEQUENCE_NUMBER, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("hello".as_bytes().into(), 200, TYPE_VALUE)
            )
        );

        // When start user key is prefix of limit user key
        assert_eq!(
            i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("foobar".as_bytes().into(), 200, TYPE_VALUE)
            )
        );

        // When limit user key is prefix of start user key
        assert_eq!(
            i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            shorten(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
                &i_key("foobar".as_bytes().into(), 200, TYPE_VALUE)
            )
        );
    }

    #[test]
    fn internal_key_shortest_successor() {
        assert_eq!(
            i_key("g".as_bytes().into(), MAX_SEQUENCE_NUMBER, TYPE_VALUE),
            short_successor(
                &i_key("foo".as_bytes().into(), 100, TYPE_VALUE),
            )
        );

    }
}
