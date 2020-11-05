use crate::db::dbformat::{
    config, parse_internal_key, InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey,
    SequenceNumber,
};
use crate::db::dbformat::{TYPE_DELETION, TYPE_VALUE};
use crate::db::error::{Result, StatusError};
use crate::db::slice::Slice;
use crate::db::table_cache::TableCache;
use crate::db::ReadOption;

use crate::env::Env;
use crate::util::cache::Cache;
use crate::util::cmp::Comparator;

use crate::util::buffer::BufferReader;
use crate::util::coding::{put_varint32, put_varint64, DecodeVarint, VarLengthSliceReader};
use std::cmp::Ordering;
use std::collections::linked_list::LinkedList;
use std::collections::VecDeque;
use std::io::Write;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
pub struct FileMetaData {
    allowed_seeks: i32,
    number: u64,
    file_size: u64,
    smallest: InternalKey,
    largest: InternalKey,
}

impl Default for FileMetaData {
    fn default() -> Self {
        FileMetaData {
            allowed_seeks: 1 << 30,
            number: 0,
            file_size: 0,
            smallest: InternalKey::new_empty_key(),
            largest: InternalKey::new_empty_key(),
        }
    }
}

enum GetState {
    NotFound,
    Found,
    Deleted,
    Corrupt,
}

pub struct Version {
    // List of files per level
    files: [Vec<Arc<FileMetaData>>; config::NUM_LEVELS],
    file_to_compact: Option<Arc<FileMetaData>>,
    icmp: InternalKeyComparator,
}

impl Version {
    pub fn get<E: Env>(
        &self,
        option: &ReadOption,
        k: &LookupKey,
        val: &mut Vec<u8>,
        table_cache: &TableCache<E>,
    ) -> Result<(Arc<FileMetaData>, usize)> {
        let (mut seek_file, mut seek_level) = (Arc::new(FileMetaData::default()), 0);
        let mut first = false;
        let mut found = false;
        let mut corrupt = false;

        let cb = Box::new(|level: usize, file: Arc<FileMetaData>| -> bool {
            let mut state = GetState::NotFound;
            if first {
                seek_file = file.clone();
                seek_level = level;
                first = false;
            }

            let comparator = self.icmp.user_comparator();
            let user_key = k.user_key();
            let get_cb = Box::new(|key: Slice, value: Slice| {
                let mut parsed_key = ParsedInternalKey::default();
                if !parse_internal_key(key, &mut parsed_key) {
                    state = GetState::Corrupt;
                } else {
                    if comparator.compare(&user_key, &parsed_key.user_key) == Ordering::Equal {
                        state = match parsed_key.val_type {
                            TYPE_VALUE => {
                                val.extend_from_slice(value.as_ref());
                                GetState::Found
                            }
                            _ => GetState::NotFound,
                        };
                    }
                }
            });

            if table_cache
                .get(
                    option,
                    file.number,
                    file.file_size,
                    k.internal_key(),
                    get_cb,
                )
                .is_err()
            {
                return false;
            }
            match state {
                GetState::NotFound => true,
                GetState::Deleted => false,
                GetState::Found => {
                    found = true;
                    false
                }
                GetState::Corrupt => {
                    corrupt = true;
                    found = true;
                    false
                }
            }
        });

        self.foreach_overlapping(k.user_key(), k.internal_key(), cb);
        if found {
            if corrupt {
                Err(StatusError::Corruption(format!(
                    "corrupted key for {}",
                    k.user_key().to_string()
                )))
            } else {
                Ok((seek_file, seek_level))
            }
        } else {
            Err(StatusError::NotFound("not found".to_string()))
        }
    }

    // Call callback for every file that overlaps user_key in
    // order from newest to oldest.  If an invocation of func returns
    // false, makes no more calls.
    //
    // REQUIRES: user portion of internal_key == user_key.
    fn foreach_overlapping<CB>(&self, user_key: Slice, internal_key: Slice, mut callback: CB)
    where
        CB: FnMut(usize, Arc<FileMetaData>) -> bool,
    {
        let ucmp = self.icmp.user_comparator();
        let mut tmp = Vec::with_capacity(self.files[0].len());
        self.files[0].iter().for_each(|f| {
            let cmp_small = ucmp.compare(&user_key, &f.smallest.user_key());
            let cmp_big = ucmp.compare(&user_key, &f.smallest.user_key());
            if (cmp_small == Ordering::Equal || cmp_small == Ordering::Greater)
                && (cmp_big == Ordering::Less || cmp_big == Ordering::Equal)
            {
                tmp.push(f.clone());
            }
        });
        if !tmp.is_empty() {
            tmp.sort_by(|a, b| a.number.cmp(&b.number));
            for f in tmp {
                if !callback(0, f) {
                    return;
                }
            }
        }

        for i in 1..config::NUM_LEVELS {
            if self.files[i].is_empty() {
                continue;
            }
            let index = match self.files[i]
                .binary_search_by(|f| self.icmp.compare(&f.largest.encode(), &internal_key))
            {
                Ok(index) => index,
                Err(index) => index,
            };
            if index < self.files[i].len() {
                let f = self.files[i][index].clone();
                if ucmp.compare(&user_key, &f.smallest.user_key()) != Ordering::Less {
                    if !callback(i, f) {
                        return;
                    }
                }
            }
        }
    }

    fn some_file_overlaps_range(
        &self,
        icmp: &InternalKeyComparator,
        disjoint_sorted_files: bool,
        files: &Vec<Arc<FileMetaData>>,
        smallest: &Option<Slice>,
        largest: &Option<Slice>,
    ) -> bool {
        let cmp = icmp.user_comparator();
        if !disjoint_sorted_files {
            // Need to check against all files
            for file in files {
                if before_file(cmp.clone(), largest, file)
                    || after_file(cmp.clone(), smallest, file)
                {
                    // No overlap
                    continue;
                }
                return true;
            }
            false
        } else {
            // Binary search over file list
            let mut index = 0;
            if let Some(k) = smallest {
                index = match files.binary_search_by(|f| cmp.compare(&k, &f.largest.user_key())) {
                    Ok(index) => index,
                    Err(index) => index,
                }
            }

            if index > files.len() {
                false
            } else {
                !before_file(cmp, largest, &files[index])
            }
        }
    }
}

fn before_file(
    ucmp: Rc<dyn Comparator<Slice>>,
    user_key: &Option<Slice>,
    file: &Arc<FileMetaData>,
) -> bool {
    if let Some(k) = user_key {
        ucmp.compare(&k, &file.smallest.user_key()) == Ordering::Less
    } else {
        false
    }
}

fn after_file(
    ucmp: Rc<dyn Comparator<Slice>>,
    user_key: &Option<Slice>,
    file: &Arc<FileMetaData>,
) -> bool {
    if let Some(k) = user_key {
        // null user_key occurs after all keys and is therefore never before *f
        ucmp.compare(&k, &file.largest.user_key()) == Ordering::Greater
    } else {
        // null user_key occurs before all keys and is therefore never after *f
        false
    }
}

pub struct VersionSet<E: Env> {
    table_cache: TableCache<E>,
    versions: LinkedList<Arc<Version>>,
    last_sequence: SequenceNumber,
}

impl<E: Env> VersionSet<E> {
    pub fn current(&self) -> Arc<Version> {
        self.versions.front().unwrap().clone()
    }

    pub fn get_last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    pub fn set_last_sequence(&mut self, n: SequenceNumber) {
        self.last_sequence = n;
    }
}

const COMPARATOR: u32 = 1;
const LOG_NUMBER: u32 = 2;
const NEXT_FILE_NUMBER: u32 = 3;
const LAST_SEQUENCE: u32 = 4;
const COMPACTION_POINTER: u32 = 5;
const DELETED_FILES: u32 = 6;
const NEW_FILE: u32 = 7;
// 8 was used for large value refs
const PREV_LOG_NUMBER: u32 = 9;

struct VersionEdit {
    comparator: String,
    log_number: u64,
    prev_log_number: u64,
    next_file_number: u64,
    last_sequence: SequenceNumber,
    has_comparator: bool,
    has_log_number: bool,
    has_prev_log_number: bool,
    has_next_file_number: bool,
    has_last_sequence: bool,

    compact_pointers: Vec<(u32, InternalKey)>,
    deleted_files: Vec<(u32, u64)>,
    new_files: Vec<(u32, FileMetaData)>,
}

impl Default for VersionEdit {
    fn default() -> Self {
        VersionEdit {
            comparator: String::default(),
            log_number: 0,
            prev_log_number: 0,
            next_file_number: 0,
            last_sequence: 0,
            has_comparator: false,
            has_log_number: false,
            has_prev_log_number: false,
            has_next_file_number: false,
            has_last_sequence: false,
            compact_pointers: Vec::new(),
            deleted_files: Vec::new(),
            new_files: Vec::new(),
        }
    }
}

impl VersionEdit {
    pub fn clear(&mut self) {
        self.comparator.clear();
        self.log_number = 0;
        self.prev_log_number = 0;
        self.next_file_number = 0;
        self.last_sequence = 0;
        self.has_comparator = false;
        self.has_log_number = false;
        self.has_prev_log_number = false;
        self.has_next_file_number = false;
        self.has_last_sequence = false;
        self.compact_pointers.clear();
        self.deleted_files.clear();
        self.new_files.clear();
    }

    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        if self.has_comparator {
            put_varint32(dst, COMPARATOR);
            put_varint32(dst, self.comparator.as_bytes().len() as u32);
            dst.write_all(self.comparator.as_bytes()).unwrap();
        }
        if self.has_log_number {
            put_varint32(dst, LOG_NUMBER);
            put_varint64(dst, self.log_number);
        }
        if self.has_prev_log_number {
            put_varint32(dst, PREV_LOG_NUMBER);
            put_varint64(dst, self.prev_log_number);
        }
        if self.has_next_file_number {
            put_varint32(dst, NEXT_FILE_NUMBER);
            put_varint64(dst, self.next_file_number);
        }
        if self.has_last_sequence {
            put_varint32(dst, LAST_SEQUENCE);
            put_varint64(dst, self.last_sequence);
        }
        for (n, k) in self.compact_pointers.iter() {
            put_varint32(dst, COMPACTION_POINTER);
            put_varint32(dst, *n);
            let key = k.encode();
            put_varint32(dst, key.size() as u32);
            dst.write_all(key.as_ref());
        }
        for (n, m) in self.deleted_files.iter() {
            put_varint32(dst, DELETED_FILES);
            put_varint32(dst, *n);
            put_varint64(dst, *m);
        }
        for (n, f) in self.new_files.iter() {
            put_varint32(dst, NEW_FILE);
            put_varint32(dst, *n);
            put_varint64(dst, f.number);
            put_varint64(dst, f.file_size);
            let (small, large) = (f.smallest.encode(), f.largest.encode());
            put_varint32(dst, small.size() as u32);
            dst.write_all(small.as_ref());
            put_varint32(dst, large.size() as u32);
            dst.write_all(large.as_ref());
        }
    }

    pub fn decode_from(&mut self, mut src: &[u8]) -> Result<()> {
        self.clear();
        let mut level: u32 = 0;
        let mut msg = None;
        let mut file = FileMetaData::default();
        let mut key = InternalKey::default();

        while msg.is_none() && !src.is_empty() {
            if let Ok(tag) = src.decode_varint32() {
                match tag {
                    COMPARATOR => {
                        if let Ok(s) = src.get_length_prefixed_slice() {
                            self.comparator = String::from_utf8_lossy(s).to_string();
                            self.has_comparator = true;
                        } else {
                            msg = Some(String::from("compation pointer"));
                        }
                    }

                    LOG_NUMBER => {
                        if let Ok(n) = src.decode_varint64() {
                            self.log_number = n;
                            self.has_log_number = true;
                        } else {
                            msg = Some(String::from("log number"));
                        }
                    }

                    PREV_LOG_NUMBER => {
                        if let Ok(n) = src.decode_varint64() {
                            self.prev_log_number = n;
                            self.has_prev_log_number = true;
                        } else {
                            msg = Some(String::from("prev log number"));
                        }
                    }

                    NEXT_FILE_NUMBER => {
                        if let Ok(n) = src.decode_varint64() {
                            self.next_file_number = n;
                            self.has_next_file_number = true;
                        } else {
                            msg = Some(String::from("next file number"));
                        }
                    }

                    LAST_SEQUENCE => {
                        if let Ok(n) = src.decode_varint64() {
                            self.last_sequence = n;
                            self.has_last_sequence = true;
                        } else {
                            msg = Some(String::from("last sequence"));
                        }
                    }

                    COMPACTION_POINTER => {
                        if get_level(&mut src, &mut level).is_ok()
                            && get_internal_key(&mut src, &mut key).is_ok()
                        {
                            self.compact_pointers.push((level, key.clone()))
                        } else {
                            msg = Some(String::from("compaction pointer"));
                        }
                    }

                    DELETED_FILES => {
                        let (level_ret, num_res) =
                            (get_level(&mut src, &mut level), src.decode_varint64());
                        if level_ret.is_ok() && num_res.is_ok() {
                            self.deleted_files.push((level, num_res.unwrap()));
                        } else {
                            msg = Some(String::from("deleted files"));
                        }
                    }

                    NEW_FILE => {
                        let level_res = get_level(&mut src, &mut level);
                        let num_res = src.decode_varint64();
                        let size_res = src.decode_varint64();
                        let small_res = get_internal_key(&mut src, &mut file.smallest);
                        let large_res = get_internal_key(&mut src, &mut file.largest);
                        if level_res.is_ok()
                            && num_res.is_ok()
                            && size_res.is_ok()
                            && small_res.is_ok()
                            && large_res.is_ok()
                        {
                            file.number = num_res.unwrap();
                            file.file_size = size_res.unwrap();
                            self.new_files.push((level, file.clone()))
                        } else {
                            msg = Some(String::from("new files"));
                        }
                    }

                    _ => {
                        msg = Some(String::from("unknown tag"));
                    }
                }
            } else {
                break;
            }
        }

        if msg.is_none() && !src.is_empty() {
            msg = Some("invalid tag".to_string());
        }

        if let Some(s) = msg {
            Err(StatusError::Corruption(format!("VersionEdit {}", s)))
        } else {
            Ok(())
        }
    }

    pub fn set_comparator_name(&mut self, name: &String) {
        self.has_comparator = true;
        self.comparator = name.clone();
    }

    pub fn set_log_number(&mut self, num: u64) {
        self.has_log_number = true;
        self.log_number = num;
    }

    pub fn set_prev_log_number(&mut self, num: u64) {
        self.has_prev_log_number = true;
        self.prev_log_number = num;
    }

    pub fn set_next_file(&mut self, num: u64) {
        self.has_next_file_number = true;
        self.next_file_number = num;
    }

    pub fn set_last_sequence(&mut self, seq: SequenceNumber) {
        self.has_last_sequence = true;
        self.last_sequence = seq;
    }

    pub fn set_compact_pointer(&mut self, level: u32, key: InternalKey) {
        self.compact_pointers.push((level, key));
    }

    // Add the specified file at the specified number.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    pub fn add_file(
        &mut self,
        level: u32,
        file: u64,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
    ) {
        let mut f = FileMetaData::default();
        f.number = file;
        f.file_size = file;
        f.smallest = smallest;
        f.largest = largest;
        self.new_files.push((level, f));
    }

    // Delete the specified "file" from the specified "level".
    pub fn delete_file(&mut self, level: u32, file: u64) {
        self.deleted_files.push((level, file));
    }
}

fn get_level(src: &mut &[u8], level: &mut u32) -> Result<()> {
    let l = (*src).decode_varint32()?;
    if l < config::NUM_LEVELS as u32 {
        *level = l;
        Ok(())
    } else {
        Err(StatusError::Corruption(
            "level larger than configed max".to_string(),
        ))
    }
}

fn get_internal_key(src: &mut &[u8], dst: &mut InternalKey) -> Result<()> {
    let data = (*src).get_length_prefixed_slice()?;
    if !dst.decode_from(data.into()) {
        Err(StatusError::Corruption(
            "internal key decode failed".to_string(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use failure::_core::hint::unreachable_unchecked;

    fn encode_decode(edit: &VersionEdit) {
        let mut encoded = Vec::new();
        let mut encoded2 = Vec::new();
        edit.encode_to(&mut encoded);
        let mut parsed = VersionEdit::default();
        parsed.decode_from(encoded.as_slice()).unwrap();
        parsed.encode_to(&mut encoded2);
        assert_eq!(encoded, encoded2);
    }

    #[test]
    fn test_encode_decode() {
        let big = 1u64 << 50;
        let mut edit = VersionEdit::default();
        for i in 0..4 {
            encode_decode(&edit);
            edit.add_file(
                3,
                big + 300 + i,
                big + 400 + i,
                InternalKey::new("foo".as_bytes().into(), big + 500 + i, TYPE_VALUE),
                InternalKey::new("zoo".as_bytes().into(), big + 600 + i, TYPE_DELETION),
            );
            edit.delete_file(4, big + 700 + i);
            edit.set_compact_pointer(
                i as u32,
                InternalKey::new("x".as_bytes().into(), big + 900 + i, TYPE_VALUE),
            );
        }

        edit.set_comparator_name(&"foo".to_string());
        edit.set_log_number(big + 100);
        edit.set_next_file(big + 200);
        edit.set_last_sequence(big + 1000);
        encode_decode(&edit);
    }
}
