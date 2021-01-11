use crate::db::dbformat::{
    config, parse_internal_key, InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey,
    SequenceNumber, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK,
};
use crate::db::dbformat::{TYPE_DELETION, TYPE_VALUE};
use crate::db::error::{Result, StatusError};
use crate::db::slice::Slice;
use crate::db::table_cache::TableCache;
use crate::db::{ReadOption, Reporter};

use crate::env::{read_file_to_vec, Env};
use crate::util::cache::Cache;
use crate::util::cmp::Comparator;

use crate::db::filename::FileType::CurrentFile;
use crate::db::filename::{current_file_name, descriptor_file_name, set_current_file};
use crate::db::log::{LogReader, LogWriter};
use crate::db::option::Options;
use crate::util::buffer::BufferReader;
use crate::util::coding::{put_varint32, put_varint64, DecodeVarint, VarLengthSliceReader};

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::hash_set::Difference;
use std::collections::linked_list::LinkedList;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::fs::Metadata;
use std::io::Write;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct FileMetaData {
    pub allowed_seeks: i32,
    pub number: u64,
    pub file_size: u64,
    pub smallest: InternalKey,
    pub largest: InternalKey,
}

pub struct CompactionState {
    pub micros: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl CompactionState {
    pub fn add(&mut self, c: &CompactionState) {
        self.micros += c.micros;
        self.bytes_read += c.bytes_read;
        self.bytes_written += c.bytes_written;
    }
}

impl Default for CompactionState {
    fn default() -> Self {
        CompactionState {
            micros: 0,
            bytes_read: 0,
            bytes_written: 0,
        }
    }
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

struct ReporterStatus(Option<StatusError>);

#[derive(Clone)]
struct LogReporter {
    inner: Rc<RefCell<ReporterStatus>>,
}

impl LogReporter {
    fn new() -> Self {
        LogReporter {
            inner: Rc::new(RefCell::new(ReporterStatus(None))),
        }
    }

    fn result(&self) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.0.is_some() {
            Err(inner.0.take().unwrap())
        } else {
            Ok(())
        }
    }
}

impl Reporter for LogReporter {
    fn corruption(&mut self, n: usize, status: StatusError) {
        let mut innner = self.inner.borrow_mut();
        if innner.0.is_none() {
            innner.0 = Some(status);
        }
    }
}

pub struct Version<E: Env> {
    // List of files per level
    table_cache: TableCache<E>,
    options: Arc<Options>,
    files: Vec<Vec<Arc<FileMetaData>>>,
    file_to_compact: RwLock<Option<(Arc<FileMetaData>, usize)>>,
    icmp: InternalKeyComparator,
    compaction_score: f64,
    compaction_level: i32,
}

impl<E: Env> Version<E> {
    pub fn new(
        icmp: InternalKeyComparator,
        options: Arc<Options>,
        table_cache: TableCache<E>,
    ) -> Self {
        let mut files = Vec::with_capacity(config::NUM_LEVELS);
        files.resize(config::NUM_LEVELS, Vec::new());
        Version {
            table_cache,
            options,
            files,
            file_to_compact: RwLock::new(None),
            icmp,
            compaction_score: -1f64,
            compaction_level: -1,
        }
    }

    pub fn get(
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
            let cmp_big = ucmp.compare(&user_key, &f.largest.user_key());
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

            let index = find_file(&self.icmp, &self.files[i], internal_key);
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

    pub fn overlap_in_level(
        &self,
        level: usize,
        smallest: &Option<Slice>,
        largest: &Option<Slice>,
    ) -> bool {
        some_file_overlaps_range(&self.icmp, level > 0, &self.files[level], smallest, largest)
    }

    pub fn get_overlapping_inputs(
        &self,
        level: usize,
        begin: &Option<InternalKey>,
        end: &Option<InternalKey>,
        input: &mut Vec<Arc<FileMetaData>>,
    ) {
        assert!(level < config::NUM_LEVELS);
        input.clear();

        let user_cmp = self.icmp.user_comparator();
        let mut user_begin = begin.as_ref().map(|k| k.user_key());
        let mut user_end = end.as_ref().map(|k| k.user_key());
        let mut i = 0;
        while i < self.files[level].len() {
            let f = &self.files[level][i];
            i += 1;
            if !before_file(&user_cmp, &user_end, f) && !after_file(&user_cmp, &user_begin, f) {
                input.push(f.clone());
                if level == 0 {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search.
                    if begin.is_some()
                        && user_cmp.compare(&f.smallest.user_key(), user_begin.as_ref().unwrap())
                            == Ordering::Less
                    {
                        i = 0;
                        input.clear();
                        user_begin = user_begin.map(|_| f.smallest.user_key());
                    } else if end.is_some()
                        && user_cmp.compare(&f.largest.user_key(), user_end.as_ref().unwrap())
                            == Ordering::Greater
                    {
                        i = 0;
                        input.clear();
                        user_end = user_end.map(|_| f.largest.user_key());
                    }
                }
            }
        }
    }

    pub fn pick_level_for_memtable_output(
        &self,
        smallest_user_key: &Option<Slice>,
        largest_user_key: &Option<Slice>,
    ) -> usize {
        let mut level = 0;
        if !self.overlap_in_level(0, smallest_user_key, largest_user_key) {
            // Push to next level if there is no overlap in next level,
            // and the #bytes overlapping in the level after that are limited.
            let start = smallest_user_key
                .as_ref()
                .map(|s| InternalKey::new(s.clone(), MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK));
            let limit = smallest_user_key
                .as_ref()
                .map(|s| InternalKey::new(s.clone(), 0, 0));
            let mut overlaps = Vec::new();
            while level < config::MAX_MEM_COMPACT_LEVEL {
                if self.overlap_in_level(level + 1, smallest_user_key, largest_user_key) {
                    break;
                }

                if level + 2 < config::NUM_LEVELS {
                    self.get_overlapping_inputs(level + 2, &start, &limit, &mut overlaps);
                    let sum = total_size(&overlaps);
                    if sum > grand_parent_overlap_bytes(&self.options) as u64 {
                        break;
                    }
                }

                level += 1;
            }
        }

        level
    }

    pub fn update_stats(&self, meta: Arc<FileMetaData>, level: usize) -> bool {
        let mut to_compact = self.file_to_compact.write().unwrap();
        if meta.allowed_seeks - 1 <= 0 && to_compact.is_none() {
            *to_compact = Some((meta, level));
            true
        } else {
            false
        }
    }
}

fn grand_parent_overlap_bytes(options: &Arc<Options>) -> usize {
    10 * target_file_size(options)
}

fn find_file(icmp: &InternalKeyComparator, files: &Vec<Arc<FileMetaData>>, key: Slice) -> usize {
    match files.binary_search_by(|f| icmp.compare(&f.largest.encode(), &key)) {
        Ok(index) => index,
        Err(index) => index,
    }
}

fn some_file_overlaps_range(
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
            if before_file(&cmp, largest, file) || after_file(&cmp, smallest, file) {
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
            index = match files.binary_search_by(|f| cmp.compare(&f.largest.user_key(), &k)) {
                Ok(index) => index,
                Err(index) => index,
            }
        }

        if index >= files.len() {
            false
        } else {
            !before_file(&cmp, largest, &files[index])
        }
    }
}

fn before_file(
    ucmp: &Arc<dyn Comparator<Slice>>,
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
    ucmp: &Arc<dyn Comparator<Slice>>,
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

fn total_file_size(files: &Vec<Arc<FileMetaData>>) -> u64 {
    let mut sum = 0;
    files.iter().for_each(|f| sum += f.file_size);
    sum
}

fn max_grand_parent_overlap_bytes(options: &Arc<Options>) -> usize {
    10 * target_file_size(options)
}

fn target_file_size(options: &Arc<Options>) -> usize {
    options.max_file_size
}

pub struct VersionSet<E: Env> {
    env: E,
    db_name: String,
    table_cache: TableCache<E>,
    options: Arc<Options>,
    icmp: InternalKeyComparator,
    last_sequence: SequenceNumber,
    next_file_number: u64,
    manifest_file_number: u64,
    log_number: u64,
    prev_log_number: u64,

    versions: LinkedList<Arc<Version<E>>>,

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    compact_pointer: Vec<Vec<u8>>,
    // manifest log for version_set
    descriptor_log: Option<LogWriter<E::WrFile>>,

    pub pending_outputs: HashSet<u64>,
    pub stats: Vec<CompactionState>,
}

impl<E: Env> VersionSet<E> {
    pub fn new(
        env: E,
        db_name: String,
        options: Arc<Options>,
        table_cache: TableCache<E>,
        icmp: InternalKeyComparator,
    ) -> Self {
        let mut compact_pointer = Vec::with_capacity(config::NUM_LEVELS);
        let mut stats = Vec::with_capacity(config::NUM_LEVELS);
        for _ in 0..config::NUM_LEVELS {
            compact_pointer.push(Vec::new());
            stats.push(CompactionState::default());
        }
        let mut version_set = VersionSet {
            env,
            db_name,
            table_cache,
            options,
            icmp,
            next_file_number: 2,
            last_sequence: 0,
            log_number: 0,
            prev_log_number: 0,
            manifest_file_number: 0,
            descriptor_log: None,
            versions: LinkedList::new(),
            compact_pointer,
            pending_outputs: HashSet::new(),
            stats,
        };

        let v = Version::new(
            version_set.icmp.clone(),
            version_set.options.clone(),
            version_set.table_cache.clone(),
        );
        version_set.versions.push_front(Arc::new(v));
        version_set
    }
    pub fn current(&self) -> Arc<Version<E>> {
        self.versions.front().unwrap().clone()
    }

    pub fn get_last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    pub fn set_last_sequence(&mut self, n: SequenceNumber) {
        self.last_sequence = n;
    }

    // Return the current manifest file number
    pub fn manifest_file_number(&mut self) -> u64 {
        self.manifest_file_number
    }

    // Allocate and return a new file number
    pub fn new_file_number(&mut self) -> u64 {
        let ret = self.next_file_number;
        self.next_file_number += 1;
        ret
    }

    // Arrange to reuse "file_number" unless a newer file number has
    // already been allocated.
    // REQUIRES: "file_number" was returned by a call to NewFileNumber().
    pub fn reuse_file_number(&mut self, file_number: u64) {
        if self.next_file_number == file_number + 1 {
            self.next_file_number = file_number;
        }
    }

    pub fn mark_file_number_used(&mut self, number: u64) {
        if self.next_file_number <= number {
            self.next_file_number = number + 1;
        }
    }

    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    pub fn prev_log_number(&self) -> u64 {
        self.prev_log_number
    }

    pub fn recover(&mut self) -> Result<bool> {
        let mut current: Vec<u8> = Vec::new();
        read_file_to_vec(
            self.env.clone(),
            &current_file_name(&self.db_name),
            &mut current,
        )?;
        let mut current_name = String::from_utf8(current)?;
        if current_name.is_empty() || !current_name.ends_with("\n") {
            return Err(StatusError::Corruption(
                "CURRENT file does not end with newline".to_string(),
            ));
        }

        current_name.truncate(current_name.len() - 1);
        let mut dscname = String::from(self.db_name.as_str());
        dscname.push('/');
        dscname.push_str(current_name.as_str());

        let mut file = self.env.new_sequential_file(&dscname)?;
        let mut have_log_number = false;
        let mut have_prev_log_number = false;
        let mut have_next_file = false;
        let mut have_last_sequence = false;
        let mut next_file = 0;
        let mut last_sequence = 0;
        let mut log_number = 0;
        let mut prev_log_number = 0;
        let icmp = self.icmp.clone();
        let mut builder = Builder::new(self.current(), icmp.clone());

        let mut reporter = LogReporter::new();
        let mut reader = LogReader::new(file, reporter.clone());
        let mut record = Vec::new();
        while reader.read_record(&mut record).is_ok() {
            let mut edit = VersionEdit::default();
            edit.decode_from(record.as_slice())?;
            if edit.has_comparator && edit.comparator.as_str() == icmp.user_comparator().name() {
                return Err(StatusError::InvalidArgument(format!(
                    "{} does not match existing comparator {}",
                    edit.comparator,
                    icmp.user_comparator().name()
                )));
            }

            builder.apply(&edit, &mut self.compact_pointer);

            if edit.has_log_number {
                log_number = edit.log_number;
                have_log_number = true;
            }

            if edit.has_prev_log_number {
                prev_log_number = edit.prev_log_number;
                have_prev_log_number = true;
            }

            if edit.has_next_file_number {
                next_file = edit.next_file_number;
                have_next_file = true;
            }

            if edit.has_last_sequence {
                last_sequence = edit.last_sequence;
                have_last_sequence = true;
            }
        }

        let mut res = reporter.result();
        if res.is_ok() {
            if !have_next_file {
                res = Err(StatusError::Corruption(
                    "no meta-nextfile entry in descriptor".to_string(),
                ))
            } else if !have_log_number {
                res = Err(StatusError::Corruption(
                    "no meta-lognumber entry in descriptor".to_string(),
                ))
            } else if have_last_sequence {
                res = Err(StatusError::Corruption(
                    "no last-sequence-number entry in descriptor".to_string(),
                ))
            }

            if !have_prev_log_number {
                prev_log_number = 0;
            }

            self.mark_file_number_used(prev_log_number);
            self.mark_file_number_used(log_number);
        }

        if res.is_ok() {
            let mut v = Version::new(icmp, self.options.clone(), self.table_cache.clone());
            builder.save_to(&mut v);
            // Install recovered version
            self.finalize(&mut v);
            self.versions.push_front(Arc::new(v));
            self.manifest_file_number = next_file;
            self.next_file_number = next_file + 1;
            self.last_sequence = last_sequence;
            self.log_number = log_number;
            self.prev_log_number = prev_log_number;
        }

        if res.is_err() {
            return Err(res.unwrap_err());
        }

        // See if we can reuse the existing MANIFEST file.
        let mut save_manifest = false;
        if self.reuse_manifest(&dscname, &current_name) {
            save_manifest = true;
        }

        Ok(save_manifest)
    }

    fn reuse_manifest(&mut self, dscname: &String, dscbase: &String) -> bool {
        false
    }

    fn finalize(&self, v: &mut Version<E>) {
        // Precomputed best level for next compaction
        let mut best_level = -1;
        let mut best_score = -1f64;
        for level in 0..config::NUM_LEVELS - 1 {
            let mut score = 0f64;
            if level == 0 {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:
                //
                // (1) With larger write-buffer sizes, it is nice not to do too
                // many level-0 compactions.
                //
                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer
                // setting, or very high compression ratios, or lots of
                // overwrites/deletions).
                score = (v.files.len() / config::L0_COMPACTION_TRIGGER) as f64;
            } else {
                let level_bytes = total_size(&v.files[level]);
                score = level_bytes as f64 / max_bytes_for_level(level);
            }

            if score > best_score {
                best_score = score;
                best_level = level as i32;
            }
        }

        v.compaction_level = best_level;
        v.compaction_score = best_score;
    }

    pub fn add_live_files(&self, live: &mut HashSet<u64>) {
        for v in self.versions.iter() {
            for level in v.files.iter() {
                for f in level.iter() {
                    live.insert(f.number);
                }
            }
        }
    }

    pub fn log_and_apply(&mut self, edit: &mut VersionEdit) -> Result<()> {
        if edit.has_log_number {
            assert!(edit.log_number >= self.log_number);
            assert!(edit.log_number < self.next_file_number);
        } else {
            edit.set_prev_log_number(self.log_number);
        }

        if !edit.has_prev_log_number {
            edit.set_prev_log_number(self.prev_log_number);
        }

        edit.set_next_file(self.next_file_number);
        edit.set_last_sequence(self.prev_log_number);

        let mut v = Version::new(
            self.icmp.clone(),
            self.options.clone(),
            self.table_cache.clone(),
        );
        let mut builder = Builder::new(self.current(), self.icmp.clone());
        builder.apply(edit, &mut self.compact_pointer);
        builder.save_to(&mut v);
        self.finalize(&mut v);

        // Initialize new descriptor log file if necessary by creating
        // a temporary file that contains a snapshot of the current version.
        let mut create_new_manifest = false;
        if self.descriptor_log.is_none() {
            create_new_manifest = true;
            let manifest_name = descriptor_file_name(&self.db_name, self.manifest_file_number);
            let manifest_file = self.env.new_writable_file(&manifest_name)?;
            let mut writer = LogWriter::new(manifest_file);
            match self.write_snapshot(&mut writer) {
                Ok(_) => self.descriptor_log = Some(writer),
                Err(e) => {
                    self.env.delete_file(&manifest_name)?;
                    return Err(e);
                }
            }
        }

        // Write new record to MANIFEST log
        let mut record = Vec::new();
        edit.encode_to(&mut record);
        let mut writer = self.descriptor_log.as_mut().unwrap();
        writer.add_record(record.as_slice())?;
        writer.sync()?;

        // If we just created a new descriptor file, install it by writing a
        // new CURRENT file that points to it.
        if create_new_manifest {
            set_current_file(self.env.clone(), &self.db_name, self.manifest_file_number)?;
        }

        self.versions.push_front(Arc::new(v));

        Ok(())
    }

    fn write_snapshot(&self, writer: &mut LogWriter<E::WrFile>) -> Result<()> {
        // Save metadata
        let mut edit = VersionEdit::default();
        edit.set_comparator_name(&self.icmp.user_comparator().name().to_string());

        // Save compaction pointers
        for (i, c) in self.compact_pointer.iter().enumerate() {
            if !c.is_empty() {
                let mut key = InternalKey::new_empty_key();
                key.decode_from(c.as_slice().into());
                edit.set_compact_pointer(i as u32, key);
            }
        }

        // Save files
        for (i, files) in self.current().files.iter().enumerate() {
            for f in files.iter() {
                edit.add_file(
                    i as u32,
                    f.number,
                    f.file_size,
                    f.smallest.clone(),
                    f.largest.clone(),
                );
            }
        }

        let mut record = Vec::new();
        edit.encode_to(&mut record);
        writer.add_record(record.as_slice())
    }
}

fn total_size(files: &Vec<Arc<FileMetaData>>) -> u64 {
    let mut sum = 0;
    files.iter().for_each(|f| sum += f.file_size);
    sum
}

fn max_bytes_for_level(mut level: usize) -> f64 {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    let mut result = 10f64 * 1048576.0f64;
    while level > 1 {
        result *= 10f64;
        level -= 1;
    }

    result
}

struct Builder<E: Env> {
    base: Arc<Version<E>>,
    icmp: InternalKeyComparator,
    deleted_files: Vec<HashSet<u64>>,
    added_files: Vec<Vec<Arc<FileMetaData>>>,
}

impl<E: Env> Builder<E> {
    fn new(base: Arc<Version<E>>, icmp: InternalKeyComparator) -> Self {
        let mut deleted_files = Vec::with_capacity(config::NUM_LEVELS);
        for _ in 0..config::NUM_LEVELS {
            deleted_files.push(HashSet::new())
        }
        let mut added_files = Vec::with_capacity(config::NUM_LEVELS);
        for _ in 0..config::NUM_LEVELS {
            added_files.push(Vec::new())
        }

        Builder {
            base,
            icmp,
            deleted_files,
            added_files,
        }
    }

    fn apply(&mut self, edit: &VersionEdit, compact_pointers: &mut Vec<Vec<u8>>) {
        for (level, key) in edit.compact_pointers.iter() {
            let v = &mut compact_pointers[*level as usize];
            v.clear();
            v.extend_from_slice(key.encode().as_ref());
        }

        for (level, f) in edit.deleted_files.iter() {
            let deleted = &mut self.deleted_files[*level as usize];
            deleted.insert(*f);
        }

        for (level, file) in edit.new_files.iter() {
            let mut f = file.clone();
            // We arrange to automatically compact this file after
            // a certain number of seeks.  Let's assume:
            //   (1) One seek costs 10ms
            //   (2) Writing or reading 1MB costs 10ms (100MB/s)
            //   (3) A compaction of 1MB does 25MB of IO:
            //         1MB read from this level
            //         10-12MB read from next level (boundaries may be misaligned)
            //         10-12MB written to next level
            // This implies that 25 seeks cost the same as the compaction
            // of 1MB of data.  I.e., one seek costs approximately the
            // same as the compaction of 40KB of data.  We are a little
            // conservative and allow approximately one seek for every 16KB
            // of data before triggering a compaction.
            f.allowed_seeks = (f.file_size / 16384) as i32;
            if f.allowed_seeks < 100 {
                f.allowed_seeks = 100;
            }

            self.deleted_files[*level as usize].remove(&f.number);
            let a = &mut self.added_files[*level as usize];
            a.push(Arc::new(f));
        }
    }

    fn save_to(&mut self, v: &mut Version<E>) {
        let cmp = self.icmp.clone();
        for x in self.added_files.iter_mut() {
            x.sort_by(|f1, f2| cmp.compare(&f1.smallest.encode(), &f2.smallest.encode()))
        }

        for level in 0..config::NUM_LEVELS {
            // Merge the set of added files with the set of pre-existing files.
            // Drop any deleted files.  Store the result in *v.
            let base_files = &self.base.files[level];
            let (mut add_iter, mut base_iter) = (
                self.added_files[level].iter().peekable(),
                base_files.iter().peekable(),
            );
            while let Some(add_file) = add_iter.next() {
                while let Some(&base_file) = base_iter.peek() {
                    if cmp.compare(&base_file.smallest.encode(), &add_file.smallest.encode())
                        == Ordering::Less
                    {
                        self.maybe_add_file(v, level, base_file.clone());
                        base_iter.next();
                    } else {
                        break;
                    }
                }
                self.maybe_add_file(v, level, add_file.clone());
            }
            base_iter.for_each(|f| self.maybe_add_file(v, level, f.clone()));
        }
    }

    fn maybe_add_file(&self, v: &mut Version<E>, level: usize, f: Arc<FileMetaData>) {
        // if file is deleted, do nothing
        if !self.deleted_files[level].contains(&f.number) {
            let last = &mut v.files[level].last();
            if level != 0 && last.is_some() {
                let last = last.unwrap();
                assert_eq!(
                    self.icmp
                        .compare(&last.largest.encode(), &f.smallest.encode()),
                    Ordering::Less
                );
            }
            v.files[level].push(f)
        }
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

pub struct VersionEdit {
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
    use crate::util::cmp::BitWiseComparator;
    use std::io::Read;

    struct FindFileTest {
        disjoint_sorted_files: bool,
        files: Vec<Arc<FileMetaData>>,
    }

    impl FindFileTest {
        fn new() -> Self {
            FindFileTest {
                disjoint_sorted_files: true,
                files: Vec::new(),
            }
        }

        fn add(
            &mut self,
            smallest: Slice,
            largest: Slice,
            smallest_seq: SequenceNumber,
            largest_seq: SequenceNumber,
        ) {
            let mut f = FileMetaData::default();
            f.smallest = InternalKey::new(smallest, smallest_seq, TYPE_VALUE);
            f.largest = InternalKey::new(largest, largest_seq, TYPE_VALUE);

            self.files.push(Arc::new(f));
        }

        fn find(&self, key: Slice) -> usize {
            let target = InternalKey::new(key, 100, TYPE_VALUE);
            let cmp = InternalKeyComparator::new(Arc::new(BitWiseComparator {}));
            find_file(&cmp, &self.files, target.encode())
        }

        fn overlaps(&self, smallest: Option<Slice>, largest: Option<Slice>) -> bool {
            let cmp = InternalKeyComparator::new(Arc::new(BitWiseComparator {}));
            some_file_overlaps_range(
                &cmp,
                self.disjoint_sorted_files,
                &self.files,
                &smallest,
                &largest,
            )
        }
    }

    #[test]
    fn test_find_file_empty() {
        let tester = FindFileTest::new();
        assert_eq!(0, tester.find("foo".into()));
        assert!(!tester.overlaps(Some("a".into()), Some("z".into())));
        assert!(!tester.overlaps(None, Some("z".into())));
        assert!(!tester.overlaps(Some("a".into()), None));
        assert!(!tester.overlaps(None, None));
    }

    #[test]
    fn test_find_file_single() {
        let mut tester = FindFileTest::new();
        tester.add("p".into(), "q".into(), 100, 100);
        assert_eq!(0, tester.find("a".into()));
        assert_eq!(0, tester.find("p".into()));
        assert_eq!(0, tester.find("p1".into()));
        assert_eq!(0, tester.find("q".into()));
        assert_eq!(1, tester.find("q1".into()));
        assert_eq!(1, tester.find("z".into()));

        assert!(!tester.overlaps(Some("a".into()), Some("b".into())));
        assert!(!tester.overlaps(Some("z1".into()), Some("z2".into())));
        assert!(tester.overlaps(Some("a".into()), Some("p".into())));
        assert!(tester.overlaps(Some("a".into()), Some("q".into())));
        assert!(tester.overlaps(Some("a".into()), Some("z".into())));
        assert!(tester.overlaps(Some("p".into()), Some("p1".into())));
        assert!(tester.overlaps(Some("p".into()), Some("q".into())));
        assert!(tester.overlaps(Some("p".into()), Some("z".into())));
        assert!(tester.overlaps(Some("p1".into()), Some("p2".into())));
        assert!(tester.overlaps(Some("p1".into()), Some("z".into())));
        assert!(tester.overlaps(Some("q".into()), Some("q".into())));
        assert!(tester.overlaps(Some("q".into()), Some("q1".into())));

        assert!(!tester.overlaps(None, Some("j".into())));
        assert!(!tester.overlaps(Some("r".into()), None));
        assert!(tester.overlaps(None, Some("p".into())));
        assert!(tester.overlaps(None, Some("p1".into())));
        assert!(tester.overlaps(Some("q".into()), None));
        assert!(tester.overlaps(None, None));
    }

    #[test]
    fn test_find_file_multiple() {
        let mut tester = FindFileTest::new();
        tester.add("150".into(), "200".into(), 100, 100);
        tester.add("200".into(), "250".into(), 100, 100);
        tester.add("300".into(), "350".into(), 100, 100);
        tester.add("400".into(), "450".into(), 100, 100);

        assert_eq!(0, tester.find("100".into()));
        assert_eq!(0, tester.find("150".into()));
        assert_eq!(0, tester.find("151".into()));
        assert_eq!(0, tester.find("199".into()));
        assert_eq!(0, tester.find("200".into()));
        assert_eq!(1, tester.find("201".into()));
        assert_eq!(1, tester.find("249".into()));
        assert_eq!(1, tester.find("250".into()));
        assert_eq!(2, tester.find("251".into()));
        assert_eq!(2, tester.find("299".into()));
        assert_eq!(2, tester.find("300".into()));
        assert_eq!(2, tester.find("349".into()));
        assert_eq!(2, tester.find("350".into()));
        assert_eq!(3, tester.find("351".into()));
        assert_eq!(3, tester.find("400".into()));
        assert_eq!(3, tester.find("450".into()));
        assert_eq!(4, tester.find("451".into()));

        assert!(!tester.overlaps(Some("100".into()), Some("149".into())));
        assert!(!tester.overlaps(Some("251".into()), Some("299".into())));
        assert!(!tester.overlaps(Some("451".into()), Some("500".into())));
        assert!(!tester.overlaps(Some("351".into()), Some("399".into())));

        assert!(tester.overlaps(Some("100".into()), Some("150".into())));
        assert!(tester.overlaps(Some("100".into()), Some("200".into())));
        assert!(tester.overlaps(Some("100".into()), Some("300".into())));
        assert!(tester.overlaps(Some("100".into()), Some("400".into())));
        assert!(tester.overlaps(Some("100".into()), Some("500".into())));
        assert!(tester.overlaps(Some("375".into()), Some("400".into())));
        assert!(tester.overlaps(Some("450".into()), Some("450".into())));
        assert!(tester.overlaps(Some("450".into()), Some("500".into())));
    }

    #[test]
    fn test_find_file_multiple_null_boundaries() {
        let mut tester = FindFileTest::new();
        tester.add("150".into(), "200".into(), 100, 100);
        tester.add("200".into(), "250".into(), 100, 100);
        tester.add("300".into(), "350".into(), 100, 100);
        tester.add("400".into(), "450".into(), 100, 100);

        assert!(!tester.overlaps(None, Some("149".into())));
        assert!(!tester.overlaps(Some("451".into()), None));
        assert!(tester.overlaps(None, None));
        assert!(tester.overlaps(None, Some("150".into())));
        assert!(tester.overlaps(None, Some("150".into())));
        assert!(tester.overlaps(None, Some("199".into())));
        assert!(tester.overlaps(None, Some("200".into())));
        assert!(tester.overlaps(None, Some("201".into())));
        assert!(tester.overlaps(None, Some("400".into())));
        assert!(tester.overlaps(None, Some("800".into())));

        assert!(tester.overlaps(Some("100".into()), None));
        assert!(tester.overlaps(Some("200".into()), None));
        assert!(tester.overlaps(Some("449".into()), None));
        assert!(tester.overlaps(Some("450".into()), None));
    }

    #[test]
    fn test_find_file_overlap_sequence_checks() {
        let mut tester = FindFileTest::new();
        tester.add("200".into(), "200".into(), 5000, 3000);
        assert!(!tester.overlaps(Some("199".into()), Some("199".into())));
        assert!(!tester.overlaps(Some("201".into()), Some("300".into())));
        assert!(tester.overlaps(Some("200".into()), Some("200".into())));
        assert!(tester.overlaps(Some("190".into()), Some("200".into())));
        assert!(tester.overlaps(Some("200".into()), Some("210".into())));
    }

    #[test]
    fn test_find_file_overlapping_files() {
        let mut tester = FindFileTest::new();
        tester.add("150".into(), "600".into(), 100, 100);
        tester.add("400".into(), "500".into(), 100, 100);
        tester.disjoint_sorted_files = false;
        assert!(!tester.overlaps(Some("100".into()), Some("149".into())));
        assert!(!tester.overlaps(Some("601".into()), Some("700".into())));
        assert!(tester.overlaps(Some("100".into()), Some("150".into())));
        assert!(tester.overlaps(Some("100".into()), Some("200".into())));
        assert!(tester.overlaps(Some("100".into()), Some("300".into())));
        assert!(tester.overlaps(Some("100".into()), Some("400".into())));
        assert!(tester.overlaps(Some("100".into()), Some("500".into())));
        assert!(tester.overlaps(Some("375".into()), Some("400".into())));
        assert!(tester.overlaps(Some("450".into()), Some("450".into())));
        assert!(tester.overlaps(Some("450".into()), Some("500".into())));
        assert!(tester.overlaps(Some("450".into()), Some("700".into())));
        assert!(tester.overlaps(Some("600".into()), Some("700".into())));
    }

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
