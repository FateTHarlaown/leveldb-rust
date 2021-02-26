use crate::db::builder::build_table;
use crate::db::dbformat::config;
use crate::db::dbformat::{
    append_internal_key, extract_user_key, parse_internal_key, InternalKeyComparator, LookupKey,
    ParsedInternalKey, SequenceNumber, TYPE_DELETION, VALUE_TYPE_FOR_SEEK,
};
use crate::db::error::StatusError::NotFound;
use crate::db::error::{Result, StatusError};
use crate::db::filename::{
    current_file_name, descriptor_file_name, lock_file_name, log_file_name, parse_file_name,
    set_current_file, table_file_name, FileType,
};
use crate::db::log::LogReader;
use crate::db::log::LogWriter;
use crate::db::memtable::MemTable;
use crate::db::option::Options;
use crate::db::table_cache::TableCache;
use crate::db::version::{CompactionState, FileMetaData, Version, VersionEdit, VersionSet};
use crate::db::write_batch::WriteBatch;
use crate::db::Iterator;
use crate::db::{slice::Slice, ReadOption, Reporter, DB};
use crate::db::{WritableFile, WriteOption};
use crate::env::Env;
use crate::sstable::merge::MergingIterator;
use crate::util::cmp::Comparator;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use crossbeam_utils::sync::ShardedLock;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::rc::Rc;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread;
use std::time::Duration;
use std::time::Instant;

const NUM_NON_TABLE_CACHE_FILES: u64 = 10;

pub struct LevelDB<E: Env> {
    inner: Arc<DBImplInner<E>>,
}

impl<E: Env> LevelDB<E> {
    // open a new db
    pub fn open(options: Options, dbname: String, env: E) -> Result<Self> {
        //let db = LevelDB::new(options, dbname.clone(), env.clone());
        let db = DBImplInner::new(options, dbname.clone(), env.clone());
        let mut edit = VersionEdit::default();
        let mut save_manifest = false;
        db.recover(&mut edit, &mut save_manifest)?;

        {
            let mut mem = db.mem.write().unwrap();
            let mut versions = db.versions.lock().unwrap();
            let mut wal = db.wal.lock().unwrap();
            if mem.is_none() {
                // Create new log and a corresponding memtable.
                let new_log_number = versions.new_file_number();
                let file = env.new_writable_file(&log_file_name(&dbname, new_log_number))?;
                wal.log_file_number = new_log_number;
                wal.log = Some(LogWriter::new(file));
                edit.set_log_number(new_log_number);
                *mem = Some(Arc::new(MemTable::new(db.internal_comparator.clone())));
            }

            if save_manifest {
                edit.set_prev_log_number(0);
                edit.set_log_number(wal.log_file_number);
                versions.log_and_apply(&mut edit);
            }
        }

        db.deleted_obsoleted_files();

        let leveldb = LevelDB {
            inner: Arc::new(db),
        };

        leveldb.run_write_worker();
        leveldb.run_compaction_worker();
        leveldb.inner.maybe_schedule_compaction();

        Ok(leveldb)
    }

    fn run_write_worker(&self) {
        let inner = self.inner.clone();
        thread::Builder::new()
            .name("Writer".to_string())
            .spawn(move || {
                loop {
                    let mut queue = inner.batch_write_queue.lock().unwrap();
                    // db closed, clean the write queue.
                    if inner.shutdown.load(Ordering::Acquire) == true {
                        while let Some(task) = queue.pop_front() {
                            match task {
                                BatchTask::Write(w) => {
                                    w.notifier.send(Err(StatusError::DBClose(
                                        "leveldb is closing".to_string(),
                                    )));
                                }
                                _ => {}
                            }
                        }
                        break;
                    }

                    if queue.is_empty() {
                        queue = inner
                            .batch_write_cond
                            .wait_while(queue, |q| q.is_empty())
                            .unwrap();
                    }
                    let first = queue.pop_front().unwrap();
                    let (force_compaction, frist_writer) = match first {
                        BatchTask::Write(w) => (w.batch.is_none(), w),
                        BatchTask::Close => break,
                    };
                    // unlock the queue when make room for write so other writers can add request to the queue
                    std::mem::drop(queue);

                    match inner.make_room_for_write(force_compaction) {
                        Ok(versions) => {
                            let (mut batch, senders, sync) = inner.build_batch_group(frist_writer);
                            if batch.count() > 0 {
                                let mut last_sequence = versions.get_last_sequence();
                                batch.set_sequence(last_sequence + 1);
                                last_sequence += batch.count() as u64;

                                let mut wal = inner.wal.lock().unwrap();
                                let log_writer = wal.log.as_mut().unwrap();
                                let mut res = log_writer.add_record(batch.get_content().as_slice());
                                let mut sync_error = false;
                                if res.is_ok() && sync {
                                    res = log_writer.sync();
                                    sync_error = res.is_err();
                                }
                                if res.is_ok() {
                                    let mem = inner.mem.read().unwrap();
                                    let mem = mem.as_ref().unwrap();
                                    res = batch.insert_into(mem.clone());
                                }

                                match res {
                                    Ok(_) => {
                                        //TODO: log the error
                                        senders
                                            .iter()
                                            .for_each(|s| s.send(Ok(())).unwrap_or_else(|e| {}))
                                    }
                                    Err(e) => {
                                        senders.iter().for_each(|s| {
                                            s.send(Err(StatusError::Customize(
                                                "meet error when write".to_string(),
                                            )))
                                            .unwrap_or_else(|e| {})
                                        });
                                        if sync_error {
                                            inner.record_back_ground_error(e);
                                        }
                                    }
                                }
                            }
                        }

                        Err(e) => frist_writer
                            .notifier
                            .send(Err(StatusError::Customize(format!(
                                "meet error when making room for write request {:?}",
                                e
                            ))))
                            .unwrap_or_else(|e| {}),
                    }
                }
            })
            .unwrap();
    }

    fn run_compaction_worker(&self) {
        let inner = self.inner.clone();
        thread::Builder::new()
            .name("compaction".to_string())
            .spawn(move || {
                while let Ok(()) = inner.compaction_trigger.1.recv() {
                    if inner.shutdown.load(Ordering::Acquire) {
                        break;
                    }

                    if !inner.versions.lock().unwrap().need_compaction()
                        || inner.background_error.read().unwrap().is_some()
                        || inner.imm.read().unwrap().is_none()
                    {
                    } else {
                        inner.background_compaction();
                    }

                    inner.maybe_schedule_compaction();
                    inner.back_ground_work_finish_signal.notify_all();
                }
            })
            .unwrap();
    }
}

/*
impl<E: Env> DB for LevelDB<E> {
    fn get(&self, options: &ReadOption, key: Slice, val: &mut Vec<u8>) -> Result<()> {

        Ok(())
    }

    fn write(&self, options: &WriteOption, updates: Option<WriteBatch>) -> Result<()> {

        Ok(())
    }

    fn new_iterator(&self, options: &ReadOption) -> Result<dyn Iterator> {

        Ok(())
    }
}
 */

#[derive(Clone)]
struct LogReporter {
    pub record_status: bool,
    status: Rc<RefCell<Option<StatusError>>>,
}

impl LogReporter {
    pub fn new(record_status: bool) -> Self {
        LogReporter {
            record_status,
            status: Rc::new(RefCell::new(None)),
        }
    }

    fn result(&mut self) -> Result<()> {
        let mut state = self.status.borrow_mut();
        if state.is_some() {
            Err(state.take().unwrap())
        } else {
            Ok(())
        }
    }
}

impl Reporter for LogReporter {
    fn corruption(&mut self, n: usize, status: StatusError) {
        if self.record_status {
            let mut s = self.status.borrow_mut();
            if s.is_none() {
                *s = Some(status)
            }
        }
    }
}

struct Writer {
    // none means force to compact memtable
    batch: Option<WriteBatch>,
    notifier: Sender<Result<()>>,
    sync: bool,
}

enum BatchTask {
    Write(Writer),
    Close,
}

struct Wal<W: WritableFile> {
    log_file_number: u64,
    log: Option<LogWriter<W>>,
}

// unsafe impl Send and Sync to operate fields log，table_cache and mem.
unsafe impl<E: Env> Send for DBImplInner<E> {}
unsafe impl<E: Env> Sync for DBImplInner<E> {}

pub(crate) struct DBImplInner<E: Env> {
    dbname: String,
    internal_comparator: InternalKeyComparator,
    env: E,
    options: Arc<Options>,
    table_cache: TableCache<E>,
    // record wal log
    wal: Mutex<Wal<E::WrFile>>,
    mem: ShardedLock<Option<Arc<MemTable>>>,
    imm: ShardedLock<Option<Arc<MemTable>>>,
    versions: Mutex<VersionSet<E>>,
    // for write batch
    batch_write_queue: Mutex<VecDeque<BatchTask>>,
    batch_write_cond: Condvar,

    // Have we encountered a background error in paranoid mode
    background_error: RwLock<Option<StatusError>>,
    shutdown: AtomicBool,

    compaction_trigger: (Sender<()>, Receiver<()>),
    back_ground_work_finish_signal: Condvar,
}

impl<E: Env> DBImplInner<E> {
    pub fn new(raw_options: Options, dbname: String, env: E) -> Self {
        // TODO: sanitize options
        let op = Arc::new(raw_options);
        let table_cache = TableCache::new(
            dbname.clone(),
            op.clone(),
            env.clone(),
            table_cache_size(&op),
        );
        let icmp = InternalKeyComparator::new(op.comparator.clone());
        DBImplInner {
            dbname: dbname.clone(),
            internal_comparator: icmp.clone(),
            env: env.clone(),
            options: op.clone(),
            table_cache: table_cache.clone(),
            wal: Mutex::new(Wal {
                log: None,
                log_file_number: 0,
            }),
            mem: ShardedLock::new(None),
            imm: ShardedLock::new(None),
            versions: Mutex::new(VersionSet::new(env, dbname, op, table_cache, icmp)),
            batch_write_queue: Mutex::new(VecDeque::new()),
            batch_write_cond: Condvar::new(),
            background_error: RwLock::new(None),
            shutdown: AtomicBool::new(false),
            compaction_trigger: unbounded(),
            back_ground_work_finish_signal: Condvar::new(),
        }
    }

    pub fn write(&self, options: &WriteOption, updates: Option<WriteBatch>) -> Result<()> {
        let (sender, recv) = bounded(1);
        let task = BatchTask::Write(Writer {
            batch: updates,
            notifier: sender,
            sync: options.sync,
        });

        self.batch_write_queue.lock().unwrap().push_back(task);
        self.batch_write_cond.notify_all();
        recv.recv()?;
        Ok(())
    }

    pub fn get(&self, read_options: &ReadOption, key: Slice, val: &mut Vec<u8>) -> Result<()> {
        // TODO: surport snapshot read
        let snapshot = self.versions.lock().unwrap().get_last_sequence();
        let lookup_key = LookupKey::new(key, snapshot);
        if let Some(v) = self
            .mem
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .get(&lookup_key)?
        {
            val.extend_from_slice(v.as_ref());
            return Ok(());
        }

        if let Some(m) = self.imm.read().unwrap().as_ref() {
            if let Some(v) = m.get(&lookup_key)? {
                val.extend_from_slice(v.as_ref());
                return Ok(());
            }
        }

        let current = self.versions.lock().unwrap().current();
        let (meta, level) = current.get(read_options, &lookup_key, val, &self.table_cache)?;
        if current.update_stats(meta, level) {
            self.maybe_schedule_compaction();
        }

        Ok(())
    }

    pub fn recover(&self, edit: &mut VersionEdit, save_manifest: &mut bool) -> Result<()> {
        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        self.env.create_dir(&self.dbname);
        // TODO: add file lock
        if !self.env.file_exists(&current_file_name(&self.dbname)) {
            if self.options.create_if_missing {
                self.new_db()?;
            } else {
                return Err(StatusError::InvalidArgument(format!(
                    "{} does not exist (create missing is false)",
                    self.dbname
                )));
            }
        } else if self.options.error_if_exists {
            return Err(StatusError::InvalidArgument(format!(
                "{} exists (error_if_exists is true)",
                self.dbname
            )));
        }

        let mut versions = self.versions.lock().unwrap();
        *save_manifest = versions.recover()?;

        let mut max_sequence = 0;
        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).
        //
        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an older version of leveldb.
        let min_log = versions.log_number();
        let prev_log = versions.prev_log_number();

        let mut file_names = Vec::new();
        self.env.get_children(&self.dbname, &mut file_names)?;
        let mut expect = HashSet::new();
        versions.add_live_files(&mut expect);
        let mut logs = Vec::new();
        for f in file_names.iter() {
            if let Ok((number, file_type)) = parse_file_name(f) {
                expect.remove(&number);
                if file_type == FileType::LogFile && (number >= min_log || number == prev_log) {
                    logs.push(number);
                }
            }
        }
        if !expect.is_empty() {
            let example = expect.iter().next().unwrap();
            return Err(StatusError::Corruption(format!(
                "{} missing files; e.g: {}",
                expect.len(),
                table_file_name(&self.dbname, *example)
            )));
        }
        std::mem::drop(versions);

        // Recover in the order in which the logs were generated
        logs.sort();
        for (i, number) in logs.iter().enumerate() {
            self.recovery_log_file(
                *number,
                i == logs.len() - 1,
                save_manifest,
                edit,
                &mut max_sequence,
            )?;
        }

        // The previous incarnation may not have written any MANIFEST
        // records after allocating this log number.  So we manually
        // update the file number allocation counter in VersionSet.
        let mut versions = self.versions.lock().unwrap();
        versions.mark_file_number_used(logs[logs.len() - 1]);
        if versions.get_last_sequence() < max_sequence {
            versions.set_last_sequence(max_sequence);
        }

        Ok(())
    }

    fn recovery_log_file(
        &self,
        log_number: u64,
        last_log: bool,
        save_manifest: &mut bool,
        edit: &mut VersionEdit,
        max_sequence: &mut SequenceNumber,
    ) -> Result<()> {
        let fname = log_file_name(&self.dbname, log_number);
        let file = self.env.new_sequential_file(&fname)?;
        let mut res = Ok(());
        let mut compaction = 0;
        let mut reporter = LogReporter::new(self.options.paranoid_checks);
        let mut reader = LogReader::new(file, reporter.clone());
        let mut record = Vec::new();
        let mut batch = WriteBatch::new();
        let mut mem = None;
        let buffer_size = self.options.write_buffer_size;
        let paranoid_checks = self.options.paranoid_checks;
        while reader.read_record(&mut record).is_ok() {
            if record.len() < 12 {
                reporter.corruption(
                    record.len(),
                    StatusError::Corruption("log record too small".to_string()),
                );
                continue;
            }
            batch.set_content(&record);
            if mem.is_none() {
                mem.replace(Arc::new(MemTable::new(self.internal_comparator.clone())));
            }

            let memtable = mem.as_ref().unwrap();
            res = batch.insert_into(memtable.clone());
            maybe_ignore_error(&mut res, paranoid_checks);
            if res.is_err() {
                break;
            }

            let last_sequence = batch.sequence() + batch.count() as SequenceNumber - 1;
            if last_sequence > *max_sequence {
                *max_sequence = last_sequence;
            }

            if memtable.approximate_memory_usage() > buffer_size {
                compaction += 1;
                *save_manifest = true;
                res = self.write_level0_table(memtable.clone(), edit, None);
                mem = None;
                if res.is_err() {
                    // Reflect errors immediately so that conditions like full
                    // file-systems cause the DB::Open() to fail.
                    break;
                }
            }
        }

        if res.is_ok() && self.options.reuse_log && last_log && compaction == 0 {
            let mut inner_mem = self.mem.write().unwrap();
            assert!(inner_mem.is_none());

            let log_file = self.env.new_appendable_file(&fname);
            let lfile_size = self.env.get_file_size(&fname);
            if let (Ok(log), Ok(s)) = (log_file, lfile_size) {
                let mut wal = self.wal.lock().unwrap();
                assert!(wal.log.is_none());
                let writer = LogWriter::new_with_dest_len(log, s);
                wal.log = Some(writer);
                wal.log_file_number = log_number;
                if mem.is_some() {
                    inner_mem.get_or_insert(mem.take().unwrap());
                } else {
                    inner_mem
                        .get_or_insert(Arc::new(MemTable::new(self.internal_comparator.clone())));
                }
            }
        }

        if let Some(m) = mem {
            // mem did not get reused; compact it.
            if res.is_ok() {
                *save_manifest = true;
                res = self.write_level0_table(m, edit, None);
            }
        }

        res
    }

    fn write_level0_table(
        &self,
        mem: Arc<MemTable>,
        edit: &mut VersionEdit,
        base: Option<Arc<Version<E>>>,
    ) -> Result<()> {
        let mut versions = self.versions.lock().unwrap();
        let start_time = Instant::now();
        let mut meta = FileMetaData::default();
        meta.number = versions.new_file_number();
        versions.pending_outputs.insert(meta.number);
        let iter = mem.iter();

        // unlock when build table
        std::mem::drop(versions);
        let res = build_table(
            &self.dbname,
            self.env.clone(),
            &self.options,
            self.table_cache.clone(),
            iter,
            &mut meta,
        );

        let mut versions = self.versions.lock().unwrap();
        versions.pending_outputs.remove(&meta.number);

        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest.
        let mut level = 0;
        if res.is_ok() && meta.file_size > 0 {
            let min_user_key = meta.smallest.user_key();
            let max_user_key = meta.largest.user_key();
            if let Some(v) = base {
                level = v.pick_level_for_memtable_output(&Some(min_user_key), &Some(max_user_key));
            }
            edit.add_file(
                level as u32,
                meta.number,
                meta.file_size,
                meta.smallest.clone(),
                meta.largest.clone(),
            );
        }

        let state = CompactionState {
            micros: Instant::now().duration_since(start_time).as_micros() as u64,
            bytes_read: 0,
            bytes_written: meta.file_size,
        };
        versions.stats[level].add(&state);
        res
    }

    fn new_db(&self) -> Result<()> {
        let mut new_db = VersionEdit::default();
        let comparator = self.internal_comparator.user_comparator();
        new_db.set_comparator_name(&comparator.name().to_string());
        new_db.set_log_number(0);
        new_db.set_next_file(2);
        new_db.set_last_sequence(0);

        let manifest = descriptor_file_name(&self.dbname, 1);
        let file = self.env.new_writable_file(&manifest)?;
        let mut log = LogWriter::new(file);
        let mut record = Vec::new();
        new_db.encode_to(&mut record);
        let mut res = log.add_record(record.as_slice());
        if res.is_ok() {
            // Make "CURRENT" file that points to the new manifest file.
            res = set_current_file(self.env.clone(), &self.dbname, 1);
        } else {
            self.env.delete_file(&manifest);
        }

        res
    }

    fn deleted_obsoleted_files(&self) {}

    fn maybe_schedule_compaction(&self) {
        // db is closing
        if self.shutdown.load(Ordering::Acquire)
            // Already got an error; no more changes
            || self.background_error.read().unwrap().is_some()
        {
            return;
        }

        self.compaction_trigger.0.send(()).unwrap();
    }

    fn build_batch_group(&self, first: Writer) -> (WriteBatch, Vec<Sender<Result<()>>>, bool) {
        let sync = first.sync;
        let mut batch = WriteBatch::new();
        let mut senders = Vec::new();
        senders.push(first.notifier.clone());

        if let Some(b) = first.batch {
            batch.append(&b);
            let mut size = batch.approximate_size();
            // Allow the group to grow up to a maximum size, but if the
            // original write is small, limit the growth so we do not slow
            // down the small write too much.
            let max_size = if size < 128 << 10 {
                size + 128 << 10
            } else {
                1 << 20
            };

            let mut queue = self.batch_write_queue.lock().unwrap();
            while let Some(task) = queue.front() {
                match task {
                    BatchTask::Write(w) => {
                        // Do not include a sync write into a batch handled by a non-sync write and do not include a force compaction task.
                        if (w.sync && !sync) || w.batch.is_none() {
                            break;
                        }
                        batch.append(w.batch.as_ref().unwrap());
                        senders.push(w.notifier.clone());
                        queue.pop_front();
                        size = batch.approximate_size();
                        if size > max_size {
                            break;
                        }
                    }
                    BatchTask::Close => break,
                }
            }
        }

        (batch, senders, sync)
    }

    fn make_room_for_write(&self, mut force: bool) -> Result<MutexGuard<VersionSet<E>>> {
        #![cfg_attr(feature = "cargo-clippy", allow(clippy::if_same_then_else))]
        let mut allow_delay = !force;
        let mut versions = self.versions.lock().unwrap();
        loop {
            if let Some(e) = self.background_error.write().unwrap().take() {
                return Err(e);
            } else if allow_delay
                && versions.num_level_files(0) >= config::L0_SLOW_DOWN_WRITES_TRIGGER
            {
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                thread::sleep(Duration::from_micros(1000));
                allow_delay = false;
            } else if !force
                && self
                    .mem
                    .read()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .approximate_memory_usage()
                    <= self.options.write_buffer_size
            {
                // There is room in current memtable
                break;
            } else if self.imm.read().unwrap().is_some() {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                versions = self.back_ground_work_finish_signal.wait(versions).unwrap();
            } else if versions.num_level_files(0) >= config::L0_STOP_WRITES_TRIGGER {
                // There are too many level-0 files.
                versions = self.back_ground_work_finish_signal.wait(versions).unwrap();
            } else {
                // Attempt to switch to a new memtable and trigger compaction of old
                assert_eq!(versions.prev_log_number(), 0);
                let new_log_number = versions.new_file_number();
                match self
                    .env
                    .new_writable_file(&log_file_name(&self.dbname, new_log_number))
                {
                    Ok(log_file) => {
                        let mut wal = self.wal.lock().unwrap();
                        let log = LogWriter::new(log_file);
                        wal.log = Some(log);
                        wal.log_file_number = new_log_number;

                        let mut imm = self.imm.write().unwrap();
                        let mut mem = self.mem.write().unwrap();
                        *imm = mem.take();
                        let new_mem = MemTable::new(self.internal_comparator.clone());
                        *mem = Some(Arc::new(new_mem));
                        force = false; // Do not force another compaction if have room
                        self.maybe_schedule_compaction();
                    }

                    Err(e) => {
                        // Avoid chewing through file number space in a tight loop.
                        versions.reuse_file_number(new_log_number);
                        return Err(e);
                    }
                }
            }
        }

        Ok(versions)
    }

    fn background_compaction(&self) {
        if self.imm.read().unwrap().is_some() {
            self.compact_memtable();
            return;
        }

        // TODO: major compation
    }

    fn compact_memtable(&self) {
        if let Err(e) = self.do_compaction_memtable() {
            self.record_back_ground_error(e);
        }
    }

    fn do_compaction_memtable(&self) -> Result<()> {
        let imm = self.imm.read().unwrap().as_ref().unwrap().clone();
        let mut edit = VersionEdit::default();
        let current = self.versions.lock().unwrap().current();
        self.write_level0_table(imm, &mut edit, Some(current))?;

        if self.shutdown.load(Ordering::Acquire) {
            return Err(StatusError::Customize(
                "Deleting DB during memtable compaction".to_string(),
            ));
        }

        // Replace immutable memtable with the generated Table
        edit.set_prev_log_number(0);
        edit.set_log_number(self.wal.lock().unwrap().log_file_number);
        self.versions.lock().unwrap().log_and_apply(&mut edit)?;

        let mut imm = self.imm.write().unwrap();
        *imm = None;
        self.deleted_obsoleted_files();

        Ok(())
    }

    fn record_back_ground_error(&self, e: StatusError) {
        let mut back_ground_error = self.background_error.write().unwrap();
        back_ground_error.get_or_insert(e);
    }

    fn new_internal_iterator(
        &self,
        options: &ReadOption,
    ) -> Result<(MergingIterator<InternalKeyComparator>, SequenceNumber)> {
        let versions = self.versions.lock().unwrap();
        let latest_snapshot = versions.get_last_sequence();
        let mem = self.mem.read().unwrap();
        let mut iters = Vec::new();
        if let Some(m) = mem.as_ref() {
            iters.push(m.iter());
        }

        let imm = self.imm.read().unwrap();
        if let Some(m) = imm.as_ref() {
            iters.push(m.iter());
        }

        let current = versions.current();
        current.add_iterators(options, &mut iters);
        let merge_iter = MergingIterator::new(self.internal_comparator.clone(), iters);
        Ok((merge_iter, latest_snapshot))
    }
}

fn maybe_ignore_error(res: &mut Result<()>, paranoid_checks: bool) {
    if !paranoid_checks && res.is_err() {
        *res = Ok(());
    }
}

fn clip_to_range<T: PartialOrd + Copy>(source: &mut T, min: &T, max: &T) {
    if *source > *max {
        *source = *max;
    }

    if *source < *min {
        *source = *min;
    }
}

fn table_cache_size(sanitized_options: &Arc<Options>) -> u64 {
    sanitized_options.max_open_files - NUM_NON_TABLE_CACHE_FILES
}

// Which direction is the iterator currently moving?
// (1) When moving forward, the internal iterator is positioned at
//     the exact entry that yields this->key(), this->value()
// (2) When moving backwards, the internal iterator is positioned
//     just before all entries whose user key == this->key().
const FORWARD: u8 = 0;
const BACKWARD: u8 = 1;

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
// TODO: add sample record
pub(crate) struct DBIter<E: Env> {
    // TODO: add sample record
    db: Arc<DBImplInner<E>>,
    user_comparator: Arc<dyn Comparator<Slice>>,
    iter: Box<dyn Iterator>,
    sequence: SequenceNumber,
    status: Option<StatusError>,
    saved_key: Vec<u8>,
    saved_value: Vec<u8>,
    direction: u8,
    valid: bool,
}

impl<E: Env> DBIter<E> {
    pub(crate) fn new(
        db: Arc<DBImplInner<E>>,
        iter: Box<dyn Iterator>,
        sequence: SequenceNumber,
    ) -> Self {
        let user_comparator = db.internal_comparator.user_comparator();
        DBIter {
            db,
            user_comparator,
            iter,
            sequence,
            status: None,
            saved_key: Vec::new(),
            saved_value: Vec::new(),
            direction: FORWARD,
            valid: false,
        }
    }

    #[inline]
    fn clear_saved_value(&mut self) {
        if self.saved_value.capacity() > 1048576 {
            let mut tmp = Vec::new();
            std::mem::swap(&mut self.saved_value, &mut tmp);
        } else {
            self.saved_value.clear();
        }
    }

    #[inline]
    fn parse_key(&mut self, ikey: &mut ParsedInternalKey) -> bool {
        let k = self.iter.key();
        // TODO: add sample and trigger compaction if needed
        if !parse_internal_key(k, ikey) {
            self.status = Some(StatusError::Corruption(
                "corrupted internal key in DBIter".to_string(),
            ));
            false
        } else {
            true
        }
    }

    fn find_next_user_entry(&mut self, mut skipping: bool) {
        assert!(self.iter.valid());
        assert_eq!(self.direction, FORWARD);
        loop {
            let mut ikey = ParsedInternalKey::default();
            if self.parse_key(&mut ikey) && ikey.sequence <= self.sequence {
                if ikey.val_type == TYPE_DELETION {
                    // Arrange to skip all upcoming entries for this key since
                    // they are hidden by this deletion.
                    self.saved_key.clear();
                    self.saved_key.extend_from_slice(ikey.user_key.as_ref());
                    skipping = true;
                } else {
                    let cmp_res = self
                        .user_comparator
                        .compare(&ikey.user_key, &self.saved_key.as_slice().into());
                    if skipping
                        && (cmp_res == std::cmp::Ordering::Less
                            || cmp_res == std::cmp::Ordering::Equal)
                    {
                        // Entry hidden
                    } else {
                        self.valid = true;
                        self.saved_key.clear();
                        return;
                    }
                }
            }
            self.iter.next();
            if !self.iter.valid() {
                break;
            }
        }
        self.saved_key.clear();
        self.valid = false;
    }

    fn find_prev_user_entry(&mut self) {
        assert_eq!(self.direction, BACKWARD);
        let mut val_type = TYPE_DELETION;
        if self.iter.valid() {
            loop {
                let mut ikey = ParsedInternalKey::default();
                if self.parse_key(&mut ikey) && ikey.sequence <= self.sequence {
                    if val_type != TYPE_DELETION
                        && self
                            .user_comparator
                            .compare(&ikey.user_key, &self.saved_key.as_slice().into())
                            == std::cmp::Ordering::Less
                    {
                        break;
                    }
                    val_type = ikey.val_type;
                    if val_type == TYPE_DELETION {
                        self.saved_key.clear();
                        self.clear_saved_value();
                    } else {
                        let raw_value = self.iter.value();
                        if self.saved_value.capacity() > raw_value.size() + 1048576 {
                            let mut empty = Vec::new();
                            std::mem::swap(&mut self.saved_value, &mut empty);
                        }
                        self.saved_key.clear();
                        let user_key = extract_user_key(self.iter.key());
                        self.saved_key.extend_from_slice(user_key.as_ref());
                        self.saved_value.extend_from_slice(raw_value.as_ref());
                    }
                }
                self.iter.prev();
                if !self.iter.valid() {
                    break;
                }
            }
        }

        if val_type == TYPE_DELETION {
            self.valid = false;
            self.saved_key.clear();
            self.clear_saved_value();
        } else {
            self.valid = true;
        }
    }
}

impl<E: Env> Iterator for DBIter<E> {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.direction = FORWARD;
        self.clear_saved_value();
        self.iter.seek_to_first();
        if self.iter.valid() {
            self.find_next_user_entry(false);
        } else {
            self.valid = false;
        }
    }

    fn seek_to_last(&mut self) {
        self.direction = BACKWARD;
        self.clear_saved_value();
        self.iter.seek_to_last();
        self.find_prev_user_entry();
    }

    fn seek(&mut self, target: Slice) {
        self.direction = FORWARD;
        self.clear_saved_value();
        self.saved_key.clear();
        let parsed_key = ParsedInternalKey {
            user_key: target,
            sequence: self.sequence,
            val_type: VALUE_TYPE_FOR_SEEK,
        };
        append_internal_key(&mut self.saved_key, &parsed_key);
        self.iter.seek(self.saved_key.as_slice().into());
        if self.iter.valid() {
            self.find_next_user_entry(false);
        } else {
            self.valid = false;
        }
    }

    fn next(&mut self) {
        assert!(self.valid);
        if self.direction == BACKWARD {
            self.direction = FORWARD;
            // iter_ is pointing just before the entries for this->key(),
            // so advance into the range of entries for this->key() and then
            // use the normal skipping code below.
            if !self.iter.valid() {
                self.iter.seek_to_first();
            } else {
                self.iter.next();
            }

            if !self.iter.valid() {
                self.valid = false;
                self.saved_key.clear();
                return;
            }
            // saved_key_ already contains the key to skip past.
        } else {
            // Store in saved_key_ the current key so we skip it below.
            let user_key = extract_user_key(self.iter.key());
            self.saved_key.clear();
            self.saved_key.extend_from_slice(user_key.as_ref());
            // iter_ is pointing to current key. We can now safely move to the next to
            // avoid checking current key.
            self.iter.next();
            if !self.iter.valid() {
                self.valid = false;
                self.saved_key.clear();
                return;
            }
        }

        self.find_next_user_entry(true);
    }

    fn prev(&mut self) {
        assert!(self.valid);
        if self.direction == FORWARD {
            // iter_ is pointing at the current entry.  Scan backwards until
            // the key changes so we can use the normal reverse scanning code.
            // Otherwise valid_ would have been false
            assert!(self.iter.valid());
            let user_key = extract_user_key(self.iter.key());
            self.saved_key.clear();
            self.saved_key.extend_from_slice(user_key.as_ref());
            loop {
                self.iter.prev();
                if !self.iter.valid() {
                    self.valid = false;
                    self.saved_key.clear();
                    self.clear_saved_value();
                    return;
                }
                let user_key = extract_user_key(self.iter.key());
                if self
                    .user_comparator
                    .compare(&user_key, &self.saved_key.as_slice().into())
                    == std::cmp::Ordering::Less
                {
                    break;
                }
            }
            self.direction = BACKWARD;
        }

        self.find_prev_user_entry();
    }

    fn key(&self) -> Slice {
        assert!(self.valid);
        if self.direction == FORWARD {
            extract_user_key(self.iter.key())
        } else {
            self.saved_key.as_slice().into()
        }
    }

    fn value(&self) -> Slice {
        assert!(self.valid);
        if self.direction == FORWARD {
            self.iter.value()
        } else {
            self.saved_value.as_slice().into()
        }
    }

    fn status(&mut self) -> Result<()> {
        if self.status.is_some() {
            Err(self.status.take().unwrap())
        } else {
            self.iter.status()
        }
    }
}
