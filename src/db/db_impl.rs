use crate::db::builder::build_table;
use crate::db::dbformat::{InternalKeyComparator, LookupKey, SequenceNumber};
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
use crate::db::version::{FileMetaData, Version, VersionEdit, VersionSet, CompactionState};
use crate::db::write_batch::WriteBatch;
use crate::db::{slice::Slice, ReadOption, Reporter, DB};
use crate::db::{WritableFile, WriteOption};
use crate::env::Env;
use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_utils::sync::ShardedLock;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::rc::Rc;
use std::sync::{atomic::AtomicBool, Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread;
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

    }

    fn run_compaction_worker(&self) {

    }

    /*
    fn run_write_worker(&self) -> Result<()> {
        let inner = self.inner.clone();
        thread::spawn(move || {
            loop {
                // TODO: is DB closed ?

                let mut batch;
                let mut senders;
                let mut sync;
                let mut last_seq;
                {
                    let mut locked_fields = inner.lock_data.lock().unwrap();
                    if locked_fields.write_queue.is_empty() {
                        locked_fields = inner
                            .batch_write_cond
                            .wait_while(locked_fields, |f| f.write_queue.is_empty())
                            .unwrap();
                    }

                    let first = locked_fields.write_queue.pop_front().unwrap();
                    let joined = match first {
                        WriteTask::Write(w) => {
                            join_write_batch_from_queue(w, &mut locked_fields.write_queue)
                        }
                        // TODO：colse or compaction
                        WriteTask::Compaction(c) => unimplemented!(),
                        WriteTask::Close => unimplemented!(),
                    };
                    batch = joined.0;
                    senders = joined.1;
                    sync = joined.2;

                    // TODO: make room
                    last_seq = locked_fields.versions.get_last_sequence();
                    batch.set_sequence(last_seq + 1);
                    last_seq += u64::from(batch.count());
                }

                // Add to log and apply to memtable.  We can release the lock
                // during this phase since &w is currently responsible for logging
                // and protects against concurrent loggers and concurrent writes
                // into mem_.
                let mut ret;
                let mut sync_err;
                {
                    // flush wal log
                    let mut wal = inner.wal.lock().unwrap();
                    ret = wal.log.add_record(batch.get_content().as_slice());
                    sync_err = false;
                    if ret.is_ok() && sync {
                        ret = wal.log.sync();
                        sync_err = ret.is_err();
                    }
                }

                if ret.is_ok() {
                    ret = batch.insert_into(inner.mem.clone())
                }

                {
                    let mut locked_fields = inner.lock_data.lock().unwrap();
                    if sync_err {
                        // record background error.
                        // The state of the log file is indeterminate: the log record we
                        // just added may or may not show up when the DB is re-opened.
                        // So we force the DB into a mode where all future writes fail.
                    }
                    locked_fields.versions.set_last_sequence(last_seq);
                }

                for sender in senders {
                    if let Err(e) = sender.send(()) {
                        // TODO: log error
                    }
                }
            }
        });

        Ok(())
    }
     */


}


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
    batch: WriteBatch,
    sync: bool,
    notifier: Sender<()>,
}

struct Compacter {
    notifier: Sender<()>,
}

enum WriteTask {
    Write(Writer),
    Compaction(Compacter),
    Close,
}

struct Wal<W: WritableFile> {
    log_file_number: u64,
    log: Option<LogWriter<W>>,
}

// unsafe impl Send and Sync to operate fields log，table_cache and mem.
unsafe impl<E: Env> Send for DBImplInner<E> {}
unsafe impl<E: Env> Sync for DBImplInner<E> {}
struct DBImplInner<E: Env> {
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
    batch_write_queue: Mutex<VecDeque<WriteTask>>,
    batch_write_cond: Condvar,

    // Have we encountered a background error in paranoid mode
    background_error: RwLock<Option<StatusError>>,
}

impl<E: Env> DBImplInner<E> {
    pub fn new(raw_options: Options, dbname: String, env: E) -> Self {
        // TODO: sanitize options
        let op = Arc::new(raw_options);
        let table_cache = TableCache::new(dbname.clone(), op.clone(), env.clone(), table_cache_size(&op));
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
        }
    }

    pub fn write(&self, options: &WriteOption, updates: Option<WriteBatch>) -> Result<()> {
        let (sender, recv) = bounded(1);
        let task = if let Some(w) = updates {
            WriteTask::Write(Writer {
                batch: w,
                sync: options.sync,
                notifier: sender,
            })
        } else {
            WriteTask::Compaction(Compacter { notifier: sender })
        };

        self.batch_write_queue
            .lock()
            .unwrap()
            .push_back(task);
        self.batch_write_cond.notify_all();
        recv.recv()?;
        Ok(())
    }

    pub fn recover(
        &self,
        edit: &mut VersionEdit,
        save_manifest: &mut bool,
    ) -> Result<()> {
        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        self.env.create_dir(&self.dbname);
        // TODO: add file lock
        if !self
            .env
            .file_exists(&current_file_name(&self.dbname))
        {
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
                mem.replace(Arc::new(MemTable::new(
                    self.internal_comparator.clone(),
                )));
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
                    inner_mem.get_or_insert(Arc::new(MemTable::new(
                        self.internal_comparator.clone(),
                    )));
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
        base: Option<&mut Version<E>>,
    ) -> Result<()> {
        let mut versions = self.versions.lock().unwrap();
        let start_time = Instant::now();
        let mut meta = FileMetaData::default();
        meta.number = versions.new_file_number();
        versions.pending_outputs.insert(meta.number);
        let mut iter = mem.iter();

        // unlock when build table
        std::mem::drop(versions);
        let mut res = build_table(
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
        let mut file = self.env.new_writable_file(&manifest)?;
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

    fn deleted_obsoleted_files(&self) {

    }


    fn maybe_schedule_compaction(&self) {

    }
}

fn maybe_ignore_error(res: &mut Result<()>, paranoid_checks: bool) {
    if !paranoid_checks && res.is_err() {
        *res = Ok(());
    }
}

fn join_write_batch_from_queue(
    mut first: Writer,
    queue: &mut VecDeque<WriteTask>,
) -> (WriteBatch, Vec<Sender<()>>, bool) {
    let mut senders = Vec::new();
    let mut batch = WriteBatch::new();
    batch.append(&first.batch);
    let mut size = batch.approximate_size();
    let max_size = if size < 128 << 10 {
        size + 128 << 10
    } else {
        1 << 20
    };

    while !queue.is_empty() {
        let cur = queue.pop_front().unwrap();
        let continue_next = match cur {
            WriteTask::Write(w) => {
                if !first.sync && w.sync {
                    size += w.batch.approximate_size();
                    if size > max_size {
                        break;
                    }
                    senders.push(w.notifier.clone());
                    batch.append(&w.batch);
                    true
                } else {
                    queue.push_front(WriteTask::Write(w));
                    false
                }
            }
            WriteTask::Compaction(c) => {
                queue.push_front(WriteTask::Compaction(c));
                false
            }
            _ => {
                queue.push_front(WriteTask::Close);
                false
            }
        };

        if !continue_next {
            break;
        }
    }

    (batch, senders, first.sync)
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
