use crate::db::dbformat::LookupKey;
use crate::db::error::{Result, StatusError};
use crate::db::log::LogWriter;
use crate::db::memtable::MemTable;
use crate::db::table_cache::TableCache;
use crate::db::version::VersionSet;
use crate::db::write_batch::WriteBatch;
use crate::db::{slice::Slice, ReadOption, DB};
use crate::db::{WritableFile, WriteOption};
use crate::env::Env;
use crossbeam_channel::{bounded, Receiver, Sender};
use failure::_core::option::Option::Some;
use std::collections::VecDeque;
use std::sync::{atomic::AtomicBool, Arc, Condvar, Mutex, MutexGuard};
use std::thread;

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

struct DBImpl<E: Env> {
    inner: Arc<DBImplInner<E>>,
}

struct DBImplInner<E: Env> {
    table_cache: TableCache<E>,
    wal: Mutex<Wal<E::WrFile>>,
    mem: Arc<MemTable>,
    has_imm: AtomicBool, // So bg thread can detect non-null imm_
    batch_write_cond: Condvar,
    lock_data: Mutex<LockField<E>>,
}

struct Wal<W: WritableFile> {
    log: LogWriter<W>,
}

// unsafe impl Send and Sync to operate fields log，table_cache and mem.
unsafe impl<E: Env> Send for DBImplInner<E> {}
unsafe impl<E: Env> Sync for DBImplInner<E> {}

struct LockField<E: Env> {
    pub versions: VersionSet<E>,
    pub imm: Option<Arc<MemTable>>,
    pub write_queue: VecDeque<WriteTask>,
}

impl<E: Env> DBImpl<E> {
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

        self.inner
            .lock_data
            .lock()
            .unwrap()
            .write_queue
            .push_back(task);
        self.inner.batch_write_cond.notify_all();
        recv.recv()?;
        Ok(())
    }

    // TODO： support snapshot read.
    pub fn get(&self, options: &ReadOption, key: Slice, val: &mut Vec<u8>) -> Result<()> {
        let mut cur_version;
        let mut snapshot;
        let mut mem;
        let mut imm;
        let mut have_stat_update = false;
        {
            let locked_field = self.inner.lock_data.lock().unwrap();
            cur_version = locked_field.versions.current();
            snapshot = locked_field.versions.get_last_sequence();
            imm = locked_field.imm.clone();
            mem = self.inner.mem.clone();
        };

        // unlock while reading from files and memtables
        let lkey = LookupKey::new(key, snapshot);
        if let Some(v) = mem.get(&lkey)? {
            val.extend_from_slice(v.as_ref());
        } else if let Some(m) = imm {
            if let Some(v) = m.get(&lkey)? {
                val.extend_from_slice(v.as_ref());
            }
        } else {
            let (file, file_size) =
                cur_version.get(options, &lkey, val, &self.inner.table_cache)?;
            // TODO: lock and update state for a version and compaction if needed.
            let locked_field = self.inner.lock_data.lock().unwrap();
        }

        Ok(())
    }

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
