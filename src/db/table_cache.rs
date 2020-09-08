use crate::db::error::{Result, StatusError};
use crate::db::filename::{sst_table_file_name, table_file_name};
use crate::db::option::Options;
use crate::db::slice::Slice;
use crate::db::Iterator;
use crate::db::{RandomAccessFile, ReadOption};
use crate::env::Env;
use crate::sstable::table::Table;
use crate::util::cache::{Cache, ShardedLruCache};
use failure::_core::marker::PhantomData;
use std::sync::Arc;

pub struct TableCache<C, E>
where
    C: Cache<u64, Table<E::RndFile>>,
    E: Env
{
    env: E,
    dbname: String,
    option: Arc<Options>,
    cache: C,
}

impl<C, E> TableCache<C, E>
where
    C: Cache<u64, Table<E::RndFile>>,
    E: Env
{
    pub fn new(dbname: String, option: Arc<Options>, cache: C, env: E) -> Self {
        TableCache {
            env,
            dbname,
            option,
            cache,
        }
    }

    pub fn evict(&self, file_number: u64) {
        self.cache.erase(&file_number)
    }

    pub fn get(
        &self,
        option: &ReadOption,
        file_number: u64,
        file_size: u64,
        key: Slice,
        callback: Box<dyn Fn(Slice, Slice)>,
    ) -> Result<()> {
        let hanlde = self.find_table(file_number, file_size)?;
        hanlde.internal_get(option, key, callback)
    }

    pub fn find_table(&self, file_number: u64, file_size: u64) -> Result<Arc<Table<E::RndFile>>> {
        if let Some(handle) = self.cache.look_up(&file_number) {
            Ok(handle)
        } else {
            let file = self.open_table_file(file_number)?;
            let table = Table::open(self.option.clone(), file, file_size)?;
            if let Some(handle) = self.cache.insert(file_number, table, 1) {
                Ok(handle)
            } else {
                Err(StatusError::NotSupported(
                    "create a table but can't insert into cache".to_string(),
                ))
            }
        }
    }

    fn open_table_file(&self, file_number: u64) -> Result<E::RndFile> {
        let file_name = table_file_name(&self.dbname, file_number);
        if let Ok(file) = self.env.new_random_access_file(&file_name) {
            Ok(file)
        } else {
            let old_file_name = sst_table_file_name(&self.dbname, file_number);
            self.env.new_random_access_file(&old_file_name)
        }
    }
}
