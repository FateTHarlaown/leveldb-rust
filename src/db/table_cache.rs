use crate::db::error::{Result, StatusError};
use crate::db::filename::{sst_table_file_name, table_file_name};
use crate::db::option::Options;
use crate::db::slice::Slice;
use crate::db::Iterator;
use crate::db::{RandomAccessFile, ReadOption};
use crate::env::Env;
use crate::sstable::table::Table;
use crate::util::cache::{Cache, ShardedLruCache};
use byteorder::{LittleEndian, WriteBytesExt};
use failure::_core::marker::PhantomData;
use std::sync::Arc;

const KEY_LEN: usize = std::mem::size_of::<u64>();

pub struct TableCache<E: Env> {
    env: E,
    dbname: String,
    option: Arc<Options>,
    cache: Arc<dyn Cache<Vec<u8>, Table<E::RndFile>>>,
}

impl<E: Env> Clone for TableCache<E> {
    fn clone(&self) -> Self {
        TableCache {
            env: self.env.clone(),
            dbname: self.dbname.clone(),
            option: self.option.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<E: Env> TableCache<E> {
    pub fn new(dbname: String, option: Arc<Options>, env: E, size: u64) -> Self {
        let cache = Arc::new(ShardedLruCache::new(size));
        TableCache {
            env,
            dbname,
            option,
            cache,
        }
    }

    pub fn evict(&self, file_number: u64) {
        let mut key = Vec::with_capacity(KEY_LEN);
        key.write_u64::<LittleEndian>(file_number).unwrap();
        self.cache.erase(&key)
    }

    pub fn get<CB>(
        &self,
        option: &ReadOption,
        file_number: u64,
        file_size: u64,
        key: Slice,
        callback: CB,
    ) -> Result<()>
    where
        CB: FnMut(Slice, Slice),
    {
        let handle = self.find_table(file_number, file_size)?;
        handle.internal_get(option, key, callback)
    }

    pub fn find_table(&self, file_number: u64, file_size: u64) -> Result<Arc<Table<E::RndFile>>> {
        let mut key = Vec::with_capacity(KEY_LEN);
        key.write_u64::<LittleEndian>(file_number).unwrap();
        if let Some(handle) = self.cache.look_up(&key) {
            Ok(handle)
        } else {
            let file = self.open_table_file(file_number)?;
            let table = Table::open(self.option.clone(), file, file_size)?;
            if let Some(handle) = self.cache.insert(key, table, 1) {
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
