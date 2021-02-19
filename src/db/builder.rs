use crate::db::dbformat::InternalKey;
use crate::db::filename::table_file_name;
use crate::db::table_cache::TableCache;
use crate::db::version::FileMetaData;
use crate::db::Iterator;
use crate::db::Result;
use crate::db::{Options, ReadOption, WritableFile};
use crate::env::Env;
use crate::sstable::table::{Table, TableBuilder};
use std::sync::Arc;

pub fn build_table<'a, E: Env>(
    dbname: &String,
    env: E,
    options: &Arc<Options>,
    table_cache: TableCache<E>,
    mut iter: Box<dyn Iterator + 'a>,
    meta: &mut FileMetaData,
) -> Result<()> {
    meta.file_size = 0;
    iter.seek_to_first();
    let fname = table_file_name(dbname, meta.number);
    let mut res = Ok(());
    if iter.valid() {
        let mut file = env.new_writable_file(&fname)?;
        let mut builder = TableBuilder::new(options.clone(), file);
        let mut smallest = InternalKey::new_empty_key();
        smallest.decode_from(iter.key());
        meta.smallest = smallest;
        meta.largest = InternalKey::new_empty_key();

        while iter.valid() {
            builder.add(iter.key(), iter.value());
            meta.largest.decode_from(iter.key());
            iter.next();
        }

        res = builder.finish(true);
        if res.is_ok() {
            meta.file_size = builder.file_size();
            assert!(meta.file_size > 0);
        }

        if res.is_ok() {
            // Verify that the table is usable
            let res_table = table_cache.find_table(meta.number, meta.file_size);
            if res_table.is_ok() {
                let mut table = res_table.unwrap();
                let mut iter = Table::new_iterator(table, &ReadOption::default());
                res = iter.status();
            }
        }
    }

    if !iter.status().is_ok() {
        // Check for input iterator errors
        res = iter.status();
    }
    if !res.is_ok() || meta.file_size == 0 {
        env.delete_file(&fname);
    }

    return res;
}
