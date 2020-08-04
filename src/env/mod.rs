mod memory;
mod posix;

use crate::db::error::{Result, StatusError};
use crate::db::{RandomAccessFile, SequentialFile, WritableFile};

pub trait Env: Clone {
    type SeqFile: SequentialFile;
    type RndFile: RandomAccessFile;
    type WrFile: WritableFile;
    // Create an object that sequentially reads the file with the specified name.
    // Implementations should return Err(NotFound) when the file does not exist.
    // The returned file will only be accessed by one thread at a time.
    fn new_sequential_file(&self, name: &String) -> Result<Self::SeqFile>;

    // Create an object supporting random-access reads from the file with the
    // specified name. On failurementations should
    // return a Err(NotFound) when the file does not exist.
    // The returned file may be concurrently accessed by multiple threads.
    fn new_random_access_file(&self, name: &String) -> Result<Self::RndFile>;

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a new file.
    // The returned file will only be accessed by one thread at a time.
    fn new_writable_file(&self, name: &String) -> Result<Self::WrFile>;

    // Create an object that either appends to an existing file, or
    // writes to a new file (if the file does not exist to begin with).
    // The returned file will only be accessed by one thread at a time.
    // May return an IsNotSupportedError error if this Env does
    // not allow appending to an existing file.  Users of Env (including
    // the leveldb implementation) must be prepared to deal with
    // an Env that does not support appending.
    fn new_appendable_file(&self, name: &String) -> Result<Self::WrFile>;

    // Returns true iff the named file exists.
    fn file_exists(&self, name: &String) -> bool;

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    fn get_children(&self, name: &String, result: &mut Vec<String>) -> Result<()>;

    // Delete the named file.
    fn delete_file(&self, name: &String) -> Result<()>;

    // Create the specified directory.
    fn create_dir(&self, dirname: &String) -> Result<()>;

    // Delete the specified directory.
    fn delete_dir(&self, dirname: &String) -> Result<()>;

    fn get_file_size(&self, name: &String) -> Result<usize>;

    fn rename_file(&self, src: &String, target: &String) -> Result<()>;

    // *path is set to a temporary directory that can be used for testing. It may
    // or may not have just been created. The directory may or may not differ
    // between runs of the same process, but subsequent calls will return the
    // same directory.
    fn get_test_dir(&self) -> Result<String>;
    //TODO: add many other func
}

pub fn do_write_string_to_file<E: Env>(
    env: E,
    data: &[u8],
    file_name: &String,
    sync: bool,
) -> Result<()> {
    let mut file = env.new_writable_file(file_name)?;
    file.append(data)?;
    if sync {
        file.sync()?;
    }
    Ok(())
}

pub fn write_string_to_file<E: Env>(env: E, data: &[u8], file_name: &String) -> Result<()> {
    let ret = do_write_string_to_file(env.clone(), data, file_name, false);
    if ret.is_err() {
        env.delete_file(file_name);
    }
    ret
}

pub fn write_string_to_file_sync<E: Env>(env: E, data: &[u8], file_name: &String) -> Result<()> {
    let ret = do_write_string_to_file(env.clone(), data, file_name, true);
    if ret.is_err() {
        env.delete_file(file_name);
    }
    ret
}
