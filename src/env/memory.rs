use std::sync::{Arc, Mutex};
use crate::env::Env;
use crate::db::error::{Result, StatusError};
use crate::db::slice::Slice;
use crate::util::buffer::{BufferReader, BufferWriter};
use crate::db::{SequentialFile, RandomAccessFile, WritableFile};
use std::collections::BTreeMap;

const BLOCK_SIZE: usize = 8 * 1024;

#[derive(Clone)]
pub struct FileState {
    inner: Arc<Mutex<InnerFileState>>
}

pub struct InnerFileState {
    blocks: Vec<Vec<u8>>,
    size: usize,
}

impl FileState {
    pub fn new() -> Self {
        let inner_state = InnerFileState {
            blocks: Vec::new(),
            size: 0,
        };

        FileState {
            inner: Arc::new(Mutex::new(inner_state)),
        }
    }

    pub fn truncate(&mut self) {
        let mut state = self.inner.lock().unwrap();
        state.blocks.clear();
        state.size = 0;
    }

    pub fn read(&self, offset: usize, mut n: usize, scratch: &mut Vec<u8>) -> Result<usize> {
        let state = self.inner.lock().unwrap();
        if offset > state.size {
            return Err(StatusError::Eof("Offset greater than file size".to_string()));
        }

        let available = state.size - offset;
        if n > available {
            n = available
        }
        if n == 0 {
            return Ok(0);
        }

        scratch.clear();
        let mut block = offset / BLOCK_SIZE;
        let mut block_offset = offset % BLOCK_SIZE;
        let mut bytes_to_copy = n;
        while bytes_to_copy > 0 {
            let mut avail = BLOCK_SIZE - block_offset;
            if avail > bytes_to_copy {
                avail = bytes_to_copy;
            }
            let mut buf = state.blocks.get(block).unwrap().as_slice();
            buf.advance(block_offset);
            let data = buf.read_bytes(avail).unwrap();
            scratch.extend_from_slice(data);
            bytes_to_copy -= avail;
            block += 1;
            block_offset = 0;
        }

        Ok(n)

    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        let mut buf = data;
        let mut state = self.inner.lock().unwrap();
        while buf.len() > 0  {
            let mut avail = 0;
            let offset = state.size % BLOCK_SIZE;
            if offset != 0 {
                // There is some room in the last block.
                avail = BLOCK_SIZE - offset;
            } else {
                state.blocks.push(Vec::with_capacity(BLOCK_SIZE));
                avail = BLOCK_SIZE;
            }

            if avail > buf.len() {
                avail = buf.len()
            }
            let append_data = buf.read_bytes(avail).unwrap();
            let last = state.blocks.len()-1;
            let block = state.blocks.get_mut(last).unwrap();
            block.extend_from_slice(append_data);
            state.size += avail;
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        let state = self.inner.lock().unwrap();
        state.size
    }
}

pub struct MemSequentialFile {
    file: FileState,
    pos: usize,
}

impl MemSequentialFile {
    pub fn new(file: FileState) -> Self {
        MemSequentialFile {
            file,
            pos: 0,
        }
    }
}

impl SequentialFile for MemSequentialFile {
    fn read(&mut self, mut buf: &mut [u8], n: usize) -> Result<usize> {
        let mut v = Vec::with_capacity(buf.len());
        let ret = self.file.read(self.pos, n, &mut v)?;
        buf.write_bytes(v.as_slice())?;
        self.pos += ret;
        Ok(ret)
    }

    fn skip(&mut self, mut n: usize) -> Result<()> {
        if self.pos > self.file.size() {
            Err(StatusError::Eof("self.pos > self.file->Size()".to_string()))
        } else {
            let avail = self.file.size() - self.pos;
            if n > avail {
                n = avail
            }
            self.pos += n;
            Ok(())
        }
    }
}

pub struct MemRandomAccessFile {
    file: FileState,
}

impl MemRandomAccessFile {
    pub fn new(file: FileState) -> Self {
        MemRandomAccessFile {
            file
        }
    }
}

impl RandomAccessFile for MemRandomAccessFile {
    fn read(&self, offset: usize, n: usize, scratch: &mut Vec<u8>) -> Result<Slice> {
        let len = self.file.read(offset, n , scratch)?;
        Ok(Slice::new(scratch.as_ptr(), len))
    }
}

pub struct MemWritableFile {
    file: FileState
}

impl MemWritableFile {
    pub fn new(file: FileState) -> Self {
        MemWritableFile {
            file
        }
    }
}

impl WritableFile for MemWritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.append(data)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }

}

#[derive(Clone)]
pub struct MemEnv {
    file_map: Arc<Mutex<BTreeMap<String, FileState>>>
}

impl MemEnv {
    pub fn new() -> Self {
        MemEnv {
            file_map: Arc::new(Mutex::new(BTreeMap::new()))
        }
    }
}

impl Env for MemEnv {
    type SeqFile = MemSequentialFile;
    type RndFile = MemRandomAccessFile;
    type WrFile = MemWritableFile;

    fn new_sequential_file(&self, name: &String) -> Result<Self::SeqFile> {
        let map = self.file_map.lock().unwrap();
        if let Some(file) = map.get(name) {
            Ok(MemSequentialFile::new(file.clone()))
        } else {
            Err(StatusError::Eof("File not found".to_string()))
        }
    }

    fn new_random_access_file(&self, name: &String) -> Result<Self::RndFile> {
        let map = self.file_map.lock().unwrap();
        if let Some(file) = map.get(name) {
            Ok(MemRandomAccessFile::new(file.clone()))
        } else {
            Err(StatusError::Eof("File not found".to_string()))
        }
    }

    fn new_writable_file(&self, name: &String) -> Result<Self::WrFile> {
        let mut map = self.file_map.lock().unwrap();
        if let Some(file) = map.get_mut(name) {
            file.truncate();
            Ok(MemWritableFile::new((*file).clone()))
        } else {
            let file = FileState::new();
            map.insert(name.clone(), file.clone());
            Ok(MemWritableFile::new(file))
        }
    }

    fn new_appendable_file(&self, name: &String) -> Result<Self::WrFile> {
        let map = self.file_map.lock().unwrap();
        if let Some(file) = map.get(name) {
            Ok(MemWritableFile::new(file.clone()))
        } else {
            Err(StatusError::Eof("File not found".to_string()))
        }
    }

    fn file_exists(&self, name: &String) -> bool {
        let map = self.file_map.lock().unwrap();
        map.contains_key(name)
    }

    fn get_children(&self, name: &String, result: &mut Vec<String>) -> Result<()> {
        let map = self.file_map.lock().unwrap();
        result.clear();
        map.iter().for_each(|(file_name, _)|{
            if file_name.len() >= name.len() + 1 && file_name.starts_with(name) {
                if let Some(ch)  = file_name.chars().nth(name.len()) {
                    if ch == '/' {
                        result.push(file_name[name.len()+1..].to_string())
                    }
                }
            }
        });

        Ok(())
    }

    fn delete_file(&self, name: &String) -> Result<()> {
        let mut map = self.file_map.lock().unwrap();
        if map.remove(name).is_some() {
            Ok(())
        } else {
            Err(StatusError::Eof("File not found".to_string()))
        }
    }

    // Create the specified directory.
    fn create_dir(&self, dirname: &String) -> Result<()> {
        Ok(())
    }

    // Delete the specified directory.
    fn delete_dir(&self, dirname: &String) -> Result<()> {
        Ok(())
    }

    fn get_file_size(&self, name: &String) -> Result<usize> {
        let map = self.file_map.lock().unwrap();
        if let Some(file) = map.get(name) {
            Ok(file.size())
        } else {
            Err(StatusError::Eof("File not found".to_string()))
        }
    }

    fn rename_file(&self, src: &String, target: &String) -> Result<()> {
        let mut map = self.file_map.lock().unwrap();
        if let Some(file) = map.remove(src) {
            map.insert(target.clone(), file);
            Ok(())
        } else {
            Err(StatusError::Eof("File not found".to_string()))
        }
    }

    fn get_test_dir(&self) -> Result<String> {
        Ok("/test".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::write_string_to_file;

    #[test]
    fn test_basic() {
        let env = MemEnv::new();
        let mut children = Vec::new();
        assert!(env.create_dir(&"/dir".to_string()).is_ok());

        // Check that the directory is empty.
        assert!(!env.file_exists(&"/dir/non_existent".to_string()));
        assert!(env.get_file_size(&"/dir/non_existent".to_string()).is_err());
        assert!(env.get_children(&"dir".to_string(), &mut children).is_ok());
        assert!(children.is_empty());

        // Create a file.
        assert!(env.new_writable_file(&"/dir/f".to_string()).is_ok());
        assert_eq!(env.get_file_size(&"/dir/f".to_string()).unwrap(), 0);

        // Check that the file exists.
        assert!(env.file_exists(&"/dir/f".to_string()));
        assert_eq!(env.get_file_size(&"/dir/f".to_string()).unwrap(), 0);
        assert!(env.get_children(&"/dir".to_string(), &mut children).is_ok());
        assert_eq!(children.len(), 1);
        assert_eq!(children[0], "f".to_string());

        // Write to the file.
        let mut f = env.new_writable_file(&"/dir/f".to_string()).unwrap();
        f.append("abc".as_bytes()).unwrap();

        // Check that append works.
        let mut f = env.new_appendable_file(&"/dir/f".to_string()).unwrap();
        assert_eq!(env.get_file_size(&"/dir/f".to_string()).unwrap(), 3);
        f.append("hello".as_bytes()).unwrap();

        // Check for expected size.
        assert_eq!(env.get_file_size(&"/dir/f".to_string()).unwrap(), 8);

        // Check that renaming works.
        assert!(env.rename_file(&"/dir/non_existent".to_string(), &"/dir/g".to_string()).is_err());
        assert!(env.rename_file(&"/dir/f".to_string(), &"/dir/g".to_string()).is_ok());
        assert!(!env.file_exists(&"/dir/f".to_string()));
        assert!(env.file_exists(&"/dir/g".to_string()));
        assert_eq!(env.get_file_size(&"/dir/g".to_string()).unwrap(), 8);

        // Check that opening non-existent file fails.
        assert!(env.new_sequential_file(&"/dir/non_existent".to_string()).is_err());
        assert!(env.new_random_access_file(&"/dir/non_existent".to_string()).is_err());

        // Check that deleting works.
        assert!(env.delete_file(&"/dir/non_existent".to_string()).is_err());
        assert!(env.delete_file(&"/dir/g".to_string()).is_ok());
        assert!(!env.file_exists(&"/dir/g".to_string()));
        assert!(env.get_children(&"dir".to_string(), &mut children).is_ok());
        assert!(children.is_empty());
        assert!(env.delete_dir(&"dir".to_string()).is_ok())
    }

    #[test]
    fn test_read_write() {
        let mut scratch = Vec::new();
        scratch.resize(100, 0);
        let env = MemEnv::new();

        assert!(env.create_dir(&"/dir".to_string()).is_ok());
        let mut writable_file = env.new_writable_file(&"/dir/f".to_string()).unwrap();
        writable_file.append("hello ".as_bytes()).unwrap();
        writable_file.append("world".as_bytes()).unwrap();

        // Read sequentially.
        let mut sequential_file = env.new_sequential_file(&"/dir/f".to_string()).unwrap();
        sequential_file.read(scratch.as_mut(), 5);
        assert_eq!(&scratch[0..5], "hello".as_bytes());
        sequential_file.skip(1).unwrap();
        sequential_file.read(scratch.as_mut(), 1000).unwrap();
        assert_eq!(&scratch[0..5], "world".as_bytes());
        assert_eq!(sequential_file.read(scratch.as_mut(), 1000).unwrap(), 0);
        sequential_file.skip(100).unwrap();
        assert_eq!(sequential_file.read(scratch.as_mut(), 1000).unwrap(), 0);

        // Random reads.
        let mut random_access_file = env.new_random_access_file(&"/dir/f".to_string()).unwrap();
        random_access_file.read(6, 5, scratch.as_mut()).unwrap();
        assert_eq!(&scratch[0..5], "world".as_bytes());
        random_access_file.read(0, 5, scratch.as_mut()).unwrap();
        assert_eq!(&scratch[0..5], "hello".as_bytes());
        random_access_file.read(10, 100, scratch.as_mut()).unwrap();
        assert_eq!(&scratch[0..1], "d".as_bytes());
        // Too high offset.
        assert!(random_access_file.read(1000, 5, scratch.as_mut()).is_err());
    }

    #[test]
    fn test_misc() {
        let env = MemEnv::new();
        let test_dir = env.get_test_dir().unwrap();
        assert!(!test_dir.is_empty());

        let mut writable_file = env.new_writable_file(&"/a/b".to_string()).unwrap();
        // These are no-ops, but we test they return success.
        assert!(writable_file.sync().is_ok());
        assert!(writable_file.flush().is_ok());
        assert!(writable_file.close().is_ok());
    }

    #[test]
    fn test_large_write() {
        let env = MemEnv::new();

        let write_size = 300 * 1024;
        let mut scratch = Vec::new();
        scratch.resize((write_size * 2) as usize, 0);

        let mut write_data = Vec::new();
        for i in 0.. write_size {
            write_data.push(i as u8);
        }

        let mut writable_file = env.new_writable_file(&"/dir/f".to_string()).unwrap();
        assert!(writable_file.append("foo".as_bytes()).is_ok());
        assert!(writable_file.append(write_data.as_slice()).is_ok());

        let mut seq_file = env.new_sequential_file(&"/dir/f".to_string()).unwrap();
        let n = seq_file.read(scratch.as_mut(), 3).unwrap();
        assert_eq!(&scratch[0..3], "foo".as_bytes());

        let mut read = 0;
        let mut read_data = Vec::new();
        while read < write_size {
            let n = seq_file.read(scratch.as_mut(), write_size - read).unwrap();
            read_data.extend_from_slice(&scratch[0..n]);
            read += n;
        }
        assert_eq!(write_data.as_slice(), read_data.as_slice());
    }

    #[test]
    fn test_overwrite_open_file() {
        let env = MemEnv::new();
        let write_1_data = "Write #1 data".as_bytes();
        let file_data_len = write_1_data.len();
        let test_file_name = "/tmp/leveldb-TestFile.dat".to_string();

        assert!(write_string_to_file(env.clone(), write_1_data, &test_file_name).is_ok());
        let rand_file = env.new_random_access_file(&test_file_name).unwrap();
        let write_2_data = "Write #2 data".as_bytes();
        assert!(write_string_to_file(env, write_2_data, &test_file_name).is_ok());
        // Verify that overwriting an open file will result in the new file data
        // being read from files opened before the write.
        let mut scratch = Vec::new();
        let ret_data = rand_file.read(0, file_data_len, &mut scratch).unwrap();
        assert_eq!(ret_data.as_ref(), write_2_data);
    }
}

