use crc32fast::Hasher;
use crate::db::slice::Slice;
use crate::db::{RecordType, WritableFile, SequentialFile, BLOCK_SIZE, HEADER_SIZE};
use crate::util::buffer::{BufferReader};
use crate::db::error::{Result, StatusError};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};

pub struct LogWriter<W: WritableFile> {
    writer: W,
    offset: usize,
}

impl<W: WritableFile> LogWriter<W> {
    pub fn new(writer: W) -> Self {
        LogWriter {
            writer,
            offset: 0,
        }
    }

    pub fn new_with_dest_len(writer: W, offset: usize) -> Self {
        LogWriter {
            writer,
            offset,
        }
    }

    pub fn add_record(&mut self, mut slice: &Slice) -> Result<()> {
        let mut  begin = true;
        loop {
            if slice.is_empty() {
                break;
            }

            let left = slice.len();
            let leftover = BLOCK_SIZE - self.offset;
            assert!(leftover >= 0);
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    self.writer.append(b"\x00\x00\x00\x00\x00\x00\x00"[..leftover].as_ref())?;
                }
                self.offset = 0;
            }

            assert!(BLOCK_SIZE - self.offset >= HEADER_SIZE);
            let avail = BLOCK_SIZE - self.offset - HEADER_SIZE;
            let (record_type, fragment_length) = match (begin, left <= avail) {
                (true, true) => (RecordType::FullType, left),
                (true, false) => (RecordType::FirstType, avail),
                (false, true) => (RecordType::LastType, left),
                (false, false) => (RecordType::MiddleType, avail),
            };
            let data = slice.read_bytes(fragment_length)?;
            self.emit_physical_record(record_type, data)?;
            begin = false;
        }
        Ok(())
    }

    fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        assert!(self.offset + HEADER_SIZE + data.len() <= BLOCK_SIZE);

        let mut hasher = Hasher::new();
        hasher.update([record_type as u8].as_ref());
        hasher.update(data);
        let checksum = hasher.finalize();
        let mut header: [u8; HEADER_SIZE] = [0; HEADER_SIZE];

        let mut checksum_buf = header[..4].as_mut();
        checksum_buf.write_u32::<LittleEndian>(checksum)?;
        let mut length_buf = header[4..6].as_mut();
        length_buf.write_u16::<LittleEndian>(data.len() as u16)?;
        let mut record_type_buf = header[6..].as_mut();
        record_type_buf.write_u8(record_type as u8)?;

        self.writer.append(header.as_ref())?;
        self.writer.append(data.to_vec().as_slice())?;
        self.writer.flush()?;
        self.offset += HEADER_SIZE + data.len();

        Ok(())
    }
}

pub struct LogReader<R: SequentialFile> {
    file: R,
    buffer: Vec<u8>,
    consumed: usize,
    cap: usize,
    eof: bool,
}

impl<R: SequentialFile> LogReader<R> {
    pub fn new(file: R) -> Self {
        let mut buffer = Vec::new();
        buffer.resize(BLOCK_SIZE, 0);
        LogReader {
            file,
            buffer,
            consumed: 0,
            cap: 0,
            eof: false,
        }
    }

    pub fn read_record(&mut self, record: &mut Vec<u8>) -> Result<()> {
        let mut in_fragment_record = false;
        record.clear();
        loop {
            match self.read_physical_record(record)? {
                RecordType::FullType => {
                    if in_fragment_record {
                        // TODO: report error
                    }
                    return Ok(());
                }
                RecordType::FirstType => {
                    if in_fragment_record {
                        // TODO: report error
                    }
                    in_fragment_record = true;
                }
                RecordType::MiddleType => {
                    if !in_fragment_record {
                        // TODO: report error
                    }
                }
                RecordType::LastType => {
                    if !in_fragment_record {
                        // TODO: report error
                    } else {
                        return Ok(());
                    }

                }
                RecordType::Eof => {
                    if in_fragment_record {
                        record.clear();
                    }
                    return Err(StatusError::Eof("meet a eof".to_string()));
                }
                RecordType::BadRecord => {
                    if in_fragment_record {
                        // TODO: report error
                        record.clear();
                        in_fragment_record = false;
                    }
                }
                _ => {
                    // TODO: report error
                    in_fragment_record = false;
                    record.clear();
                }
            }
        }
    }

    pub fn read_physical_record(&mut self, record: &mut Vec<u8>) -> Result<RecordType> {
        loop {
            if self.cap - self.consumed < HEADER_SIZE {
                if !self.eof {
                    self.consumed = 0;
                    if let Ok(n) = self.file.read(self.buffer.as_mut(), BLOCK_SIZE) {
                        self.cap = n;
                        if n < BLOCK_SIZE {
                            self.eof = true;
                        }
                    } else {
                        // TODO: report coruption
                        self.eof = true;
                        return Ok(RecordType::Eof);
                    }
                    continue;
                } else {
                    self.consumed = 0;
                    self.cap = 0;
                    return Ok(RecordType::Eof);
                }
            }

            let mut buf = self.buffer.as_slice();
            buf.read_bytes(self.consumed)?;
            // start to read header
            let mut header = buf.read_bytes(7)?;
            let checksum = header.read_u32::<LittleEndian>()?;
            let length = header.read_u16::<LittleEndian>()?;
            let record_type = header.read_u8()?;

            if HEADER_SIZE + length as usize > self.cap - self.consumed {
                // TODO report coruption
                self.consumed = 0;
                self.cap = 0;
                if !self.eof {
                    return Ok(RecordType::BadRecord);
                }
                return Ok(RecordType::Eof)
            }
            if record_type == RecordType::ZeroType as u8 && length == 0 {
                self.consumed = 0;
                self.cap = 0;
                return Ok(RecordType::BadRecord)
            }

            let data = buf.read_bytes(length as usize)?;
            let mut hasher = crc32fast::Hasher::new();
            hasher.update([record_type].as_ref());
            hasher.update(data);
            if checksum != hasher.finalize() {
                // TODO report coruption
                self.consumed = 0;
                self.cap = 0;
                return Ok(RecordType::BadRecord)
            }

            self.consumed += HEADER_SIZE + length as usize;
            record.extend_from_slice(data);
            return Ok(record_type.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use std::cell::RefCell;
    use crate::db::SequentialFile;
    use std::io::{Write, Read};
    use failure::Fail;
    use crate::util::buffer::BufferWriter;

    struct MockWritAbleFile {
        vec: Rc<RefCell<Vec<u8>>>,
    }

    impl MockWritAbleFile {
        pub fn new(vec: Rc<RefCell<Vec<u8>>>) -> Self {
            MockWritAbleFile {
                vec,
            }
        }
    }

    impl WritableFile for MockWritAbleFile {
        fn append(&mut self, data: &Slice) -> Result<()> {
            self.vec.borrow_mut().extend_from_slice(data);
            Ok(())
        }

        fn close(&mut self) -> Result<()> {return Ok(())}
        fn flush(&mut self) -> Result<()> {return Ok(())}
        fn sync(&mut self) -> Result<()> {return Ok(())}
    }

    struct MockDataSource {
        vec: Rc<RefCell<Vec<u8>>>,
        consume_num: usize,
    }

    impl MockDataSource {
        pub fn new(vec: Rc<RefCell<Vec<u8>>>) -> Self {
            MockDataSource {
                vec,
                consume_num: 0,
            }
        }
    }

    impl SequentialFile for MockDataSource {
        fn read(&mut self, buf: &mut [u8], n: usize) -> Result<usize> {
            let vec = self.vec.borrow_mut();
            let mut data = vec.as_slice();
            data.read_bytes(self.consume_num);
            let mut cap = n;
            if data.len() < n {
                cap = data.len()
            }
            let mut buf = buf;
            self.consume_num += n;
            buf.write(&data[..cap]);
            Ok(cap)
        }

        fn skip(&mut self, n: usize) -> Result<()> {
            self.consume_num += n;
            Ok(())
        }
    }

    struct LogTest {
        reading: bool,
        writer: LogWriter<MockWritAbleFile>,
        reader: LogReader<MockDataSource>,
    }

    impl LogTest {
        pub fn new() -> Self {
            let vec = Rc::new(RefCell::new(Vec::new()));
            let dest = MockWritAbleFile::new(vec.clone());
            let src = MockDataSource::new(vec);
            let writer = LogWriter::new(dest);
            let reader = LogReader::new(src);

            LogTest {
                writer,
                reader,
                reading: false,
            }
        }

        pub fn write(&mut self, data: &Slice) -> Result<()> {
            self.writer.add_record(data)
        }

        pub fn read(&mut self) -> Result<Vec<u8>> {
            let mut v = Vec::new();
            self.reader.read_record(&mut v)?;
            Ok(v)
        }
    }

    fn number_string(n: i32) -> String {
        format!("{}", n)
    }

    #[test]
    fn test_read_write() {
        let mut tester = LogTest::new();
        let cases = vec![
            "foo".to_string(),
            "bar".to_string(),
            "abcdefg".to_string(),
            "xxxx".to_string(),
            "leveldb牛逼".to_string(),
            "1234567890".to_string(),
        ];

        for input in cases.iter() {
            assert!(tester.write(input.as_bytes()).is_ok())
        }
        for expect in cases.iter() {
            assert_eq!(String::from_utf8(tester.read().unwrap()).unwrap().as_str(), expect.as_str());
        }

        assert_eq!(tester.read().unwrap_err().to_string(), format!("meet a eof"));
        assert_eq!(tester.read().unwrap_err().to_string(), format!("meet a eof"));
    }

    #[test]
    fn test_many_blocks() {
        let mut tester = LogTest::new();
        for i in 0..1000000 {
            assert!(tester.write(number_string(i).as_bytes()).is_ok())
        }
        for i in 0..1000000 {
            assert_eq!(String::from_utf8(tester.read().unwrap()).unwrap(), number_string(i));
        }

        assert_eq!(tester.read().unwrap_err().to_string(), format!("meet a eof"));
    }
}
