use crate::db::error::{Result, StatusError};
use crate::db::{RecordType, Reporter, SequentialFile, WritableFile, BLOCK_SIZE, HEADER_SIZE};
use crate::util::buffer::BufferReader;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

pub struct LogWriter<W: WritableFile> {
    writer: W,
    offset: usize,
}

impl<W: WritableFile> LogWriter<W> {
    pub fn new(writer: W) -> Self {
        LogWriter { writer, offset: 0 }
    }

    pub fn new_with_dest_len(writer: W, offset: usize) -> Self {
        LogWriter { writer, offset }
    }

    pub fn add_record(&mut self, mut slice: &[u8]) -> Result<()> {
        let mut begin = true;
        loop {
            if slice.is_empty() {
                break;
            }

            let left = slice.len();
            assert!(BLOCK_SIZE >= self.offset);
            let leftover = BLOCK_SIZE - self.offset;
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    self.writer
                        .append(b"\x00\x00\x00\x00\x00\x00\x00"[..leftover].as_ref())?;
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

    pub fn sync(&mut self) -> Result<()> {
        self.writer.sync()
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

pub struct LogReader<S: SequentialFile, R: Reporter> {
    file: S,
    reporter: R,
    buffer: Vec<u8>,
    consumed: usize,
    cap: usize,
    eof: bool,
}

impl<S: SequentialFile, R: Reporter> LogReader<S, R> {
    pub fn new(file: S, reporter: R) -> Self {
        let mut buffer = Vec::new();
        buffer.resize(BLOCK_SIZE, 0);
        LogReader {
            file,
            reporter,
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
                (RecordType::FullType, n) => {
                    if in_fragment_record && !record.is_empty() {
                        // Handle bug in earlier versions of log::Writer where
                        // it could emit an empty kFirstType record at the tail end
                        // of a block followed by a kFullType or kFirstType record
                        // at the beginning of the next block.
                        let dropped = record.len() - n;
                        if dropped > 0 {
                            self.reporter.corruption(
                                dropped,
                                StatusError::Corruption(
                                    "partial record without end(1)".to_string(),
                                ),
                            );
                        }
                        record.drain(0..dropped);
                    }
                    return Ok(());
                }
                (RecordType::FirstType, n) => {
                    if in_fragment_record && !record.is_empty() {
                        let dropped = record.len() - n;
                        if dropped > 0 {
                            self.reporter.corruption(
                                dropped,
                                StatusError::Corruption(
                                    "partial record without end(2)".to_string(),
                                ),
                            );
                        }
                        record.drain(0..dropped);
                    }
                    in_fragment_record = true;
                }
                (RecordType::MiddleType, n) => {
                    if !in_fragment_record {
                        self.reporter.corruption(
                            n,
                            StatusError::Corruption(
                                "missing start of fragmented record(1)".to_string(),
                            ),
                        );
                        let remain = record.len() - n;
                        record.truncate(remain);
                    }
                }
                (RecordType::LastType, n) => {
                    if !in_fragment_record {
                        self.reporter.corruption(
                            n,
                            StatusError::Corruption(
                                "missing start of fragmented record(2)".to_string(),
                            ),
                        );
                        let remain = record.len() - n;
                        record.truncate(remain);
                    } else {
                        return Ok(());
                    }
                }
                (RecordType::Eof, _) => {
                    if in_fragment_record {
                        // This can be caused by the writer dying immediately after
                        // writing a physical record but before completing the next; don't
                        // treat it as a corruption, just ignore the entire logical record.
                        record.clear();
                    }
                    return Err(StatusError::Eof("meet a eof".to_string()));
                }
                (RecordType::BadRecord, _) => {
                    // when get `BadRecord`, the data has not been added to `record`.
                    if in_fragment_record {
                        self.reporter.corruption(
                            record.len(),
                            StatusError::Corruption("error in middle of record".to_string()),
                        );
                        record.clear();
                        in_fragment_record = false;
                    }
                }
                _ => {
                    self.reporter.corruption(
                        record.len(),
                        StatusError::Corruption("unknown record type".to_string()),
                    );
                    in_fragment_record = false;
                    record.clear();
                }
            }
        }
    }

    // second returned parameter means how much bytes has been appended to record.
    pub fn read_physical_record(&mut self, record: &mut Vec<u8>) -> Result<(RecordType, usize)> {
        loop {
            if self.cap - self.consumed < HEADER_SIZE {
                if !self.eof {
                    self.consumed = 0;
                    match self.file.read(self.buffer.as_mut(), BLOCK_SIZE) {
                        Ok(n) => {
                            self.cap = n;
                            if n < BLOCK_SIZE {
                                self.eof = true;
                            }
                        }
                        Err(e) => {
                            self.reporter.corruption(BLOCK_SIZE, e);
                            self.eof = true;
                            return Ok((RecordType::Eof, 0));
                        }
                    }
                    continue;
                } else {
                    self.consumed = 0;
                    self.cap = 0;
                    return Ok((RecordType::Eof, 0));
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
                let dropped = self.cap - self.consumed;
                self.consumed = 0;
                self.cap = 0;
                if !self.eof {
                    self.reporter.corruption(
                        dropped,
                        StatusError::Corruption("bad record length".to_string()),
                    );
                    return Ok((RecordType::BadRecord, 0));
                }
                // If the end of the file has been reached without reading |length| bytes
                // of payload, assume the writer died in the middle of writing the record.
                // Don't report a corruption.
                return Ok((RecordType::Eof, 0));
            }
            if record_type == RecordType::ZeroType as u8 && length == 0 {
                self.consumed = 0;
                self.cap = 0;
                return Ok((RecordType::BadRecord, 0));
            }

            let data = buf.read_bytes(length as usize)?;
            let mut hasher = crc32fast::Hasher::new();
            hasher.update([record_type].as_ref());
            hasher.update(data);
            if checksum != hasher.finalize() {
                let dropped = self.cap - self.consumed;
                self.consumed = 0;
                self.cap = 0;
                self.reporter.corruption(
                    dropped,
                    StatusError::Corruption("checksum mismatch".to_string()),
                );
                return Ok((RecordType::BadRecord, length as usize));
            }

            self.consumed += HEADER_SIZE + length as usize;
            record.extend_from_slice(data);
            return Ok((record_type.into(), length as usize));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SequentialFile;
    use rand;
    use rand::Rng;
    use std::cell::RefCell;
    use std::io::Write;
    use std::rc::Rc;

    struct MemoryFile {
        pub vec: Vec<u8>,
        // for mock read
        pub consume_num: usize,
        pub force_error: bool,
        pub returned_partial: bool,
    }

    impl MemoryFile {
        pub fn new() -> Self {
            MemoryFile {
                vec: Vec::new(),
                consume_num: 0,
                force_error: false,
                returned_partial: false,
            }
        }
    }

    type MockWritableFile = Rc<RefCell<MemoryFile>>;

    impl WritableFile for MockWritableFile {
        fn append(&mut self, data: &[u8]) -> Result<()> {
            let mut file = self.borrow_mut();
            file.vec.extend_from_slice(data);
            Ok(())
        }

        fn close(&mut self) -> Result<()> {
            return Ok(());
        }
        fn flush(&mut self) -> Result<()> {
            return Ok(());
        }
        fn sync(&mut self) -> Result<()> {
            return Ok(());
        }
    }

    type MockSequentialFile = Rc<RefCell<MemoryFile>>;

    impl SequentialFile for MockSequentialFile {
        fn read(&mut self, buf: &mut [u8], n: usize) -> Result<usize> {
            let mut file = self.borrow_mut();
            if file.force_error {
                file.force_error = false;
                file.returned_partial = true;
                return Err(StatusError::Corruption("read error".to_string()));
            }

            let consume = file.consume_num;
            let remain = file.vec.len() - consume;
            let mut data_len = n;
            if remain < n {
                data_len = remain;
                file.returned_partial = true;
            }
            file.consume_num += data_len;

            let mut buf = buf;
            let mut data = file.vec.as_slice();
            data.advance(consume);
            buf.write(&data[..data_len]).unwrap();

            Ok(data_len)
        }

        fn skip(&mut self, n: usize) -> Result<()> {
            let mut file = self.borrow_mut();
            if file.consume_num + n > file.vec.len() {
                return Err(StatusError::NotFound(
                    "in-memory file skipped past end".to_string(),
                ));
            }
            file.consume_num += n;
            Ok(())
        }
    }

    struct ReporterController {
        pub dropped_bytes: usize,
        pub message: String,
    }

    impl ReporterController {
        pub fn new() -> Self {
            ReporterController {
                dropped_bytes: 0,
                message: String::new(),
            }
        }
    }

    type MockReporter = Rc<RefCell<ReporterController>>;

    impl Reporter for MockReporter {
        fn corruption(&mut self, n: usize, status: StatusError) {
            let mut reporter = self.borrow_mut();
            reporter.dropped_bytes += n;
            reporter.message.push_str(status.to_string().as_str())
        }
    }

    struct LogTest {
        writer: LogWriter<MockWritableFile>,
        reader: LogReader<MockSequentialFile, MockReporter>,
        dest: MockWritableFile,
        src: MockSequentialFile,
        reporter: MockReporter,
    }

    impl LogTest {
        pub fn new() -> Self {
            let dest = Rc::new(RefCell::new(MemoryFile::new()));
            let src = dest.clone();
            let reporter = Rc::new(RefCell::new(ReporterController::new()));
            let writer = LogWriter::new(dest.clone());
            let reader = LogReader::new(src.clone(), reporter.clone());

            LogTest {
                writer,
                reader,
                dest,
                src,
                reporter,
            }
        }

        pub fn write(&mut self, data: &[u8]) -> Result<()> {
            self.writer.add_record(data)
        }

        pub fn read(&mut self) -> Result<Vec<u8>> {
            let mut v = Vec::new();
            self.reader.read_record(&mut v)?;
            Ok(v)
        }

        pub fn read_string(&mut self) -> String {
            let v = self.read().unwrap();
            String::from_utf8(v).unwrap()
        }

        pub fn writen_bytes(&self) -> usize {
            let file = self.dest.borrow();
            file.vec.len()
        }

        pub fn assert_read_eof(&mut self) {
            assert_eq!(self.read().unwrap_err().to_string(), format!("meet a eof"));
        }

        pub fn dropped_bytes(&self) -> usize {
            let reporter = self.reporter.borrow_mut();
            reporter.dropped_bytes
        }

        pub fn report_message(&self) -> String {
            let reporter = self.reporter.borrow();
            reporter.message.clone()
        }

        pub fn reopen_for_append(&mut self) {
            self.writer = LogWriter::new(self.dest.clone());
        }

        pub fn force_error(&mut self) {
            let mut file = self.src.borrow_mut();
            file.force_error = true;
        }

        pub fn match_error(&mut self, partial: &'static str) -> bool {
            self.report_message().find(partial).is_some()
        }

        pub fn increment_byte(&mut self, offset: usize, delta: u8) {
            let mut file = self.src.borrow_mut();
            file.vec[offset] += delta;
        }

        pub fn set_byte(&mut self, offset: usize, new_byte: u8) {
            let mut file = self.src.borrow_mut();
            file.vec[offset] = new_byte;
        }

        pub fn fix_checksum(&mut self, header_offset: usize, len: usize) {
            let mut file = self.src.borrow_mut();
            let mut data = file.vec.as_slice();
            data.advance(header_offset + 6);
            let data = data.read_bytes(len + 1).unwrap();
            let mut hash = crc32fast::Hasher::new();
            hash.update(data);
            let checksum = hash.finalize();
            let mut writer = file.vec.as_mut_slice();
            writer.write_u32::<LittleEndian>(checksum).unwrap()
        }

        pub fn shrink_size(&mut self, bytes: usize) {
            let mut file = self.src.borrow_mut();
            let len = file.vec.len();
            file.vec.truncate(len - bytes);
        }
    }

    fn number_string(n: i32) -> String {
        format!("{}", n)
    }

    fn big_string(partial: &str, n: usize) -> String {
        let mut s = String::new();
        s.reserve(n);
        while s.len() < n {
            s.push_str(partial);
        }
        s.truncate(n);
        s
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
            "!@#@#%#$GGTH&FD^^^'fdt'GDDfdfgdfhd21545".to_string(),
        ];

        for input in cases.iter() {
            assert!(tester.write(input.as_bytes()).is_ok());
        }
        for expect in cases.iter() {
            assert_eq!(tester.read_string().as_str(), expect.as_str(),);
        }

        tester.assert_read_eof();
        tester.assert_read_eof();
    }

    #[test]
    fn test_many_blocks() {
        let mut tester = LogTest::new();
        for i in 0..1000000 {
            assert!(tester.write(number_string(i).as_bytes()).is_ok())
        }
        for i in 0..1000000 {
            assert_eq!(tester.read_string(), number_string(i));
        }

        tester.assert_read_eof();
    }

    #[test]
    fn test_fragment() {
        let mut tester = LogTest::new();
        let cases = vec![
            "small".to_string(),
            big_string("medium", 50000),
            big_string("large", 100000),
            big_string("larger", 200000),
        ];

        for case in cases.iter() {
            tester.write(case.as_bytes()).unwrap();
        }

        for case in cases.iter() {
            assert_eq!(tester.read_string().as_str(), case.as_str());
        }

        tester.assert_read_eof();
    }

    #[test]
    fn test_marginal_trailer() {
        let mut tester = LogTest::new();
        let n = BLOCK_SIZE - 2 * HEADER_SIZE;
        tester.write(big_string("foo", n).as_bytes()).unwrap();
        assert_eq!(BLOCK_SIZE - HEADER_SIZE, tester.writen_bytes());

        tester.write(b"\x00").unwrap();
        tester.write("bar".as_bytes()).unwrap();

        assert_eq!(tester.read_string().as_str(), big_string("foo", n));
        assert_eq!(tester.read_string().as_bytes(), b"\x00");
        assert_eq!(tester.read_string().as_str(), "bar");
    }

    #[test]
    fn test_marginal_trailer2() {
        let mut tester = LogTest::new();
        let n = BLOCK_SIZE - 2 * HEADER_SIZE;
        tester.write(big_string("foo", n).as_bytes()).unwrap();
        assert_eq!(BLOCK_SIZE - HEADER_SIZE, tester.writen_bytes());
        tester.write("bar".as_bytes()).unwrap();

        assert_eq!(tester.read_string().as_str(), big_string("foo", n));
        assert_eq!(tester.read_string().as_str(), "bar");
        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 0);
        assert_eq!(tester.report_message().as_str(), "");
    }

    #[test]
    fn test_shorter_trailer() {
        let mut tester = LogTest::new();
        let n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        tester.write(big_string("foo", n).as_bytes()).unwrap();
        assert_eq!(BLOCK_SIZE - HEADER_SIZE + 4, tester.writen_bytes());
        tester.write(b"\x00").unwrap();
        tester.write("bar".as_bytes()).unwrap();

        assert_eq!(tester.read_string().as_str(), big_string("foo", n));
        assert_eq!(tester.read_string().as_bytes(), b"\x00");
        assert_eq!(tester.read_string().as_str(), "bar");
        tester.assert_read_eof();
    }

    #[test]
    fn test_aligned_eof() {
        let mut tester = LogTest::new();
        let n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        tester.write(big_string("foo", n).as_bytes()).unwrap();

        assert_eq!(BLOCK_SIZE - HEADER_SIZE + 4, tester.writen_bytes());
        assert_eq!(tester.read_string().as_str(), big_string("foo", n));
        tester.assert_read_eof();
    }

    #[test]
    fn test_open_for_append() {
        let mut tester = LogTest::new();
        tester.write("hello".as_bytes()).unwrap();
        tester.reopen_for_append();
        tester.write("world".as_bytes()).unwrap();

        assert_eq!(tester.read_string().as_str(), "hello");
        assert_eq!(tester.read_string().as_str(), "world");
        tester.assert_read_eof();
    }

    #[test]
    fn test_random_read() {
        let mut tester = LogTest::new();
        let mut rng = rand::thread_rng();
        let mut rand_number_strings = Vec::new();
        for i in 0..300 {
            let high: u32 = 1 << rng.gen_range(1, 17);
            let n: u32 = rng.gen_range(1, high);
            let s = big_string(number_string(i).as_str(), n as usize);
            rand_number_strings.push(s);
        }

        for s in rand_number_strings.iter() {
            tester.write(s.as_bytes()).unwrap();
        }

        for s in rand_number_strings.iter() {
            assert_eq!(tester.read_string().as_str(), s.as_str());
        }
    }

    #[test]
    fn test_read_error() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.force_error();

        tester.assert_read_eof();
        assert!(tester.match_error("read error"));
    }

    #[test]
    fn test_bad_record_type() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.increment_byte(6, 100);
        tester.fix_checksum(0, 3);

        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 3);
        assert!(tester.match_error("unknown record type"));
    }

    #[test]
    fn truncated_trailing_record_is_ignored() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.shrink_size(4);

        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 0);
        assert_eq!("", tester.report_message().as_str());
    }

    #[test]
    fn test_bad_length() {
        let mut tester = LogTest::new();
        let payload_size = BLOCK_SIZE - HEADER_SIZE;
        tester
            .write(big_string("bar", payload_size).as_bytes())
            .unwrap();
        tester.increment_byte(4, 1);
        tester.write("foo".as_bytes()).unwrap();

        assert_eq!(tester.read_string().as_str(), "foo");
        assert_eq!(tester.dropped_bytes(), BLOCK_SIZE);
        assert!(tester.match_error("bad record length"));
    }

    #[test]
    fn test_bad_length_at_end_is_ignored() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.shrink_size(1);

        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 0);
        assert_eq!(tester.report_message().as_str(), "");
    }

    #[test]
    fn test_checksum_mismatch() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.increment_byte(0, 10);
        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 10);
        assert!(tester.match_error("checksum mismatch"));
    }

    #[test]
    fn test_unexpected_middle_type() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.set_byte(6, 3);
        tester.fix_checksum(0, 3);

        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 3);
        assert!(tester.match_error("missing start"));
    }

    #[test]
    fn test_unexpected_last_type() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.set_byte(6, 4);
        tester.fix_checksum(0, 3);

        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 3);
        assert!(tester.match_error("missing start"));
    }

    #[test]
    fn test_unexpected_full_type() {
        let mut tester = LogTest::new();
        tester.write("foo".as_bytes()).unwrap();
        tester.write("bar".as_bytes()).unwrap();
        tester.set_byte(6, 2);
        tester.fix_checksum(0, 3);

        assert_eq!(tester.read_string().as_str(), "bar");
        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 3);
        assert!(tester.match_error("partial record without end"));
    }

    #[test]
    fn test_missing_last_is_ignored() {
        let mut tester = LogTest::new();
        tester
            .write(big_string("bar", BLOCK_SIZE).as_bytes())
            .unwrap();
        tester.shrink_size(14);

        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 0);
        assert_eq!(tester.report_message().as_str(), "");
    }

    #[test]
    fn test_partial_last_is_ignored() {
        let mut tester = LogTest::new();
        tester
            .write(big_string("bar", BLOCK_SIZE).as_bytes())
            .unwrap();
        tester.shrink_size(1);

        tester.assert_read_eof();
        assert_eq!(tester.dropped_bytes(), 0);
        assert_eq!(tester.report_message().as_str(), "");
    }

    #[test]
    fn test_error_joins_record() {
        let mut tester = LogTest::new();
        tester
            .write(big_string("foo", BLOCK_SIZE).as_bytes())
            .unwrap();
        tester
            .write(big_string("bar", BLOCK_SIZE).as_bytes())
            .unwrap();
        tester.write("correct".as_bytes()).unwrap();

        for i in BLOCK_SIZE..2 * BLOCK_SIZE {
            tester.set_byte(i, b'x')
        }

        assert_eq!(tester.read_string().as_str(), "correct");
        tester.assert_read_eof();
        let dropped = tester.dropped_bytes();
        assert!(dropped <= 2 * BLOCK_SIZE + 100);
        assert!(dropped >= 2 * BLOCK_SIZE);
    }
}
