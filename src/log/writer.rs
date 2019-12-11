use crc32fast::Hasher;
use crate::log::slice::Slice;
use crate::log::{RecordType, Writable, BLOCK_SIZE, HEADER_SIZE};
use crate::util::buffer::BufferReader;
use crate::util::error::Result;
use byteorder::{LittleEndian, WriteBytesExt};

pub struct LogWriter<W: Writable> {
    writer: W,
    offset: usize,
}

impl<W: Writable> LogWriter<W> {
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
            assert!(BLOCK_SIZE > self.offset);
            let leftover = BLOCK_SIZE - self.offset;

            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    self.writer.append(b"\x00\x00\x00\x00\x00\x00\x00".to_vec().as_slice())?;
                }
                self.offset = 0;
            }

            assert!(BLOCK_SIZE - self.offset - HEADER_SIZE > 0);
            let avail = BLOCK_SIZE - self.offset - HEADER_SIZE;
            let (record_type, fragment_length) = match (begin, left < avail) {
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
        assert!(data.len() <= 0xff );
        assert!(self.offset + HEADER_SIZE + data.len() <= BLOCK_SIZE);

        let mut hasher = Hasher::new();
        hasher.update(data);
        let checksum = hasher.finalize();
        let mut header_buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];

        header_buf.as_mut().write_u32::<LittleEndian>(checksum)?;
        header_buf.as_mut().write_u16::<LittleEndian>(data.len() as u16)?;
        header_buf.as_mut().write_u8(record_type as u8)?;

        self.writer.append(header_buf.to_vec().as_slice())?;
        self.writer.append(data.to_vec().as_slice())?;
        self.writer.flush()?;
        let data_len = data.len();
        self.offset += HEADER_SIZE + data_len;

        Ok(())
    }
}
