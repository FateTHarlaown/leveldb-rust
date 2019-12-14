use crate::db::error::Result;

pub trait BufferWriter {
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8];
    unsafe fn advance_mut(&mut self, count: usize);
    fn write_bytes(&mut self, values: &[u8]) -> Result<()>;
}

pub trait BufferReader {
    fn bytes(&self) -> &[u8];
    fn advance(&mut self, count: usize);
    fn read_bytes(&mut self, count: usize) -> Result<&[u8]>;
}
