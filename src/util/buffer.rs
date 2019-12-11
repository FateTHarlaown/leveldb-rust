use crate::util::error::Result;

pub trait BufferReader {
    fn bytes(&self) -> &[u8];
    fn advance(&mut self, count: usize);
    fn read_bytes(&mut self, count: usize) -> Result<&[u8]>;
}
