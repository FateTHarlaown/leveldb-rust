// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
pub const NO_COMPRESSION: u8 = 0x0;
pub const SNAPPY_COMPRESSION: u8 = 0x1;
