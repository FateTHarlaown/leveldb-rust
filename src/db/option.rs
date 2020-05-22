use crate::db::slice::Slice;
use crate::util::cmp::{BitWiseComparator, Comparator};
use std::rc::Rc;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
pub const NO_COMPRESSION: u8 = 0x0;
pub const SNAPPY_COMPRESSION: u8 = 0x1;

pub struct Options {
    // -------------------
    // Parameters that affect behavior

    // Comparator used to define the order of keys in the table.
    // Default: a comparator that uses lexicographic byte-wise ordering
    //
    // REQUIRES: The client must ensure that the comparator supplied
    // here has the same name and orders keys *exactly* the same as the
    // comparator provided to previous open calls on the same DB.
    pub comparator: Rc<dyn Comparator<Slice>>,
    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically.  Most clients should
    // leave this parameter alone.
    pub block_restart_interval: u32,
    // Compress blocks using the specified compression algorithm.  This
    // parameter can be changed dynamically.
    //
    // Default: kSnappyCompression, which gives lightweight but fast
    // compression.
    //
    // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
    //    ~200-500MB/s compression
    //    ~400-800MB/s decompression
    // Note that these speeds are significantly faster than most
    // persistent storage speeds, and therefore it is typically never
    // worth switching to kNoCompression.  Even if the input data is
    // incompressible, the kSnappyCompression implementation will
    // efficiently detect that and will switch to uncompressed mode.
    pub compression_type: u8,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparator: Rc::new(BitWiseComparator {}),
            block_restart_interval: 16,
            compression_type: SNAPPY_COMPRESSION,
        }
    }
}
