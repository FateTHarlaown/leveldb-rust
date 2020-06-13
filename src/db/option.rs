use crate::db::slice::Slice;
use crate::sstable::block::Block;
use crate::util::cache::Cache;
use crate::util::cmp::{BitWiseComparator, Comparator};
use crate::util::filter::FilterPolicy;
use std::rc::Rc;
use std::sync::Arc;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
pub const NO_COMPRESSION: u8 = 0x0;
pub const SNAPPY_COMPRESSION: u8 = 0x1;

pub struct Options {
    // -------------------
    // Parameters that affect behavior

    // Comparator used to define the order of keys in the sstable.
    // Default: a comparator that uses lexicographic byte-wise ordering
    //
    // REQUIRES: The client must ensure that the comparator supplied
    // here has the same name and orders keys *exactly* the same as the
    // comparator provided to previous open calls on the same DB.
    pub comparator: Arc<dyn Comparator<Slice>>,

    // If true, the implementation will do aggressive checking of the
    // data it is processing and will stop early if it detects any
    // errors.  This may have unforeseen ramifications: for example, a
    // corruption of one DB entry may cause a large number of entries to
    // become unreadable or for the entire DB to become unopenable.
    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically.  Most clients should
    // leave this parameter alone.
    pub paranoid_checks: bool,

    // Control over blocks (user data is stored in a set of blocks, and
    // a block is the unit of reading from disk).

    // If Some, use the specified cache for blocks.
    // If None, leveldb will automatically create and use an 8MB internal cache.
    pub block_cache: Option<Arc<dyn Cache<Vec<u8>, Block>>>,

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

    // If non-null, use the specified filter policy to reduce disk reads.
    // Many applications will benefit from passing the result of
    // NewBloomFilterPolicy() here.
    pub filter_policy: Option<Rc<dyn FilterPolicy>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            comparator: Arc::new(BitWiseComparator {}),
            block_restart_interval: 16,
            compression_type: SNAPPY_COMPRESSION,
            paranoid_checks: false,
            block_cache: None,
            filter_policy: None,
        }
    }
}
