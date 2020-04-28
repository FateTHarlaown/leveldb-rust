use crate::db::slice::Slice;
use crate::util::bit::memcmp;
use std::cmp::{min, Ordering};

pub trait Comparator<T: ?Sized> {
    // Three-way comparison.  Returns value:
    // Ordering::Less iff "a" < "b",
    // Ordering::Equal iff "a" == "b",
    // Ordering::Greater "a" > "b"
    fn compare<'a>(&self, left: &'a T, right: &'a T) -> Ordering;

    // The name of the comparator.  Used to check for comparator
    // mismatches (i.e., a DB created with one comparator is
    // accessed using a different comparator.
    //
    // The client of this package should switch to a new name whenever
    // the comparator implementation changes in a way that will cause
    // the relative ordering of any two keys to change.
    //
    // Names starting with "leveldb." are reserved and should not be used
    // by any clients of this package.
    fn name(&self) -> &'static str;

    // Advanced functions: these are used to reduce the space requirements
    // for internal data structures like index blocks.

    // If *start < limit, changes *start to a short string in [start,limit).
    // Simple comparator implementations may return with *start unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: Slice);

    // Changes *key to a short string >= *key.
    // Simple comparator implementations may return with *key unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    fn find_short_successor(&self, key: &mut Vec<u8>);
}

pub struct BitWiseComparator {}

impl Comparator<Slice> for BitWiseComparator {
    fn compare(&self, left: &Slice, right: &Slice) -> Ordering {
        let len = if left.size() < right.size() {
            left.size()
        } else {
            right.size()
        };

        let ret = unsafe { memcmp(left.data(), right.data(), len) };

        if ret < 0 {
            Ordering::Less
        } else if ret > 0 {
            Ordering::Greater
        } else if left.size() < right.size() {
            Ordering::Less
        } else if left.size() > right.size() {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    fn name(&self) -> &'static str {
        "Leveldb.BitwiseComaparator"
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: Slice) {
        let min_length = min(start.len(), limit.size());
        let mut diff_index = 0;
        while diff_index < min_length && limit.as_ref()[diff_index] == start[diff_index] {
            diff_index += 1;
        }

        // Do not shorten if one is a prefix of the other
        if diff_index < min_length {
            let diff_byte = start[diff_index];
            if diff_byte < 0xff && diff_byte + 1 < limit.as_ref()[diff_index] {
                start[diff_index] += 1;
                start.truncate(diff_index + 1);
                assert_eq!(
                    self.compare(&start.as_slice().into(), &limit),
                    Ordering::Less
                )
            }
        }
    }

    fn find_short_successor(&self, key: &mut Vec<u8>) {
        // Find first character that can be incremented
        let mut truncate_len = 0;
        for (i, byte) in key.iter_mut().enumerate() {
            if *byte != 0xff {
                *byte = *byte + 1;
                truncate_len = i + 1;
                break;
            }
        }
        if truncate_len != 0 {
            key.truncate(truncate_len)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_wise_comparator() {
        let tests: Vec<(Slice, Slice, Ordering)> = vec![
            (
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                [1u8, 2u8, 3u8, 4u8, 6u8].as_ref().into(),
                Ordering::Less,
            ),
            (
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                [1u8, 2u8, 3u8, 4u8, 5u8, 1u8].as_ref().into(),
                Ordering::Less,
            ),
            (
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                Ordering::Equal,
            ),
            (
                [1u8, 2u8, 4u8, 4u8, 5u8].as_ref().into(),
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                Ordering::Greater,
            ),
            (
                [1u8, 2u8, 3u8, 4u8, 5u8, 1u8].as_ref().into(),
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                Ordering::Greater,
            ),
            (
                [1u8, 1u8, 3u8, 4u8, 5u8, 6u8].as_ref().into(),
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                Ordering::Less,
            ),
            (
                [1u8, 2u8, 3u8, 4u8, 5u8, 7u8].as_ref().into(),
                [1u8, 2u8, 3u8, 4u8, 5u8].as_ref().into(),
                Ordering::Greater,
            ),
            (Slice::default(), Slice::default(), Ordering::Equal),
            ([0u8].as_ref().into(), Slice::default(), Ordering::Greater),
        ];

        let comparator = BitWiseComparator {};
        for (a, b, expect) in tests.iter() {
            assert_eq!(comparator.compare(a, b), *expect);
        }
    }
}
