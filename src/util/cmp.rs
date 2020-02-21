use crate::db::slice::Slice;
use crate::util::bit::memcmp;
use std::cmp::Ordering;

pub trait Comparator<T: ?Sized> {
    fn compare(&self, left: &T, right: &T) -> Ordering;
}

pub struct BitWiseComparator {}

impl Comparator<Slice> for BitWiseComparator {
    fn compare(&self, left: &Slice, right: &Slice) -> Ordering {
        let len = if left.len() < right.len() {
            left.len()
        } else {
            right.len()
        };

        let ret = unsafe { memcmp(left.as_ptr(), right.as_ptr(), len) };

        if ret == 0 {
            Ordering::Equal
        } else if ret > 0 {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}
