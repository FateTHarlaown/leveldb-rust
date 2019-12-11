#![feature(core_intrinsics)]

pub mod log;
pub mod util;

#[cfg(test)]
mod tests {
    use crate::log::RecordType;
    use std::mem::size_of;

    #[test]
    fn it_works() {
        assert_eq!(size_of::<RecordType>(), 4);
        assert_eq!(2 + 2, 4);
    }
}
