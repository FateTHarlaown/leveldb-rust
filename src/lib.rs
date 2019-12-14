#![feature(core_intrinsics)]

pub mod db;
pub mod util;

#[cfg(test)]
mod tests {
    use crate::db::RecordType;

    fn number_string(n: i32) -> String {
        format!("{}", n)
    }

    #[test]
    fn it_works() {
        let a = RecordType::FullType as u8;
        assert_eq!(a, 1 as u8);
    }
}
