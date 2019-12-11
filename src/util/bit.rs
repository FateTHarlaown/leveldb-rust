extern "C" {
    pub fn memcmp(s1: *const u8, s2: *const u8, n: usize) -> i32;
}

#[inline]
pub fn memcpy(dst: &mut [u8], src: &[u8]) {
    assert!(dst.len() >= src.len());
    unsafe { ::std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) }
}
