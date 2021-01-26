use rand::prelude::ThreadRng;
use rand::Rng;

pub fn random_key(rnd: &mut ThreadRng, len: usize) -> Vec<u8> {
    // Make sure to generate a wide variety of characters so we
    // test the boundary conditions for short-key optimizations.
    let test_chars = vec![
        0u8, 1u8, 'a' as u8, 'b' as u8, 'c' as u8, 'd' as u8, 'e' as u8, 0xfd, 0xfe, 0xff,
    ];
    let mut res = Vec::with_capacity(len);
    for _ in 0..len {
        let pos = rnd.gen_range(0, test_chars.len());
        res.push(test_chars[pos] as u8)
    }
    res
}

pub fn random_vec_str(rnd: &mut ThreadRng, len: usize) -> Vec<u8> {
    let mut res = Vec::with_capacity(len);
    for _ in 0..len {
        let num = ' ' as u8 + rnd.gen_range(0, 95);
        res.push(num);
    }
    res
}

pub fn compressible_vec_str(rnd: &mut ThreadRng, compressed_fraction: f64, len: usize) -> Vec<u8> {
    let raw = len as f64 * compressed_fraction;
    let raw = if raw < 1f64 { 1f64 } else { raw };
    let raw = raw as usize;

    let mut res = Vec::with_capacity(len);
    let raw_data = random_vec_str(rnd, raw);
    while res.len() < len {
        res.extend_from_slice(raw_data.as_slice());
    }

    res
}
