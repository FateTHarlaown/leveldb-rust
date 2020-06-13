use byteorder::{LittleEndian, ReadBytesExt};

pub fn hash(data: &[u8], seed: u32) -> u32 {
    // Similar to murmur hash
    let n = data.len();
    let m: u32 = 0xc6a4a793;
    let r: u32 = 24;
    let mut h = seed ^ (m.wrapping_mul(n as u32));
    let mut buf = data;
    while buf.len() >= 4 {
        let w = buf.read_u32::<LittleEndian>().unwrap();
        h = h.wrapping_add(w);
        h = h.wrapping_mul(m);
        h ^= h >> 16;
    }

    for i in (0..buf.len()).rev() {
        h += u32::from(buf[i]) << (i * 8) as u32;
        if i == 0 {
            h = h.wrapping_mul(m);
            h ^= h >> r;
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use super::hash;

    #[test]
    fn signed_unsigned_issue() {
        let data1 = vec![0x62];
        let data2 = vec![0xc3, 0x97];
        let data3 = vec![0xe2, 0x99, 0xa5];
        let data4 = vec![0xe1, 0x80, 0xb9, 0x32];
        let data5 = vec![
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(hash(data1.as_slice(), 0xbc9f1d34), 0xef1345c4,);
        assert_eq!(hash(data2.as_slice(), 0xbc9f1d34), 0x5b663814,);
        assert_eq!(hash(data3.as_slice(), 0xbc9f1d34), 0x323c078f,);
        assert_eq!(hash(data4.as_slice(), 0xbc9f1d34), 0xed21633a,);
        assert_eq!(hash(data5.as_slice(), 0x12345678), 0xf333dabb,);
    }
}
