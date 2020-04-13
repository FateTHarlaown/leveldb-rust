use crate::db::error::{Result, StatusError};
use byteorder::{ReadBytesExt, WriteBytesExt};

const B: u32 = 128;

pub fn varint_length(mut v: u64) -> usize {
    let mut len = 1;
    while v >= 128 {
        v >>= 7;
        len += 1;
    }
    len
}

pub fn put_varint32(dst: &mut Vec<u8>, v: u32) {
    let data_len = varint_length(u64::from(v));
    let old_len = dst.len();
    unsafe {
        dst.reserve(data_len);
        dst.set_len(data_len + old_len);
    }
    dst[old_len..].as_mut().encode_varint32(v).unwrap();
}

pub fn put_varint64(dst: &mut Vec<u8>, v: u64) {
    let data_len = varint_length(v);
    let old_len = dst.len();
    unsafe {
        dst.reserve(data_len);
        dst.set_len(data_len + old_len);
    }
    dst[old_len..].as_mut().encode_varint64(v).unwrap();
}

pub trait EncodeVarint {
    fn encode_varint32(&mut self, v: u32) -> Result<()>;
    fn encode_varint64(&mut self, v: u64) -> Result<()>;
}

pub trait DecodeVarint {
    fn decode_varint32(&mut self) -> Result<u32>;
    fn decode_varint64(&mut self) -> Result<u64>;
}

impl EncodeVarint for &mut [u8] {
    fn encode_varint32(&mut self, v: u32) -> Result<()> {
        if v < (1 << 7) {
            self.write_u8(v as u8)?;
        } else if v < (1 << 14) {
            self.write_u8((v | B) as u8)?;
            self.write_u8((v >> 7) as u8)?;
        } else if v < (1 << 21) {
            self.write_u8((v | B) as u8)?;
            self.write_u8(((v >> 7) | B) as u8)?;
            self.write_u8((v >> 14) as u8)?;
        } else if v < (1 << 28) {
            self.write_u8((v | B) as u8)?;
            self.write_u8(((v >> 7) | B) as u8)?;
            self.write_u8(((v >> 14) | B) as u8)?;
            self.write_u8((v >> 21) as u8)?;
        } else {
            self.write_u8((v | B) as u8)?;
            self.write_u8(((v >> 7) | B) as u8)?;
            self.write_u8(((v >> 14) | B) as u8)?;
            self.write_u8(((v >> 21) | B) as u8)?;
            self.write_u8((v >> 28) as u8)?;
        }
        Ok(())
    }

    fn encode_varint64(&mut self, mut v: u64) -> Result<()> {
        while v >= u64::from(B) {
            let n = (v | u64::from(B)) & 0xFF;
            self.write_u8(n as u8).unwrap();
            v >>= 7;
        }
        self.write_u8(v as u8)?;
        Ok(())
    }
}

impl DecodeVarint for &[u8] {
    fn decode_varint32(&mut self) -> Result<u32> {
        let mut shift = 0;
        let mut result = 0;
        while shift <= 28 {
            let byte = self.read_u8()?;
            if u32::from(byte) & B == 0 {
                result |= (u32::from(byte)) << shift;
                return Ok(result);
            } else {
                result |= ((u32::from(byte)) & 127) << shift;
            }
            shift += 7;
        }

        Err(StatusError::Corruption(
            "Error when decoding varint32".to_string(),
        ))
    }

    fn decode_varint64(&mut self) -> Result<u64> {
        let mut shift = 0;
        let mut result = 0;
        while shift <= 63 {
            let byte = self.read_u8()?;
            if u64::from(byte) & u64::from(B) == 0 {
                result |= (u64::from(byte)) << shift;
                return Ok(result);
            } else {
                result |= ((u64::from(byte)) & 127) << shift;
            }
            shift += 7;
        }

        Err(StatusError::Corruption(
            "Error when decoding varint64".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint32() {
        let mut buf = Vec::new();
        let mut len = 0;
        for i in 0..32 * 32 {
            let v = (i / 32) << (i % 32);
            put_varint32(&mut buf, v);
            len += varint_length(v as u64);
        }

        let mut data = buf.as_slice();
        for i in 0..32 * 32 {
            let expected = (i / 32) << (i % 32);
            let actual = data.decode_varint32().unwrap();
            assert_eq!(actual, expected);
        }
        assert_eq!(len, buf.as_slice().len());
    }

    #[test]
    fn test_varint64() {
        let mut values = Vec::new();
        values.push(0);
        values.push(100);
        values.push(!0u64);
        values.push(!0u64 - 1);
        for k in 0..64 {
            let power = 1 << k;
            values.push(power);
            values.push(power - 1);
            values.push(power + 1);
        }

        let mut buf = Vec::new();
        for val in values.iter() {
            put_varint64(&mut buf, *val);
        }

        let mut data = buf.as_slice();
        let mut len = 0;
        for val in values.iter() {
            len += varint_length(*val as u64);
            let actual = data.decode_varint64().unwrap();
            assert_eq!(actual, *val);
        }
        assert_eq!(len, buf.len());
    }

    #[test]
    fn test_varint32_overflow() {
        let mut data = b"\x81\x82\x83\x84\x85\x11".as_ref();
        assert!(data.decode_varint32().is_err());
    }

    #[test]
    fn test_varint32_truncation() {
        let mut buf = Vec::new();
        let large_value = (1 << 31) + 100;
        put_varint32(&mut buf, large_value);
        for len in 0..buf.len() - 1 {
            let mut truncated_data = &buf[0..len];
            assert!(truncated_data.decode_varint32().is_err());
        }

        let mut data = buf.as_slice();
        let actual = data.decode_varint32().unwrap();
        assert_eq!(actual, large_value);
    }

    #[test]
    fn test_varint64_overflow() {
        let mut data = b"\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11".as_ref();
        assert!(data.decode_varint64().is_err());
    }

    #[test]
    fn test_varint64_truncation() {
        let mut buf = Vec::new();
        let large_value = (1 << 63) + 100;
        put_varint64(&mut buf, large_value);
        for len in 0..buf.len() - 1 {
            let mut truncated_data = &buf[0..len];
            assert!(truncated_data.decode_varint64().is_err());
        }

        let mut data = buf.as_slice();
        let actual = data.decode_varint64().unwrap();
        assert_eq!(actual, large_value);
    }
}
