use crate::error::YsonError;

#[inline]
pub fn read_uvarint(input: &[u8]) -> Result<(u64, usize), YsonError> {
    let mut result: u64 = 0;
    let mut shift = 0;

    for (i, &byte) in input.iter().enumerate() {
        if i >= 10 {
            return Err(YsonError::Custom("Varint too long (overflow u64)".into()));
        }

        let bits = u64::from(byte & 0x7F);
        result |= bits << shift;
        if (byte & 0x80) == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }

    // Running out of bytes mid-varint is truncation, not corruption. Reporting
    // it as `UnexpectedEof` lets a streaming reader tell "buffer ends inside a
    // record, read more" apart from "this data is broken" — a `Custom` error
    // here is indistinguishable from a genuine parse failure.
    Err(YsonError::UnexpectedEof(input.len()))
}

#[inline]
pub fn read_varint(input: &[u8]) -> Result<(i64, usize), YsonError> {
    let (u_val, consumed) = read_uvarint(input)?;
    let val = ((u_val >> 1) as i64) ^ (-((u_val & 1) as i64));
    Ok((val, consumed))
}

#[inline]
pub fn write_uvarint(mut val: u64, buf: &mut Vec<u8>) {
    while val >= 0x80 {
        buf.push((val as u8) | 0x80);
        val >>= 7;
    }
    buf.push(val as u8);
}

#[inline]
pub fn write_varint(val: i64, buf: &mut Vec<u8>) {
    let zigzag = ((val << 1) ^ (val >> 63)) as u64;
    write_uvarint(zigzag, buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_overflow_exact() {
        let mut input = vec![0x80; 11];
        input.push(0x01);
        let res = read_uvarint(&input);
        assert!(res.is_err());
    }

    #[test]
    fn test_roundtrip_varint() {
        let mut buf = Vec::new();
        write_varint(-12345, &mut buf);
        let (val, consumed) = read_varint(&buf).unwrap();
        assert_eq!(val, -12345);
        assert_eq!(consumed, buf.len());
    }
}
