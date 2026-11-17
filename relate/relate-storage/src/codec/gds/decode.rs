//! The GDS compressed adjacency list encoding: sorted target ids, delta
//! encoded, each delta written as little-endian 7-bit groups where the
//! byte with the high bit **set** terminates a value (the inverse of
//! LEB128).

use relate_core::{Error, Result};

/// Decode one node's adjacency list into absolute target ids.
pub(super) fn decode_targets(bytes: &[u8]) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(encoded_value_count(bytes));
    let mut prev = 0i64;
    let mut delta = 0i64;
    let mut shift = 0u32;
    for &byte in bytes {
        if shift > 56 {
            return Err(Error::storage(
                "GDS adjacency list holds a varlong longer than 9 bytes",
            ));
        }
        delta |= i64::from(byte & 0x7F) << shift;
        if byte & 0x80 != 0 {
            prev = prev
                .checked_add(delta)
                .ok_or_else(|| Error::storage("GDS adjacency delta overflows a node id"))?;
            out.push(prev);
            delta = 0;
            shift = 0;
        } else {
            shift += 7;
        }
    }
    if shift != 0 {
        return Err(Error::storage(
            "GDS adjacency list ends inside a truncated varlong",
        ));
    }
    Ok(out)
}

fn encoded_value_count(bytes: &[u8]) -> usize {
    bytes.iter().filter(|b| *b & 0x80 != 0).count()
}

pub(super) fn encode_targets(sorted: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut prev = 0i64;
    for &target in sorted {
        let mut delta = (target - prev) as u64;
        prev = target;
        loop {
            let group = (delta & 0x7F) as u8;
            delta >>= 7;
            if delta == 0 {
                out.push(group | 0x80);
                break;
            }
            out.push(group);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_small_large_and_parallel_targets() {
        let targets = [0, 2, 2, 3, 200, 70_000, 1 << 40];
        let bytes = encode_targets(&targets);
        assert_eq!(decode_targets(&bytes).unwrap(), targets);
    }

    #[test]
    fn single_byte_deltas_have_the_terminator_bit() {
        assert_eq!(encode_targets(&[1, 3]), vec![0x81, 0x82]);
        assert_eq!(decode_targets(&[0x81, 0x82]).unwrap(), vec![1, 3]);
    }

    #[test]
    fn truncated_value_is_an_error() {
        assert!(decode_targets(&[0x81, 0x05]).is_err());
    }
}
