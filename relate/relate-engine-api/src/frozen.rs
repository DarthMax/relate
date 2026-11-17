//! Relationships handed to an engine as per-source target lists it may
//! read in place, instead of one row per relationship.
//!
//! A list is its sorted target node ids, delta encoded, each delta written
//! as little-endian 7-bit groups where the byte with the high bit **set**
//! ends a value (the inverse of LEB128; the GDS adjacency encoding).

use relate_core::{Error, NodeId, Result};

/// Walk one list's targets, checking the encoding; returns their count.
fn try_for_each_target(bytes: &[u8], mut f: impl FnMut(NodeId)) -> Result<u32> {
    let mut count = 0u32;
    let mut prev = 0i64;
    let mut delta = 0u64;
    let mut shift = 0u32;
    for &byte in bytes {
        if shift > 56 {
            return Err(Error::storage(
                "target list holds a varlong longer than 9 bytes",
            ));
        }
        delta |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            shift += 7;
            continue;
        }
        prev = i64::try_from(delta)
            .ok()
            .and_then(|delta| prev.checked_add(delta))
            .ok_or_else(|| Error::storage("target delta overflows a node id"))?;
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::storage("target list exceeds u32 targets"))?;
        f(prev);
        delta = 0;
        shift = 0;
    }
    if shift != 0 {
        return Err(Error::storage(
            "target list ends inside a truncated varlong",
        ));
    }
    Ok(count)
}

/// Decode one list into absolute target ids.
pub fn decode_targets(bytes: &[u8]) -> Result<Vec<NodeId>> {
    let mut out = Vec::new();
    try_for_each_target(bytes, |target| out.push(target))?;
    Ok(out)
}

/// Encode ascending target ids as one list.
pub fn encode_targets(sorted: &[NodeId]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut prev = 0;
    for &target in sorted {
        let mut delta = (target - prev) as u64;
        prev = target;
        while delta >= 0x80 {
            out.push((delta & 0x7F) as u8);
            delta >>= 7;
        }
        out.push(delta as u8 | 0x80);
    }
    out
}

/// The targets of a list [`FrozenRelType::push`] accepted, unchecked.
pub struct Targets<'a> {
    bytes: &'a [u8],
    prev: i64,
}

impl<'a> Targets<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, prev: 0 }
    }
}

impl Iterator for Targets<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<NodeId> {
        let mut delta = 0u64;
        let mut shift = 0u32;
        loop {
            let (&byte, rest) = self.bytes.split_first()?;
            self.bytes = rest;
            delta |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 != 0 {
                break;
            }
            shift += 7;
        }
        self.prev += delta as i64;
        Some(self.prev)
    }
}

/// One relationship type's outgoing lists, pushed in ascending source
/// order. Every target is one directed relationship; a loader of an
/// undirected source format pushes each relationship once.
pub struct FrozenRelType {
    parts: FrozenRelTypeParts,
}

/// What a [`FrozenRelType`] collected. `nodes` are the sources that have a
/// list; `offsets` (into `bytes`) and `first_rel` (the type-local id of a
/// source's first relationship) carry one trailing entry. Each of
/// `properties` aligns with `property_keys` and holds one value per
/// relationship.
pub struct FrozenRelTypeParts {
    pub name: String,
    pub property_keys: Vec<String>,
    pub nodes: Vec<u32>,
    pub offsets: Vec<u32>,
    pub first_rel: Vec<u32>,
    pub bytes: Vec<u8>,
    pub properties: Vec<Vec<f64>>,
}

impl FrozenRelType {
    pub fn new(name: impl Into<String>, property_keys: Vec<String>) -> Self {
        let properties = property_keys.iter().map(|_| Vec::new()).collect();
        Self {
            parts: FrozenRelTypeParts {
                name: name.into(),
                property_keys,
                nodes: Vec::new(),
                offsets: vec![0],
                first_rel: vec![0],
                bytes: Vec::new(),
                properties,
            },
        }
    }

    /// Append `source`'s list: `targets` is its encoded targets,
    /// `properties` one slice per property key, each aligned with the
    /// targets. An empty list is skipped.
    pub fn push(&mut self, source: NodeId, targets: &[u8], properties: &[&[f64]]) -> Result<()> {
        let parts = &mut self.parts;
        let degree = try_for_each_target(targets, |_| {})?;
        if properties.len() != parts.property_keys.len()
            || properties.iter().any(|p| p.len() != degree as usize)
        {
            return Err(Error::storage(format!(
                "relationship properties of node {source} do not match its {degree} targets"
            )));
        }
        if degree == 0 {
            return Ok(());
        }
        let node = u32::try_from(source)
            .map_err(|_| Error::storage(format!("source node {source} outside u32")))?;
        if parts.nodes.last().is_some_and(|&last| last >= node) {
            return Err(Error::storage(format!(
                "source node {source} is not after its predecessor"
            )));
        }
        let count = parts.first_rel[parts.nodes.len()]
            .checked_add(degree)
            .ok_or_else(|| Error::storage("relationship type exceeds u32 relationships"))?;
        let end = u32::try_from(parts.bytes.len() + targets.len())
            .map_err(|_| Error::storage("target lists of a relationship type exceed 4 GiB"))?;
        parts.bytes.extend_from_slice(targets);
        parts.offsets.push(end);
        parts.first_rel.push(count);
        parts.nodes.push(node);
        for (column, values) in parts.properties.iter_mut().zip(properties) {
            column.extend_from_slice(values);
        }
        Ok(())
    }

    /// Relationships pushed so far.
    pub fn rel_count(&self) -> u64 {
        u64::from(*self.parts.first_rel.last().expect("a trailing entry"))
    }

    pub fn into_parts(self) -> FrozenRelTypeParts {
        self.parts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_small_large_and_parallel_targets() {
        let targets = [0, 2, 2, 3, 200, 70_000, 1 << 40];
        let bytes = encode_targets(&targets);
        assert_eq!(decode_targets(&bytes).unwrap(), targets);
        assert_eq!(Targets::new(&bytes).collect::<Vec<_>>(), targets);
    }

    #[test]
    fn single_byte_deltas_have_the_terminator_bit() {
        assert_eq!(encode_targets(&[1, 3]), vec![0x81, 0x82]);
        assert_eq!(decode_targets(&[0x81, 0x82]).unwrap(), vec![1, 3]);
    }

    #[test]
    fn malformed_lists_are_errors() {
        assert!(decode_targets(&[0x81, 0x05]).is_err());
        assert!(decode_targets(&[0x00; 10]).is_err());
    }

    #[test]
    fn push_refuses_misaligned_properties_and_unordered_sources() {
        let storage = |r: Result<()>| matches!(r, Err(Error::Storage(_)));
        let mut t = FrozenRelType::new("A", vec!["w".into()]);
        assert!(storage(t.push(0, &[0x01], &[&[]])));
        assert!(storage(t.push(0, &[0x81], &[])));
        assert!(storage(t.push(0, &[0x81], &[&[1.0, 2.0]])));
        assert!(storage(t.push(-1, &[0x81], &[&[1.0]])));
        t.push(5, &[0x81, 0x81], &[&[1.0, 2.0]]).unwrap();
        t.push(6, &[], &[&[]]).unwrap();
        assert!(storage(t.push(5, &[0x81], &[&[1.0]])));
        assert!(storage(t.push(4, &[0x81], &[&[1.0]])));
        let parts = t.into_parts();
        assert_eq!(parts.nodes, vec![5]);
        assert_eq!(parts.first_rel, vec![0, 2]);
        assert_eq!(parts.offsets, vec![0, 2]);
        assert_eq!(parts.properties, vec![vec![1.0, 2.0]]);
    }
}
