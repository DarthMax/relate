//! Building a [`FrozenTopology`]: the per-type input a loader fills, the
//! transpose that derives the incoming side, and the graph's install.

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use relate_core::{Error, NodeId, Result};

use super::{FrozenTopology, InLists, OutLists, Present, PropColumn, TypeTopology};
use crate::graph::Graph;
use crate::store::Interner;

/// One relationship type's outgoing lists, pushed in ascending source
/// order. Every list is a directed relationship per target; a loader of an
/// undirected source format pushes each relationship once.
pub struct FrozenRelType {
    name: String,
    property_keys: Vec<String>,
    nodes: Vec<u32>,
    offsets: Vec<u32>,
    first_rel: Vec<u32>,
    bytes: Vec<u8>,
    properties: Vec<Vec<f64>>,
}

/// The value count of one varlong stream, or why it is malformed.
fn checked_count(bytes: &[u8]) -> Result<u32> {
    let mut count = 0u32;
    let mut prev = 0i64;
    let mut delta = 0u64;
    let mut shift = 0u32;
    for &byte in bytes {
        if shift > 56 {
            return Err(Error::storage(
                "frozen target list holds a varlong longer than 9 bytes",
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
            .ok_or_else(|| Error::storage("frozen target delta overflows a node id"))?;
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::storage("frozen target list exceeds u32 hops"))?;
        delta = 0;
        shift = 0;
    }
    if shift != 0 {
        return Err(Error::storage(
            "frozen target list ends inside a truncated varlong",
        ));
    }
    Ok(count)
}

fn offset(len: usize, what: &str) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::storage(format!("frozen {what} exceeds u32")))
}

impl FrozenRelType {
    pub fn new(name: impl Into<String>, property_keys: Vec<String>) -> Self {
        let properties = property_keys.iter().map(|_| Vec::new()).collect();
        Self {
            name: name.into(),
            property_keys,
            nodes: Vec::new(),
            offsets: vec![0],
            first_rel: vec![0],
            bytes: Vec::new(),
            properties,
        }
    }

    /// Append `source`'s list: `targets` is its sorted targets as a delta
    /// varlong stream, `properties` one slice per property key, each
    /// aligned with the targets. An empty list is skipped.
    pub fn push(&mut self, source: NodeId, targets: &[u8], properties: &[&[f64]]) -> Result<()> {
        let degree = checked_count(targets)?;
        if properties.len() != self.property_keys.len()
            || properties.iter().any(|p| p.len() != degree as usize)
        {
            return Err(Error::storage(format!(
                "frozen relationship properties of node {source} do not match its {degree} targets"
            )));
        }
        if degree == 0 {
            return Ok(());
        }
        let node = u32::try_from(source)
            .map_err(|_| Error::storage(format!("frozen source node {source} outside u32")))?;
        if self.nodes.last().is_some_and(|&last| last >= node) {
            return Err(Error::storage(format!(
                "frozen source node {source} is not after its predecessor"
            )));
        }
        let count = self.first_rel[self.nodes.len()]
            .checked_add(degree)
            .ok_or_else(|| Error::storage("frozen relationship type exceeds u32 relationships"))?;
        self.bytes.extend_from_slice(targets);
        self.offsets
            .push(offset(self.bytes.len(), "target bytes of a type")?);
        self.first_rel.push(count);
        self.nodes.push(node);
        for (column, values) in self.properties.iter_mut().zip(properties) {
            column.extend_from_slice(values);
        }
        Ok(())
    }
}

impl Present {
    fn of(words: Vec<u64>) -> Self {
        let mut ranks = Vec::with_capacity(words.len());
        let mut rank = 0u32;
        for word in &words {
            ranks.push(rank);
            rank += word.count_ones();
        }
        Self { words, ranks }
    }

    fn of_nodes(nodes: &[u32], bound: usize) -> Self {
        let mut words = vec![0u64; bound.div_ceil(64)];
        for &node in nodes {
            words[node as usize / 64] |= 1 << (node % 64);
        }
        Self::of(words)
    }
}

impl InLists {
    /// The transpose of `out`: per target, its sources and type-local ids,
    /// ascending by id.
    fn transpose(out: &OutLists, bound: usize) -> Result<Self> {
        let index = |target: NodeId| {
            usize::try_from(target)
                .ok()
                .filter(|&t| t < bound)
                .ok_or_else(|| {
                    Error::storage(format!("frozen target node {target} outside 0..{bound}"))
                })
        };
        let mut cursors = vec![0u32; bound];
        for slot in 0..out.nodes.len() {
            for target in out.targets(slot) {
                cursors[index(target)?] += 1;
            }
        }
        let mut words = vec![0u64; bound.div_ceil(64)];
        let mut offsets = vec![0u32];
        let mut total = 0u32;
        for (node, cursor) in cursors.iter_mut().enumerate() {
            if *cursor == 0 {
                continue;
            }
            words[node / 64] |= 1 << (node % 64);
            let start = total;
            total += *cursor;
            offsets.push(total);
            *cursor = start;
        }
        let mut sources = vec![0u32; total as usize];
        let mut rels = vec![0u32; total as usize];
        for (slot, &source) in out.nodes.iter().enumerate() {
            for (rel, target) in (out.first_rel[slot]..).zip(out.targets(slot)) {
                let cursor = &mut cursors[index(target)?];
                sources[*cursor as usize] = source;
                rels[*cursor as usize] = rel;
                *cursor += 1;
            }
        }
        Ok(Self {
            present: Present::of(words),
            offsets,
            sources,
            rels,
        })
    }
}

impl FrozenTopology {
    /// `node_bound` is one past the largest node id; every source and
    /// target must lie below it.
    fn build(interner: &Interner, types: Vec<FrozenRelType>, node_bound: NodeId) -> Result<Self> {
        let bound = usize::try_from(node_bound)
            .map_err(|_| Error::storage("frozen topology over a negative node bound"))?;
        let mut names = HashSet::new();
        let mut built = Vec::with_capacity(types.len());
        let mut base = 0i64;
        for ty in types {
            if !names.insert(ty.name.clone()) {
                return Err(Error::storage(format!(
                    "frozen relationship type {} appears twice",
                    ty.name
                )));
            }
            if ty.nodes.last().is_some_and(|&last| last as usize >= bound) {
                return Err(Error::storage(format!(
                    "frozen source node of {} outside 0..{bound}",
                    ty.name
                )));
            }
            let out = OutLists {
                present: Present::of_nodes(&ty.nodes, bound),
                nodes: ty.nodes,
                offsets: ty.offsets,
                first_rel: ty.first_rel,
                bytes: ty.bytes,
            };
            let inn = InLists::transpose(&out, bound)?;
            let mut props: Vec<PropColumn> = ty
                .property_keys
                .iter()
                .zip(ty.properties)
                .map(|(key, values)| {
                    let key_id = interner.intern(key);
                    PropColumn {
                        key_id,
                        key: interner.get(key_id),
                        values,
                    }
                })
                .collect();
            props.sort_by(|a, b| a.key.cmp(&b.key));
            let type_id = interner.intern(&ty.name);
            let count = i64::from(out.count());
            built.push(TypeTopology {
                type_id,
                name: interner.get(type_id),
                base,
                out,
                inn,
                props,
            });
            base += count;
        }
        Ok(Self {
            types: built,
            rel_count: base,
        })
    }
}

impl Graph {
    pub(in crate::graph) fn frozen(&self) -> Option<&FrozenTopology> {
        self.frozen.get()
    }

    /// Make `types` this graph's relationships, read in place: no
    /// relationship record is stored and no adjacency is built. Only valid
    /// on a graph being restored, after its last node and instead of any
    /// `load_checkpoint_rel`. Relationship ids run from 0 in the order of
    /// `types`, then by source, then by position in the source's list.
    #[doc(hidden)]
    pub fn install_frozen_topology(&self, types: Vec<FrozenRelType>) -> Result<()> {
        self.check_restoring()?;
        if !self.rels.is_empty() {
            return Err(Error::storage(
                "frozen topology installed into a graph that holds relationships",
            ));
        }
        let topology = FrozenTopology::build(&self.interner, types, self.next_node_id())?;
        {
            let mut catalog = self.catalog.lock().unwrap();
            for name in topology.type_names() {
                catalog.record_rel_type(name);
            }
        }
        self.next_rel_id
            .fetch_max(topology.rel_count(), Ordering::Relaxed);
        self.frozen
            .set(topology)
            .map_err(|_| Error::storage("frozen topology installed twice"))
    }
}

/// The inverse of [`super::Targets`], for fixtures.
#[cfg(test)]
pub(in crate::graph) fn encode_targets(sorted: &[NodeId]) -> Vec<u8> {
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

#[cfg(test)]
pub(super) fn targets_of(bytes: &[u8]) -> Vec<NodeId> {
    super::Targets { bytes, prev: 0 }.collect()
}
