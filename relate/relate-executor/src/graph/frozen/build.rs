//! Building a [`FrozenTopology`] from a loader's per-type lists: the
//! transpose that derives the incoming side, and the graph's install.

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use relate_core::{Error, NodeId, Result};
use relate_engine_api::FrozenRelType;

use super::{FrozenTopology, InLists, OutLists, Present, PropColumn, TypeTopology};
use crate::graph::Graph;
use crate::store::Interner;

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
            let ty = ty.into_parts();
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

    /// A frozen topology has no write path: its relationships are not
    /// records a write could version.
    pub(in crate::graph) fn check_not_frozen(&self) -> Result<()> {
        if self.frozen().is_some() {
            return Err(Error::execution(
                "this graph is read-only: its relationships are a frozen topology",
            ));
        }
        Ok(())
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
