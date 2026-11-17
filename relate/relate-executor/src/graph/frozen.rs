//! A read-only topology that answers relationship reads from per-node
//! target lists instead of relationship records and rebuilt adjacency.
//!
//! Each relationship type holds, per source node, its sorted targets as a
//! delta-encoded varlong stream (the GDS adjacency encoding: 7-bit groups,
//! the byte with the high bit set ends a value). A relationship has no
//! record: its id is `type base + first id of its source + position in
//! the source's list`, so ids are dense and ordered by (type, source,
//! position). The incoming side is the transpose, built once.
//!
//! Hops are emitted in descending relationship id per type, types in id
//! order — the order a graph restored from the same relationships yields.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use relate_core::{NodeId, PropertyMap, RelId, Relationship, Value};
use relate_engine_api::Targets;
use relate_ir::Direction;

use super::RelRow;
use crate::store::{empty_properties, RelTypes};

mod build;
#[cfg(test)]
mod tests;

pub(super) struct FrozenTopology {
    /// Ascending by `base`.
    types: Vec<TypeTopology>,
    rel_count: i64,
}

struct TypeTopology {
    type_id: u32,
    name: Arc<str>,
    base: RelId,
    out: OutLists,
    inn: InLists,
    /// Ascending by key name; `values` is indexed by type-local id.
    props: Vec<PropColumn>,
}

struct PropColumn {
    key_id: u32,
    key: Arc<str>,
    values: Vec<f64>,
}

/// Which nodes have a list, and each one's slot among them.
struct Present {
    words: Vec<u64>,
    ranks: Vec<u32>,
}

/// `nodes`, `offsets` and `first_rel` are indexed by slot; the latter two
/// carry one trailing entry, so a slot's range is `[slot]..[slot + 1]`.
struct OutLists {
    present: Present,
    nodes: Vec<u32>,
    offsets: Vec<u32>,
    first_rel: Vec<u32>,
    bytes: Vec<u8>,
}

/// A slot's incoming hops are `sources`/`rels` at
/// `offsets[slot]..offsets[slot + 1]`, ascending by type-local id.
struct InLists {
    present: Present,
    offsets: Vec<u32>,
    sources: Vec<u32>,
    rels: Vec<u32>,
}

impl std::fmt::Debug for FrozenTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrozenTopology")
            .field("types", &self.types.len())
            .field("rel_count", &self.rel_count)
            .finish()
    }
}

impl Present {
    fn slot(&self, node: NodeId) -> Option<usize> {
        let node = usize::try_from(node).ok()?;
        let word = *self.words.get(node / 64)?;
        let bit = 1u64 << (node % 64);
        (word & bit != 0)
            .then(|| self.ranks[node / 64] as usize + (word & (bit - 1)).count_ones() as usize)
    }
}

impl OutLists {
    fn targets(&self, slot: usize) -> Targets<'_> {
        Targets::new(&self.bytes[self.offsets[slot] as usize..self.offsets[slot + 1] as usize])
    }

    fn degree(&self, node: NodeId) -> usize {
        self.present.slot(node).map_or(0, |slot| {
            (self.first_rel[slot + 1] - self.first_rel[slot]) as usize
        })
    }

    fn count(&self) -> u32 {
        *self
            .first_rel
            .last()
            .expect("first_rel has a trailing entry")
    }

    /// The slot whose id range holds type-local id `local`.
    fn slot_of(&self, local: u32) -> usize {
        self.first_rel.partition_point(|&first| first <= local) - 1
    }
}

impl InLists {
    fn range(&self, node: NodeId) -> Range<usize> {
        self.present.slot(node).map_or(0..0, |slot| {
            self.offsets[slot] as usize..self.offsets[slot + 1] as usize
        })
    }
}

impl TypeTopology {
    fn rel_id(&self, local: u32) -> RelId {
        self.base + i64::from(local)
    }

    fn ids(&self) -> Range<RelId> {
        self.base..self.rel_id(self.out.count())
    }

    fn out_hops<E>(
        &self,
        node: NodeId,
        scratch: &mut Vec<NodeId>,
        f: &mut impl FnMut(RelId, NodeId) -> Result<(), E>,
    ) -> Result<(), E> {
        let Some(slot) = self.out.present.slot(node) else {
            return Ok(());
        };
        scratch.clear();
        scratch.extend(self.out.targets(slot));
        let first = self.rel_id(self.out.first_rel[slot]);
        for (pos, &target) in scratch.iter().enumerate().rev() {
            f(first + pos as i64, target)?;
        }
        Ok(())
    }

    /// `skip_loops` drops self-loops, which a `Both` walk already met on
    /// the outgoing side under the same id.
    fn in_hops<E>(
        &self,
        node: NodeId,
        skip_loops: bool,
        f: &mut impl FnMut(RelId, NodeId) -> Result<(), E>,
    ) -> Result<(), E> {
        for i in self.inn.range(node).rev() {
            let source = NodeId::from(self.inn.sources[i]);
            if skip_loops && source == node {
                continue;
            }
            f(self.rel_id(self.inn.rels[i]), source)?;
        }
        Ok(())
    }

    fn ends(&self, local: u32) -> (NodeId, NodeId) {
        let slot = self.out.slot_of(local);
        let target = self
            .out
            .targets(slot)
            .nth((local - self.out.first_rel[slot]) as usize)
            .expect("a relationship id lies inside its source's list");
        (NodeId::from(self.out.nodes[slot]), target)
    }

    fn properties(&self, local: u32) -> Arc<PropertyMap> {
        let map: BTreeMap<String, Value> = self
            .props
            .iter()
            .map(|p| {
                (
                    String::from(&*p.key),
                    Value::Float(p.values[local as usize]),
                )
            })
            .collect();
        PropertyMap::share(map)
    }

    fn relationship(&self, local: u32, properties: Arc<PropertyMap>) -> Relationship {
        let (start, end) = self.ends(local);
        Relationship {
            id: self.rel_id(local),
            start,
            end,
            rel_type: String::from(&*self.name),
            properties,
        }
    }
}

impl FrozenTopology {
    pub(super) fn rel_count(&self) -> i64 {
        self.rel_count
    }

    pub(super) fn contains(&self, id: RelId) -> bool {
        (0..self.rel_count).contains(&id)
    }

    pub(super) fn type_names(&self) -> impl Iterator<Item = &str> {
        self.types.iter().map(|t| &*t.name)
    }

    fn locate(&self, id: RelId) -> Option<(&TypeTopology, u32)> {
        if !self.contains(id) {
            return None;
        }
        let ty = &self.types[self.types.partition_point(|t| t.base <= id) - 1];
        Some((ty, (id - ty.base) as u32))
    }

    fn by_type_id(&self, type_id: u32) -> Option<&TypeTopology> {
        self.types.iter().find(|t| t.type_id == type_id)
    }

    fn try_for_types<E>(
        &self,
        types: &RelTypes,
        mut f: impl FnMut(&TypeTopology) -> Result<(), E>,
    ) -> Result<(), E> {
        match types.ids() {
            None => self.types.iter().try_for_each(f),
            Some(ids) => ids
                .filter_map(|id| self.by_type_id(id))
                .try_for_each(&mut f),
        }
    }

    /// Each relationship of `node` in `dir` with the node it reaches.
    pub(super) fn try_for_each_hop<E>(
        &self,
        node: NodeId,
        dir: Direction,
        types: &RelTypes,
        mut f: impl FnMut(RelId, NodeId) -> Result<(), E>,
    ) -> Result<(), E> {
        if dir != Direction::Incoming {
            let mut scratch = Vec::new();
            self.try_for_types(types, |t| t.out_hops(node, &mut scratch, &mut f))?;
        }
        if dir != Direction::Outgoing {
            let skip_loops = dir == Direction::Both;
            self.try_for_types(types, |t| t.in_hops(node, skip_loops, &mut f))?;
        }
        Ok(())
    }

    /// Hop count of `node` in `dir`; a self-loop counts on both sides.
    pub(super) fn degree(&self, node: NodeId, dir: Direction, types: &RelTypes) -> usize {
        let mut total = 0;
        let count = |t: &TypeTopology| -> Result<(), std::convert::Infallible> {
            if dir != Direction::Incoming {
                total += t.out.degree(node);
            }
            if dir != Direction::Outgoing {
                total += t.inn.range(node).len();
            }
            Ok(())
        };
        let Ok(()) = self.try_for_types(types, count);
        total
    }

    pub(super) fn out_degree_of_type(&self, node: NodeId, type_id: u32) -> usize {
        self.by_type_id(type_id).map_or(0, |t| t.out.degree(node))
    }

    pub(super) fn ends(&self, id: RelId) -> Option<(NodeId, NodeId)> {
        self.locate(id).map(|(t, local)| t.ends(local))
    }

    pub(super) fn type_id(&self, id: RelId) -> Option<u32> {
        self.locate(id).map(|(t, _)| t.type_id)
    }

    pub(super) fn type_name(&self, id: RelId) -> Option<Arc<str>> {
        self.locate(id).map(|(t, _)| Arc::clone(&t.name))
    }

    pub(super) fn rel(&self, id: RelId) -> Option<Relationship> {
        self.locate(id)
            .map(|(t, local)| t.relationship(local, t.properties(local)))
    }

    pub(super) fn stub(&self, id: RelId) -> Option<Relationship> {
        self.locate(id)
            .map(|(t, local)| t.relationship(local, empty_properties()))
    }

    pub(super) fn prop(&self, id: RelId, key: &str) -> Option<Value> {
        let (t, local) = self.locate(id)?;
        let column = t.props.iter().find(|p| &*p.key == key)?;
        Some(Value::Float(column.values[local as usize]))
    }

    pub(super) fn prop_by_id(&self, id: RelId, key_id: u32) -> Option<Value> {
        let (t, local) = self.locate(id)?;
        let column = t.props.iter().find(|p| p.key_id == key_id)?;
        Some(Value::Float(column.values[local as usize]))
    }

    pub(super) fn prop_keys(&self, id: RelId) -> Vec<String> {
        self.locate(id).map_or_else(Vec::new, |(t, _)| {
            t.props.iter().map(|p| String::from(&*p.key)).collect()
        })
    }

    pub(super) fn all_ids(&self) -> Vec<RelId> {
        (0..self.rel_count).collect()
    }

    /// `node`'s relationship ids on one side, in walk order.
    pub(super) fn rel_ids(&self, node: NodeId, dir: Direction) -> Vec<RelId> {
        let mut ids = Vec::new();
        let push = |id, _| -> Result<(), std::convert::Infallible> {
            ids.push(id);
            Ok(())
        };
        let Ok(()) = self.try_for_each_hop(node, dir, &RelTypes::Any, push);
        ids
    }

    pub(super) fn ids_of_type(&self, type_id: u32) -> Range<RelId> {
        self.by_type_id(type_id).map_or(0..0, TypeTopology::ids)
    }

    /// Every relationship as a rebuild row, ascending by id.
    pub(super) fn rows(&self) -> Vec<RelRow> {
        let mut rows = Vec::with_capacity(self.rel_count as usize);
        for t in &self.types {
            for (slot, &source) in t.out.nodes.iter().enumerate() {
                let first = t.rel_id(t.out.first_rel[slot]);
                for (pos, target) in t.out.targets(slot).enumerate() {
                    rows.push(RelRow {
                        id: first + pos as i64,
                        start: NodeId::from(source),
                        type_id: t.type_id,
                        end: target,
                        ts: None,
                    });
                }
            }
        }
        rows
    }
}
