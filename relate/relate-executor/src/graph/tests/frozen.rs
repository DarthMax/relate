//! A graph with a frozen topology answers every relationship read exactly
//! as a graph restored from the same relationships, materialized.

use super::*;
use crate::group_key::GroupKey;
use relate_engine_api::{encode_targets, FrozenRelType};
use relate_ir::Direction;
use std::sync::Arc;

const NODES: i64 = 40;
const HUB: i64 = 7;
const TS: u64 = 1;

/// One type's lists: per source, its sorted targets and their weights.
struct TypeLists {
    name: &'static str,
    keys: Vec<&'static str>,
    lists: Vec<(NodeId, Vec<NodeId>, Vec<f64>)>,
}

fn lists(name: &'static str, keys: Vec<&'static str>, seed: u64) -> TypeLists {
    let mut state = seed;
    let mut next = |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % bound
    };
    let mut lists = Vec::new();
    for source in 0..NODES {
        let degree = if source == HUB { 30 } else { next(6) };
        let mut targets: Vec<NodeId> = (0..degree).map(|_| next(NODES as u64) as i64).collect();
        if source % 9 == 0 && degree > 0 {
            targets.push(source);
            targets.push(targets[0]);
        }
        targets.sort_unstable();
        let weights = (0..targets.len())
            .map(|pos| (source * 100) as f64 + pos as f64 / 4.0)
            .collect();
        lists.push((source, targets, weights));
    }
    TypeLists { name, keys, lists }
}

fn fixture() -> Vec<TypeLists> {
    vec![
        lists("A", vec!["w", "v"], 1),
        lists("B", vec![], 2),
        TypeLists {
            name: "C",
            keys: vec![],
            lists: Vec::new(),
        },
        lists("D", vec!["w"], 3),
    ]
}

fn with_nodes() -> Graph {
    let g = Graph::default();
    for id in 0..NODES {
        let label = if id % 2 == 0 { "Even" } else { "Odd" };
        g.load_checkpoint_node(Node {
            id,
            labels: Arc::new(vec![label.into()]),
            properties: relate_core::PropertyMap::share(BTreeMap::new()),
        })
        .unwrap();
    }
    g
}

fn finish(g: Graph) -> Graph {
    g.set_commit_ts(TS);
    g.finish_restore();
    g.rebuild_indexes();
    g
}

fn materialized(types: &[TypeLists]) -> Graph {
    let g = with_nodes();
    let mut id = 0;
    for ty in types {
        g.catalog.lock().unwrap().record_rel_type(ty.name);
        for (source, targets, weights) in &ty.lists {
            for (target, weight) in targets.iter().zip(weights) {
                let properties: BTreeMap<String, Value> = ty
                    .keys
                    .iter()
                    .map(|key| ((*key).to_string(), Value::Float(*weight)))
                    .collect();
                g.load_checkpoint_rel(Relationship {
                    id,
                    start: *source,
                    end: *target,
                    rel_type: ty.name.into(),
                    properties: relate_core::PropertyMap::share(properties),
                })
                .unwrap();
                id += 1;
            }
        }
    }
    finish(g)
}

fn frozen(types: &[TypeLists]) -> Graph {
    let g = with_nodes();
    let input = types
        .iter()
        .map(|ty| {
            let keys = ty.keys.iter().map(|key| (*key).to_string()).collect();
            let mut out = FrozenRelType::new(ty.name, keys);
            for (source, targets, weights) in &ty.lists {
                let properties: Vec<&[f64]> = ty.keys.iter().map(|_| weights.as_slice()).collect();
                out.push(*source, &encode_targets(targets), &properties)
                    .unwrap();
            }
            out
        })
        .collect();
    g.install_frozen_topology(input).unwrap();
    finish(g)
}

fn hops(g: &Graph, node: NodeId, dir: Direction, types: &[&str]) -> Vec<(RelId, NodeId)> {
    let mut out = Vec::new();
    let types = g.rel_types(types);
    g.try_for_each_hop_resolved_at(node, dir, TS, &types, |rid, peer| {
        out.push((rid, peer));
        Ok::<(), ()>(())
    })
    .unwrap();
    out
}

const DIRECTIONS: [Direction; 3] = [Direction::Outgoing, Direction::Incoming, Direction::Both];
const FILTERS: [&[&str]; 7] = [
    &[],
    &["A"],
    &["B"],
    &["D", "A"],
    &["C"],
    &["Unknown"],
    &["B", "Unknown", "D"],
];

#[test]
fn walks_match_the_materialized_restore() {
    let types = fixture();
    let (m, f) = (materialized(&types), frozen(&types));
    for node in -1..NODES + 2 {
        for dir in DIRECTIONS {
            for filter in FILTERS {
                let expected = hops(&m, node, dir, filter);
                assert_eq!(
                    hops(&f, node, dir, filter),
                    expected,
                    "node {node} {dir:?} {filter:?}"
                );
                assert_eq!(
                    f.typed_adj_len(node, dir, &f.rel_types(filter)),
                    m.typed_adj_len(node, dir, &m.rel_types(filter)),
                    "node {node} {dir:?} {filter:?}"
                );
            }
            assert_eq!(f.adj_len(node, dir), m.adj_len(node, dir));
        }
        assert_eq!(f.degree_out(node), m.degree_out(node));
        assert_eq!(f.degree_in(node), m.degree_in(node));
    }
    assert!(!hops(&f, HUB, Direction::Both, &[]).is_empty());
}

#[test]
fn relationship_reads_match_the_materialized_restore() {
    let types = fixture();
    let (m, f) = (materialized(&types), frozen(&types));
    let ids = m.all_rel_ids_at(TS);
    assert!(ids.len() > 200);
    assert_eq!(f.all_rel_ids_at(TS), ids);
    assert_eq!(f.next_rel_id(), m.next_rel_id());
    for id in -1..=ids.len() as i64 {
        assert_eq!(f.rel_at(id, TS), m.rel_at(id, TS), "rel {id}");
        assert_eq!(f.rel_stub_at(id, TS), m.rel_stub_at(id, TS));
        assert_eq!(f.rel_ends_at(id, TS), m.rel_ends_at(id, TS));
        assert_eq!(f.rel_visible_at(id, TS), m.rel_visible_at(id, TS));
        assert_eq!(
            f.rel_type_name_shared(id, TS),
            m.rel_type_name_shared(id, TS)
        );
        assert_eq!(f.rel_prop_keys_at(id, TS), m.rel_prop_keys_at(id, TS));
        for key in ["w", "v", "missing"] {
            assert_eq!(f.rel_prop_at(id, TS, key), m.rel_prop_at(id, TS, key));
            let kid = |g: &Graph| g.interner.get_id(key);
            assert_eq!(
                kid(&f).and_then(|kid| f.rel_prop_id_at(id, TS, key, kid)),
                kid(&m).and_then(|kid| m.rel_prop_id_at(id, TS, key, kid)),
            );
        }
    }
}

#[test]
fn type_scans_statistics_and_postings_match_the_materialized_restore() {
    let types = fixture();
    let (m, f) = (materialized(&types), frozen(&types));
    let weight = Value::Float((HUB * 100) as f64 + 0.5);
    let gk = GroupKey::for_property_index(&weight).unwrap();
    for name in ["A", "B", "C", "D", "Unknown"] {
        assert_eq!(
            f.rel_ids_of_type_at(name, TS),
            m.rel_ids_of_type_at(name, TS)
        );
        assert_eq!(f.has_rel_type(name), m.has_rel_type(name));
        // The materialized load never interns a type without relationships.
        let (Some(ft), Some(mt)) = (f.rel_type_id(name), m.rel_type_id(name)) else {
            let rows = f
                .rel_type_id(name)
                .map_or(0, |ft| f.pattern_rel_type_rows_by_id(ft));
            assert_eq!(rows, 0, "{name}");
            continue;
        };
        assert_eq!(
            f.pattern_rel_type_rows_by_id(ft),
            m.pattern_rel_type_rows_by_id(mt)
        );
        for node in 0..NODES {
            assert_eq!(f.out_chunk_len(node, ft), m.out_chunk_len(node, mt));
        }
        let (fk, mk) = (f.interner.get_id("w"), m.interner.get_id("w"));
        if let (Some(fk), Some(mk)) = (fk, mk) {
            let postings = m.rel_prop_postings(mt, mk, "w", gk.clone());
            assert_eq!(f.rel_prop_postings(ft, fk, "w", gk.clone()), postings);
            assert_eq!(postings.is_empty(), name != "A" && name != "D");
        }
    }
}

#[test]
fn a_frozen_graph_refuses_writes_materialized_relationships_and_a_second_topology() {
    let f = frozen(&fixture());
    let rel = Relationship {
        id: 0,
        start: 0,
        end: 1,
        rel_type: "A".into(),
        properties: relate_core::PropertyMap::share(BTreeMap::new()),
    };
    assert!(matches!(
        f.load_checkpoint_rel(rel.clone()),
        Err(relate_core::Error::Storage(_))
    ));
    assert!(matches!(
        f.install_frozen_topology(Vec::new()),
        Err(relate_core::Error::Storage(_))
    ));
    assert!(f.begin_write_at(TS + 1).is_err());
    assert!(f.apply_redo(Vec::new(), TS + 1).is_err());
    assert_eq!(f.all_rel_ids_at(TS).len(), f.next_rel_id() as usize);
    let m = with_nodes();
    m.load_checkpoint_rel(rel).unwrap();
    assert!(matches!(
        m.install_frozen_topology(Vec::new()),
        Err(relate_core::Error::Storage(_))
    ));
}
