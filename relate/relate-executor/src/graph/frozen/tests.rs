use relate_core::{Error, NodeId};
use relate_ir::Direction;

use relate_engine_api::{encode_targets, FrozenRelType};

use super::FrozenTopology;
use crate::graph::Graph;
use crate::store::RelTypes;

fn graph(bound: NodeId, types: Vec<FrozenRelType>) -> relate_core::Result<Graph> {
    let g = Graph::default();
    g.set_next_node_id(bound);
    g.install_frozen_topology(types)?;
    Ok(g)
}

fn topology(g: &Graph) -> &FrozenTopology {
    g.frozen().unwrap()
}

fn hops(t: &FrozenTopology, node: NodeId, dir: Direction) -> Vec<(i64, NodeId)> {
    let mut out = Vec::new();
    t.try_for_each_hop(node, dir, &RelTypes::Any, |rid, peer| {
        out.push((rid, peer));
        Ok::<(), ()>(())
    })
    .unwrap();
    out
}

#[test]
fn ids_follow_type_then_source_then_position_across_word_boundaries() {
    let mut a = FrozenRelType::new("A", vec![]);
    a.push(3, &encode_targets(&[64, 70_000]), &[]).unwrap();
    a.push(64, &[], &[]).unwrap();
    a.push(70_000, &encode_targets(&[3, 3, 70_000]), &[])
        .unwrap();
    let mut b = FrozenRelType::new("B", vec!["w".into()]);
    b.push(64, &encode_targets(&[3]), &[&[1.5]]).unwrap();
    let g = graph(70_001, vec![a, FrozenRelType::new("Empty", vec![]), b]).unwrap();
    let t = topology(&g);

    assert_eq!(t.rel_count(), 6);
    assert_eq!(t.ends(0), Some((3, 64)));
    assert_eq!(t.ends(1), Some((3, 70_000)));
    assert_eq!(t.ends(3), Some((70_000, 3)));
    assert_eq!(t.ends(4), Some((70_000, 70_000)));
    assert_eq!(t.ends(5), Some((64, 3)));
    assert_eq!(t.ends(6), None);
    assert_eq!(t.type_name(5).as_deref(), Some("B"));
    assert_eq!(t.prop(5, "w"), Some(relate_core::Value::Float(1.5)));
    assert_eq!(t.prop(4, "w"), None);

    assert_eq!(hops(t, 3, Direction::Outgoing), vec![(1, 70_000), (0, 64)]);
    assert_eq!(
        hops(t, 3, Direction::Incoming),
        vec![(3, 70_000), (2, 70_000), (5, 64)]
    );
    assert_eq!(
        hops(t, 70_000, Direction::Both),
        vec![(4, 70_000), (3, 3), (2, 3), (1, 3)]
    );
    assert_eq!(t.degree(70_000, Direction::Both, &RelTypes::Any), 5);
    assert!(hops(t, 64, Direction::Outgoing) == vec![(5, 3)]);
    assert!(hops(t, 1, Direction::Both).is_empty());
}

#[test]
fn nodes_outside_the_bound_and_repeated_types_are_refused() {
    let list = |source, target| {
        let mut t = FrozenRelType::new("A", vec![]);
        t.push(source, &encode_targets(&[target]), &[]).unwrap();
        t
    };
    assert!(matches!(
        graph(10, vec![list(0, 10)]),
        Err(Error::Storage(_))
    ));
    assert!(graph(10, vec![list(10, 0)]).is_err());
    assert!(graph(10, vec![list(0, 1), list(1, 2)]).is_err());
    assert!(graph(10, vec![list(9, 9)]).is_ok());
}
