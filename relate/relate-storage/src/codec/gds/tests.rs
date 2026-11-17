use std::collections::BTreeSet;

use relate_core::{Relationship, Value};
use relate_executor::Graph;

use super::fixture::{GdsExport, PropertyColumn, RelType};
use crate::{open_store_readonly, parse_uri_opening_with_config, SnapshotCodecKind};

fn export() -> GdsExport {
    GdsExport {
        labels: vec!["A".into(), "B".into()],
        nodes: vec![
            (100, vec!["A".into()]),
            (101, vec!["A".into(), "B".into()]),
            (102, vec![]),
        ],
        node_properties: vec![
            (
                "age".into(),
                PropertyColumn::Long(vec![Some(1), None, Some(3)]),
            ),
            (
                "score".into(),
                PropertyColumn::Double(vec![0.5, f64::NAN, 2.5]),
            ),
            (
                "tags".into(),
                PropertyColumn::LongArray(vec![Some(vec![1, 2]), None, Some(vec![])]),
            ),
            (
                "emb".into(),
                PropertyColumn::FloatVector(2, vec![Some(vec![0.5, 1.5]), None, None]),
            ),
        ],
        rel_types: vec![
            RelType {
                name: "KNOWS".into(),
                undirected: false,
                property_keys: vec!["w".into()],
                edges: vec![(0, 1, vec![1.0]), (0, 1, vec![2.0]), (2, 0, vec![3.0])],
            },
            RelType {
                name: "NEAR".into(),
                undirected: true,
                property_keys: vec![],
                edges: vec![(0, 2, vec![]), (1, 1, vec![])],
            },
        ],
    }
}

fn open(dir: &std::path::Path) -> Graph {
    let target = parse_uri_opening_with_config(dir.to_str().unwrap(), None, None, true, None)
        .expect("gds target");
    assert_eq!(target.codec, SnapshotCodecKind::Gds);
    open_store_readonly::<Graph>(&target).expect("open gds").0
}

fn rels(graph: &Graph) -> Vec<Relationship> {
    let ts = graph.commit_ts();
    graph
        .all_rel_ids_at(ts)
        .into_iter()
        .map(|id| graph.rel_at(id, ts).unwrap())
        .collect()
}

#[test]
fn loads_nodes_labels_and_properties() {
    for batch_size in [1, 2, 100] {
        let dir = tempfile::tempdir().unwrap();
        export().write(dir.path(), batch_size);
        let graph = open(dir.path());
        let ts = graph.commit_ts();
        assert_eq!(graph.all_node_ids_at(ts), vec![0, 1, 2]);

        let n0 = graph.node_at(0, ts).unwrap();
        assert_eq!(*n0.labels, vec!["A".to_string()]);
        assert_eq!(n0.properties.get("originalId"), Some(&Value::Int(100)));
        assert_eq!(n0.properties.get("age"), Some(&Value::Int(1)));
        assert_eq!(n0.properties.get("score"), Some(&Value::Float(0.5)));
        assert_eq!(
            n0.properties.get("tags"),
            Some(&Value::List(vec![Value::Int(1), Value::Int(2)].into()))
        );
        assert_eq!(
            n0.properties.get("emb"),
            Some(&Value::List(
                vec![Value::Float(0.5), Value::Float(1.5)].into()
            ))
        );

        let n1 = graph.node_at(1, ts).unwrap();
        assert_eq!(*n1.labels, vec!["A".to_string(), "B".to_string()]);
        assert_eq!(n1.properties.get("age"), None);
        assert_eq!(n1.properties.get("score"), None);
        assert_eq!(n1.properties.get("tags"), None);

        let n2 = graph.node_at(2, ts).unwrap();
        assert!(n2.labels.is_empty());
        assert_eq!(n2.properties.get("tags"), Some(&Value::List(vec![].into())));
    }
}

#[test]
fn directed_rels_keep_parallel_edges_and_properties() {
    let dir = tempfile::tempdir().unwrap();
    export().write(dir.path(), 2);
    let graph = open(dir.path());
    let mut knows: Vec<(i64, i64, f64)> = rels(&graph)
        .into_iter()
        .filter(|r| r.rel_type == "KNOWS")
        .map(|r| match r.properties.get("w") {
            Some(Value::Float(w)) => (r.start, r.end, *w),
            other => panic!("unexpected weight {other:?}"),
        })
        .collect();
    knows.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(knows, vec![(0, 1, 1.0), (0, 1, 2.0), (2, 0, 3.0)]);
}

#[test]
fn undirected_edges_load_once_including_self_loops() {
    let dir = tempfile::tempdir().unwrap();
    export().write(dir.path(), 100);
    let graph = open(dir.path());
    let near: BTreeSet<(i64, i64)> = rels(&graph)
        .into_iter()
        .filter(|r| r.rel_type == "NEAR")
        .map(|r| (r.start, r.end))
        .collect();
    assert_eq!(near, BTreeSet::from([(0, 2), (1, 1)]));
    assert_eq!(rels(&graph).len(), 5);
}

#[test]
fn rel_ids_are_stable_across_opens() {
    let dir = tempfile::tempdir().unwrap();
    export().write(dir.path(), 1);
    let first: Vec<_> = rels(&open(dir.path()))
        .into_iter()
        .map(|r| (r.id, r.start, r.end, r.rel_type))
        .collect();
    let second: Vec<_> = rels(&open(dir.path()))
        .into_iter()
        .map(|r| (r.id, r.start, r.end, r.rel_type))
        .collect();
    assert_eq!(first, second);
}

#[test]
fn export_dir_refuses_writable_open_and_leaves_no_trace() {
    let dir = tempfile::tempdir().unwrap();
    export().write(dir.path(), 100);
    let err = parse_uri_opening_with_config(dir.path().to_str().unwrap(), None, None, false, None)
        .err()
        .expect("writable open must fail");
    assert!(err.to_string().contains("read-only"), "{err}");
    let _ = open(dir.path());
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(entries, vec![std::ffi::OsString::from("graph.arrow")]);
}

#[test]
fn checkpoint_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    export().write(dir.path(), 100);
    let target =
        parse_uri_opening_with_config(dir.path().to_str().unwrap(), None, None, true, None)
            .unwrap();
    let (graph, _wal) = open_store_readonly::<Graph>(&target).unwrap();
    let err = crate::checkpoint(&target, &graph, None).unwrap_err();
    assert!(err.to_string().contains("read-only"), "{err}");
}

#[test]
fn empty_graph_without_labels_loads() {
    let dir = tempfile::tempdir().unwrap();
    GdsExport::default().write(dir.path(), 10);
    let graph = open(dir.path());
    assert!(graph.all_node_ids_at(graph.commit_ts()).is_empty());
}

#[test]
fn frozen_and_materialized_loads_hold_the_same_relationships() {
    use super::load::{load_graph, Topology};
    for batch_size in [1, 2, 100] {
        let dir = tempfile::tempdir().unwrap();
        export().write(dir.path(), batch_size);
        let bytes = std::fs::read(dir.path().join(super::GRAPH_ARROW)).unwrap();
        let frozen: Graph = load_graph(bytes.clone(), Topology::Frozen).unwrap();
        let materialized: Graph = load_graph(bytes, Topology::Materialized).unwrap();
        assert_eq!(
            rels(&frozen),
            rels(&materialized),
            "batch size {batch_size}"
        );
        assert_eq!(rels(&frozen).len(), 5);
        assert_eq!(frozen.next_rel_id(), materialized.next_rel_id());
        for node in 0..3 {
            assert_eq!(frozen.degree_out(node), materialized.degree_out(node));
            assert_eq!(frozen.degree_in(node), materialized.degree_in(node));
        }
    }
}
