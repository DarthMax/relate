//! POC: Cypher over a read-only Neo4j GDS Arrow export (`gds-codec`).
#![cfg(feature = "gds-codec")]

use std::path::Path;

use relate::{CloneOptions, Database, SnapshotCodecKind, Value};
use relate_storage::gds_fixture::{GdsExport, PropertyColumn, RelType};

fn export(dir: &Path) {
    GdsExport {
        labels: vec!["Person".into(), "City".into()],
        nodes: vec![
            (10, vec!["Person".into()]),
            (11, vec!["Person".into()]),
            (12, vec!["Person".into()]),
            (20, vec!["City".into()]),
        ],
        node_properties: vec![(
            "age".into(),
            PropertyColumn::Long(vec![Some(30), Some(40), Some(50), None]),
        )],
        rel_types: vec![
            RelType {
                name: "KNOWS".into(),
                undirected: false,
                property_keys: vec!["since".into()],
                edges: vec![(0, 1, vec![2001.0]), (1, 2, vec![2002.0])],
            },
            RelType {
                name: "LIVES_NEAR".into(),
                undirected: true,
                property_keys: vec![],
                edges: vec![(0, 3, vec![]), (2, 3, vec![])],
            },
        ],
    }
    .write(dir, 2);
}

fn query(db: &Database, cypher: &str) -> Vec<Vec<Value>> {
    db.connect().unwrap().query(cypher).unwrap().rows
}

fn uri(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[test]
fn cypher_reads_a_gds_export() {
    let dir = tempfile::tempdir().unwrap();
    export(dir.path());
    let db = Database::open_read_only(uri(dir.path())).unwrap();

    assert_eq!(
        query(&db, "MATCH (p:Person) RETURN count(p)"),
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(
        query(
            &db,
            "MATCH (a:Person {age: 30})-[:KNOWS*2]->(c) RETURN c.originalId"
        ),
        vec![vec![Value::Int(12)]]
    );
    assert_eq!(
        query(
            &db,
            "MATCH (b:Person)<-[k:KNOWS]-(a) WHERE b.age = 40 RETURN a.originalId, k.since"
        ),
        vec![vec![Value::Int(10), Value::Float(2001.0)]]
    );
    assert_eq!(
        query(
            &db,
            "MATCH (c:City)-[:LIVES_NEAR]-(p) RETURN p.originalId ORDER BY p.originalId"
        ),
        vec![vec![Value::Int(10)], vec![Value::Int(12)]]
    );
    assert_eq!(
        query(&db, "MATCH ()-[r]->() RETURN count(r)"),
        vec![vec![Value::Int(4)]]
    );
}

#[test]
fn writes_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    export(dir.path());
    assert!(Database::open(uri(dir.path())).is_err());
    let db = Database::open_read_only(uri(dir.path())).unwrap();
    assert!(db.connect().unwrap().execute("CREATE (:Person)").is_err());
}

#[test]
fn clone_converts_a_gds_export_to_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("gds");
    std::fs::create_dir(&src).unwrap();
    export(&src);
    let dst = dir.path().join("relate");
    Database::clone(
        uri(&src),
        uri(&dst),
        CloneOptions {
            to_snapshot_format: Some(SnapshotCodecKind::Parquet),
            ..CloneOptions::default()
        },
    )
    .unwrap();

    let db = Database::open(uri(&dst)).unwrap();
    db.connect()
        .unwrap()
        .execute("MATCH (p:Person {age: 30}) SET p.age = 31")
        .unwrap();
    assert_eq!(
        query(
            &db,
            "MATCH (p:Person)-[:KNOWS]->(q) RETURN p.age, q.age ORDER BY p.age"
        ),
        vec![
            vec![Value::Int(31), Value::Int(40)],
            vec![Value::Int(40), Value::Int(50)],
        ]
    );
}

#[test]
fn a_gds_graph_opens_from_in_memory_bytes() {
    let dir = tempfile::tempdir().unwrap();
    export(dir.path());
    let bytes = std::fs::read(dir.path().join("graph.arrow")).unwrap();
    let from_dir = Database::open_read_only(uri(dir.path())).unwrap();
    let db = Database::open_gds_arrow(&bytes).unwrap();
    drop(bytes);
    for cypher in [
        "MATCH (n) RETURN n.originalId, labels(n), n.age ORDER BY n.originalId",
        "MATCH (a)-[r]->(b) RETURN a.originalId, type(r), r.since, b.originalId ORDER BY id(r)",
        "MATCH (c:City)-[:LIVES_NEAR]-(p) RETURN p.originalId ORDER BY p.originalId",
    ] {
        assert_eq!(query(&db, cypher), query(&from_dir, cypher), "{cypher}");
        assert!(!query(&db, cypher).is_empty());
    }
    assert!(db.connect().unwrap().query("CREATE ()").is_err());
    assert!(Database::open_gds_arrow(b"not arrow").is_err());
}
