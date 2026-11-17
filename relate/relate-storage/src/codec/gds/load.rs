//! Decode a GDS `graph.arrow` export into checkpoint rows.
//!
//! One Arrow row per node: `nodeId` (dense internal id), `originalId`,
//! one `n_type_<Label>` boolean column per label, one `n_prop_<key>`
//! column per node property, and per relationship type `r_type_<T>`
//! (the node's outgoing compressed adjacency list) plus
//! `r_prop_<T>_<key>` (a `List<Int64>` of double bit patterns aligned
//! with the decoded targets).
//!
//! Node ids are the GDS dense ids. Relationship ids are assigned in
//! (type, source node, adjacency position) order, which is stable for an
//! immutable export. An undirected type stores every edge in both
//! endpoints' lists; only the copy with `source <= target` is kept (every
//! second copy of a self-loop), so each edge becomes one relationship.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
    LargeBinaryArray, ListArray,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use relate_core::{Error, Node, PropertyMap, Relationship, Result, Value};
use relate_engine_api::{decode_targets, encode_targets, FrozenRelType, SnapshotGraph};

use super::metadata::{self, Direction, GraphStoreMetadata, RelationshipSchemaEntry, ValueType};
use crate::codec::catalog::{self, CatalogWire};

/// The node property holding the Neo4j id the GDS graph was projected
/// from.
pub(super) const ORIGINAL_ID_PROPERTY: &str = "originalId";

const NODE_ID_COLUMN: &str = "nodeId";
const ORIGINAL_ID_COLUMN: &str = "originalId";

fn arrow_err(e: arrow::error::ArrowError) -> Error {
    Error::storage(format!("GDS Arrow graph: {e}"))
}

/// How the export's relationships reach the graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Topology {
    /// The adjacency lists, read in place by the engine.
    Frozen,
    /// One relationship row per target, adjacency rebuilt by the engine.
    Materialized,
}

impl Topology {
    /// `RELATE_GDS_TOPOLOGY`, a diagnostic switch: unset, empty or `frozen`
    /// selects [`Topology::Frozen`], `materialized` the row-per-relationship
    /// load it is compared against.
    pub(super) fn from_env() -> Result<Self> {
        match std::env::var("RELATE_GDS_TOPOLOGY").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("" | "frozen") => Ok(Self::Frozen),
            Ok("materialized") => Ok(Self::Materialized),
            other => Err(Error::storage(format!(
                "RELATE_GDS_TOPOLOGY must be 'frozen' or 'materialized', not {other:?}"
            ))),
        }
    }
}

pub(super) fn load_graph<G: SnapshotGraph>(bytes: Vec<u8>, topology: Topology) -> Result<G> {
    let reader = FileReader::try_new(Cursor::new(bytes), None).map_err(arrow_err)?;
    let meta = metadata::parse(reader.schema().metadata())?;
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<_, _>>()
        .map_err(arrow_err)?;
    let node_props = meta.node_properties()?;
    if node_props.contains_key(ORIGINAL_ID_PROPERTY) {
        return Err(Error::storage(format!(
            "GDS node property '{ORIGINAL_ID_PROPERTY}' collides with the original-id property"
        )));
    }
    let node_count = i64::try_from(meta.id_map_info.node_count)
        .map_err(|_| Error::storage("GDS node count exceeds the node id range"))?;

    let graph = G::default();
    graph.set_next_node_id(node_count);
    let mut loaded_nodes = 0usize;
    for batch in &batches {
        loaded_nodes += load_nodes(&graph, batch, &meta, &node_props, node_count)?;
    }
    if loaded_nodes as u64 != meta.id_map_info.node_count {
        return Err(Error::storage(format!(
            "GDS metadata names {} nodes but the file holds {loaded_nodes}",
            meta.id_map_info.node_count
        )));
    }
    let mut next_rel_id = 0i64;
    let mut frozen = Vec::new();
    for (rel_type, entry) in &meta.relationship_schema {
        let keys = entry.property_schemas.keys().cloned().collect();
        let mut lists = FrozenRelType::new(rel_type.as_str(), keys);
        for batch in &batches {
            let rels = RelColumns::resolve(batch, rel_type, entry)?;
            match topology {
                Topology::Frozen => freeze_rels(&mut lists, batch, &rels)?,
                Topology::Materialized => {
                    load_rels(&graph, batch, &rels, node_count, &mut next_rel_id)?;
                }
            }
        }
        frozen.push(lists);
    }
    if topology == Topology::Frozen {
        let count: u64 = frozen.iter().map(FrozenRelType::rel_count).sum();
        next_rel_id = i64::try_from(count)
            .map_err(|_| Error::storage("GDS relationship count exceeds the id range"))?;
        graph.install_frozen_topology(frozen)?;
    }
    drop(batches);
    graph.set_next_rel_id(next_rel_id);
    let rows = loaded_nodes + usize::try_from(next_rel_id).expect("rel ids are non-negative");
    crate::snapshot::ensure_commit_ts(&graph, rows);
    catalog::restore(&graph, CatalogWire::default(), None)?;
    Ok(graph)
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    downcast(
        batch
            .column_by_name(name)
            .ok_or_else(|| Error::storage(format!("GDS Arrow graph is missing column '{name}'")))?,
        name,
    )
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, name: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::storage(format!(
            "GDS column '{name}' has unexpected Arrow type {}",
            array.data_type()
        ))
    })
}

fn load_nodes<G: SnapshotGraph>(
    graph: &G,
    batch: &RecordBatch,
    meta: &GraphStoreMetadata,
    node_props: &BTreeMap<&str, ValueType>,
    node_count: i64,
) -> Result<usize> {
    let node_ids = column::<Int64Array>(batch, NODE_ID_COLUMN)?;
    let original_ids = column::<Int64Array>(batch, ORIGINAL_ID_COLUMN)?;
    let labels = meta
        .labels()
        .map(|label| {
            Ok((
                label,
                column::<BooleanArray>(batch, &format!("n_type_{label}"))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let props = node_props
        .iter()
        .map(|(key, value_type)| {
            let name = format!("n_prop_{key}");
            let array = batch.column_by_name(&name).ok_or_else(|| {
                Error::storage(format!("GDS Arrow graph is missing column '{name}'"))
            })?;
            Ok((*key, *value_type, array, name))
        })
        .collect::<Result<Vec<_>>>()?;

    for row in 0..batch.num_rows() {
        let id = node_ids.value(row);
        if !(0..node_count).contains(&id) {
            return Err(Error::storage(format!(
                "GDS node id {id} outside 0..{node_count}"
            )));
        }
        let node_labels = labels
            .iter()
            .filter(|(_, column)| column.is_valid(row) && column.value(row))
            .map(|(label, _)| (*label).to_owned())
            .collect();
        let mut properties = BTreeMap::new();
        properties.insert(
            ORIGINAL_ID_PROPERTY.to_owned(),
            Value::Int(original_ids.value(row)),
        );
        for (key, value_type, array, name) in &props {
            if let Some(value) = node_property(array, *value_type, row, name)? {
                properties.insert((*key).to_owned(), value);
            }
        }
        graph.load_checkpoint_node(Node {
            id,
            labels: Arc::new(node_labels),
            properties: PropertyMap::share(properties),
        })?;
    }
    Ok(batch.num_rows())
}

/// A node property cell as a Relate value. `None` for an absent value: a
/// null cell, or the NaN GDS writes for a double the node never had.
fn node_property(
    array: &ArrayRef,
    value_type: ValueType,
    row: usize,
    name: &str,
) -> Result<Option<Value>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match value_type {
        ValueType::Long => Value::Int(downcast::<Int64Array>(array, name)?.value(row)),
        ValueType::Double => {
            let v = downcast::<Float64Array>(array, name)?.value(row);
            if v.is_nan() {
                return Ok(None);
            }
            Value::Float(v)
        }
        ValueType::LongArray | ValueType::DoubleArray | ValueType::FloatArray => {
            list_elements(&downcast::<ListArray>(array, name)?.value(row), name)?
        }
        ValueType::DoubleVector | ValueType::FloatVector => list_elements(
            &downcast::<FixedSizeListArray>(array, name)?.value(row),
            name,
        )?,
    };
    Ok(Some(value))
}

fn list_elements(values: &ArrayRef, name: &str) -> Result<Value> {
    if values.null_count() > 0 {
        return Err(Error::storage(format!(
            "GDS array property '{name}' holds a null element"
        )));
    }
    let items: Vec<Value> = match values.data_type() {
        DataType::Int64 => downcast::<Int64Array>(values, name)?
            .values()
            .iter()
            .map(|v| Value::Int(*v))
            .collect(),
        DataType::Float64 => downcast::<Float64Array>(values, name)?
            .values()
            .iter()
            .map(|v| Value::Float(*v))
            .collect(),
        DataType::Float32 => downcast::<Float32Array>(values, name)?
            .values()
            .iter()
            .map(|v| Value::Float(f64::from(*v)))
            .collect(),
        other => {
            return Err(Error::storage(format!(
                "GDS array property '{name}' has unsupported element type {other}"
            )))
        }
    };
    Ok(Value::List(Arc::new(items)))
}

struct RelColumns<'a> {
    rel_type: &'a str,
    direction: Direction,
    adjacency: &'a LargeBinaryArray,
    properties: Vec<(&'a str, &'a ListArray, String)>,
}

impl<'a> RelColumns<'a> {
    fn resolve(
        batch: &'a RecordBatch,
        rel_type: &'a str,
        entry: &'a RelationshipSchemaEntry,
    ) -> Result<Self> {
        let properties = entry
            .property_schemas
            .iter()
            .map(|(key, schema)| {
                if schema.value_type != ValueType::Double {
                    return Err(Error::storage(format!(
                        "GDS relationship property '{rel_type}.{key}' has unsupported type {:?}",
                        schema.value_type
                    )));
                }
                let name = format!("r_prop_{rel_type}_{key}");
                Ok((key.as_str(), column::<ListArray>(batch, &name)?, name))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            rel_type,
            direction: entry.direction,
            adjacency: column::<LargeBinaryArray>(batch, &format!("r_type_{rel_type}"))?,
            properties,
        })
    }
}

fn load_rels<G: SnapshotGraph>(
    graph: &G,
    batch: &RecordBatch,
    rels: &RelColumns<'_>,
    node_count: i64,
    next_rel_id: &mut i64,
) -> Result<()> {
    let node_ids = column::<Int64Array>(batch, NODE_ID_COLUMN)?;
    for row in 0..batch.num_rows() {
        if rels.adjacency.is_null(row) {
            continue;
        }
        let source = node_ids.value(row);
        let targets = decode_targets(rels.adjacency.value(row))?;
        let weights = rels
            .properties
            .iter()
            .map(|(key, column, name)| {
                let values = column.value(row);
                let values = downcast::<Int64Array>(&values, name)?.clone();
                if values.len() != targets.len() {
                    return Err(Error::storage(format!(
                        "GDS column '{name}' has {} values for node {source} of degree {}",
                        values.len(),
                        targets.len()
                    )));
                }
                Ok((*key, values))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut kept = undirected_copies(rels.direction, source);
        for (pos, &target) in targets.iter().enumerate() {
            if !(0..node_count).contains(&target) {
                return Err(Error::storage(format!(
                    "GDS relationship target {target} outside 0..{node_count}"
                )));
            }
            if !kept(target) {
                continue;
            }
            let properties: BTreeMap<String, Value> = weights
                .iter()
                .map(|(key, values)| {
                    (
                        (*key).to_owned(),
                        Value::Float(f64::from_bits(values.value(pos) as u64)),
                    )
                })
                .collect();
            graph.load_checkpoint_rel(Relationship {
                id: *next_rel_id,
                start: source,
                end: target,
                rel_type: rels.rel_type.to_owned(),
                properties: PropertyMap::share(properties),
            })?;
            *next_rel_id += 1;
        }
    }
    Ok(())
}

/// Which targets of `source`'s list become relationships. An undirected
/// type lists every edge at both endpoints: the copy pointing at a larger
/// node id is kept, and every second copy of a self-loop.
fn undirected_copies(direction: Direction, source: i64) -> impl FnMut(i64) -> bool {
    let mut self_loops = 0usize;
    move |target| {
        if direction != Direction::Undirected {
            return true;
        }
        match target.cmp(&source) {
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => {
                self_loops += 1;
                self_loops % 2 == 1
            }
        }
    }
}

/// Push one batch's lists of a type as they are; an undirected list is
/// re-encoded from the copies [`undirected_copies`] keeps.
fn freeze_rels(
    lists: &mut FrozenRelType,
    batch: &RecordBatch,
    rels: &RelColumns<'_>,
) -> Result<()> {
    let node_ids = column::<Int64Array>(batch, NODE_ID_COLUMN)?;
    let mut weights: Vec<Vec<f64>> = vec![Vec::new(); rels.properties.len()];
    for row in 0..batch.num_rows() {
        if rels.adjacency.is_null(row) {
            continue;
        }
        let source = node_ids.value(row);
        let encoded = rels.adjacency.value(row);
        let kept: Option<(Vec<i64>, Vec<bool>)> = match rels.direction {
            Direction::Undirected => {
                let targets = decode_targets(encoded)?;
                let keep = targets
                    .iter()
                    .map(|target| *target)
                    .map(undirected_copies(rels.direction, source))
                    .collect();
                Some((targets, keep))
            }
            Direction::Directed => None,
        };
        for (out, (_, column, name)) in weights.iter_mut().zip(&rels.properties) {
            let values = column.value(row);
            let values = downcast::<Int64Array>(&values, name)?;
            let bits = values
                .values()
                .iter()
                .map(|bits| f64::from_bits(*bits as u64));
            out.clear();
            match &kept {
                None => out.extend(bits),
                Some((_, keep)) if keep.len() == values.len() => {
                    out.extend(bits.zip(keep).filter_map(|(v, keep)| keep.then_some(v)));
                }
                Some((targets, _)) => {
                    return Err(Error::storage(format!(
                        "GDS column '{name}' has {} values for node {source} of degree {}",
                        values.len(),
                        targets.len()
                    )));
                }
            }
        }
        let weights: Vec<&[f64]> = weights.iter().map(Vec::as_slice).collect();
        match kept {
            None => lists.push(source, encoded, &weights)?,
            Some((targets, keep)) => {
                let targets: Vec<i64> = targets
                    .into_iter()
                    .zip(keep)
                    .filter_map(|(target, keep)| keep.then_some(target))
                    .collect();
                lists.push(source, &encode_targets(&targets), &weights)?;
            }
        }
    }
    Ok(())
}
