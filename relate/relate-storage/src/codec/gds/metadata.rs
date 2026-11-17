//! The graph-level metadata a GDS export carries as JSON in the Arrow
//! schema's custom metadata. Only the fields the loader needs are modeled;
//! unknown fields are ignored.

use std::collections::{BTreeMap, HashMap};

use relate_core::{Error, Result};
use serde::Deserialize;

pub(super) const METADATA_KEY: &str = "gds.graphexport.metadata";

/// The implicit label of a GDS graph projected without labels: it has no
/// label column and maps to an unlabeled node.
pub(super) const ALL_NODES_LABEL: &str = "__ALL__";

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum ValueType {
    Long,
    Double,
    LongArray,
    DoubleArray,
    FloatArray,
    DoubleVector,
    FloatVector,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum Direction {
    Directed,
    Undirected,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct PropertySchema {
    pub(super) value_type: ValueType,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct NodeSchemaEntry {
    #[serde(default)]
    pub(super) property_schemas: BTreeMap<String, PropertySchema>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct RelationshipSchemaEntry {
    pub(super) direction: Direction,
    #[serde(default)]
    pub(super) property_schemas: BTreeMap<String, PropertySchema>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct IdMapInfo {
    pub(super) node_count: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct GraphStoreMetadata {
    pub(super) id_map_info: IdMapInfo,
    pub(super) node_schema: BTreeMap<String, NodeSchemaEntry>,
    #[serde(default)]
    pub(super) relationship_schema: BTreeMap<String, RelationshipSchemaEntry>,
}

impl GraphStoreMetadata {
    /// The labels that have a label column, in schema order.
    pub(super) fn labels(&self) -> impl Iterator<Item = &str> {
        self.node_schema
            .keys()
            .map(String::as_str)
            .filter(|label| *label != ALL_NODES_LABEL)
    }

    /// Every node property key once, with its value type. GDS stores one
    /// column per key, shared by every label carrying it.
    pub(super) fn node_properties(&self) -> Result<BTreeMap<&str, ValueType>> {
        let mut out = BTreeMap::new();
        for entry in self.node_schema.values() {
            for (key, schema) in &entry.property_schemas {
                if let Some(seen) = out.insert(key.as_str(), schema.value_type) {
                    if seen != schema.value_type {
                        return Err(Error::storage(format!(
                            "GDS node property '{key}' has conflicting value types {seen:?} and {:?}",
                            schema.value_type
                        )));
                    }
                }
            }
        }
        Ok(out)
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
enum Version {
    V1,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GraphExportMetadata {
    graph_store_metadata: GraphStoreMetadata,
    #[allow(dead_code, reason = "parsed to refuse unknown versions")]
    version: Version,
}

#[derive(Deserialize, Debug)]
struct Envelope {
    #[serde(rename = "gds.graphexport.metadata")]
    inner: GraphExportMetadata,
}

pub(super) fn parse(schema_metadata: &HashMap<String, String>) -> Result<GraphStoreMetadata> {
    let json = schema_metadata.get(METADATA_KEY).ok_or_else(|| {
        Error::storage(format!(
            "GDS Arrow graph has no '{METADATA_KEY}' schema metadata"
        ))
    })?;
    let envelope: Envelope = serde_json::from_str(json)
        .map_err(|e| Error::storage(format!("invalid GDS graph metadata: {e}")))?;
    Ok(envelope.inner.graph_store_metadata)
}
