//! Test-only writer of GDS `graph.arrow` exports, following the layout the
//! GDS Java serializer writes: one row per node, compressed outgoing
//! adjacency per relationship type, undirected edges in both endpoints'
//! lists, null label cells for absent labels.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeBinaryArray,
    ListArray,
};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int64Type, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value as Json};

use super::decode::encode_targets;
use super::GRAPH_ARROW;

/// One node property column, one entry per node.
pub enum PropertyColumn {
    Long(Vec<Option<i64>>),
    Double(Vec<f64>),
    LongArray(Vec<Option<Vec<i64>>>),
    DoubleArray(Vec<Option<Vec<f64>>>),
    FloatVector(i32, Vec<Option<Vec<f32>>>),
}

impl PropertyColumn {
    fn value_type(&self) -> &'static str {
        match self {
            Self::Long(_) => "LONG",
            Self::Double(_) => "DOUBLE",
            Self::LongArray(_) => "LONG_ARRAY",
            Self::DoubleArray(_) => "DOUBLE_ARRAY",
            Self::FloatVector(..) => "FLOAT_VECTOR",
        }
    }

    fn array(&self) -> ArrayRef {
        match self {
            Self::Long(v) => Arc::new(Int64Array::from(v.clone())),
            Self::Double(v) => Arc::new(Float64Array::from(v.clone())),
            Self::LongArray(v) => Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                v.iter()
                    .map(|cell| cell.as_ref().map(|xs| xs.iter().map(|x| Some(*x)))),
            )),
            Self::DoubleArray(v) => Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
                v.iter()
                    .map(|cell| cell.as_ref().map(|xs| xs.iter().map(|x| Some(*x)))),
            )),
            Self::FloatVector(dim, v) => {
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        v.iter()
                            .map(|cell| cell.as_ref().map(|xs| xs.iter().map(|x| Some(*x)))),
                        *dim,
                    ),
                )
            }
        }
    }
}

pub struct RelType {
    pub name: String,
    pub undirected: bool,
    pub property_keys: Vec<String>,
    /// `(source, target, property values)` in internal node ids, one
    /// entry per logical edge.
    pub edges: Vec<(i64, i64, Vec<f64>)>,
}

#[derive(Default)]
pub struct GdsExport {
    pub labels: Vec<String>,
    /// `(original id, labels)` per node; the index is the internal id.
    pub nodes: Vec<(i64, Vec<String>)>,
    pub node_properties: Vec<(String, PropertyColumn)>,
    pub rel_types: Vec<RelType>,
}

impl GdsExport {
    pub fn write(&self, dir: &Path, batch_size: usize) {
        let n = self.nodes.len();
        let mut fields = vec![Field::new("nodeId", DataType::Int64, false)];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..n as i64))];
        for label in &self.labels {
            fields.push(Field::new(
                format!("n_type_{label}"),
                DataType::Boolean,
                true,
            ));
            columns.push(Arc::new(BooleanArray::from(
                self.nodes
                    .iter()
                    .map(|(_, ls)| ls.contains(label).then_some(true))
                    .collect::<Vec<_>>(),
            )));
        }
        fields.push(Field::new("originalId", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from_iter_values(
            self.nodes.iter().map(|(id, _)| *id),
        )));
        for (key, column) in &self.node_properties {
            let array = column.array();
            fields.push(Field::new(
                format!("n_prop_{key}"),
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }
        for rel_type in &self.rel_types {
            let mut lists: Vec<Vec<(i64, Vec<f64>)>> = vec![Vec::new(); n];
            for (s, t, props) in &rel_type.edges {
                lists[*s as usize].push((*t, props.clone()));
                if rel_type.undirected {
                    lists[*t as usize].push((*s, props.clone()));
                }
            }
            for list in &mut lists {
                list.sort_by_key(|(t, _)| *t);
            }
            let encoded: Vec<Option<Vec<u8>>> = lists
                .iter()
                .map(|list| {
                    (!list.is_empty())
                        .then(|| encode_targets(&list.iter().map(|(t, _)| *t).collect::<Vec<_>>()))
                })
                .collect();
            let adjacency =
                LargeBinaryArray::from_opt_vec(encoded.iter().map(|e| e.as_deref()).collect());
            fields.push(Field::new(
                format!("r_type_{}", rel_type.name),
                DataType::LargeBinary,
                true,
            ));
            columns.push(Arc::new(adjacency));
            for (i, key) in rel_type.property_keys.iter().enumerate() {
                let array =
                    ListArray::from_iter_primitive::<Int64Type, _, _>(lists.iter().map(|list| {
                        (!list.is_empty()).then(|| {
                            list.iter()
                                .map(|(_, props)| Some(props[i].to_bits() as i64))
                                .collect::<Vec<_>>()
                        })
                    }));
                fields.push(Field::new(
                    format!("r_prop_{}_{key}", rel_type.name),
                    array.data_type().clone(),
                    true,
                ));
                columns.push(Arc::new(array));
            }
        }
        let schema = Arc::new(Schema::new(fields).with_metadata(HashMap::from([(
            super::metadata::METADATA_KEY.to_owned(),
            self.metadata().to_string(),
        )])));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("fixture batch");
        let file = std::fs::File::create(dir.join(GRAPH_ARROW)).expect("create graph.arrow");
        let mut writer = FileWriter::try_new(file, &schema).expect("arrow writer");
        let mut offset = 0;
        while offset < n || offset == 0 {
            let len = batch_size.min(n - offset);
            writer
                .write(&batch.slice(offset, len))
                .expect("write batch");
            offset += len.max(1);
        }
        writer.finish().expect("finish arrow file");
    }

    fn metadata(&self) -> Json {
        let property_schemas: serde_json::Map<String, Json> = self
            .node_properties
            .iter()
            .map(|(key, column)| {
                (
                    key.clone(),
                    json!({"valueType": column.value_type(), "propertyState": "PERSISTENT"}),
                )
            })
            .collect();
        let labels: Vec<&str> = if self.labels.is_empty() {
            vec![super::metadata::ALL_NODES_LABEL]
        } else {
            self.labels.iter().map(String::as_str).collect()
        };
        let node_schema: serde_json::Map<String, Json> = labels
            .iter()
            .map(|l| {
                (
                    (*l).to_owned(),
                    json!({"propertySchemas": property_schemas}),
                )
            })
            .collect();
        let rel_schema: serde_json::Map<String, Json> = self
            .rel_types
            .iter()
            .map(|r| {
                let props: BTreeMap<&str, Json> = r
                    .property_keys
                    .iter()
                    .map(|k| {
                        (
                            k.as_str(),
                            json!({"valueType": "DOUBLE", "aggregation": "NONE"}),
                        )
                    })
                    .collect();
                (
                    r.name.clone(),
                    json!({
                        "direction": if r.undirected { "UNDIRECTED" } else { "DIRECTED" },
                        "propertySchemas": props,
                    }),
                )
            })
            .collect();
        json!({
            "gds.graphexport.metadata": {
                "graphStoreMetadata": {
                    "idMapInfo": {"idMapType": "array", "nodeCount": self.nodes.len()},
                    "nodeSchema": node_schema,
                    "relationshipSchema": rel_schema,
                },
                "version": "V1",
            }
        })
    }
}
