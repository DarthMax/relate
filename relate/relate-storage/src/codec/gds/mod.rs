//! POC, default off (`gds-codec`): a read-only snapshot codec over a Neo4j
//! Graph Data Science Arrow export — a directory holding one
//! `graph.arrow` IPC file. The export is loaded into the in-memory graph
//! at open; nothing is ever written back. The store runs on an in-memory
//! WAL, so the export directory is opened without a lease and stays
//! untouched.

#[cfg(gds_codec)]
mod decode;
#[cfg(gds_codec)]
#[doc(hidden)]
pub mod fixture;
#[cfg(gds_codec)]
mod load;
#[cfg(gds_codec)]
mod metadata;
#[cfg(all(test, gds_codec))]
mod tests;

use relate_core::{Error, Result};

use super::SnapshotCodecKind;
use crate::snapshot::LoadedSnapshot;
use crate::StoreTarget;

/// The one data file of a GDS export.
pub(crate) const GRAPH_ARROW: &str = "graph.arrow";

fn read_only_error() -> Error {
    Error::storage(
        "GDS Arrow graphs are read-only; clone the store to another snapshot format to write",
    )
}

/// Whether `path` is a GDS export directory rather than a Relate store:
/// it holds `graph.arrow` and no Relate lock marker.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn is_export_dir(path: &std::path::Path) -> bool {
    path.join(GRAPH_ARROW).is_file() && !path.join(crate::native_ownership::LOCK_MARKER).exists()
}

/// The store target of a GDS export directory: the export read through a
/// leaseless native VFS, the WAL in memory (seeded with one segment,
/// since a read-only open requires an existing WAL).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn export_target(
    path: &std::path::Path,
    requested: Option<SnapshotCodecKind>,
    read_only: bool,
    wal_dir: Option<std::path::PathBuf>,
    config: Option<&super::open_config::CodecOpenConfig>,
) -> Result<StoreTarget> {
    use std::sync::Arc;

    use crate::vfs::{MemoryVfs, NativeVfs, Vfs};

    super::check_available(SnapshotCodecKind::Gds)?;
    if let Some(want) = requested.filter(|want| *want != SnapshotCodecKind::Gds) {
        return Err(Error::storage(format!(
            "snapshot codec mismatch: directory is gds, requested {want}"
        )));
    }
    if !read_only {
        return Err(Error::storage(format!(
            "GDS Arrow graph at {} can only be opened read-only",
            path.display()
        )));
    }
    if wal_dir.is_some() || config.is_some() {
        return Err(Error::storage(
            "GDS Arrow graphs take no WAL directory or codec configuration",
        ));
    }
    let wal_vfs: Arc<dyn Vfs> = Arc::new(MemoryVfs::new());
    let wal_prefix = SnapshotCodecKind::Gds.wal_dir();
    crate::Wal::open(Arc::clone(&wal_vfs), wal_prefix, 0)?;
    Ok(StoreTarget {
        vfs: Arc::new(NativeVfs::new(path)),
        wal_vfs,
        wal_prefix: wal_prefix.to_owned(),
        native_root: None,
        wal_native: None,
        codec: SnapshotCodecKind::Gds,
        codec_config: None,
    })
}

#[cfg(gds_codec)]
pub(crate) fn load_with_checkpoint_lsn<G: relate_engine_api::SnapshotGraph>(
    target: &StoreTarget,
) -> Result<LoadedSnapshot<G>> {
    let bytes = crate::snapshot::get_object_opt(target, GRAPH_ARROW)?
        .ok_or_else(|| Error::storage(format!("GDS Arrow store has no {GRAPH_ARROW}")))?;
    Ok(LoadedSnapshot {
        graph: load::load_graph(bytes)?,
        checkpoint_lsn: 0,
        ontology: None,
        marker: crate::version::SnapshotMarker::Current,
        deferred: None,
    })
}

#[cfg(not(gds_codec))]
pub(crate) fn load_with_checkpoint_lsn<G: relate_engine_api::SnapshotGraph>(
    _: &StoreTarget,
) -> Result<LoadedSnapshot<G>> {
    Err(super::codec_unavailable(SnapshotCodecKind::Gds))
}

pub(crate) fn dump(
    _: &StoreTarget,
    _: &dyn relate_engine_api::GraphImport,
    _: u64,
    _: u64,
    _: Option<&str>,
) -> Result<()> {
    Err(read_only_error())
}

pub(crate) fn snapshot_data_bytes(target: &StoreTarget) -> Result<u64> {
    crate::snapshot::sum_object_bytes(target, SnapshotCodecKind::Gds.checkpoint_objects())
}
