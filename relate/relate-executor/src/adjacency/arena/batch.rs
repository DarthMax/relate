//! The wholesale build's per-node seal batch (ADR-0608): the fold writes a
//! node's sealed chunks here, encoded outside the arena lock, and the
//! arena installs them under one write lock as a fresh tail span. Sealing
//! straight into the arena from parallel shards interleaves nodes, so a
//! node's later chunks land mid-array and every one pays a span-table
//! shift scan under the tier's one lock.

use relate_core::NodeId;

use super::{
    codec, fit_reserved, hop_count, span_len, span_start, ArenaStore, Entry, SealSink, Tombstone,
    MAX_SPAN_TYPES,
};
use crate::adjacency::Hop;

/// One encoded chunk awaiting installation.
pub(super) struct BatchChunk {
    type_id: u32,
    stream: Vec<u8>,
    hops: u32,
    tombstones: Vec<Tombstone>,
}

/// One node's sealed chunks, collected by [`SealSink::put_chunk`] and
/// installed by [`super::SealedArena::install_node`]. It answers the fold's
/// reads from what it holds, exactly as the node's arena region would after
/// the same puts.
#[derive(Default)]
pub(crate) struct NodeSealBatch {
    chunks: Vec<BatchChunk>,
}

impl NodeSealBatch {
    pub(super) fn into_chunks(self) -> Vec<BatchChunk> {
        self.chunks
    }

    fn position(&self, type_id: u32) -> Option<usize> {
        self.chunks.iter().position(|c| c.type_id == type_id)
    }
}

impl SealSink for &mut NodeSealBatch {
    fn has_chunk(&self, type_id: u32) -> bool {
        self.position(type_id).is_some()
    }

    fn decoded(&self, type_id: u32) -> Vec<Hop> {
        self.position(type_id).map_or_else(Vec::new, |i| {
            let c = &self.chunks[i];
            codec::decode_chunk(&c.stream, c.hops as usize)
        })
    }

    fn tombstones(&self, type_id: u32) -> Vec<Tombstone> {
        self.position(type_id)
            .map_or_else(Vec::new, |i| self.chunks[i].tombstones.clone())
    }

    /// The region's put, minus the arena-offset wall: that one is checked
    /// at installation, against the arena the batch lands in.
    fn put_chunk(&mut self, type_id: u32, hops: &[Hop], tombstones: Vec<Tombstone>) -> bool {
        let existing = self.position(type_id);
        if hops.is_empty() && tombstones.is_empty() {
            if let Some(i) = existing {
                self.chunks.remove(i);
            }
            return true;
        }
        if existing.is_none() && self.chunks.len() >= MAX_SPAN_TYPES {
            return false;
        }
        let chunk = BatchChunk {
            type_id,
            stream: codec::encode_chunk(hops),
            hops: hop_count(hops.len()),
            tombstones,
        };
        match existing {
            Some(i) => self.chunks[i] = chunk,
            None => self.chunks.push(chunk),
        }
        true
    }
}

impl ArenaStore {
    /// Install one node's batch. A node without a span gets a fresh,
    /// type-sorted span at the entry array's tail — no mid-array insert and
    /// no span shift. A node that already has one (never the wholesale
    /// build's case: it installs into a cleared arena) takes the per-chunk
    /// store path. Refuses, installing nothing, when the batch would push
    /// the arena past its `u32` offset.
    pub(super) fn install_node(&mut self, node: NodeId, mut chunks: Vec<BatchChunk>) -> bool {
        let added: u64 = chunks.iter().map(|c| c.stream.len() as u64).sum();
        if self.bytes.len() as u64 + added > u64::from(u32::MAX) {
            return false;
        }
        if !self.range_of(node).is_empty() {
            return chunks
                .into_iter()
                .all(|c| self.store_stream(node, c.type_id, &c.stream, c.hops, c.tombstones));
        }
        chunks.sort_unstable_by_key(|c| c.type_id);
        let start = span_start(self.entries.len());
        let len = span_len(chunks.len());
        for chunk in chunks {
            let off = u32::try_from(self.bytes.len()).expect("the arena stays under 4 GiB");
            self.bytes.extend_from_slice(&chunk.stream);
            self.entries.push(Entry {
                type_id: chunk.type_id,
                off,
                hops: chunk.hops,
            });
            let mut tombstones = chunk.tombstones;
            fit_reserved(&mut tombstones);
            self.tombstones.push(tombstones);
        }
        fit_reserved(&mut self.entries);
        fit_reserved(&mut self.tombstones);
        self.set_span(node, start, len);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::super::{SealSink, SealedArena};
    use super::NodeSealBatch;
    use crate::adjacency::Hop;

    fn hops(ids: &[i64]) -> Vec<Hop> {
        ids.iter().map(|&id| Hop { ts: None, id }).collect()
    }

    fn batch(chunks: &[(u32, &[i64])]) -> NodeSealBatch {
        let mut batch = NodeSealBatch::default();
        for (type_id, ids) in chunks {
            assert!((&mut batch).put_chunk(*type_id, &hops(ids), Vec::new()));
        }
        batch
    }

    #[test]
    fn interleaved_installs_keep_every_span_readable() {
        let arena = SealedArena::default();
        assert!(arena.install_node(7, batch(&[(2, &[70, 71]), (1, &[72])])));
        assert!(arena.install_node(3, batch(&[(5, &[30]), (1, &[31, 32, 33])])));
        assert!(arena.install_node(9, batch(&[])));
        let r7 = arena.region(7).unwrap();
        assert_eq!(r7.type_ids(), vec![1, 2]);
        assert_eq!(r7.decoded(2), hops(&[70, 71]));
        assert_eq!(r7.decoded(1), hops(&[72]));
        let r3 = arena.region(3).unwrap();
        assert_eq!(r3.decoded(1), hops(&[31, 32, 33]));
        assert_eq!(r3.decoded(5), hops(&[30]));
        assert!(arena.region(9).is_none());
    }

    #[test]
    fn a_batch_answers_like_the_region_after_the_same_puts() {
        let mut batch = batch(&[(4, &[1, 2])]);
        let mut sink = &mut batch;
        assert!(sink.has_chunk(4));
        assert_eq!(sink.decoded(4), hops(&[1, 2]));
        assert!(sink.put_chunk(4, &hops(&[3]), Vec::new()));
        assert_eq!(sink.decoded(4), hops(&[3]));
        assert!(sink.put_chunk(4, &[], Vec::new()));
        assert!(!sink.has_chunk(4));
    }

    #[test]
    fn installing_into_an_existing_span_merges_by_type() {
        let arena = SealedArena::default();
        assert!(arena.install_node(1, batch(&[(3, &[10])])));
        assert!(arena.install_node(1, batch(&[(1, &[11]), (3, &[12])])));
        let region = arena.region(1).unwrap();
        assert_eq!(region.type_ids(), vec![1, 3]);
        assert_eq!(region.decoded(1), hops(&[11]));
        assert_eq!(region.decoded(3), hops(&[12]));
    }
}
