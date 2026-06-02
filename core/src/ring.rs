/// A sealed-or-open unit of replayable output. `prologue` re-establishes
/// sticky state so the chunk is self-contained; `data` is the sanitized
/// visual byte stream.
#[derive(Debug, Clone)]
struct Chunk {
    prologue: Vec<u8>,
    data: Vec<u8>,
}

impl Chunk {
    fn new(prologue: Vec<u8>) -> Self {
        Self {
            prologue,
            data: Vec::new(),
        }
    }
}

/// A ring of growable, self-contained chunks. Storage only — it does not
/// parse. The scanner decides when to `cut` (ground state past the watermark)
/// and `purge` (screen clear).
#[derive(Debug)]
pub struct ChunkRing {
    chunks: Vec<Chunk>,
    /// Soft byte target for an open chunk before the scanner should cut it.
    watermark: usize,
}

impl ChunkRing {
    /// `watermark` is the soft cut target (128 * 1024 in production).
    pub fn new(watermark: usize) -> Self {
        Self {
            chunks: vec![Chunk::new(Vec::new())],
            watermark,
        }
    }

    /// Append sanitized visual bytes to the currently open (last) chunk.
    pub fn append(&mut self, bytes: &[u8]) {
        self.open_mut().data.extend_from_slice(bytes);
    }

    /// True once the open chunk's data has reached the soft watermark. The
    /// scanner only acts on this at parser ground state, so a single long
    /// sequence is never split — the chunk simply grows past the watermark.
    #[must_use]
    pub fn should_cut(&self) -> bool {
        self.chunks
            .last()
            .is_some_and(|c| c.data.len() >= self.watermark)
    }

    /// Seal the open chunk and open a new one carrying `prologue` (the
    /// serialized sticky state at this moment).
    pub fn cut(&mut self, prologue: Vec<u8>) {
        self.chunks.push(Chunk::new(prologue));
    }

    /// Drop all history on a screen clear: discard every chunk and start
    /// fresh with a single open chunk whose prologue is the clear sequence
    /// itself (a known-good replay base).
    pub fn purge(&mut self, clear_prologue: Vec<u8>) {
        self.chunks = vec![Chunk::new(clear_prologue)];
    }

    /// Replay the most recent `want_chunks` chunks:
    /// `first_selected.prologue + concat(selected.data)`.
    pub fn replay(&self, want_chunks: usize) -> Vec<u8> {
        let n = self.chunks.len();
        let start = n.saturating_sub(want_chunks.max(1));
        let selected = &self.chunks[start..];

        let mut out = Vec::new();
        if let Some(first) = selected.first() {
            out.extend_from_slice(&first.prologue);
        }
        for chunk in selected {
            out.extend_from_slice(&chunk.data);
        }
        out
    }

    fn open_mut(&mut self) -> &mut Chunk {
        self.chunks
            .last_mut()
            .expect("ring always has an open chunk")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replays_open_chunk_data_with_empty_prologue() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"hello ");
        r.append(b"world");
        assert_eq!(r.replay(usize::MAX), b"hello world".to_vec());
    }

    #[test]
    fn cut_seals_chunk_and_starts_new_with_prologue() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"AAA");
        r.cut(b"\x1b[0m\x1b[31m".to_vec());
        r.append(b"BBB");
        assert_eq!(r.replay(usize::MAX), b"AAABBB".to_vec());
    }

    #[test]
    fn want_chunks_caps_to_most_recent_and_uses_their_prologue() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"AAA");
        r.cut(b"P1".to_vec());
        r.append(b"BBB");
        r.cut(b"P2".to_vec());
        r.append(b"CCC");
        assert_eq!(r.replay(2), b"P1BBBCCC".to_vec());
        assert_eq!(r.replay(1), b"P2CCC".to_vec());
    }

    #[test]
    fn should_cut_reports_when_open_chunk_passes_watermark() {
        let mut r = ChunkRing::new(8);
        assert!(!r.should_cut());
        r.append(b"1234567");
        assert!(!r.should_cut());
        r.append(b"89");
        assert!(r.should_cut());
    }

    #[test]
    fn purge_drops_prior_chunks_and_resets_open_chunk() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"OLD");
        r.cut(b"P1".to_vec());
        r.append(b"MORE");
        r.purge(b"\x1bc".to_vec());
        r.append(b"NEW");
        assert_eq!(r.replay(usize::MAX), b"\x1bcNEW".to_vec());
    }

    #[test]
    fn replay_more_chunks_than_exist_returns_all() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"X");
        r.cut(b"P1".to_vec());
        r.append(b"Y");
        assert_eq!(r.replay(999), b"XY".to_vec());
    }
}
