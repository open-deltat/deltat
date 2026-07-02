use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::model::Event;

/// Upper bound on a single WAL record's payload. A serialized `Event` is at most a few kilobytes
/// (a label is capped at MAX_LABEL_LEN), so anything larger is a corrupt length prefix; replay
/// treats it as corruption instead of allocating up to 4 GiB from an untrusted u32.
const MAX_WAL_RECORD_BYTES: usize = 1 << 20; // 1 MiB

/// Encode a single event to [len][bincode][crc32] format.
fn encode_event(writer: &mut impl Write, event: &Event) -> io::Result<()> {
    let payload =
        bincode::serialize(event).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = payload.len() as u32;
    let crc = crc32fast::hash(&payload);
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.write_all(&crc.to_le_bytes())?;
    Ok(())
}

/// Append-only Write-Ahead Log.
///
/// Format per entry: `[u32: len][bincode: Event][u32: crc32]`
/// - `len` is the byte length of the bincode payload (not including the CRC).
/// - Truncated last entry (crash) is safely discarded via length-prefix + CRC check.
pub struct Wal {
    writer: BufWriter<File>,
    path: PathBuf,
    appends_since_compact: u64,
}

impl Wal {
    /// Open (or create) the WAL file at `path`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
            appends_since_compact: 0,
        })
    }

    /// Append a single event to the WAL and fsync. Used by tests only.
    /// Production code uses `append_buffered` + `flush_sync` for group commit.
    #[cfg(test)]
    pub fn append(&mut self, event: &Event) -> io::Result<()> {
        self.append_buffered(event)?;
        self.flush_sync()
    }

    /// Append a single event to the BufWriter without flushing or syncing.
    /// Call `flush_sync()` after the batch to durably commit all buffered events.
    pub fn append_buffered(&mut self, event: &Event) -> io::Result<()> {
        encode_event(&mut self.writer, event)?;
        self.appends_since_compact += 1;
        Ok(())
    }

    /// Flush the BufWriter and fsync the underlying file.
    pub fn flush_sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Return the WAL file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write compacted events to a temp file and fsync.
    /// This is the slow I/O phase. Call OUTSIDE the WAL lock.
    pub fn write_compact_file(path: &Path, events: &[Event]) -> io::Result<()> {
        let tmp_path = path.with_extension("wal.tmp");
        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        for event in events {
            encode_event(&mut writer, event)?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Atomic swap: rename temp file over the WAL and reopen.
    /// This is fast. Call while holding the WAL lock.
    pub fn swap_compact_file(&mut self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("wal.tmp");
        fs::rename(&tmp_path, &self.path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.appends_since_compact = 0;
        Ok(())
    }

    /// Replace the WAL with a minimal set of events that recreates the current state.
    /// Convenience method that does both phases. Used by tests.
    #[cfg(test)]
    pub fn compact(&mut self, events: &[Event]) -> io::Result<()> {
        Self::write_compact_file(&self.path, events)?;
        self.swap_compact_file()
    }

    pub fn appends_since_compact(&self) -> u64 {
        self.appends_since_compact
    }

    /// Replay the WAL from disk, returning all valid events.
    /// Truncated/corrupt trailing entries are silently discarded.
    pub fn replay(path: &Path) -> io::Result<Vec<Event>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut events = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            // Reject an implausible length before allocating, so a corrupt prefix cannot drive a
            // multi-gigabyte allocation ahead of the CRC check. Trailing corruption ends replay.
            if len > MAX_WAL_RECORD_BYTES {
                break;
            }

            let mut payload = vec![0u8; len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // truncated
                Err(e) => return Err(e),
            }

            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // truncated
                Err(e) => return Err(e),
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&payload);

            if stored_crc != computed_crc {
                // Corrupt entry, stop replaying
                break;
            }

            match bincode::deserialize::<Event>(&payload) {
                Ok(event) if event.spans_valid() => events.push(event),
                // A CRC-valid record whose span violates start < end is a crafted or corrupt
                // entry; bincode would otherwise admit it past Span::new's invariant. Stop here.
                Ok(_) => break,
                Err(_) => break, // corrupt payload
            }
        }

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("deltat_test_wal");
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn append_and_replay() {
        let path = tmp_path("append_and_replay.wal");
        let _ = fs::remove_file(&path);

        let events = vec![
            Event::ResourceCreated {
                id: Ulid::new(),
                parent_id: None,
                name: None,
                capacity: 1,
                buffer_after: None,
            },
            Event::RuleAdded {
                id: Ulid::new(),
                resource_id: Ulid::new(),
                span: crate::model::Span::new(1000, 2000),
                blocking: false,
            },
        ];

        {
            let mut wal = Wal::open(&path).unwrap();
            for e in &events {
                wal.append(e).unwrap();
            }
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed, events);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_handles_truncation() {
        let path = tmp_path("truncation.wal");
        let _ = fs::remove_file(&path);

        let event = Event::ResourceCreated {
            id: Ulid::new(),
            parent_id: None,
            name: None,
            capacity: 1,
            buffer_after: None,
        };

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&event).unwrap();
        }

        // Append garbage to simulate a truncated second entry
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0u8; 6]).unwrap(); // partial length + some bytes
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0], event);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_nonexistent_file() {
        let path = tmp_path("nonexistent.wal");
        let _ = fs::remove_file(&path);
        let replayed = Wal::replay(&path).unwrap();
        assert!(replayed.is_empty());
    }

    #[test]
    fn replay_corrupt_crc() {
        let path = tmp_path("corrupt_crc.wal");
        let _ = fs::remove_file(&path);

        let event = Event::ResourceDeleted { id: Ulid::new() };

        // Manually write an entry with bad CRC
        {
            let payload = bincode::serialize(&event).unwrap();
            let len = payload.len() as u32;
            let bad_crc: u32 = 0xDEADBEEF;

            let mut f = File::create(&path).unwrap();
            f.write_all(&len.to_le_bytes()).unwrap();
            f.write_all(&payload).unwrap();
            f.write_all(&bad_crc.to_le_bytes()).unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert!(replayed.is_empty());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_rejects_oversized_length_prefix() {
        let path = tmp_path("oversized_len.wal");
        let _ = fs::remove_file(&path);

        // A corrupt length prefix far larger than any real record must be rejected before any
        // allocation, not drive a multi-gigabyte vec.
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&u32::MAX.to_le_bytes()).unwrap();
            f.write_all(b"trailing").unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert!(replayed.is_empty());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_rejects_inverted_span_record() {
        let path = tmp_path("inverted_span.wal");
        let _ = fs::remove_file(&path);

        // A CRC-valid record whose span violates start < end (the engine never writes this, but a
        // crafted or corrupt WAL could): replay must reject it rather than admit an inverted span.
        let event = Event::BookingConfirmed {
            id: Ulid::new(),
            resource_id: Ulid::new(),
            span: crate::model::Span { start: 2000, end: 1000 },
            label: None,
        };
        {
            let payload = bincode::serialize(&event).unwrap();
            let len = payload.len() as u32;
            let crc = crc32fast::hash(&payload);
            let mut f = File::create(&path).unwrap();
            f.write_all(&len.to_le_bytes()).unwrap();
            f.write_all(&payload).unwrap();
            f.write_all(&crc.to_le_bytes()).unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert!(replayed.is_empty());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn compact_reduces_wal() {
        let path = tmp_path("compact_reduce.wal");
        let _ = fs::remove_file(&path);

        let rid = Ulid::new();
        let rule_id = Ulid::new();

        // Write many events: create, add rule, remove rule, add again
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&Event::ResourceCreated {
                id: rid,
                parent_id: None,
                name: Some("Room".into()),
                capacity: 1,
                buffer_after: None,
            }).unwrap();
            wal.append(&Event::RuleAdded {
                id: rule_id,
                resource_id: rid,
                span: crate::model::Span::new(0, 1000),
                blocking: false,
            }).unwrap();
            wal.append(&Event::RuleRemoved { id: rule_id, resource_id: rid }).unwrap();
            // 10 more churn events
            for _ in 0..10 {
                let tmp_id = Ulid::new();
                wal.append(&Event::RuleAdded {
                    id: tmp_id,
                    resource_id: rid,
                    span: crate::model::Span::new(0, 500),
                    blocking: false,
                }).unwrap();
                wal.append(&Event::RuleRemoved { id: tmp_id, resource_id: rid }).unwrap();
            }
        }

        let before = fs::metadata(&path).unwrap().len();
        assert!(before > 0);

        // Compact: final state is just the resource (no rules)
        let compacted_events = vec![Event::ResourceCreated {
            id: rid,
            parent_id: None,
            name: Some("Room".into()),
            capacity: 1,
            buffer_after: None,
        }];

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.compact(&compacted_events).unwrap();
        }

        let after = fs::metadata(&path).unwrap().len();
        assert!(after < before, "compacted WAL should be smaller: {after} < {before}");

        // Replay should produce just the one event
        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed, compacted_events);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn compact_then_append() {
        let path = tmp_path("compact_append.wal");
        let _ = fs::remove_file(&path);

        let rid = Ulid::new();
        let compacted = vec![Event::ResourceCreated {
            id: rid,
            parent_id: None,
            name: None,
            capacity: 1,
            buffer_after: None,
        }];

        let new_event = Event::RuleAdded {
            id: Ulid::new(),
            resource_id: rid,
            span: crate::model::Span::new(1000, 2000),
            blocking: false,
        };

        {
            let mut wal = Wal::open(&path).unwrap();
            // Seed some data
            wal.append(&compacted[0]).unwrap();
            // Compact
            wal.compact(&compacted).unwrap();
            // Append new event after compaction
            wal.append(&new_event).unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0], compacted[0]);
        assert_eq!(replayed[1], new_event);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn append_buffered_then_flush_sync() {
        let path = tmp_path("buffered_flush.wal");
        let _ = fs::remove_file(&path);

        let events: Vec<Event> = (0..5)
            .map(|_| Event::ResourceCreated {
                id: Ulid::new(),
                parent_id: None,
                name: None,
                capacity: 1,
                buffer_after: None,
            })
            .collect();

        {
            let mut wal = Wal::open(&path).unwrap();
            for e in &events {
                wal.append_buffered(e).unwrap();
            }
            assert_eq!(wal.appends_since_compact(), 5);
            wal.flush_sync().unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed, events);

        let _ = fs::remove_file(&path);
    }
}
