//! The append-only write-ahead log: one `[len][bincode payload][crc32]` record per event.
//!
//! Every mutation is durable here before it touches memory, and the engine rebuilds its state
//! by replaying the log. A torn or corrupt record at the tail is a normal crash artifact:
//! replay stops there, and `open` truncates back to the last good record boundary so new
//! appends land on valid ground instead of behind bytes replay will never read past. A bad
//! record FOLLOWED by more data is mid-log corruption and a hard error, because silently
//! stopping there would drop acknowledged records.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::model::Event;

/// Upper bound on a single WAL record's payload. A serialized `Event` is at most a few kilobytes
/// (a label is capped at MAX_LABEL_LEN), so anything larger is a corrupt length prefix; replay
/// treats it as corruption instead of allocating up to 4 GiB from an untrusted u32.
const MAX_WAL_RECORD_BYTES: usize = 1 << 20; // 1 MiB

/// Bytes of framing around a payload: the u32 length prefix plus the u32 CRC suffix.
const RECORD_FRAMING_BYTES: usize = 8;

fn mid_log_corruption(offset: u64, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "WAL corrupt at byte {offset}: {what}, but valid data follows; \
             refusing to silently drop acknowledged records"
        ),
    )
}

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
/// - Truncated last entry (crash) is discarded on replay and cut off by `open`.
pub struct Wal {
    writer: BufWriter<File>,
    path: PathBuf,
    appends_since_compact: u64,
    /// Set when a flush failed and `recover` could not re-establish a clean tail. Every append
    /// errors while poisoned (each failed flush retries recovery), so nothing is ever
    /// acknowledged behind bytes replay will stop at.
    poisoned: bool,
}

impl Wal {
    /// Open (or create) the WAL file at `path`, truncating any torn or corrupt tail left by a
    /// crash back to the last good record boundary. Without the truncation every subsequent
    /// append would land AFTER the bad bytes and vanish on the next replay, which stops there.
    /// Errors on mid-log corruption, exactly like `replay`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let (_, valid_len) = Self::scan(path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        if file.metadata()?.len() > valid_len {
            file.set_len(valid_len)?;
            file.sync_all()?;
        }
        Ok(Self {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
            appends_since_compact: 0,
            poisoned: false,
        })
    }

    /// Reset after a failed flush: the file tail may hold a torn record, and the buffer may hold
    /// bytes whose callers were already told they are lost. Discard the buffer without flushing
    /// it and reopen, truncating back to the last good record boundary, so the next append is
    /// again durable-then-visible. On failure the WAL stays poisoned (see `poisoned`).
    pub fn recover(&mut self) -> io::Result<()> {
        match Self::open(&self.path) {
            Ok(fresh) => {
                let stale = std::mem::replace(&mut self.writer, fresh.writer);
                // into_parts discards the stale writer WITHOUT flushing; a plain drop would
                // flush its buffered bytes onto the freshly truncated tail.
                let _ = stale.into_parts();
                self.poisoned = false;
                Ok(())
            }
            Err(e) => {
                self.poisoned = true;
                Err(e)
            }
        }
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
        if self.poisoned {
            return Err(io::Error::other("WAL poisoned by an earlier flush failure"));
        }
        encode_event(&mut self.writer, event)?;
        self.appends_since_compact += 1;
        Ok(())
    }

    /// Flush the BufWriter and fsync the underlying file.
    pub fn flush_sync(&mut self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other("WAL poisoned by an earlier flush failure"));
        }
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
        // POSIX makes the rename durable only once the directory itself is synced; without this a
        // power loss can resurrect the pre-compaction inode, losing every record acked to the new
        // file since the swap while replaying stale state.
        let dir = self.path.parent().filter(|p| !p.as_os_str().is_empty());
        File::open(dir.unwrap_or(Path::new(".")))?.sync_all()?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.appends_since_compact = 0;
        // The rewrite replaced whatever tail a failed flush left behind, so the WAL is clean
        // again. (The old writer's fd points at the unlinked pre-rename inode; its drop-flush
        // goes nowhere visible.)
        self.poisoned = false;
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
    /// A truncated/corrupt TAIL entry is silently discarded (crash artifact); a bad record
    /// followed by more data is mid-log corruption and errors instead of silently dropping
    /// everything after it.
    pub fn replay(path: &Path) -> io::Result<Vec<Event>> {
        Ok(Self::scan(path)?.0)
    }

    /// Walk the log, returning the events of the valid prefix and its byte length (the boundary
    /// `open` truncates to). Shared by `replay` and `open` so both agree on where valid data ends.
    fn scan(path: &Path) -> io::Result<(Vec<Event>, u64)> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
            Err(e) => return Err(e),
        };
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut events = Vec::new();
        let mut valid_len: u64 = 0;

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // clean EOF or torn tail
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            // Reject an implausible length before allocating, so a corrupt prefix cannot drive a
            // multi-gigabyte allocation ahead of the CRC check. The record's extent is unknowable,
            // so classify by size: a single torn append leaves at most one record's worth of bytes
            // past the last good boundary; anything larger cannot be a tail tear.
            if len > MAX_WAL_RECORD_BYTES {
                let remaining = file_len - valid_len;
                if remaining <= (RECORD_FRAMING_BYTES + MAX_WAL_RECORD_BYTES) as u64 {
                    break;
                }
                return Err(mid_log_corruption(valid_len, "implausible record length"));
            }

            let mut payload = vec![0u8; len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // torn tail
                Err(e) => return Err(e),
            }

            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // torn tail
                Err(e) => return Err(e),
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&payload);

            // This record's bytes were all present; whether a defect in it is a tolerable crash
            // artifact depends on whether it is the last thing in the file.
            let record_end = valid_len + (RECORD_FRAMING_BYTES + len) as u64;
            let at_tail = record_end == file_len;

            if stored_crc != computed_crc {
                if at_tail {
                    break; // torn tail write: the payload's sectors never all made it
                }
                return Err(mid_log_corruption(valid_len, "record fails its CRC"));
            }

            match bincode::deserialize::<Event>(&payload) {
                Ok(event) if event.spans_valid() => events.push(event),
                // A CRC-valid record whose span violates start < end is a crafted or corrupt
                // entry; bincode would otherwise admit it past Span::new's invariant.
                Ok(_) if at_tail => break,
                Ok(_) => return Err(mid_log_corruption(valid_len, "record has an invalid span")),
                Err(_) if at_tail => break, // corrupt payload
                Err(_) => return Err(mid_log_corruption(valid_len, "record fails to decode")),
            }
            valid_len = record_end;
        }

        Ok((events, valid_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("deltat_test_wal_{}", std::process::id()));
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

    /// Write one raw `[len][payload][crc]` record, with an optional CRC override to corrupt it.
    fn write_raw_record(f: &mut impl Write, event: &Event, crc_override: Option<u32>) {
        let payload = bincode::serialize(event).unwrap();
        let len = payload.len() as u32;
        let crc = crc_override.unwrap_or_else(|| crc32fast::hash(&payload));
        f.write_all(&len.to_le_bytes()).unwrap();
        f.write_all(&payload).unwrap();
        f.write_all(&crc.to_le_bytes()).unwrap();
    }

    fn create_event() -> Event {
        Event::ResourceCreated {
            id: Ulid::new(),
            parent_id: None,
            name: None,
            capacity: 1,
            buffer_after: None,
        }
    }

    #[test]
    fn open_truncates_torn_tail_so_later_appends_survive_replay() {
        // The broken cycle: crash leaves a partial record at the tail, restart appends past it,
        // and every later replay stops at the tear, losing the acknowledged appends. Open must
        // truncate back to the last good record boundary so new appends land on valid ground.
        let path = tmp_path("torn_tail_reopen.wal");
        let _ = fs::remove_file(&path);

        let first = create_event();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&first).unwrap();
        }
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[7u8; 6]).unwrap(); // partial record: torn mid-flush crash
        }

        let second = create_event();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&second).unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed, vec![first, second]);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn open_truncates_crc_corrupt_tail_record() {
        // A fully written record whose payload bytes were torn (CRC fails) at end-of-file is the
        // same crash artifact as a partial record: truncate it and keep appending.
        let path = tmp_path("crc_tail_reopen.wal");
        let _ = fs::remove_file(&path);

        let first = create_event();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&first).unwrap();
        }
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            write_raw_record(&mut f, &create_event(), Some(0xDEAD_BEEF));
        }

        let second = create_event();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&second).unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed, vec![first, second]);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn recover_discards_buffered_bytes_and_truncates_the_tear() {
        // The no-crash poisoning path: a flush fails mid-write, leaving torn bytes on disk and
        // unflushed bytes in the buffer whose callers were already told they are lost. Recover
        // must discard both so the next acknowledged append survives replay.
        let path = tmp_path("recover_flush_failure.wal");
        let _ = fs::remove_file(&path);

        let first = create_event();
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&first).unwrap();

        // Torn bytes reached the file behind the writer (the failed flush's partial write)...
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[7u8; 6]).unwrap();
        }
        // ...and the failed batch's records are still sitting in the buffer.
        wal.append_buffered(&create_event()).unwrap();

        wal.recover().unwrap();

        let second = create_event();
        wal.append(&second).unwrap();
        drop(wal);

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed, vec![first, second]);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_errors_on_mid_log_corruption() {
        // A bad record FOLLOWED by valid data is not a crash artifact: silently stopping there
        // would drop acknowledged records that replayed fine yesterday. Hard error instead.
        let path = tmp_path("mid_log_corruption.wal");
        let _ = fs::remove_file(&path);

        {
            let mut f = File::create(&path).unwrap();
            write_raw_record(&mut f, &create_event(), None);
            write_raw_record(&mut f, &create_event(), Some(0xDEAD_BEEF)); // corrupt middle
            write_raw_record(&mut f, &create_event(), None); // valid data after it
        }

        let err = Wal::replay(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // Open must refuse too: appending past mid-log corruption is never safe.
        assert!(Wal::open(&path).is_err());

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

    #[test]
    fn tmp_path_dir_is_namespaced_per_process() {
        // Two cargo test processes sharing one dir delete and replay each other's files.
        let path = tmp_path("pid_probe.wal");
        let dir = path.parent().unwrap().file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            dir.contains(&std::process::id().to_string()),
            "wal test dir {dir} is shared across processes"
        );
    }
}
