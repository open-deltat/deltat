//! Per-database isolation: each tenant owns its own engine, WAL, and background loops.
//!
//! The pgwire database name selects the tenant, and engines are created lazily on first use.
//! Names are sanitized so a tenant can never escape its own data directory.

use std::path::PathBuf;
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use crate::engine::Engine;
use crate::limits::*;
use crate::notify::NotifyHub;
use crate::reaper;

/// Manages per-tenant engines. Each tenant gets its own Engine + WAL + reaper.
/// Tenant = database name from the pgwire connection.
pub struct TenantManager {
    engines: DashMap<String, Arc<Engine>>,
    data_dir: PathBuf,
    compact_threshold: u64,
    gc_retention_ms: i64,
}

impl TenantManager {
    pub fn new(data_dir: PathBuf, compact_threshold: u64, gc_retention_ms: i64) -> Self {
        Self {
            engines: DashMap::new(),
            data_dir,
            compact_threshold,
            gc_retention_ms,
        }
    }

    /// Get or lazily create an engine for the given tenant.
    pub fn get_or_create(&self, tenant: &str) -> std::io::Result<Arc<Engine>> {
        if tenant.len() > MAX_TENANT_NAME_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "tenant name too long",
            ));
        }

        // Sanitize FIRST, then key the map by the sanitized name. The WAL path is derived from
        // safe_name, so raw names that differ only in stripped punctuation ("prod", "prod!",
        // "pr.od") must resolve to ONE engine on ONE WAL file. Keying by the raw name instead let
        // distinct names collide onto one WAL: separate Engines replaying+appending the same file
        // (cross-tenant exposure) and the compactor's tmp-rename unlinking each other's inode.
        let safe_name = Self::sanitize(tenant)?;

        // Hot path: an already-created tenant needs only a shard read lock.
        if let Some(engine) = self.engines.get(&safe_name) {
            return Ok(engine.value().clone());
        }

        // Bound the tenant count before taking the entry's shard write lock below: DashMap::len
        // walks every shard, so calling it while holding an entry guard would deadlock.
        if self.engines.len() >= MAX_TENANTS {
            return Err(std::io::Error::other("too many tenants"));
        }

        // entry() serializes creation per key so Engine construction + background-task spawn happen
        // exactly once. Two racing first-connections would otherwise each build an Engine on the
        // same WAL path and each spawn a reaper/compactor/GC set, and the loser's tasks would leak
        // forever. Engine::new is synchronous, so it runs inside the entry guard directly.
        match self.engines.entry(safe_name.clone()) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let wal_path = self.data_dir.join(format!("{safe_name}.wal"));
                let notify = Arc::new(NotifyHub::new());
                let engine = Arc::new(Engine::new(wal_path, notify)?);

                // Spawn reaper + compactor + GC for this tenant
                let reaper_engine = engine.clone();
                tokio::spawn(async move {
                    reaper::run_reaper(reaper_engine).await;
                });
                let compactor_engine = engine.clone();
                let threshold = self.compact_threshold;
                tokio::spawn(async move {
                    reaper::run_compactor(compactor_engine, threshold).await;
                });
                let gc_engine = engine.clone();
                let retention = self.gc_retention_ms;
                tokio::spawn(async move {
                    reaper::run_gc(gc_engine, retention).await;
                });

                e.insert(engine.clone());
                metrics::gauge!(crate::observability::TENANTS_ACTIVE).set(self.engines.len() as f64);
                Ok(engine)
            }
        }
    }

    /// Strip path-traversal characters, keeping only alphanumerics, `_`, and `-`. A name that is
    /// empty after stripping is rejected so it can never map to a bare `.wal` file.
    fn sanitize(tenant: &str) -> std::io::Result<String> {
        let safe_name: String = tenant
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
            .collect();
        if safe_name.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty tenant name",
            ));
        }
        Ok(safe_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use ulid::Ulid;
    use crate::model::*;

    fn test_data_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("deltat_test_tenant").join(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let dir = test_data_dir("isolation");
        let tm = TenantManager::new(dir, 1000, 604_800_000);

        let eng_a = tm.get_or_create("tenant_a").unwrap();
        let eng_b = tm.get_or_create("tenant_b").unwrap();

        let rid = Ulid::new();

        // Create same resource ID in both tenants
        eng_a.create_resource(rid, None, None, 1, None).await.unwrap();
        eng_b.create_resource(rid, None, None, 1, None).await.unwrap();

        // Add rule in tenant A
        eng_a
            .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
            .await
            .unwrap();

        // Tenant B's resource should have no rules
        let avail_b = eng_b.compute_availability(rid, 0, 10000, None).await.unwrap();
        assert!(avail_b.is_empty()); // no rules → no availability

        // Tenant A should have availability
        let avail_a = eng_a.compute_availability(rid, 0, 10000, None).await.unwrap();
        assert_eq!(avail_a, vec![Span::new(0, 10000)]);
    }

    #[tokio::test]
    async fn tenant_lazy_creation() {
        let dir = test_data_dir("lazy");
        let tm = TenantManager::new(dir.clone(), 1000, 604_800_000);

        // No WAL files should exist yet
        let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
        assert!(entries.is_empty());

        // Create a tenant
        let _eng = tm.get_or_create("my_db").unwrap();

        // WAL file should now exist
        assert!(dir.join("my_db.wal").exists());
    }

    #[tokio::test]
    async fn tenant_same_engine_returned() {
        let dir = test_data_dir("same_eng");
        let tm = TenantManager::new(dir, 1000, 604_800_000);

        let eng1 = tm.get_or_create("foo").unwrap();
        let eng2 = tm.get_or_create("foo").unwrap();

        // Should be the same Arc
        assert!(Arc::ptr_eq(&eng1, &eng2));
    }

    #[tokio::test]
    async fn tenant_names_colliding_after_sanitize_share_one_engine() {
        let dir = test_data_dir("collide");
        let tm = TenantManager::new(dir, 1000, 604_800_000);

        // "prod", "prod!" and "pr.od" all sanitize to "prod". They must resolve to ONE engine on
        // ONE WAL, not distinct engines behind a shared WAL file. Keyed by the raw name (the old
        // behavior) these were three separate Arcs on one "prod.wal".
        let e1 = tm.get_or_create("prod").unwrap();
        let e2 = tm.get_or_create("prod!").unwrap();
        let e3 = tm.get_or_create("pr.od").unwrap();
        assert!(Arc::ptr_eq(&e1, &e2));
        assert!(Arc::ptr_eq(&e1, &e3));

        // Same engine instance => same data: a resource created via one raw name is visible via
        // another that sanitizes to the same key.
        let rid = Ulid::new();
        e1.create_resource(rid, None, None, 1, None).await.unwrap();
        assert!(e2.get_resource(&rid).is_some());
    }

    #[tokio::test]
    async fn tenant_name_sanitized() {
        let dir = test_data_dir("sanitize");
        let tm = TenantManager::new(dir.clone(), 1000, 604_800_000);

        // Path traversal attempt
        let _eng = tm.get_or_create("../evil").unwrap();
        // Should create "evil.wal", not "../evil.wal"
        assert!(dir.join("evil.wal").exists());

        // Empty after sanitization
        let result = tm.get_or_create("../..");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tenant_name_too_long() {
        let dir = test_data_dir("name_too_long");
        let tm = TenantManager::new(dir, 1000, 604_800_000);

        let long_name = "x".repeat(MAX_TENANT_NAME_LEN + 1);
        let result = tm.get_or_create(&long_name);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("tenant name too long"));
    }

    #[tokio::test]
    async fn tenant_name_at_limit() {
        let dir = test_data_dir("name_at_limit");
        let tm = TenantManager::new(dir, 1000, 604_800_000);

        // Use a name just under the OS filename limit (256 - ".wal" = 252 is safe)
        // but at our MAX_TENANT_NAME_LEN (256). Since the WAL appends ".wal" (4 chars),
        // the actual test verifies the *length check passes*, then the name gets
        // used as a WAL filename. Use a shorter name that still hits the boundary.
        let name = "x".repeat(MAX_TENANT_NAME_LEN);
        // The length check itself should pass
        assert!(name.len() <= MAX_TENANT_NAME_LEN);
        // But creating the WAL file may fail on OS filename limits, so just verify
        // the length check doesn't reject it, the actual io::Error would be from
        // the OS, not our limit.
        let result = tm.get_or_create(&name);
        // Either succeeds or fails with an OS error (not our "tenant name too long" error)
        if let Err(ref e) = result {
            assert!(!e.to_string().contains("tenant name too long"));
        }
    }

    #[tokio::test]
    async fn tenant_count_limit() {
        let dir = test_data_dir("count_limit");
        let tm = TenantManager::new(dir, 1000, 604_800_000);

        for i in 0..MAX_TENANTS {
            tm.get_or_create(&format!("t{i}")).unwrap();
        }
        let result = tm.get_or_create("one_more");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("too many tenants"));
    }
}
