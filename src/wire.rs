//! The pgwire server: the transport that runs commands and streams results back.
//!
//! It drives the startup handshake, the simple and extended query protocols, and LISTEN/NOTIFY
//! forwarding. A parsed [`crate::command::Command`] becomes engine calls whose rows it encodes,
//! and this is where a connection's lifetime and subscription limits are enforced.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;
use futures::{Sink, SinkExt, StreamExt};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopHandler, PgWireConnectionState, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::NotificationResponse;
use pgwire::messages::PgWireBackendMessage;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use ulid::Ulid;

use crate::auth::{DeltaTAuthSource, DeltaTStartupHandler};
use crate::engine::Engine;
use crate::limits::{MAX_PARAMS, MAX_QUERY_LEN, MAX_SUBSCRIPTIONS_PER_CONNECTION};
use crate::model::*;
use crate::command::Command;
use crate::sql;
use crate::tenant::TenantManager;

// ── Subscription plumbing ────────────────────────────────────────

pub enum SubscriptionCommand {
    Subscribe(Ulid),
    Unsubscribe(Ulid),
    UnsubscribeAll,
}

pub struct DeltaTHandler {
    tenant_manager: Arc<TenantManager>,
    query_parser: Arc<DeltaTQueryParser>,
    subscribe_tx: Option<mpsc::UnboundedSender<SubscriptionCommand>>,
    slow_query_threshold_ms: u64,
}

impl DeltaTHandler {
    #[allow(dead_code)]
    pub fn new(tenant_manager: Arc<TenantManager>) -> Self {
        Self {
            tenant_manager,
            query_parser: Arc::new(DeltaTQueryParser),
            subscribe_tx: None,
            slow_query_threshold_ms: 0,
        }
    }

    pub fn with_subscriptions(
        tenant_manager: Arc<TenantManager>,
        subscribe_tx: mpsc::UnboundedSender<SubscriptionCommand>,
    ) -> Self {
        Self {
            tenant_manager,
            query_parser: Arc::new(DeltaTQueryParser),
            subscribe_tx: Some(subscribe_tx),
            slow_query_threshold_ms: 0,
        }
    }

    pub fn with_slow_query_threshold(mut self, threshold_ms: u64) -> Self {
        self.slow_query_threshold_ms = threshold_ms;
        self
    }

    /// The tenant is the pgwire database name; each tenant gets its own engine and WAL.
    fn tenant_name<C: ClientInfo>(client: &C) -> String {
        client
            .metadata()
            .get("database")
            .cloned()
            .unwrap_or_else(|| "default".to_string())
    }

    /// Resolves the client's engine and the tenant label every per-query metric carries.
    /// The label is the sanitized name engines are keyed on, not the raw database name:
    /// raw aliases that differ only in stripped punctuation ("acme!", "a.c.m.e") all reach
    /// tenant "acme"'s engine, so labeling with the raw name would mint a fresh series per
    /// alias and the exporter's cardinality would no longer be bounded by MAX_TENANTS.
    fn resolve_engine<C: ClientInfo>(&self, client: &C) -> PgWireResult<(Arc<Engine>, String)> {
        let db = Self::tenant_name(client);
        let tenant = TenantManager::sanitize(&db).map_err(tenant_err)?;
        let engine = self.tenant_manager.get_or_create(&db).map_err(tenant_err)?;
        Ok((engine, tenant))
    }

    fn parse_channel_resource_id(channel: &str) -> PgWireResult<Ulid> {
        let resource_id_str = channel.strip_prefix("resource_").ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "42000".into(),
                format!("invalid channel: {channel} (expected resource_{{id}})"),
            )))
        })?;
        Ulid::from_string(resource_id_str).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "42000".into(),
                format!("bad ULID in channel: {e}"),
            )))
        })
    }

    /// The single statement funnel for both protocols: length check, parse, execute, metrics.
    ///
    /// The clock opens before the parse so a statement's recorded latency includes parsing, and
    /// a statement rejected before execution still lands in PARSE_ERRORS_TOTAL: a rejection
    /// that appears in no metric goes unnoticed.
    async fn run_statement(
        &self,
        engine: &Engine,
        tenant: &str,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let start = std::time::Instant::now();
        let parsed = enforce_query_len(sql.len()).and_then(|()| sql::parse_sql(sql).map_err(sql_err));
        let cmd = match parsed {
            Ok(cmd) => cmd,
            Err(e) => {
                metrics::counter!(crate::observability::PARSE_ERRORS_TOTAL).increment(1);
                return Err(e);
            }
        };
        self.execute_command(engine, tenant, cmd, start).await
    }

    /// `start` is the caller's clock, opened before parsing, so the recorded duration covers
    /// the whole statement rather than just engine time.
    async fn execute_command(
        &self,
        engine: &Engine,
        tenant: &str,
        cmd: Command,
        start: std::time::Instant,
    ) -> PgWireResult<Vec<Response>> {
        let label = crate::observability::command_label(&cmd);
        let result = self.execute_command_inner(engine, cmd).await;
        let elapsed = start.elapsed();
        // "kind" separates the expected steady state (conflicts) from real failures. Values stay
        // bounded: the EngineError kinds plus "none" (success) and "other" (non-engine errors).
        let (status, kind) = match &result {
            Ok(_) => ("ok", "none"),
            Err(WireError::Engine(e)) => ("error", e.kind()),
            Err(WireError::Pg(_)) => ("error", "other"),
        };
        metrics::counter!(crate::observability::QUERIES_TOTAL, "command" => label, "status" => status, "kind" => kind, "tenant" => tenant.to_string())
            .increment(1);
        metrics::histogram!(crate::observability::QUERY_DURATION_SECONDS, "command" => label, "tenant" => tenant.to_string())
            .record(elapsed.as_secs_f64());
        record_slow_query(label, tenant, elapsed, self.slow_query_threshold_ms);
        result.map_err(PgWireError::from)
    }

    async fn execute_command_inner(
        &self,
        engine: &Engine,
        cmd: Command,
    ) -> Result<Vec<Response>, WireError> {
        match cmd {
            Command::InsertResource {
                id,
                parent_id,
                name,
                capacity,
                buffer_after,
            } => {
                engine
                    .create_resource(id, parent_id, name, capacity, buffer_after)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::BatchInsertResources { resources } => {
                let count = resources.len();
                engine.batch_create_resources(resources).await?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(count))])
            }
            Command::DeleteResource { id } => {
                engine.delete_resource(id).await?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::InsertRule {
                id,
                resource_id,
                start,
                end,
                blocking,
            } => {
                let span = Span::try_new(start, end).map_err(span_err)?;
                engine
                    .add_rule(id, resource_id, span, blocking)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::BatchInsertRules { rules } => {
                let count = rules.len();
                let batch: Vec<_> = rules
                    .into_iter()
                    .map(|(id, resource_id, start, end, blocking)| {
                        Span::try_new(start, end)
                            .map(|span| (id, resource_id, span, blocking))
                            .map_err(span_err)
                    })
                    .collect::<PgWireResult<Vec<_>>>()?;
                engine.batch_add_rules(batch).await?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(count))])
            }
            Command::DeleteRule { id } => {
                engine.remove_rule(id).await?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::InsertHold {
                id,
                resource_id,
                start,
                end,
                expires_at,
            } => {
                let span = Span::try_new(start, end).map_err(span_err)?;
                engine
                    .place_hold(id, resource_id, span, expires_at)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::DeleteHold { id } => {
                engine.release_hold(id).await?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::CommitHold { hold_id, booking_id, label } => {
                engine
                    .commit_hold(hold_id, booking_id, label)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("UPDATE").with_rows(1))])
            }
            Command::InsertBooking {
                id,
                resource_id,
                start,
                end,
                label,
            } => {
                let span = Span::try_new(start, end).map_err(span_err)?;
                engine
                    .confirm_booking(id, resource_id, span, label)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(1))])
            }
            Command::BatchInsertBookings { bookings } => {
                let count = bookings.len();
                let batch: Vec<_> = bookings
                    .into_iter()
                    .map(|(id, resource_id, start, end, label)| {
                        Span::try_new(start, end)
                            .map(|span| (id, resource_id, span, label))
                            .map_err(span_err)
                    })
                    .collect::<PgWireResult<Vec<_>>>()?;
                engine
                    .batch_confirm_bookings(batch)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(count))])
            }
            Command::DeleteBooking { id } => {
                engine.cancel_booking(id).await?;
                Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(1))])
            }
            Command::SelectAvailability {
                resource_id,
                start,
                end,
                min_duration,
            } => {
                let slots = engine
                    .compute_availability(resource_id, start, end, min_duration)
                    .await?;

                let schema = Arc::new(availability_schema());

                let rid_str = resource_id.to_string();
                let rows: Vec<PgWireResult<_>> = slots
                    .into_iter()
                    .map(|slot| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&rid_str)?;
                        encoder.encode_field(&slot.start)?;
                        encoder.encode_field(&slot.end)?;
                        Ok(encoder.take_row())
                    })
                    .collect();

                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(rows),
                ))])
            }
            Command::SelectMultiAvailability {
                resource_ids,
                start,
                end,
                min_available,
                min_duration,
            } => {
                let slots = engine
                    .compute_multi_availability(&resource_ids, start, end, min_available, min_duration)
                    .await?;

                let schema = Arc::new(multi_availability_schema());

                let rows: Vec<PgWireResult<_>> = slots
                    .into_iter()
                    .map(|slot| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&slot.start)?;
                        encoder.encode_field(&slot.end)?;
                        Ok(encoder.take_row())
                    })
                    .collect();

                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(rows),
                ))])
            }
            Command::SelectAvailabilityMulti {
                resource_ids,
                start,
                end,
                min_duration,
            } => {
                let slots = engine
                    .get_availability_multi(&resource_ids, start, end, min_duration)
                    .await?;

                // Per-resource rows reuse the single-availability schema (resource_id, start, end).
                let schema = Arc::new(availability_schema());

                let rows: Vec<PgWireResult<_>> = slots
                    .into_iter()
                    .map(|(rid, slot)| {
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        encoder.encode_field(&rid.to_string())?;
                        encoder.encode_field(&slot.start)?;
                        encoder.encode_field(&slot.end)?;
                        Ok(encoder.take_row())
                    })
                    .collect();

                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(rows),
                ))])
            }
            Command::UpdateResource { id, name, capacity, buffer_after } => {
                engine
                    .update_resource(id, name, capacity, buffer_after)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("UPDATE").with_rows(1))])
            }
            Command::UpdateRule { id, start, end, blocking } => {
                let span = Span::try_new(start, end).map_err(span_err)?;
                engine
                    .update_rule(id, span, blocking)
                    .await?;
                Ok(vec![Response::Execution(Tag::new("UPDATE").with_rows(1))])
            }
            Command::SelectResources { parent_id } => {
                let all = engine.list_resources().await;
                let filtered: Vec<_> = match parent_id {
                    None => all,
                    Some(None) => all.into_iter().filter(|r| r.parent_id.is_none()).collect(),
                    Some(Some(pid)) => all.into_iter().filter(|r| r.parent_id == Some(pid)).collect(),
                };

                Ok(encode_rows(Arc::new(resources_schema()), filtered, |e, r| {
                    e.encode_field(&r.id.to_string())?;
                    e.encode_field(&r.parent_id.map(|p| p.to_string()))?;
                    e.encode_field(&r.name)?;
                    e.encode_field(&(r.capacity as i64))?;
                    e.encode_field(&r.buffer_after)
                }))
            }
            Command::SelectRules { resource_id } => {
                let rules = engine.get_rules(resource_id).await?;
                Ok(encode_rows(Arc::new(rules_schema()), rules, |e, r| {
                    e.encode_field(&r.id.to_string())?;
                    e.encode_field(&r.resource_id.to_string())?;
                    e.encode_field(&r.start)?;
                    e.encode_field(&r.end)?;
                    e.encode_field(&r.blocking)
                }))
            }
            Command::SelectBookings { resource_id } => {
                let bookings = engine.get_bookings(resource_id).await?;
                Ok(encode_rows(Arc::new(bookings_schema()), bookings, |e, b| {
                    e.encode_field(&b.id.to_string())?;
                    e.encode_field(&b.resource_id.to_string())?;
                    e.encode_field(&b.start)?;
                    e.encode_field(&b.end)?;
                    e.encode_field(&b.label)
                }))
            }
            Command::SelectHolds { resource_id } => {
                let holds = engine.get_holds(resource_id).await?;
                Ok(encode_rows(Arc::new(holds_schema()), holds, |e, h| {
                    e.encode_field(&h.id.to_string())?;
                    e.encode_field(&h.resource_id.to_string())?;
                    e.encode_field(&h.start)?;
                    e.encode_field(&h.end)?;
                    e.encode_field(&h.expires_at)
                }))
            }
            Command::SelectBookingsMulti { resource_ids } => {
                let bookings = engine.get_bookings_multi(&resource_ids).await?;
                Ok(encode_rows(Arc::new(bookings_schema()), bookings, |e, b| {
                    e.encode_field(&b.id.to_string())?;
                    e.encode_field(&b.resource_id.to_string())?;
                    e.encode_field(&b.start)?;
                    e.encode_field(&b.end)?;
                    e.encode_field(&b.label)
                }))
            }
            Command::SelectHoldsMulti { resource_ids } => {
                let holds = engine.get_holds_multi(&resource_ids).await?;
                Ok(encode_rows(Arc::new(holds_schema()), holds, |e, h| {
                    e.encode_field(&h.id.to_string())?;
                    e.encode_field(&h.resource_id.to_string())?;
                    e.encode_field(&h.start)?;
                    e.encode_field(&h.end)?;
                    e.encode_field(&h.expires_at)
                }))
            }
            Command::Listen { channel } => {
                let resource_id = Self::parse_channel_resource_id(&channel)?;
                // Reject a LISTEN on a resource that does not exist. Subscribing would create a
                // NotifyHub broadcast channel (a 256-slot ring) that is reclaimed only on
                // delete_resource, so reconnect-and-LISTEN cycles on bogus ids would grow tenant
                // memory without bound. This is the sole origin of Subscribe commands, so gating
                // here keeps the forwarder path free of phantom subscriptions.
                if engine.get_resource(&resource_id).is_none() {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".into(),
                        "42704".into(),
                        format!("resource does not exist: {resource_id}"),
                    )))
                    .into());
                }
                if let Some(ref tx) = self.subscribe_tx {
                    let _ = tx.send(SubscriptionCommand::Subscribe(resource_id));
                }
                Ok(vec![Response::Execution(Tag::new("LISTEN"))])
            }
            Command::Unlisten { channel } => {
                let resource_id = Self::parse_channel_resource_id(&channel)?;
                if let Some(ref tx) = self.subscribe_tx {
                    let _ = tx.send(SubscriptionCommand::Unsubscribe(resource_id));
                }
                Ok(vec![Response::Execution(Tag::new("UNLISTEN"))])
            }
            Command::UnlistenAll => {
                if let Some(ref tx) = self.subscribe_tx {
                    let _ = tx.send(SubscriptionCommand::UnsubscribeAll);
                }
                Ok(vec![Response::Execution(Tag::new("UNLISTEN"))])
            }
        }
    }
}

/// Encode a homogeneous row set into one `QueryResponse`, applying `encode` per item.
/// The Select* read commands differ only in schema and per-row fields, so they share this.
fn encode_rows<T>(
    schema: Arc<Vec<FieldInfo>>,
    items: Vec<T>,
    encode: impl Fn(&mut DataRowEncoder, &T) -> PgWireResult<()>,
) -> Vec<Response> {
    let rows: Vec<PgWireResult<_>> = items
        .into_iter()
        .map(|item| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            encode(&mut encoder, &item)?;
            Ok(encoder.take_row())
        })
        .collect();
    vec![Response::Query(QueryResponse::new(schema, stream::iter(rows)))]
}

fn availability_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new(
            "resource_id".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        ),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
    ]
}

fn multi_availability_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
    ]
}

fn resources_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("parent_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("name".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("capacity".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("buffer_after".into(), None, None, Type::INT8, FieldFormat::Text),
    ]
}

fn rules_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("resource_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("blocking".into(), None, None, Type::BOOL, FieldFormat::Text),
    ]
}

fn bookings_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("resource_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("label".into(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]
}

fn holds_schema() -> Vec<FieldInfo> {
    vec![
        FieldInfo::new("id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("resource_id".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("start".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("end".into(), None, None, Type::INT8, FieldFormat::Text),
        FieldInfo::new("expires_at".into(), None, None, Type::INT8, FieldFormat::Text),
    ]
}

#[async_trait]
impl SimpleQueryHandler for DeltaTHandler {
    async fn do_query<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let (engine, tenant) = self.resolve_engine(client)?;
        self.run_statement(&engine, &tenant, query).await
    }
}

/// Reject SQL longer than MAX_QUERY_LEN. Enforced at Parse time (QueryParser::parse_sql) so an
/// oversized statement is rejected before count_params materializes its chars and sqlparser runs a
/// full parse, and again on each Execute path. SQLSTATE 54000 = program_limit_exceeded.
fn enforce_query_len(len: usize) -> PgWireResult<()> {
    if len > MAX_QUERY_LEN {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".into(),
            "54000".into(),
            "query too long".into(),
        ))));
    }
    Ok(())
}

// ── Extended Query Protocol ──────────────────────────────────────

#[derive(Debug)]
pub struct DeltaTQueryParser;

#[async_trait]
impl QueryParser for DeltaTQueryParser {
    type Statement = String;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<String>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        enforce_query_len(sql.len())?;
        // Reject a hostile $N index at Parse time so an over-limit statement is never stored.
        checked_param_count(sql)?;
        Ok(sql.to_string())
    }

    fn get_parameter_types(&self, stmt: &String) -> PgWireResult<Vec<Type>> {
        Ok(vec![Type::VARCHAR; checked_param_count(stmt)?])
    }

    fn get_result_schema(
        &self,
        stmt: &String,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(schema_for_sql(stmt))
    }
}

#[async_trait]
impl ExtendedQueryHandler for DeltaTHandler {
    type Statement = String;
    type QueryParser = DeltaTQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let (engine, tenant) = self.resolve_engine(client)?;
        let sql = substitute_params(portal);
        let mut responses = self.run_statement(&engine, &tenant, &sql).await?;
        Ok(responses.remove(0))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let param_types = vec![Type::VARCHAR; checked_param_count(&target.statement)?];
        Ok(DescribeStatementResponse::new(param_types, schema_for_sql(&target.statement)))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        Ok(DescribePortalResponse::new(schema_for_sql(&target.statement.statement)))
    }
}

/// Parse a `$N` placeholder index: the run of ASCII digits starting at `digit_start` in `chars`.
/// Returns the parsed index (None when the run overflows usize, so an untrusted huge run is never
/// fatal) and the position just past the digits.
fn parse_param_index(chars: &[char], digit_start: usize) -> (Option<usize>, usize) {
    let mut j = digit_start;
    while j < chars.len() && chars[j].is_ascii_digit() {
        j += 1;
    }
    let n = chars[digit_start..j]
        .iter()
        .collect::<String>()
        .parse::<usize>()
        .ok();
    (n, j)
}

/// Count the highest $N placeholder, rejecting indices over MAX_PARAMS so the result can safely
/// size a `vec![Type; N]`. SQLSTATE 54000 = program_limit_exceeded, matching enforce_query_len.
fn checked_param_count(sql: &str) -> PgWireResult<usize> {
    let count = count_params(sql);
    if count > MAX_PARAMS {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".into(),
            "54000".into(),
            format!("parameter index ${count} exceeds the maximum of {MAX_PARAMS}"),
        ))));
    }
    Ok(count)
}

/// Count the highest $N parameter placeholder in the SQL string.
fn count_params(sql: &str) -> usize {
    let chars: Vec<char> = sql.chars().collect();
    let mut max = 0usize;
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit() {
            let (n, j) = parse_param_index(&chars, i + 1);
            if let Some(n) = n
                && n > max
            {
                max = n;
            }
            i = j;
        } else {
            i += 1;
        }
    }
    max
}

/// Substitute $1, $2, ... placeholders with bound parameter values (text format).
fn substitute_params(portal: &Portal<String>) -> String {
    let sql = portal.statement.statement.to_string();
    let params = &portal.parameters;
    if params.is_empty() {
        return sql;
    }

    // Single left-to-right scan: replace each `$N` token in place. A global str::replace per
    // placeholder could clobber a `$N` sequence that appears *inside* an already-substituted
    // value; scanning once and emitting substituted values verbatim cannot. char-based so
    // multibyte UTF-8 in the SQL (or in values) is preserved.
    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit() {
            // Checked parse via the shared helper: a huge out-of-range index from untrusted input
            // becomes None and is left as a literal, never overflowing usize and panicking the task.
            let (n, j) = parse_param_index(&chars, i + 1);
            if let Some(n) = n
                && n >= 1
                && n <= params.len()
            {
                match &params[n - 1] {
                    Some(bytes) => {
                        let text = String::from_utf8_lossy(bytes);
                        result.push('\'');
                        result.push_str(&text.replace('\'', "''"));
                        result.push('\'');
                    }
                    None => result.push_str("NULL"),
                }
                i = j;
                continue;
            }
        }
        result.push(chars[i]);
        i += 1;
    }

    result
}

// ── Custom connection loop with LISTEN/NOTIFY ────────────────────

/// Forward one resource's broadcast events to the connection's notification channel as pgwire
/// `NotificationResponse`s.
///
/// A `Lagged` error means the subscriber briefly fell behind the bounded broadcast ring and lost
/// some events. It must NOT end the subscription. Ending it would let a transient burst silently
/// kill the live stream forever; instead we keep forwarding subsequent events (the listener
/// re-reads authoritative state on the next one; availability is never derived from the stream).
/// Only `Closed` (all senders dropped, e.g. the resource was deleted) ends the forwarder.
async fn forward_resource_events(
    mut rx: tokio::sync::broadcast::Receiver<Event>,
    tx: mpsc::UnboundedSender<NotificationResponse>,
    channel: String,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(event) => {
                let payload = serde_json::to_string(&event).unwrap_or_default();
                if tx
                    .send(NotificationResponse::new(0, channel.clone(), payload))
                    .is_err()
                {
                    break;
                }
            }
            Err(RecvError::Lagged(missed)) => {
                metrics::counter!(crate::observability::NOTIFICATIONS_LAGGED_TOTAL)
                    .increment(missed);
            }
            Err(RecvError::Closed) => break,
        }
    }
}

/// Single-shared-password entry point; delegates to [`process_connection_with_auth`].
pub async fn process_connection(
    tcp_socket: TcpStream,
    tenant_manager: Arc<TenantManager>,
    password: String,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    max_conn_age_ms: u64,
    max_idle_ms: u64,
    slow_query_ms: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    process_connection_with_auth(
        tcp_socket,
        tenant_manager,
        Arc::new(DeltaTAuthSource::new(password)),
        tls_acceptor,
        max_conn_age_ms,
        max_idle_ms,
        slow_query_ms,
    )
    .await
}

pub async fn process_connection_with_auth(
    tcp_socket: TcpStream,
    tenant_manager: Arc<TenantManager>,
    auth_source: Arc<DeltaTAuthSource>,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    // Post-auth lifetime guards in ms (0 = disabled). They bound a client that opens a LISTEN and
    // then squats, the only thing that reclaims a connection slot once the global semaphore is
    // full, so they are the real defense against connection-exhaustion DoS.
    max_conn_age_ms: u64,
    max_idle_ms: u64,
    // Statements at or over this many ms are warned about and counted; 0 disables it.
    slow_query_ms: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Negotiate TLS
    let mut socket: Framed<
        pgwire::tokio::server::MaybeTls,
        pgwire::tokio::server::PgWireMessageServerCodec<String>,
    > = match pgwire::tokio::server::negotiate_tls::<String>(tcp_socket, tls_acceptor).await? {
        Some(s) => s,
        None => return Ok(()),
    };

    // 2. Per-connection channels
    let (subscribe_tx, mut subscribe_rx) = mpsc::unbounded_channel::<SubscriptionCommand>();
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<NotificationResponse>();

    // 3. Per-connection handlers
    let auth_handler = Arc::new(DeltaTStartupHandler::new(auth_source));
    let handler = Arc::new(
        DeltaTHandler::with_subscriptions(tenant_manager.clone(), subscribe_tx)
            .with_slow_query_threshold(slow_query_ms),
    );
    let noop = Arc::new(NoopHandler);

    // 4. Forwarder tasks state
    let mut forwarders: HashMap<Ulid, JoinHandle<()>> = HashMap::new();
    let startup_deadline = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(startup_deadline);

    // Post-auth guards. A disabled (0) guard sleeps ~never so its select arm stays inert; an
    // enabled one fires and breaks the loop, running the same forwarder cleanup as any other exit.
    const NEVER: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);
    let max_age = if max_conn_age_ms == 0 { NEVER } else { Duration::from_millis(max_conn_age_ms) };
    let idle = if max_idle_ms == 0 { NEVER } else { Duration::from_millis(max_idle_ms) };
    let max_age_deadline = tokio::time::sleep(max_age);
    tokio::pin!(max_age_deadline);
    let idle_deadline = tokio::time::sleep(idle);
    tokio::pin!(idle_deadline);

    // 5. Main loop
    loop {
        let in_startup = matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        );

        if in_startup {
            let msg = tokio::select! {
                _ = &mut startup_deadline => break,
                msg = socket.next() => msg,
            };
            match msg {
                Some(Ok(msg)) => {
                    if let Err(e) = pgwire::tokio::server::process_message(
                        msg,
                        &mut socket,
                        auth_handler.clone(),
                        handler.clone(),
                        handler.clone(),
                        noop.clone(),
                        noop.clone(),
                    )
                    .await
                    {
                        tracing::debug!("startup error: {e}");
                        // AUTH_FAILURES_TOTAL counts any startup failure, not only bad passwords: a
                        // generic PgWireError is not trivially separable into auth-vs-other here, so
                        // the metric stays broad (its doc comment already says "startup/auth").
                        metrics::counter!(crate::observability::AUTH_FAILURES_TOTAL).increment(1);
                        // Send the error, then drop the connection. Looping back would let a client
                        // retry passwords on one TCP connection for the whole startup deadline.
                        let _ = pgwire::tokio::server::process_error(&mut socket, e, false).await;
                        break;
                    }
                }
                _ => break,
            }
        } else {
            enum Action<M, E> {
                Message(Option<Result<M, E>>),
                Subscribe(Option<SubscriptionCommand>),
                Notify(Option<NotificationResponse>),
            }

            let action = tokio::select! {
                _ = &mut max_age_deadline => {
                    tracing::info!("closing connection: max age reached");
                    break;
                }
                _ = &mut idle_deadline => {
                    tracing::info!("closing connection: idle timeout");
                    break;
                }
                msg = socket.next() => Action::Message(msg),
                cmd = subscribe_rx.recv() => Action::Subscribe(cmd),
                notif = notify_rx.recv() => Action::Notify(notif),
            };

            match action {
                Action::Message(Some(Ok(msg))) => {
                    // Client activity resets the idle clock; the max-age clock is never reset.
                    idle_deadline.as_mut().reset(tokio::time::Instant::now() + idle);
                    let is_extended = match socket.state() {
                        PgWireConnectionState::CopyInProgress(ext) => ext,
                        _ => msg.is_extended_query(),
                    };
                    if let Err(e) = pgwire::tokio::server::process_message(
                        msg,
                        &mut socket,
                        auth_handler.clone(),
                        handler.clone(),
                        handler.clone(),
                        noop.clone(),
                        noop.clone(),
                    )
                    .await
                        && pgwire::tokio::server::process_error(
                            &mut socket,
                            e,
                            is_extended,
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Action::Message(_) => break,
                Action::Subscribe(Some(cmd)) => {
                    match cmd {
                        SubscriptionCommand::Subscribe(rid) => {
                            // Prune forwarders whose task already exited (e.g. the resource was
                            // deleted, closing the broadcast). Without this a dead entry keeps
                            // contains_key true (a re-LISTEN silently no-ops) and counts against
                            // MAX_SUBSCRIPTIONS_PER_CONNECTION forever.
                            forwarders.retain(|_, h| !h.is_finished());
                            if forwarders.contains_key(&rid) {
                                continue; // already subscribed
                            }
                            if forwarders.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION {
                                continue; // limit reached
                            }
                            // Resolve the engine to get the notify hub
                            let engine = match handler.resolve_engine(&socket) {
                                Ok((e, _)) => e,
                                Err(_) => continue,
                            };
                            // Defense in depth against a delete racing between the LISTEN's
                            // existence check and this Subscribe: never recreate a broadcast channel
                            // for a resource that no longer exists (it would leak, since only delete
                            // reclaims it).
                            if engine.get_resource(&rid).is_none() {
                                continue;
                            }
                            let rx = engine.notify.subscribe(rid);
                            let tx = notify_tx.clone();
                            let channel = format!("resource_{rid}");
                            forwarders.insert(rid, tokio::spawn(forward_resource_events(rx, tx, channel)));
                        }
                        SubscriptionCommand::Unsubscribe(rid) => {
                            if let Some(handle) = forwarders.remove(&rid) {
                                handle.abort();
                            }
                        }
                        SubscriptionCommand::UnsubscribeAll => {
                            for (_, handle) in forwarders.drain() {
                                handle.abort();
                            }
                        }
                    }
                }
                Action::Subscribe(None) => {}
                Action::Notify(Some(notif)) => {
                    let msg: PgWireBackendMessage =
                        PgWireBackendMessage::NotificationResponse(notif);
                    if SinkExt::send(&mut socket, msg).await.is_err() {
                        break;
                    }
                }
                Action::Notify(None) => {}
            }
        }
    }

    // Cleanup: abort all forwarder tasks
    for (_, handle) in forwarders {
        handle.abort();
    }
    Ok(())
}

/// Result-column schema for a Describe, derived from the parsed SQL rather than scanning the
/// text. This runs at Describe time when `$N` placeholders are still unbound, so the statement
/// cannot be value-parsed into a Command yet; inspecting the AST's target table avoids both
/// that limitation and the false matches a substring scan would make against string literals.
fn schema_for_sql(sql: &str) -> Vec<FieldInfo> {
    use sqlparser::ast::{ObjectNamePart, SetExpr, Statement, TableFactor};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
        return vec![];
    };
    let Some(Statement::Query(query)) = statements.first() else {
        return vec![];
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return vec![];
    };
    let Some(from) = select.from.first() else {
        return vec![];
    };
    let TableFactor::Table { name, .. } = &from.relation else {
        return vec![];
    };
    let Some(ObjectNamePart::Identifier(table)) = name.0.last() else {
        return vec![];
    };

    // Fold case to match the execution path, which lowercases identifiers (sql.rs), so a Describe
    // of `BOOKINGS` returns the same schema the query will produce.
    match table.value.to_lowercase().as_str() {
        "availability" => {
            // Pick the schema with the exact classifier the executor uses to pick the Command, so
            // the announced column set always matches the rows produced. Only the merged form
            // (resource_id IN (...) AND min_available = N) drops resource_id for the narrower
            // combined schema; per-resource IN and single-resource forms both carry resource_id.
            match crate::sql::availability_shape(select.selection.as_ref()) {
                crate::sql::AvailabilityShape::Merged => multi_availability_schema(),
                _ => availability_schema(),
            }
        }
        "resources" => resources_schema(),
        "rules" => rules_schema(),
        "bookings" => bookings_schema(),
        "holds" => holds_schema(),
        _ => vec![],
    }
}

/// An engine error keeps its typed form until [`DeltaTHandler::execute_command`] has read its
/// kind for the metric labels; everything already wire-shaped (parse, span, protocol) passes
/// through untouched.
enum WireError {
    Engine(crate::engine::EngineError),
    Pg(PgWireError),
}

impl From<crate::engine::EngineError> for WireError {
    fn from(e: crate::engine::EngineError) -> Self {
        WireError::Engine(e)
    }
}

impl From<PgWireError> for WireError {
    fn from(e: PgWireError) -> Self {
        WireError::Pg(e)
    }
}

impl From<WireError> for PgWireError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::Engine(e) => engine_err(e),
            WireError::Pg(e) => e,
        }
    }
}

/// The taxonomy boundary: every engine error crosses the wire exactly once, so this is where
/// ENGINE_ERRORS_TOTAL fires and where the error's own SQLSTATE (retryable 40001 contention
/// vs client mistakes vs server faults) replaces a catch-all code.
fn engine_err(e: crate::engine::EngineError) -> PgWireError {
    metrics::counter!(crate::observability::ENGINE_ERRORS_TOTAL, "kind" => e.kind()).increment(1);
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        e.sqlstate().into(),
        e.to_string(),
    )))
}

/// Logs the command label, never the statement text: query text can carry customer
/// identifiers, and nothing else in this file logs it either.
fn record_slow_query(command: &'static str, tenant: &str, elapsed: Duration, threshold_ms: u64) {
    let elapsed_ms = elapsed.as_millis() as u64;
    if threshold_ms == 0 || elapsed_ms < threshold_ms {
        return;
    }
    tracing::warn!(command, tenant, elapsed_ms, threshold_ms, "slow query");
    metrics::counter!(crate::observability::SLOW_QUERIES_TOTAL, "command" => command).increment(1);
}

/// Reject an invalid time range from untrusted SQL input cleanly, instead of letting
/// `Span::new` panic the connection task (SQLSTATE 22007 = invalid_datetime_format).
fn span_err(msg: &'static str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "22007".into(),
        msg.into(),
    )))
}

fn tenant_err(e: std::io::Error) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "08006".into(),
        format!("tenant error: {e}"),
    )))
}

fn sql_err(e: crate::sql::SqlError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "42601".into(),
        e.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── forward_resource_events ──────────────────────────────────

    #[tokio::test]
    async fn forwarder_survives_broadcast_lag() {
        use tokio::sync::broadcast;
        // A cap-1 ring lets us force a Lagged deterministically: the receiver is subscribed at
        // channel() time, then three sends overflow the ring before the forwarder drains, leaving
        // it two behind. The old `while let Ok(..)` ended the task on that Lagged and dropped the
        // stream forever; the fix continues and still forwards the surviving event.
        let (btx, brx) = broadcast::channel::<Event>(1);
        let (mtx, mut mrx) = mpsc::unbounded_channel();
        let mk = || Event::BookingConfirmed {
            id: Ulid::new(),
            resource_id: Ulid::new(),
            span: Span::new(1000, 2000),
            label: None,
        };
        btx.send(mk()).unwrap();
        btx.send(mk()).unwrap();
        btx.send(mk()).unwrap(); // receiver now 2 behind a cap-1 ring → next recv() is Lagged

        tokio::spawn(forward_resource_events(brx, mtx, "resource_x".into()));

        let got = tokio::time::timeout(Duration::from_secs(1), mrx.recv())
            .await
            .expect("forwarder must not hang");
        // Some(_) only if the forwarder survived the Lagged; the old behavior dropped the sender
        // and recv() would return None.
        assert!(got.is_some(), "forwarder must survive a broadcast Lagged and keep forwarding");
    }

    // ── enforce_query_len ────────────────────────────────────────

    #[test]
    fn enforce_query_len_bounds_at_limit() {
        // At the limit is accepted; one byte over is rejected. This guard runs at Parse time so an
        // oversized statement never reaches count_params or a full sqlparser parse.
        assert!(enforce_query_len(MAX_QUERY_LEN).is_ok());
        assert!(enforce_query_len(MAX_QUERY_LEN + 1).is_err());
    }

    // ── count_params ─────────────────────────────────────────────

    #[test]
    fn count_params_none() {
        assert_eq!(count_params("SELECT * FROM resources"), 0);
    }

    #[test]
    fn count_params_single() {
        assert_eq!(count_params("SELECT * FROM resources WHERE id = $1"), 1);
    }

    #[test]
    fn count_params_multiple() {
        assert_eq!(
            count_params("INSERT INTO rules (id, resource_id, start, \"end\", blocking) VALUES ($1, $2, $3, $4, $5)"),
            5
        );
    }

    #[test]
    fn count_params_out_of_order() {
        assert_eq!(count_params("SELECT $3, $1, $2"), 3);
    }

    #[test]
    fn count_params_double_digit() {
        assert_eq!(count_params("SELECT $10, $1"), 10);
    }

    #[test]
    fn count_params_dollar_no_digit() {
        // "$" followed by non-digit should not count
        assert_eq!(count_params("SELECT $foo"), 0);
    }

    // ── checked_param_count / MAX_PARAMS ─────────────────────────

    #[test]
    fn checked_param_count_rejects_over_limit_index() {
        // A ~20-byte statement must never size an allocation from its raw $N index: an index over
        // MAX_PARAMS is rejected with an error, not fed to vec![Type; N].
        assert!(checked_param_count("SELECT $9999999999").is_err());
        assert!(checked_param_count(&format!("SELECT ${}", MAX_PARAMS + 1)).is_err());
    }

    #[test]
    fn checked_param_count_accepts_at_limit() {
        assert_eq!(checked_param_count(&format!("SELECT ${MAX_PARAMS}")).unwrap(), MAX_PARAMS);
        assert_eq!(checked_param_count("SELECT $1, $2").unwrap(), 2);
    }

    #[test]
    fn get_parameter_types_rejects_over_limit_index() {
        let parser = DeltaTQueryParser;
        assert!(parser.get_parameter_types(&"SELECT $70000".to_string()).is_err());
    }

    // ── schema_for_sql ───────────────────────────────────────────

    #[test]
    fn schema_for_select_availability() {
        let schema = schema_for_sql("SELECT * FROM availability WHERE resource_id = $1");
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name(), "resource_id");
    }

    #[test]
    fn schema_for_select_multi_availability() {
        // Merged form (min_available) drops resource_id → just start, end.
        let merged = schema_for_sql(
            "SELECT * FROM availability WHERE resource_id IN ($1, $2) AND min_available = $3",
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].name(), "start");
        // Per-resource form (no min_available) keeps resource_id so the caller can regroup.
        let per_resource =
            schema_for_sql("SELECT * FROM availability WHERE resource_id IN ($1, $2)");
        assert_eq!(per_resource.len(), 3);
        assert_eq!(per_resource[0].name(), "resource_id");
    }

    /// The number of result columns the executor will actually emit for an availability Command.
    fn executed_availability_width(cmd: &Command) -> usize {
        match cmd {
            // Merged across the set: rows are (start, end) only.
            Command::SelectMultiAvailability { .. } => 2,
            // Per-resource and single: rows carry resource_id too.
            Command::SelectAvailability { .. } | Command::SelectAvailabilityMulti { .. } => 3,
            other => panic!("not an availability command: {other:?}"),
        }
    }

    #[test]
    fn describe_schema_width_matches_executed_row_width() {
        // The merged schema (start, end) is announced ONLY for the exact form the executor routes
        // as merged: `resource_id IN (...) AND min_available = N`. Every other syntactic form must
        // announce the per-resource schema (resource_id, start, end), because that is what the
        // executor produces. A divergence here is a wire protocol break: the client reads a
        // RowDescription with N columns and then DataRows with a different column count.
        //
        // Each case pairs the placeholder SQL a client Describes with the value-bound SQL it later
        // Executes; the asserted invariant is schema_for_sql(describe) == executed row width.
        let cases = [
            // canonical merged form → 2 cols
            (
                "SELECT * FROM availability WHERE resource_id IN ($1, $2) AND min_available = $3 AND start >= $4 AND \"end\" <= $5",
                "SELECT * FROM availability WHERE resource_id IN ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW') AND min_available = 2 AND start >= 0 AND \"end\" <= 100",
            ),
            // `>` is not the merged marker (executor matches Eq only) → per-resource, 3 cols
            (
                "SELECT * FROM availability WHERE resource_id IN ($1, $2) AND min_available > $3 AND start >= $4 AND \"end\" <= $5",
                "SELECT * FROM availability WHERE resource_id IN ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW') AND min_available > 1 AND start >= 0 AND \"end\" <= 100",
            ),
            // `>=` likewise → per-resource, 3 cols
            (
                "SELECT * FROM availability WHERE resource_id IN ($1, $2) AND min_available >= $3 AND start >= $4 AND \"end\" <= $5",
                "SELECT * FROM availability WHERE resource_id IN ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW') AND min_available >= 1 AND start >= 0 AND \"end\" <= 100",
            ),
            // reversed operand `N = min_available` (column on the right) → executor does not match → per-resource, 3 cols
            (
                "SELECT * FROM availability WHERE resource_id IN ($1, $2) AND $3 = min_available AND start >= $4 AND \"end\" <= $5",
                "SELECT * FROM availability WHERE resource_id IN ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW') AND 2 = min_available AND start >= 0 AND \"end\" <= 100",
            ),
            // plain per-resource IN, no min_available → 3 cols
            (
                "SELECT * FROM availability WHERE resource_id IN ($1, $2) AND start >= $3 AND \"end\" <= $4",
                "SELECT * FROM availability WHERE resource_id IN ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW') AND start >= 0 AND \"end\" <= 100",
            ),
            // single resource → 3 cols
            (
                "SELECT * FROM availability WHERE resource_id = $1 AND start >= $2 AND \"end\" <= $3",
                "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 0 AND \"end\" <= 100",
            ),
        ];
        for (describe_sql, execute_sql) in cases {
            let announced = schema_for_sql(describe_sql).len();
            let cmd = sql::parse_sql(execute_sql).expect("execute SQL parses");
            let produced = executed_availability_width(&cmd);
            assert_eq!(
                announced, produced,
                "Describe announced {announced} cols but executor produces {produced} for: {describe_sql}",
            );
        }
    }

    #[test]
    fn schema_for_select_resources() {
        let schema = schema_for_sql("SELECT * FROM resources");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[0].name(), "id");
        assert_eq!(schema[2].name(), "name");
    }

    #[test]
    fn schema_for_select_rules() {
        let schema = schema_for_sql("SELECT * FROM rules WHERE resource_id = $1");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[4].name(), "blocking");
    }

    #[test]
    fn schema_for_select_bookings() {
        let schema = schema_for_sql("SELECT * FROM bookings WHERE resource_id = $1");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[4].name(), "label");
    }

    #[test]
    fn schema_for_select_holds() {
        let schema = schema_for_sql("SELECT * FROM holds WHERE resource_id = $1");
        assert_eq!(schema.len(), 5);
        assert_eq!(schema[4].name(), "expires_at");
    }

    #[test]
    fn schema_for_select_is_case_insensitive() {
        // The execution path lowercases identifiers, so a Describe of a mixed or upper-case table
        // must return the same schema the query will produce, not an empty one.
        let single = schema_for_sql("SELECT * FROM BOOKINGS WHERE resource_id = $1");
        assert_eq!(single.len(), 5);
        assert_eq!(single[4].name(), "label");

        let multi = schema_for_sql(
            "SELECT * FROM Availability WHERE resource_id IN ($1, $2) AND min_available = $3",
        );
        assert_eq!(multi.len(), 2);
    }

    #[test]
    fn parse_param_index_handles_overflow_and_normal_runs() {
        // A digit run that overflows usize must yield None, never panic the connection task.
        let overflow: Vec<char> = "$99999999999999999999".chars().collect();
        let (n, end) = parse_param_index(&overflow, 1);
        assert_eq!(n, None::<usize>);
        assert_eq!(end, overflow.len());

        let normal: Vec<char> = "$12 rest".chars().collect();
        let (n, end) = parse_param_index(&normal, 1);
        assert_eq!(n, Some(12));
        assert_eq!(end, 3);
    }

    #[test]
    fn count_params_ignores_overflowing_placeholder() {
        assert_eq!(count_params("SELECT 1"), 0);
        assert_eq!(count_params("WHERE a = $1 AND b = $3 AND c = $2"), 3);
        // An out-of-range run is not a valid index, so it must not inflate the count or panic.
        assert_eq!(count_params("WHERE id = $1 OR id = $99999999999999999999"), 1);
    }

    #[test]
    fn count_params_never_panics_on_arbitrary_input() {
        use proptest::prelude::*;
        proptest!(ProptestConfig::with_cases(2000), |(s in r"[\$0-9A-Za-z ]{0,48}")| {
            let _ = count_params(&s);
        });
    }

    #[test]
    fn schema_for_insert_returns_empty() {
        let schema = schema_for_sql("INSERT INTO resources (id) VALUES ($1)");
        assert!(schema.is_empty());
    }

    #[test]
    fn schema_for_delete_returns_empty() {
        let schema = schema_for_sql("DELETE FROM resources WHERE id = $1");
        assert!(schema.is_empty());
    }

    // ── substitute_params ────────────────────────────────────────

    fn make_portal(sql: &str, params: Vec<Option<bytes::Bytes>>) -> Portal<String> {
        let stored = Arc::new(StoredStatement::new(
            String::new(),
            sql.to_string(),
            vec![None; params.len()],
        ));
        let mut portal = Portal::<String>::default();
        portal.statement = stored;
        portal.parameters = params;
        portal
    }

    #[test]
    fn substitute_basic() {
        let portal = make_portal(
            "SELECT * FROM resources WHERE id = $1",
            vec![Some(bytes::Bytes::from_static(b"01ARZ3NDEKTSV4RRFFQ69G5FAV"))],
        );
        let result = substitute_params(&portal);
        assert_eq!(
            result,
            "SELECT * FROM resources WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'"
        );
    }

    #[test]
    fn substitute_null_param() {
        let portal = make_portal(
            "INSERT INTO resources (id, parent_id) VALUES ($1, $2)",
            vec![
                Some(bytes::Bytes::from_static(b"01ARZ3NDEKTSV4RRFFQ69G5FAV")),
                None,
            ],
        );
        let result = substitute_params(&portal);
        assert!(result.contains("NULL"));
        assert!(result.contains("'01ARZ3NDEKTSV4RRFFQ69G5FAV'"));
    }

    #[test]
    fn substitute_escapes_quotes() {
        let portal = make_portal(
            "INSERT INTO resources (id, parent_id, name) VALUES ($1, NULL, $2)",
            vec![
                Some(bytes::Bytes::from_static(b"01ARZ3NDEKTSV4RRFFQ69G5FAV")),
                Some(bytes::Bytes::from_static(b"O'Brien's Room")),
            ],
        );
        let result = substitute_params(&portal);
        assert!(result.contains("'O''Brien''s Room'"));
    }

    #[test]
    fn substitute_multiple_params() {
        let portal = make_portal(
            "INSERT INTO rules (id, resource_id, start, \"end\", blocking) VALUES ($1, $2, $3, $4, $5)",
            vec![
                Some(bytes::Bytes::from_static(b"RULE_ID")),
                Some(bytes::Bytes::from_static(b"RES_ID")),
                Some(bytes::Bytes::from_static(b"1000")),
                Some(bytes::Bytes::from_static(b"2000")),
                Some(bytes::Bytes::from_static(b"false")),
            ],
        );
        let result = substitute_params(&portal);
        assert!(result.contains("'RULE_ID'"));
        assert!(result.contains("'RES_ID'"));
        assert!(result.contains("'1000'"));
        assert!(result.contains("'2000'"));
        assert!(result.contains("'false'"));
    }

    // ── parse_channel_resource_id ───────────────────────────────

    #[test]
    fn parse_channel_valid() {
        let ulid = Ulid::new();
        let channel = format!("resource_{ulid}");
        let result = DeltaTHandler::parse_channel_resource_id(&channel).unwrap();
        assert_eq!(result, ulid);
    }

    #[test]
    fn parse_channel_missing_prefix() {
        let result = DeltaTHandler::parse_channel_resource_id("foobar_01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("invalid channel"));
    }

    #[test]
    fn parse_channel_bad_ulid() {
        let result = DeltaTHandler::parse_channel_resource_id("resource_notaulid");
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("bad ULID"));
    }

    #[test]
    fn parse_channel_empty_after_prefix() {
        let result = DeltaTHandler::parse_channel_resource_id("resource_");
        assert!(result.is_err());
    }

    // ── execute_command: Listen / Unlisten / UnlistenAll ─────────

    use crate::notify::NotifyHub;
    use std::path::PathBuf;

    fn test_wal_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("deltat_test_wire");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        path
    }

    /// Direct-command shim for tests that build a Command by hand; the protocol paths reach
    /// execute_command through run_statement, which opens the clock itself.
    async fn exec(
        handler: &DeltaTHandler,
        engine: &Engine,
        cmd: Command,
    ) -> PgWireResult<Vec<Response>> {
        handler.execute_command(engine, "test", cmd, std::time::Instant::now()).await
    }

    fn setup_handler_with_subs() -> (
        DeltaTHandler,
        mpsc::UnboundedReceiver<SubscriptionCommand>,
        Arc<Engine>,
    ) {
        let notify = Arc::new(NotifyHub::new());
        let path = test_wal_path(&format!("wire_{}.wal", Ulid::new()));
        let engine = Arc::new(Engine::new(path, notify).unwrap());

        let tm = Arc::new(TenantManager::new(
            std::env::temp_dir().join("deltat_test_wire_tm"),
            1000,
            604_800_000,
        ));

        let (tx, rx) = mpsc::unbounded_channel();
        let handler = DeltaTHandler::with_subscriptions(tm, tx);
        (handler, rx, engine)
    }

    #[tokio::test]
    async fn execute_listen_sends_subscribe() {
        let (handler, mut rx, engine) = setup_handler_with_subs();
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        let channel = format!("resource_{rid}");
        let cmd = Command::Listen { channel };
        let responses = exec(&handler, &engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);

        let sub_cmd = rx.try_recv().unwrap();
        match sub_cmd {
            SubscriptionCommand::Subscribe(id) => assert_eq!(id, rid),
            _ => panic!("expected Subscribe"),
        }
    }

    #[tokio::test]
    async fn execute_listen_nonexistent_resource_errors() {
        // A LISTEN on a resource that does not exist must be rejected, not silently subscribed:
        // subscribing would create a NotifyHub ring buffer that is reclaimed only on delete, so
        // bogus-id LISTENs would leak tenant memory without bound.
        let (handler, mut rx, engine) = setup_handler_with_subs();
        let rid = Ulid::new(); // never created
        let cmd = Command::Listen { channel: format!("resource_{rid}") };
        let result = exec(&handler, &engine, cmd).await;
        assert!(result.is_err());
        // And no Subscribe command was queued for the forwarder loop.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn execute_listen_invalid_channel() {
        let (handler, _rx, engine) = setup_handler_with_subs();
        let cmd = Command::Listen {
            channel: "bad_channel".into(),
        };
        let result = exec(&handler, &engine, cmd).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_listen_without_subscribe_tx() {
        let tm = Arc::new(TenantManager::new(
            std::env::temp_dir().join("deltat_test_wire_no_sub"),
            1000,
            604_800_000,
        ));
        let handler = DeltaTHandler::new(tm);

        let notify = Arc::new(NotifyHub::new());
        let path = test_wal_path(&format!("wire_nosub_{}.wal", Ulid::new()));
        let engine = Arc::new(Engine::new(path, notify).unwrap());

        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();
        let channel = format!("resource_{rid}");
        let cmd = Command::Listen { channel };
        let responses = exec(&handler, &engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);
        // No subscribe_tx, so no command sent, just returns LISTEN tag
    }

    #[tokio::test]
    async fn execute_unlisten_sends_unsubscribe() {
        let (handler, mut rx, engine) = setup_handler_with_subs();
        let rid = Ulid::new();
        let channel = format!("resource_{rid}");
        let cmd = Command::Unlisten { channel };
        let responses = exec(&handler, &engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);

        let sub_cmd = rx.try_recv().unwrap();
        match sub_cmd {
            SubscriptionCommand::Unsubscribe(id) => assert_eq!(id, rid),
            _ => panic!("expected Unsubscribe"),
        }
    }

    #[tokio::test]
    async fn execute_unlisten_all_sends_command() {
        let (handler, mut rx, engine) = setup_handler_with_subs();
        let cmd = Command::UnlistenAll;
        let responses = exec(&handler, &engine, cmd).await.unwrap();
        assert_eq!(responses.len(), 1);

        let sub_cmd = rx.try_recv().unwrap();
        assert!(matches!(sub_cmd, SubscriptionCommand::UnsubscribeAll));
    }

    // ── execute_command: CommitHold ──────────────────────────────

    #[tokio::test]
    async fn execute_commit_hold_converts_hold_to_booking() {
        let (handler, _rx, engine) = setup_handler_with_subs();
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();

        let hid = Ulid::new();
        let expires = crate::clock::now_ms() + 60_000;
        let insert = format!(
            r#"INSERT INTO holds (id, resource_id, start, "end", expires_at) VALUES ('{hid}', '{rid}', 1000, 2000, {expires})"#
        );
        handler.run_statement(&engine, "test", &insert).await.unwrap();

        let bid = Ulid::new();
        let commit = format!("UPDATE holds SET booking_id = '{bid}', label = 'picnic' WHERE id = '{hid}'");
        let responses = handler.run_statement(&engine, "test", &commit).await.unwrap();
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            Response::Execution(tag) => assert_eq!(tag, &Tag::new("UPDATE").with_rows(1)),
            other => panic!("expected Execution, got {other:?}"),
        }

        assert!(engine.get_holds(rid).await.unwrap().is_empty());
        let bookings = engine.get_bookings(rid).await.unwrap();
        assert_eq!(bookings.len(), 1);
        assert_eq!(bookings[0].id, bid);
        assert_eq!(bookings[0].start, 1000);
        assert_eq!(bookings[0].end, 2000);
        assert_eq!(bookings[0].label.as_deref(), Some("picnic"));
    }

    #[tokio::test]
    async fn execute_commit_hold_unknown_hold_errors() {
        let (handler, _rx, engine) = setup_handler_with_subs();
        let commit = format!(
            "UPDATE holds SET booking_id = '{}' WHERE id = '{}'",
            Ulid::new(),
            Ulid::new()
        );
        let result = handler.run_statement(&engine, "test", &commit).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn commit_hold_over_wire_wins_the_span_against_concurrent_bookers() {
        // The reason CommitHold exists (AVAIL-07): converting a hold to a booking must never open
        // a window a competing booker can steal. Race the SQL commit against 7 concurrent
        // INSERT INTO bookings on the held span of a capacity-1 resource: the commit must always
        // win and every competitor must get a clean conflict error.
        let (handler, _rx, engine) = setup_handler_with_subs();
        let handler = Arc::new(handler);
        let rid = Ulid::new();
        engine.create_resource(rid, None, None, 1, None).await.unwrap();

        let hid = Ulid::new();
        let expires = crate::clock::now_ms() + 60_000;
        let insert = format!(
            r#"INSERT INTO holds (id, resource_id, start, "end", expires_at) VALUES ('{hid}', '{rid}', 1000, 2000, {expires})"#
        );
        handler.run_statement(&engine, "test", &insert).await.unwrap();

        let bid = Ulid::new();
        let mut tasks = Vec::new();
        {
            let handler = handler.clone();
            let engine = engine.clone();
            let commit = format!("UPDATE holds SET booking_id = '{bid}' WHERE id = '{hid}'");
            tasks.push(tokio::spawn(async move {
                handler.run_statement(&engine, "test", &commit).await.is_ok()
            }));
        }
        for _ in 0..7 {
            let handler = handler.clone();
            let engine = engine.clone();
            let steal = format!(
                r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{}', '{rid}', 1000, 2000)"#,
                Ulid::new()
            );
            tasks.push(tokio::spawn(async move {
                handler.run_statement(&engine, "test", &steal).await.is_ok()
            }));
        }

        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        assert!(results[0], "the holder's commit must win its own span");
        assert!(results[1..].iter().all(|ok| !ok), "no competing booking may squeeze in");

        assert!(engine.get_holds(rid).await.unwrap().is_empty());
        let bookings = engine.get_bookings(rid).await.unwrap();
        assert_eq!(bookings.len(), 1);
        assert_eq!(bookings[0].id, bid);
    }

    // ── metrics recording ────────────────────────────────────────

    use crate::test_metrics::{block_on, with_metrics};

    #[tokio::test]
    async fn resolve_engine_returns_the_sanitized_tenant_label() {
        // Raw aliases like "acme!" pass auth and reach tenant "acme"'s engine (sanitize
        // collapses them), so the metric label must be the sanitized engine key: labeling
        // with the raw name mints a fresh series per alias, unbounded by MAX_TENANTS.
        let dir = std::env::temp_dir().join("deltat_test_wire_tenant_label");
        std::fs::create_dir_all(&dir).unwrap();
        let tm = Arc::new(TenantManager::new(dir, 1000, 604_800_000));
        let (tx, _rx) = mpsc::unbounded_channel();
        let handler = DeltaTHandler::with_subscriptions(tm, tx);
        let mut client =
            pgwire::api::DefaultClient::<String>::new("127.0.0.1:0".parse().unwrap(), false);
        client
            .metadata_mut()
            .insert("database".to_string(), "acme!".to_string());
        let (_engine, tenant) = handler.resolve_engine(&client).unwrap();
        assert_eq!(tenant, "acme");
    }

    #[test]
    fn run_statement_labels_tenant_status_and_error_kind() {
        // One counter answers "which tenant went hot, and were those conflicts or failures":
        // command, status, tenant, and the engine error kind ("none" on success) on every query.
        let (log, ()) = with_metrics(|| {
            block_on(async {
                let (handler, _rx, engine) = setup_handler_with_subs();
                let rid = Ulid::new();
                engine.create_resource(rid, None, None, 1, None).await.unwrap();
                let ok_sql = format!("SELECT * FROM bookings WHERE resource_id = '{rid}'");
                handler.run_statement(&engine, "acme", &ok_sql).await.unwrap();

                let missing = Ulid::new();
                let bad_sql = format!(
                    r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('{}', '{missing}', 1000, 2000)"#,
                    Ulid::new()
                );
                assert!(handler.run_statement(&engine, "acme", &bad_sql).await.is_err());
            })
        });

        let queries = crate::observability::QUERIES_TOTAL;
        assert_eq!(
            log.counter_total(
                queries,
                &[("command", "select_bookings"), ("status", "ok"), ("kind", "none"), ("tenant", "acme")],
            ),
            1
        );
        assert_eq!(
            log.counter_total(
                queries,
                &[("command", "insert_booking"), ("status", "error"), ("kind", "not_found"), ("tenant", "acme")],
            ),
            1
        );
        assert!(log.histogram_recorded(
            crate::observability::QUERY_DURATION_SECONDS,
            &[("command", "select_bookings"), ("tenant", "acme")],
        ));
        assert_eq!(
            log.counter_total(crate::observability::ENGINE_ERRORS_TOTAL, &[("kind", "not_found")]),
            1
        );
    }

    #[test]
    fn run_statement_counts_rejected_statements() {
        // Unparseable and oversized statements die before execution; they must show up in
        // PARSE_ERRORS_TOTAL instead of vanishing from every metric.
        let (log, ()) = with_metrics(|| {
            block_on(async {
                let (handler, _rx, engine) = setup_handler_with_subs();
                assert!(handler.run_statement(&engine, "t", "THIS IS NOT SQL").await.is_err());
                let oversized = format!("SELECT 1 -- {}", "x".repeat(MAX_QUERY_LEN));
                assert!(handler.run_statement(&engine, "t", &oversized).await.is_err());
            })
        });
        assert_eq!(log.counter_total(crate::observability::PARSE_ERRORS_TOTAL, &[]), 2);
        // Neither rejection became a Command, so the query counter must not move.
        assert_eq!(log.counter_total(crate::observability::QUERIES_TOTAL, &[]), 0);
    }

    #[test]
    fn slow_queries_counted_only_at_or_over_an_enabled_threshold() {
        // Matches log_min_duration_statement semantics: fires at or over the threshold, and a
        // zero threshold disables it entirely.
        let (log, ()) = with_metrics(|| {
            record_slow_query("select_bookings", "acme", Duration::from_millis(150), 100);
            record_slow_query("select_bookings", "acme", Duration::from_millis(50), 100);
            record_slow_query("select_bookings", "acme", Duration::from_millis(150), 0);
        });
        assert_eq!(
            log.counter_total(
                crate::observability::SLOW_QUERIES_TOTAL,
                &[("command", "select_bookings")],
            ),
            1
        );
    }

    #[test]
    fn engine_err_speaks_the_error_taxonomy() {
        // A lost race must reach the client as SQLSTATE 40001 (serialization_failure), the code
        // drivers already retry on, not a catch-all P0001 that forces string-matching. The same
        // conversion is where the taxonomy counter fires, one increment per engine error.
        let (log, err) =
            with_metrics(|| engine_err(crate::engine::EngineError::Conflict(Ulid::nil())));
        let PgWireError::UserError(info) = err else {
            panic!("expected UserError");
        };
        assert_eq!(info.code, "40001");
        assert_eq!(
            log.counter_total(crate::observability::ENGINE_ERRORS_TOTAL, &[("kind", "conflict")]),
            1
        );
    }

    #[test]
    fn forwarder_counts_lagged_notifications() {
        // Lagged(n) carries how many events the ring dropped for this subscriber. Those drops are
        // a client silently going stale, so they must land in NOTIFICATIONS_LAGGED_TOTAL.
        let (log, ()) = with_metrics(|| {
            block_on(async {
                use tokio::sync::broadcast;
                let (btx, brx) = broadcast::channel::<Event>(1);
                let (mtx, mut mrx) = mpsc::unbounded_channel();
                let mk = || Event::BookingConfirmed {
                    id: Ulid::new(),
                    resource_id: Ulid::new(),
                    span: Span::new(1000, 2000),
                    label: None,
                };
                btx.send(mk()).unwrap();
                btx.send(mk()).unwrap();
                btx.send(mk()).unwrap(); // 2 behind a cap-1 ring, as in forwarder_survives_broadcast_lag
                tokio::spawn(forward_resource_events(brx, mtx, "resource_x".into()));
                tokio::time::timeout(Duration::from_secs(1), mrx.recv())
                    .await
                    .expect("forwarder must not hang");
            })
        });
        assert_eq!(
            log.counter_total(crate::observability::NOTIFICATIONS_LAGGED_TOTAL, &[]),
            2
        );
    }
}
