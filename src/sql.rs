use sqlparser::ast::{self, Expr, FromTable, ObjectNamePart, SetExpr, Statement, TableFactor, TableObject, Value, ValueWithSpan};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use ulid::Ulid;

use crate::command::Command;
use crate::limits::{MAX_BATCH_SIZE, MAX_IN_CLAUSE_IDS};
use crate::model::*;

pub fn parse_sql(sql: &str) -> Result<Command, SqlError> {
    let trimmed = sql.trim();
    // Match the keyword ASCII-case-insensitively via get(..n): a byte-offset slice after an
    // uppercased starts_with can land mid-char (Unicode case-folding changes byte length) and
    // panic. get returns None off a char boundary, and an ASCII-insensitive match guarantees the
    // matched prefix is single-byte ASCII, so the trailing slice is on a boundary.
    if let Some(prefix) = trimmed.get(..7)
        && prefix.eq_ignore_ascii_case("LISTEN ")
    {
        let channel = trimmed[7..].trim().trim_matches(';').trim_matches('"').to_string();
        return Ok(Command::Listen { channel });
    }
    if let Some(prefix) = trimmed.get(..9)
        && prefix.eq_ignore_ascii_case("UNLISTEN ")
    {
        let rest = trimmed[9..].trim().trim_matches(';').trim_matches('"');
        if rest == "*" {
            return Ok(Command::UnlistenAll);
        }
        return Ok(Command::Unlisten { channel: rest.to_string() });
    }

    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    if stmts.is_empty() {
        return Err(SqlError::Empty);
    }
    // A multi-statement simple query ("INSERT ...; INSERT ...;") must not run only its first
    // statement and report success; reject it rather than silently dropping the rest.
    if stmts.len() > 1 {
        return Err(SqlError::Unsupported("multiple statements in one query".into()));
    }

    match &stmts[0] {
        Statement::Insert(insert) => parse_insert(insert),
        Statement::Delete(delete) => parse_delete(delete),
        Statement::Query(query) => parse_select(query),
        Statement::Update { table, assignments, selection, .. } => parse_update(table, assignments, selection),
        other => Err(SqlError::Unsupported(format!("{other}"))),
    }
}

fn parse_insert(insert: &ast::Insert) -> Result<Command, SqlError> {
    let table = insert_table_name(insert)?;
    let values = extract_insert_values(insert)?;
    let columns = extract_column_names(insert);

    match table.as_str() {
        "resources" => {
            let all_rows = extract_all_insert_rows(insert)?;
            if all_rows.len() == 1 {
                let (id, parent_id, name, capacity, buffer_after) =
                    parse_resource_row(&all_rows[0], &columns)?;
                Ok(Command::InsertResource { id, parent_id, name, capacity, buffer_after })
            } else {
                let mut resources = Vec::with_capacity(all_rows.len());
                for (i, row) in all_rows.iter().enumerate() {
                    resources.push(
                        parse_resource_row(row, &columns)
                            .map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                    );
                }
                Ok(Command::BatchInsertResources { resources })
            }
        }
        "rules" => {
            let all_rows = extract_all_insert_rows(insert)?;
            if all_rows.len() == 1 {
                let (id, resource_id, start, end, blocking) =
                    parse_rule_row(&all_rows[0], &columns)?;
                Ok(Command::InsertRule { id, resource_id, start, end, blocking })
            } else {
                let mut rules = Vec::with_capacity(all_rows.len());
                for (i, row) in all_rows.iter().enumerate() {
                    rules.push(
                        parse_rule_row(row, &columns)
                            .map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                    );
                }
                Ok(Command::BatchInsertRules { rules })
            }
        }
        "holds" => {
            let (id, resource_id, start, end, expires_at) = parse_hold_row(&values, &columns)?;
            Ok(Command::InsertHold { id, resource_id, start, end, expires_at })
        }
        "bookings" => {
            let all_rows = extract_all_insert_rows(insert)?;
            let label_idx = columns.iter().position(|c| c == "label");

            if all_rows.len() == 1 {
                let values = &all_rows[0];
                if !columns.is_empty() && values.len() != columns.len() {
                    return Err(SqlError::WrongArity("bookings", columns.len(), values.len()));
                }
                if values.len() < 4 {
                    return Err(SqlError::WrongArity("bookings", 4, values.len()));
                }
                let label = cell(values, label_idx)
                    .map(parse_string_or_null)
                    .transpose()?
                    .flatten();
                Ok(Command::InsertBooking {
                    id: parse_ulid(&values[0])?,
                    resource_id: parse_ulid(&values[1])?,
                    start: parse_i64(&values[2])?,
                    end: parse_i64(&values[3])?,
                    label,
                })
            } else {
                let mut bookings = Vec::with_capacity(all_rows.len());
                for (i, row) in all_rows.iter().enumerate() {
                    if !columns.is_empty() && row.len() != columns.len() {
                        return Err(SqlError::WrongArity("bookings row", columns.len(), row.len()));
                    }
                    if row.len() < 4 {
                        return Err(SqlError::WrongArity("bookings row", 4, row.len()));
                    }
                    let label = cell(row, label_idx)
                        .map(|e| parse_string_or_null(e).map_err(|e| SqlError::Parse(format!("row {i}: {e}"))))
                        .transpose()?
                        .flatten();
                    bookings.push((
                        parse_ulid(&row[0]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_ulid(&row[1]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_i64(&row[2]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        parse_i64(&row[3]).map_err(|e| SqlError::Parse(format!("row {i}: {e}")))?,
                        label,
                    ));
                }
                Ok(Command::BatchInsertBookings { bookings })
            }
        }
        _ => Err(SqlError::UnknownTable(table)),
    }
}

fn parse_delete(delete: &ast::Delete) -> Result<Command, SqlError> {
    let table = delete_table_name(delete)?;
    let id = extract_where_id(&delete.selection)?;

    match table.as_str() {
        "resources" => Ok(Command::DeleteResource { id }),
        "rules" => Ok(Command::DeleteRule { id }),
        "holds" => Ok(Command::DeleteHold { id }),
        "bookings" => Ok(Command::DeleteBooking { id }),
        _ => Err(SqlError::UnknownTable(table)),
    }
}

fn parse_select(query: &ast::Query) -> Result<Command, SqlError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(SqlError::Unsupported("non-SELECT query".into())),
    };

    if select.from.is_empty() {
        return Err(SqlError::Parse("SELECT without FROM".into()));
    }
    let table = table_factor_name(&select.from[0].relation)?;

    match table.as_str() {
        "availability" => {
            let mut filters = AvailabilityFilters::default();
            if let Some(selection) = &select.selection {
                extract_availability_filters(selection, &mut filters)?;
            }

            let start = filters.start.ok_or(SqlError::MissingFilter("start"))?;
            let end = filters.end.ok_or(SqlError::MissingFilter("end"))?;

            // Branch on the same structural classifier the Describe path uses, so the announced
            // schema always matches the rows produced. Merged => intersection across the set
            // (getCombined); PerResourceMulti => per-resource rows tagged with resource_id
            // (getMany), mirroring bookings/holds IN-list reads.
            match availability_shape(select.selection.as_ref()) {
                AvailabilityShape::Merged => Ok(Command::SelectMultiAvailability {
                    resource_ids: filters.resource_ids,
                    start,
                    end,
                    min_available: filters
                        .min_available
                        .ok_or(SqlError::MissingFilter("min_available"))?,
                    min_duration: filters.min_duration,
                }),
                AvailabilityShape::PerResourceMulti => Ok(Command::SelectAvailabilityMulti {
                    resource_ids: filters.resource_ids,
                    start,
                    end,
                    min_duration: filters.min_duration,
                }),
                AvailabilityShape::Single => Ok(Command::SelectAvailability {
                    resource_id: filters.resource_id.ok_or(SqlError::MissingFilter("resource_id"))?,
                    start,
                    end,
                    min_duration: filters.min_duration,
                }),
            }
        }
        "resources" => {
            // Optional: WHERE parent_id = 'X' or WHERE parent_id IS NULL
            let parent_id = if let Some(selection) = &select.selection {
                Some(extract_parent_id_filter(selection)?)
            } else {
                None
            };
            Ok(Command::SelectResources { parent_id })
        }
        "rules" => {
            let resource_id = extract_resource_id_filter(&select.selection)?;
            Ok(Command::SelectRules { resource_id })
        }
        "bookings" => {
            let ids = extract_resource_ids_filter(&select.selection)?;
            if let [resource_id] = ids[..] {
                Ok(Command::SelectBookings { resource_id })
            } else {
                Ok(Command::SelectBookingsMulti { resource_ids: ids })
            }
        }
        "holds" => {
            let ids = extract_resource_ids_filter(&select.selection)?;
            if let [resource_id] = ids[..] {
                Ok(Command::SelectHolds { resource_id })
            } else {
                Ok(Command::SelectHoldsMulti { resource_ids: ids })
            }
        }
        _ => Err(SqlError::UnknownTable(table)),
    }
}

#[derive(Default)]
struct AvailabilityFilters {
    resource_id: Option<Ulid>,
    resource_ids: Vec<Ulid>,
    start: Option<Ms>,
    end: Option<Ms>,
    min_duration: Option<Ms>,
    min_available: Option<usize>,
}

fn extract_availability_filters(
    expr: &Expr,
    f: &mut AvailabilityFilters,
) -> Result<(), SqlError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            ast::BinaryOperator::And => {
                extract_availability_filters(left, f)?;
                extract_availability_filters(right, f)?;
            }
            ast::BinaryOperator::Eq => {
                let col = expr_column_name(left);
                if col.as_deref() == Some("resource_id") {
                    f.resource_id = Some(parse_ulid_expr(right)?);
                } else if col.as_deref() == Some("min_duration") {
                    f.min_duration = Some(parse_i64_expr(right)?);
                } else if col.as_deref() == Some("min_available") {
                    let v = parse_i64_expr(right)?;
                    if v < 0 {
                        return Err(SqlError::Unsupported(
                            "min_available must be non-negative".into(),
                        ));
                    }
                    f.min_available = Some(v as usize);
                }
            }
            ast::BinaryOperator::GtEq if expr_column_name(left).as_deref() == Some("start") => {
                f.start = Some(parse_i64_expr(right)?);
            }
            ast::BinaryOperator::LtEq if expr_column_name(left).as_deref() == Some("end") => {
                f.end = Some(parse_i64_expr(right)?);
            }
            _ => {}
        },
        // resource_id IN ('id1', 'id2', ...)
        Expr::InList { expr: col_expr, list, negated }
            if !negated && expr_column_name(col_expr).as_deref() == Some("resource_id") =>
        {
            if list.len() > MAX_IN_CLAUSE_IDS {
                return Err(SqlError::Parse(format!(
                    "IN clause too large: {} IDs (max {})",
                    list.len(),
                    MAX_IN_CLAUSE_IDS
                )));
            }
            for item in list {
                f.resource_ids.push(parse_ulid_expr(item)?);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Structural shape of an `availability` query, derived purely from the WHERE AST. Both the
/// execution router (`parse_select`) and the Describe schema (`wire::schema_for_sql`) branch on
/// this single function so the announced column set always matches the rows that get produced.
/// It must agree at Describe time (when `$N` placeholders are still unbound and values cannot be
/// read) and at execution time, so it inspects only structure, never literal values.
#[derive(Debug, PartialEq, Eq)]
pub enum AvailabilityShape {
    /// `resource_id = X`: per-resource rows tagged with `resource_id`.
    Single,
    /// `resource_id IN (...)` without `min_available`: per-resource rows, each tagged.
    PerResourceMulti,
    /// `resource_id IN (...) AND min_available = N`: merged across the set, no `resource_id`.
    Merged,
}

pub fn availability_shape(selection: Option<&Expr>) -> AvailabilityShape {
    let has_resource_in_list = selection.is_some_and(selection_has_resource_id_in_list);
    let has_min_available_eq = selection.is_some_and(selection_has_min_available_eq);
    match (has_resource_in_list, has_min_available_eq) {
        (true, true) => AvailabilityShape::Merged,
        (true, false) => AvailabilityShape::PerResourceMulti,
        _ => AvailabilityShape::Single,
    }
}

/// Mirror `extract_availability_filters`' resource-id matching exactly: a non-negated
/// `resource_id IN (...)`, reachable only through `AND` (never `Nested`/`OR`).
fn selection_has_resource_id_in_list(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
            selection_has_resource_id_in_list(left) || selection_has_resource_id_in_list(right)
        }
        Expr::InList { expr, negated: false, .. } => {
            expr_column_name(expr).as_deref() == Some("resource_id")
        }
        _ => false,
    }
}

/// Mirror `extract_availability_filters`' merged-marker matching exactly: `min_available = ...`
/// with the column on the left (`Eq` only, not `>`, `>=`, or a reversed `N = min_available`),
/// reachable only through `AND`.
fn selection_has_min_available_eq(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
            selection_has_min_available_eq(left) || selection_has_min_available_eq(right)
        }
        Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, .. } => {
            expr_column_name(left).as_deref() == Some("min_available")
        }
        _ => false,
    }
}

fn parse_update(
    table: &ast::TableWithJoins,
    assignments: &[ast::Assignment],
    selection: &Option<Expr>,
) -> Result<Command, SqlError> {
    let table_name = table_factor_name(&table.relation)?;
    let id = extract_where_id(selection)?;

    match table_name.as_str() {
        "resources" => {
            // Only fields actually present in the SET list are emitted; an absent field stays
            // `None` so the engine leaves it unchanged. The inner Option distinguishes "set to NULL"
            // (Some(None)) from "not mentioned" (None) for the nullable columns.
            let mut name: Option<Option<String>> = None;
            let mut capacity: Option<u32> = None;
            let mut buffer_after: Option<Option<Ms>> = None;

            for a in assignments {
                let col = assignment_column_name(a)?;
                match col.as_str() {
                    "name" => name = Some(parse_string_or_null(&a.value)?),
                    "capacity" => capacity = Some(parse_u32(&a.value)?),
                    "buffer_after" => buffer_after = Some(parse_i64_or_null(&a.value)?),
                    _ => {}
                }
            }

            Ok(Command::UpdateResource { id, name, capacity, buffer_after })
        }
        "rules" => {
            let mut start: Option<Ms> = None;
            let mut end: Option<Ms> = None;
            let mut blocking: Option<bool> = None;

            for a in assignments {
                let col = assignment_column_name(a)?;
                match col.as_str() {
                    "start" => start = Some(parse_i64_expr(&a.value)?),
                    "end" => end = Some(parse_i64_expr(&a.value)?),
                    "blocking" => blocking = Some(parse_bool(&a.value)?),
                    _ => {}
                }
            }

            Ok(Command::UpdateRule {
                id,
                start: start.ok_or(SqlError::MissingFilter("start"))?,
                end: end.ok_or(SqlError::MissingFilter("end"))?,
                blocking: blocking.ok_or(SqlError::MissingFilter("blocking"))?,
            })
        }
        _ => Err(SqlError::Unsupported(format!("UPDATE {table_name}"))),
    }
}

fn assignment_column_name(a: &ast::Assignment) -> Result<String, SqlError> {
    match &a.target {
        ast::AssignmentTarget::ColumnName(name) => {
            object_name_last(name).ok_or_else(|| SqlError::Parse("empty assignment target".into()))
        }
        _ => Err(SqlError::Parse("unsupported assignment target".into())),
    }
}

fn extract_resource_id_filter(selection: &Option<Expr>) -> Result<Ulid, SqlError> {
    let sel = selection.as_ref().ok_or(SqlError::MissingFilter("resource_id"))?;
    match sel {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if expr_column_name(left).as_deref() == Some("resource_id") {
                parse_ulid_expr(right)
            } else {
                Err(SqlError::MissingFilter("resource_id"))
            }
        }
        // Handle AND expressions: find resource_id = X within ANDs
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            extract_resource_id_filter(&Some(*left.clone()))
                .or_else(|_| extract_resource_id_filter(&Some(*right.clone())))
        }
        _ => Err(SqlError::MissingFilter("resource_id")),
    }
}

/// Collect resource ids from `WHERE resource_id = X` or `WHERE resource_id IN (...)`, ignoring
/// any ANDed range predicates. One id parses to a single-resource Select; many to a Multi. The
/// `IN` length is bounded here (mirrors the availability path) so an oversized list is rejected
/// at parse time rather than fanned out in the engine.
fn extract_resource_ids_filter(selection: &Option<Expr>) -> Result<Vec<Ulid>, SqlError> {
    let mut ids = Vec::new();
    if let Some(sel) = selection {
        collect_resource_ids(sel, &mut ids)?;
    }
    if ids.is_empty() {
        return Err(SqlError::MissingFilter("resource_id"));
    }
    Ok(ids)
}

fn collect_resource_ids(expr: &Expr, ids: &mut Vec<Ulid>) -> Result<(), SqlError> {
    match expr {
        Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
            collect_resource_ids(left, ids)?;
            collect_resource_ids(right, ids)?;
        }
        Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
            if expr_column_name(left).as_deref() == Some("resource_id") {
                ids.push(parse_ulid_expr(right)?);
            }
        }
        Expr::InList { expr: col_expr, list, negated }
            if !negated && expr_column_name(col_expr).as_deref() == Some("resource_id") =>
        {
            if list.len() > MAX_IN_CLAUSE_IDS {
                return Err(SqlError::Parse(format!(
                    "IN clause too large: {} IDs (max {})",
                    list.len(),
                    MAX_IN_CLAUSE_IDS
                )));
            }
            for item in list {
                ids.push(parse_ulid_expr(item)?);
            }
        }
        _ => {}
    }
    Ok(())
}

fn extract_parent_id_filter(selection: &Expr) -> Result<Option<Ulid>, SqlError> {
    match selection {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if expr_column_name(left).as_deref() == Some("parent_id") {
                Ok(Some(parse_ulid_expr(right)?))
            } else {
                Err(SqlError::MissingFilter("parent_id"))
            }
        }
        Expr::IsNull(inner) => {
            if expr_column_name(inner).as_deref() == Some("parent_id") {
                Ok(None)
            } else {
                Err(SqlError::MissingFilter("parent_id"))
            }
        }
        _ => Err(SqlError::MissingFilter("parent_id")),
    }
}

// ── Helpers ───────────────────────────────────────────────────

fn object_name_last(name: &ast::ObjectName) -> Option<String> {
    name.0.last().and_then(|part| match part {
        ObjectNamePart::Identifier(ident) => Some(ident.value.to_lowercase()),
        _ => None,
    })
}

fn insert_table_name(insert: &ast::Insert) -> Result<String, SqlError> {
    match &insert.table {
        TableObject::TableName(name) => {
            object_name_last(name).ok_or_else(|| SqlError::Parse("empty table name".into()))
        }
        _ => Err(SqlError::Parse("unsupported table object in INSERT".into())),
    }
}

fn delete_table_name(delete: &ast::Delete) -> Result<String, SqlError> {
    let tables_with_joins = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    if let Some(first) = tables_with_joins.first() {
        table_factor_name(&first.relation)
    } else {
        Err(SqlError::Parse("DELETE without table".into()))
    }
}

fn table_factor_name(tf: &TableFactor) -> Result<String, SqlError> {
    match tf {
        TableFactor::Table { name, .. } => {
            object_name_last(name).ok_or_else(|| SqlError::Parse("empty table name".into()))
        }
        _ => Err(SqlError::Parse("complex table expression".into())),
    }
}

fn extract_column_names(insert: &ast::Insert) -> Vec<String> {
    insert.columns.iter().map(|c| c.value.to_lowercase()).collect()
}

fn extract_insert_values(insert: &ast::Insert) -> Result<Vec<Expr>, SqlError> {
    let body = insert
        .source
        .as_ref()
        .ok_or(SqlError::Parse("no VALUES".into()))?;
    match body.body.as_ref() {
        SetExpr::Values(values) => {
            if values.rows.is_empty() {
                return Err(SqlError::Parse("empty VALUES".into()));
            }
            Ok(values.rows[0].clone())
        }
        _ => Err(SqlError::Parse("expected VALUES".into())),
    }
}

/// Parse one resource VALUES row into (id, parent_id, name, capacity, buffer_after). Column-aware
/// when a column list is present, with the legacy positional order (id, parent_id, capacity,
/// buffer_after) as the fallback. Shared by the single-row (InsertResource) and multi-row
/// (BatchInsertResources) paths.
/// Resolve a declared-column value index against the actual row, returning None when the column was
/// declared but the row supplied fewer values. sqlparser accepts that column/value arity mismatch,
/// so indexing it unchecked panics on untrusted SQL, and the SQL boundary must never panic
/// (PRIN-08 / SEC-09). Optional columns become absent; a missing required column is a WrongArity.
fn cell(values: &[Expr], idx: Option<usize>) -> Option<&Expr> {
    idx.filter(|&i| i < values.len()).map(|i| &values[i])
}

fn parse_resource_row(values: &[Expr], columns: &[String]) -> Result<ResourceRow, SqlError> {
    if values.is_empty() {
        return Err(SqlError::WrongArity("resources", 1, 0));
    }
    // A declared column list must match the value count; sqlparser accepts a mismatch, so reject it
    // cleanly here rather than indexing a column position past the row (the `cell` accesses below
    // are then bounded either way, defense in depth).
    if !columns.is_empty() && values.len() != columns.len() {
        return Err(SqlError::WrongArity("resources", columns.len(), values.len()));
    }
    let col_idx = |name: &str| -> Option<usize> {
        if columns.is_empty() { None } else { columns.iter().position(|c| c == name) }
    };

    // id is required: a declared id column with no corresponding value is malformed, not a panic.
    let id = parse_ulid(
        cell(values, col_idx("id").or(Some(0)))
            .ok_or(SqlError::WrongArity("resources", 1, values.len()))?,
    )?;
    let parent_id = cell(
        values,
        col_idx("parent_id").or(if columns.is_empty() && values.len() >= 2 { Some(1) } else { None }),
    )
    .map(parse_ulid_or_null)
    .transpose()?
    .flatten();
    let name = cell(values, col_idx("name"))
        .map(parse_string_or_null)
        .transpose()?
        .flatten();
    let capacity = if let Some(e) = cell(values, col_idx("capacity")) {
        parse_u32(e)?
    } else if columns.is_empty() && values.len() >= 3 {
        parse_u32(&values[2])?
    } else {
        1
    };
    let buffer_after = if let Some(e) = cell(values, col_idx("buffer_after")) {
        parse_i64_or_null(e)?
    } else if columns.is_empty() && values.len() >= 4 {
        parse_i64_or_null(&values[3])?
    } else {
        None
    };

    Ok((id, parent_id, name, capacity, buffer_after))
}

/// Resolve one declared column's value in a VALUES row. With a column list present the value is
/// found by column name (so a reordered list lands on the right field); without one it falls back
/// to the canonical positional index. Returns None when the column/value is absent, so callers turn
/// a missing required column into a clean WrongArity rather than indexing out of bounds.
fn col_value<'a>(columns: &[String], values: &'a [Expr], name: &str, positional: usize) -> Option<&'a Expr> {
    let idx = if columns.is_empty() {
        Some(positional)
    } else {
        columns.iter().position(|c| c == name)
    };
    cell(values, idx)
}

/// A declared column list must match the value count. sqlparser accepts a mismatch, so reject it
/// cleanly here (shared by the rule/hold row parsers) rather than indexing a column position past
/// the row.
fn check_column_arity(table: &'static str, columns: &[String], values: &[Expr]) -> Result<(), SqlError> {
    if !columns.is_empty() && values.len() != columns.len() {
        return Err(SqlError::WrongArity(table, columns.len(), values.len()));
    }
    Ok(())
}

/// Parse one `rules` VALUES row into (id, resource_id, start, end, blocking), honoring a declared
/// column list. Positional order (the fallback) is (id, resource_id, start, end, blocking).
fn parse_rule_row(values: &[Expr], columns: &[String]) -> Result<(Ulid, Ulid, Ms, Ms, bool), SqlError> {
    check_column_arity("rules", columns, values)?;
    let get = |name: &str, pos: usize| {
        col_value(columns, values, name, pos).ok_or(SqlError::WrongArity("rules", 5, values.len()))
    };
    Ok((
        parse_ulid(get("id", 0)?)?,
        parse_ulid(get("resource_id", 1)?)?,
        parse_i64(get("start", 2)?)?,
        parse_i64(get("end", 3)?)?,
        parse_bool(get("blocking", 4)?)?,
    ))
}

/// Parse one `holds` VALUES row into (id, resource_id, start, end, expires_at), honoring a declared
/// column list. Positional order (the fallback) is (id, resource_id, start, end, expires_at).
fn parse_hold_row(values: &[Expr], columns: &[String]) -> Result<(Ulid, Ulid, Ms, Ms, Ms), SqlError> {
    check_column_arity("holds", columns, values)?;
    let get = |name: &str, pos: usize| {
        col_value(columns, values, name, pos).ok_or(SqlError::WrongArity("holds", 5, values.len()))
    };
    Ok((
        parse_ulid(get("id", 0)?)?,
        parse_ulid(get("resource_id", 1)?)?,
        parse_i64(get("start", 2)?)?,
        parse_i64(get("end", 3)?)?,
        parse_i64(get("expires_at", 4)?)?,
    ))
}

fn extract_all_insert_rows(insert: &ast::Insert) -> Result<Vec<Vec<Expr>>, SqlError> {
    let body = insert
        .source
        .as_ref()
        .ok_or(SqlError::Parse("no VALUES".into()))?;
    match body.body.as_ref() {
        SetExpr::Values(values) => {
            if values.rows.is_empty() {
                return Err(SqlError::Parse("empty VALUES".into()));
            }
            // Reject an over-large multi-row INSERT at the boundary (mirrors the IN-clause cap), so
            // the engine's batch cap isn't reached only after parsing+validating every row.
            if values.rows.len() > MAX_BATCH_SIZE {
                return Err(SqlError::Parse(format!(
                    "INSERT too large: {} rows (max {MAX_BATCH_SIZE})",
                    values.rows.len()
                )));
            }
            Ok(values.rows.clone())
        }
        _ => Err(SqlError::Parse("expected VALUES".into())),
    }
}

fn extract_where_id(selection: &Option<Expr>) -> Result<Ulid, SqlError> {
    let sel = selection.as_ref().ok_or(SqlError::MissingFilter("id"))?;
    match sel {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            if expr_column_name(left).as_deref() == Some("id") {
                parse_ulid_expr(right)
            } else {
                Err(SqlError::MissingFilter("id"))
            }
        }
        _ => Err(SqlError::MissingFilter("id")),
    }
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.to_lowercase()),
        _ => None,
    }
}

fn extract_value(expr: &Expr) -> Option<&Value> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => Some(value),
        _ => None,
    }
}

fn parse_ulid_expr(expr: &Expr) -> Result<Ulid, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::SingleQuotedString(s) | Value::Number(s, _) => {
                Ulid::from_string(s).map_err(|e| SqlError::Parse(format!("bad ULID: {e}")))
            }
            _ => Err(SqlError::Parse(format!("expected string, got {value:?}"))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

fn parse_i64_expr(expr: &Expr) -> Result<i64, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Number(s, _) => s
                .parse()
                .map_err(|e| SqlError::Parse(format!("bad i64: {e}"))),
            Value::SingleQuotedString(s) => s
                .parse()
                .map_err(|e| SqlError::Parse(format!("bad i64: {e}"))),
            _ => Err(SqlError::Parse(format!("expected number, got {value:?}"))),
        }
    } else if let Expr::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr,
    } = expr
    {
        // checked_neg so negating i64::MIN cannot overflow-panic. (The inner literal parse already
        // rejects i64::MAX+1, so this is belt-and-suspenders, but it makes the safety self-evident.)
        parse_i64_expr(expr)?
            .checked_neg()
            .ok_or_else(|| SqlError::Parse("integer literal out of range".into()))
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

fn parse_ulid(expr: &Expr) -> Result<Ulid, SqlError> {
    parse_ulid_expr(expr)
}

fn parse_ulid_or_null(expr: &Expr) -> Result<Option<Ulid>, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Null => Ok(None),
            Value::SingleQuotedString(s) | Value::Number(s, _) => Ok(Some(
                Ulid::from_string(s).map_err(|e| SqlError::Parse(format!("bad ULID: {e}")))?,
            )),
            _ => Err(SqlError::Parse(format!(
                "expected string or NULL, got {value:?}"
            ))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

#[allow(dead_code)]
fn parse_u16(expr: &Expr) -> Result<u16, SqlError> {
    let v = parse_i64_expr(expr)?;
    u16::try_from(v).map_err(|_| SqlError::Parse(format!("{v} out of u16 range")))
}

fn parse_u32(expr: &Expr) -> Result<u32, SqlError> {
    let v = parse_i64_expr(expr)?;
    u32::try_from(v).map_err(|_| SqlError::Parse(format!("{v} out of u32 range")))
}

fn parse_string_or_null(expr: &Expr) -> Result<Option<String>, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Null => Ok(None),
            Value::SingleQuotedString(s) => Ok(Some(s.clone())),
            _ => Err(SqlError::Parse(format!("expected string or NULL, got {value:?}"))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

fn parse_i64_or_null(expr: &Expr) -> Result<Option<i64>, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Null => Ok(None),
            _ => Ok(Some(parse_i64_expr(expr)?)),
        }
    } else {
        Ok(Some(parse_i64_expr(expr)?))
    }
}

fn parse_i64(expr: &Expr) -> Result<i64, SqlError> {
    parse_i64_expr(expr)
}

fn parse_bool(expr: &Expr) -> Result<bool, SqlError> {
    if let Some(value) = extract_value(expr) {
        match value {
            Value::Boolean(b) => Ok(*b),
            Value::SingleQuotedString(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" => Ok(true),
                "false" | "f" | "0" => Ok(false),
                _ => Err(SqlError::Parse(format!("bad bool: {s}"))),
            },
            // A numeric bool is true iff its value is nonzero. Parsing catches "0.0", "-0", "00" as
            // false (a raw `n != "0"` string compare treated all three as true).
            Value::Number(n, _) => match n.parse::<f64>() {
                Ok(v) => Ok(v != 0.0),
                Err(_) => Ok(n != "0"),
            },
            _ => Err(SqlError::Parse(format!("expected bool, got {value:?}"))),
        }
    } else {
        Err(SqlError::Parse(format!("expected value, got {expr:?}")))
    }
}

// ── Errors ────────────────────────────────────────────────────

#[derive(Debug)]
pub enum SqlError {
    Parse(String),
    Empty,
    Unsupported(String),
    UnknownTable(String),
    WrongArity(&'static str, usize, usize),
    MissingFilter(&'static str),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::Parse(s) => write!(f, "parse error: {s}"),
            SqlError::Empty => write!(f, "empty query"),
            SqlError::Unsupported(s) => write!(f, "unsupported: {s}"),
            SqlError::UnknownTable(t) => write!(f, "unknown table: {t}"),
            SqlError::WrongArity(t, expected, got) => {
                write!(f, "{t}: expected {expected} values, got {got}")
            }
            SqlError::MissingFilter(col) => write!(f, "missing filter: {col}"),
        }
    }
}

impl std::error::Error for SqlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sql_handles_extreme_integer_literals_without_panicking() {
        // Integer literals beyond i64 range must produce a clean parse error, not an overflow
        // panic (parse_i64_expr uses checked .parse()).
        for sql in [
            "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 99999999999999999999999999",
            "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= -99999999999999999999999999",
            r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW', 9223372036854775808, 1)"#,
        ] {
            let _ = parse_sql(sql);
        }
    }

    #[test]
    fn parse_sql_never_panics_on_arbitrary_input() {
        use proptest::prelude::*;
        // The SQL boundary is untrusted: parse_sql must return Ok or Err for ANY input, never
        // panic. Bias the strategy toward SQL-ish tokens, quotes, $-placeholders, and digit runs.
        proptest!(ProptestConfig::with_cases(2000), |(s in r#"[A-Za-z0-9 '"$(),=*;_-]{0,64}"#)| {
            let _ = parse_sql(&s);
        });
    }

    #[test]
    fn parse_availability_in_list_routes_by_min_available() {
        let a = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let b = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

        // IN list WITHOUT min_available => per-resource (rows tagged with resource_id).
        let per_resource = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{a}', '{b}') AND start >= 0 AND end <= 100"
        );
        match parse_sql(&per_resource).unwrap() {
            Command::SelectAvailabilityMulti { resource_ids, .. } => {
                assert_eq!(resource_ids.len(), 2);
            }
            other => panic!("expected SelectAvailabilityMulti, got {other:?}"),
        }

        // IN list WITH min_available => merged intersection.
        let merged = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{a}', '{b}') AND start >= 0 AND end <= 100 AND min_available = 2"
        );
        match parse_sql(&merged).unwrap() {
            Command::SelectMultiAvailability { min_available, .. } => {
                assert_eq!(min_available, 2);
            }
            other => panic!("expected SelectMultiAvailability, got {other:?}"),
        }
    }

    #[test]
    fn only_min_available_eq_routes_to_merged() {
        // The merged form is selected ONLY by `min_available = N` (Eq, column on the left). Any
        // other form (`>`, `>=`, reversed `N = min_available`) routes per-resource. The Describe
        // schema (wire::schema_for_sql) classifies with the same `availability_shape`, so the
        // announced column set matches the produced rows. See the wire cross-check test.
        let a = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let b = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let prefix = format!("SELECT * FROM availability WHERE resource_id IN ('{a}', '{b}')");

        for clause in ["min_available > 1", "min_available >= 1", "2 = min_available"] {
            let sql = format!("{prefix} AND {clause} AND start >= 0 AND end <= 100");
            assert!(
                matches!(parse_sql(&sql).unwrap(), Command::SelectAvailabilityMulti { .. }),
                "non-Eq min_available form must route per-resource: {clause}",
            );
        }
    }

    #[test]
    fn parse_insert_resource() {
        let sql = "INSERT INTO resources (id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV')";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { id, parent_id, name: _, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(parent_id, None);
                assert_eq!(capacity, 1);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_parent() {
        let sql = "INSERT INTO resources (id, parent_id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV')";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { id, parent_id, name: _, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(parent_id, Some(id));
                assert_eq!(capacity, 1);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_null_parent() {
        let sql = "INSERT INTO resources (id, parent_id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { parent_id, .. } => {
                assert_eq!(parent_id, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_capacity_and_buffer() {
        let sql = "INSERT INTO resources (id, parent_id, capacity, buffer_after) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 20, 1800000)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { capacity, buffer_after, .. } => {
                assert_eq!(capacity, 20);
                assert_eq!(buffer_after, Some(1800000));
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_capacity_only() {
        let sql = "INSERT INTO resources (id, parent_id, capacity) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 5)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { capacity, buffer_after, .. } => {
                assert_eq!(capacity, 5);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_multi_row_resources_insert_is_batch() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAW";
        let multi =
            format!("INSERT INTO resources (id, capacity) VALUES ('{id1}', 5), ('{id2}', 3)");
        match parse_sql(&multi).unwrap() {
            Command::BatchInsertResources { resources } => {
                assert_eq!(resources.len(), 2);
                assert_eq!(resources[0].3, 5); // capacity
                assert_eq!(resources[1].3, 3);
            }
            other => panic!("expected BatchInsertResources, got {other:?}"),
        }
        // A single-row INSERT still produces InsertResource.
        let one = format!("INSERT INTO resources (id) VALUES ('{id1}')");
        assert!(matches!(parse_sql(&one).unwrap(), Command::InsertResource { .. }));
    }

    #[test]
    fn parse_resource_insert_column_arity_mismatch_does_not_panic() {
        // Declared 5 columns, supplied 1 value: sqlparser accepts it; the parser must return a
        // clean error, never index out of bounds (the SQL boundary is untrusted, PRIN-08/SEC-09).
        let sql = "INSERT INTO resources (id, parent_id, name, capacity, buffer_after) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV')";
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_booking_insert_label_arity_mismatch_does_not_panic() {
        // Declared a label column but supplied only 4 values: must not index values[4]. The
        // declared/value arity mismatch is a clean WrongArity error, never a panic.
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW', 1000, 2000)"#;
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_over_batch_size_insert_is_rejected() {
        // A multi-row INSERT beyond the batch cap is rejected at the boundary, not after parsing
        // and validating every row.
        let r = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let rows: String = (0..=MAX_BATCH_SIZE)
            .map(|_| format!("('{r}', '{r}', 0, 1)"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES {rows}"#);
        assert!(parse_sql(&sql).is_err());
    }

    #[test]
    fn parse_resource_insert_arity_fuzz_never_panics() {
        use proptest::prelude::*;
        let cols = ["id", "parent_id", "name", "capacity", "buffer_after"];
        // Any declared-column count vs value count must yield Ok or Err, never a panic.
        proptest!(ProptestConfig::with_cases(400), |(ncols in 1usize..=5, nvals in 0usize..=5)| {
            let collist = cols[..ncols].join(", ");
            let vallist = (0..nvals)
                .map(|_| "'01ARZ3NDEKTSV4RRFFQ69G5FAV'")
                .collect::<Vec<_>>()
                .join(", ");
            let _ = parse_sql(&format!("INSERT INTO resources ({collist}) VALUES ({vallist})"));
        });
    }

    #[test]
    fn parse_delete_resource() {
        let sql = "DELETE FROM resources WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::DeleteResource { id } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected DeleteResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_rule() {
        let sql = r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, false)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertRule {
                start,
                end,
                blocking,
                ..
            } => {
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert!(!blocking);
            }
            _ => panic!("expected InsertRule, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_multi_row_rules_insert_is_batch() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAW";
        let r = "01CRZ3NDEKTSV4RRFFQ69G5FAX";
        let multi = format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{id1}', '{r}', 0, 100, false), ('{id2}', '{r}', 200, 300, true)"#
        );
        match parse_sql(&multi).unwrap() {
            Command::BatchInsertRules { rules } => {
                assert_eq!(rules.len(), 2);
                assert!(!rules[0].4); // first blocking=false
                assert!(rules[1].4); // second blocking=true
            }
            other => panic!("expected BatchInsertRules, got {other:?}"),
        }
        // A single-row INSERT still produces InsertRule, not a batch.
        let one = format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{id1}', '{r}', 0, 100, false)"#
        );
        assert!(matches!(parse_sql(&one).unwrap(), Command::InsertRule { .. }));
    }

    #[test]
    fn parse_insert_hold() {
        let sql = r#"INSERT INTO holds (id, resource_id, start, "end", expires_at) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, 3000)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertHold {
                start,
                end,
                expires_at,
                ..
            } => {
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert_eq!(expires_at, 3000);
            }
            _ => panic!("expected InsertHold, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000)"#;
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::InsertBooking { .. }));
    }

    #[test]
    fn parse_insert_rule_honors_reordered_columns() {
        // resource_id declared before id. Both are ULIDs, so a positional parse would silently swap
        // them; the column-aware parse must land each value on its named field.
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let res = "01BRZ3NDEKTSV4RRFFQ69G5FAW";
        let sql = format!(
            r#"INSERT INTO rules (resource_id, id, start, "end", blocking) VALUES ('{res}', '{id}', 0, 100, true)"#
        );
        match parse_sql(&sql).unwrap() {
            Command::InsertRule { id: pid, resource_id, start, end, blocking } => {
                assert_eq!(pid.to_string(), id);
                assert_eq!(resource_id.to_string(), res);
                assert_eq!(start, 0);
                assert_eq!(end, 100);
                assert!(blocking);
            }
            cmd => panic!("expected InsertRule, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_batch_insert_rules_honors_reordered_columns() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAW";
        let res = "01CRZ3NDEKTSV4RRFFQ69G5FAX";
        let sql = format!(
            r#"INSERT INTO rules (resource_id, id, start, "end", blocking) VALUES ('{res}', '{id1}', 0, 100, false), ('{res}', '{id2}', 200, 300, true)"#
        );
        match parse_sql(&sql).unwrap() {
            Command::BatchInsertRules { rules } => {
                assert_eq!(rules.len(), 2);
                assert_eq!(rules[0].0.to_string(), id1); // id
                assert_eq!(rules[0].1.to_string(), res); // resource_id
                assert_eq!(rules[1].0.to_string(), id2);
            }
            cmd => panic!("expected BatchInsertRules, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_hold_honors_reordered_columns() {
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let res = "01BRZ3NDEKTSV4RRFFQ69G5FAW";
        let sql = format!(
            r#"INSERT INTO holds (resource_id, id, start, "end", expires_at) VALUES ('{res}', '{id}', 1000, 2000, 3000)"#
        );
        match parse_sql(&sql).unwrap() {
            Command::InsertHold { id: hid, resource_id, start, end, expires_at } => {
                assert_eq!(hid.to_string(), id);
                assert_eq!(resource_id.to_string(), res);
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert_eq!(expires_at, 3000);
            }
            cmd => panic!("expected InsertHold, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_rule_column_arity_mismatch_does_not_panic() {
        // Declared 5 columns, supplied 4 values: sqlparser accepts it; the parser must return a
        // clean WrongArity, never index out of bounds.
        let sql = r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAW', 0, 100)"#;
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_delete_hold() {
        let sql = "DELETE FROM holds WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::DeleteHold { .. }));
    }

    #[test]
    fn parse_select_availability() {
        let sql = "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 1000 AND \"end\" <= 2000";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectAvailability {
                resource_id,
                start,
                end,
                min_duration,
            } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert_eq!(min_duration, None);
            }
            _ => panic!("expected SelectAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_availability_with_min_duration() {
        let sql = "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 1000 AND \"end\" <= 2000 AND min_duration = 1800000";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectAvailability { min_duration, .. } => {
                assert_eq!(min_duration, Some(1800000));
            }
            _ => panic!("expected SelectAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_listen() {
        let sql = "LISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unknown_table_errors() {
        let sql = "INSERT INTO foobar (id) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV')";
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_batch_insert_bookings() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000), ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 3000, 4000)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::BatchInsertBookings { bookings } => {
                assert_eq!(bookings.len(), 2);
                assert_eq!(bookings[0].2, 1000);
                assert_eq!(bookings[0].3, 2000);
                assert_eq!(bookings[1].2, 3000);
                assert_eq!(bookings[1].3, 4000);
            }
            _ => panic!("expected BatchInsertBookings, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_single_insert_booking_not_batch() {
        // A single-row INSERT should still produce InsertBooking, not BatchInsertBookings
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000)"#;
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::InsertBooking { .. }));
    }

    #[test]
    fn parse_select_multi_availability() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}', '{id2}') AND start >= 1000 AND \"end\" <= 2000 AND min_available = 2"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                start,
                end,
                min_available,
                min_duration,
            } => {
                assert_eq!(resource_ids.len(), 2);
                assert_eq!(resource_ids[0].to_string(), id1);
                assert_eq!(resource_ids[1].to_string(), id2);
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
                assert_eq!(min_available, 2); // explicit min_available => merged intersection
                assert_eq!(min_duration, None);
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_multi_availability_with_min_available() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let id3 = "01CRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}', '{id2}', '{id3}') AND start >= 1000 AND \"end\" <= 5000 AND min_available = 1"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                min_available,
                ..
            } => {
                assert_eq!(resource_ids.len(), 3);
                assert_eq!(min_available, 1); // ANY
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_single_id_in_list_is_per_resource() {
        // A single id in an IN list with no min_available is the per-resource form (tagged rows),
        // same routing as a multi-id IN list.
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}') AND start >= 0 AND \"end\" <= 10000"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectAvailabilityMulti { resource_ids, .. } => {
                assert_eq!(resource_ids.len(), 1);
            }
            _ => panic!("expected SelectAvailabilityMulti, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_multi_availability_with_min_duration() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ('{id1}', '{id2}') AND start >= 0 AND \"end\" <= 10000 AND min_available = 1 AND min_duration = 3600000"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                min_available,
                min_duration,
                ..
            } => {
                assert_eq!(resource_ids.len(), 2);
                assert_eq!(min_available, 1);
                assert_eq!(min_duration, Some(3600000));
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_single_resource_still_works() {
        // resource_id = '...' (not IN) should still produce SelectAvailability
        let sql = "SELECT * FROM availability WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV' AND start >= 1000 AND \"end\" <= 2000";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::SelectAvailability { .. }));
    }

    #[test]
    fn parse_select_multi_large_in_list() {
        // 5 IDs in the IN list
        let ids: Vec<String> = (0..5).map(|i| format!("01ARZ3NDEKTSV4RRFFQ69G5FA{i}")).collect();
        let in_list = ids.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ({in_list}) AND start >= 0 AND \"end\" <= 100000 AND min_available = 3"
        );
        let cmd = parse_sql(&sql).unwrap();
        match cmd {
            Command::SelectMultiAvailability {
                resource_ids,
                min_available,
                ..
            } => {
                assert_eq!(resource_ids.len(), 5);
                assert_eq!(min_available, 3);
            }
            _ => panic!("expected SelectMultiAvailability, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_empty_errors() {
        assert!(matches!(parse_sql(""), Err(SqlError::Empty)));
    }

    #[test]
    fn parse_multi_statement_is_rejected() {
        // Two statements in one query must be rejected, not silently reduced to the first with a
        // success reply.
        let a = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!(
            "INSERT INTO resources (id) VALUES ('{a}'); INSERT INTO resources (id) VALUES ('{a}')"
        );
        assert!(matches!(parse_sql(&sql), Err(SqlError::Unsupported(_))));
    }

    // ── SELECT resources ─────────────────────────────────────────

    #[test]
    fn parse_select_resources_all() {
        let cmd = parse_sql("SELECT * FROM resources").unwrap();
        match cmd {
            Command::SelectResources { parent_id } => assert_eq!(parent_id, None),
            _ => panic!("expected SelectResources, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_resources_by_parent_id() {
        let sql = "SELECT * FROM resources WHERE parent_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectResources { parent_id } => {
                let uid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
                assert_eq!(parent_id, Some(Some(uid)));
            }
            _ => panic!("expected SelectResources, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_resources_roots_only() {
        let sql = "SELECT * FROM resources WHERE parent_id IS NULL";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectResources { parent_id } => {
                assert_eq!(parent_id, Some(None)); // Some(None) = root only
            }
            _ => panic!("expected SelectResources, got {cmd:?}"),
        }
    }

    // ── SELECT rules/bookings/holds ──────────────────────────────

    #[test]
    fn parse_select_rules() {
        let sql = "SELECT * FROM rules WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectRules { resource_id } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected SelectRules, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_bookings() {
        let sql = "SELECT * FROM bookings WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectBookings { resource_id } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected SelectBookings, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_holds() {
        let sql = "SELECT * FROM holds WHERE resource_id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::SelectHolds { resource_id } => {
                assert_eq!(resource_id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected SelectHolds, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_bookings_multi() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!("SELECT * FROM bookings WHERE resource_id IN ('{id1}', '{id2}')");
        match parse_sql(&sql).unwrap() {
            Command::SelectBookingsMulti { resource_ids } => {
                assert_eq!(resource_ids.len(), 2);
                assert_eq!(resource_ids[0].to_string(), id1);
                assert_eq!(resource_ids[1].to_string(), id2);
            }
            cmd => panic!("expected SelectBookingsMulti, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_holds_multi() {
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let id2 = "01BRZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!("SELECT * FROM holds WHERE resource_id IN ('{id1}', '{id2}')");
        match parse_sql(&sql).unwrap() {
            Command::SelectHoldsMulti { resource_ids } => {
                assert_eq!(resource_ids.len(), 2);
            }
            cmd => panic!("expected SelectHoldsMulti, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_bookings_single_in_list_collapses() {
        // A one-element IN list is equivalent to `=`, so it stays single-resource.
        let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let sql = format!("SELECT * FROM bookings WHERE resource_id IN ('{id1}')");
        match parse_sql(&sql).unwrap() {
            Command::SelectBookings { resource_id } => assert_eq!(resource_id.to_string(), id1),
            cmd => panic!("expected SelectBookings, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_select_holds_multi_too_many_ids() {
        // MAX_IN_CLAUSE_IDS is 200 under cfg(test); 201 ids must be rejected at parse time.
        let ids = (0..=MAX_IN_CLAUSE_IDS)
            .map(|_| format!("'{}'", Ulid::new()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT * FROM holds WHERE resource_id IN ({ids})");
        assert!(parse_sql(&sql).is_err());
    }

    #[test]
    fn parse_select_rules_missing_filter() {
        let sql = "SELECT * FROM rules";
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_select_bookings_missing_filter() {
        let sql = "SELECT * FROM bookings";
        assert!(parse_sql(sql).is_err());
    }

    // ── UPDATE resources ─────────────────────────────────────────

    #[test]
    fn parse_update_resource_name_and_capacity() {
        let sql = "UPDATE resources SET name = 'Meeting Room A', capacity = 5 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateResource { id, name, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(name, Some(Some("Meeting Room A".to_string())));
                assert_eq!(capacity, Some(5));
                // buffer_after absent from the SET list => None (leave unchanged), NOT Some(None).
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected UpdateResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_update_resource_with_buffer() {
        let sql = "UPDATE resources SET capacity = 10, buffer_after = 900000 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateResource { name, capacity, buffer_after, .. } => {
                assert_eq!(name, None); // name absent => unchanged
                assert_eq!(capacity, Some(10));
                assert_eq!(buffer_after, Some(Some(900000)));
            }
            _ => panic!("expected UpdateResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_update_resource_only_buffer_leaves_others_absent() {
        // The regression at the parser boundary: a partial update must emit None for the columns it
        // does not mention, so the engine leaves name and capacity intact.
        let sql = "UPDATE resources SET buffer_after = 600000 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        match parse_sql(sql).unwrap() {
            Command::UpdateResource { name, capacity, buffer_after, .. } => {
                assert_eq!(name, None);
                assert_eq!(capacity, None);
                assert_eq!(buffer_after, Some(Some(600000)));
            }
            cmd => panic!("expected UpdateResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_update_resource_null_buffer() {
        let sql = "UPDATE resources SET buffer_after = NULL WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateResource { buffer_after, .. } => {
                // Explicit NULL => Some(None) (set to NULL), distinct from absent (None).
                assert_eq!(buffer_after, Some(None));
            }
            _ => panic!("expected UpdateResource, got {cmd:?}"),
        }
    }

    // ── UPDATE rules ─────────────────────────────────────────────

    #[test]
    fn parse_update_rule() {
        let sql = r#"UPDATE rules SET start = 5000, "end" = 10000, blocking = true WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::UpdateRule { id, start, end, blocking } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(start, 5000);
                assert_eq!(end, 10000);
                assert!(blocking);
            }
            _ => panic!("expected UpdateRule, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_update_rule_missing_field() {
        // Missing blocking field should error
        let sql = r#"UPDATE rules SET start = 5000, "end" = 10000 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'"#;
        assert!(parse_sql(sql).is_err());
    }

    #[test]
    fn parse_update_unknown_table() {
        let sql = "UPDATE foobar SET x = 1 WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        assert!(parse_sql(sql).is_err());
    }

    // ── INSERT with name / label ─────────────────────────────────

    #[test]
    fn parse_insert_resource_with_name() {
        let sql = "INSERT INTO resources (id, parent_id, name, capacity, buffer_after) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 'Room 101', 3, NULL)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { id, parent_id, name, capacity, buffer_after } => {
                assert_eq!(id.to_string(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
                assert_eq!(parent_id, None);
                assert_eq!(name, Some("Room 101".to_string()));
                assert_eq!(capacity, 3);
                assert_eq!(buffer_after, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_with_null_name() {
        let sql = "INSERT INTO resources (id, parent_id, name, capacity) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, NULL, 1)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { name, .. } => {
                assert_eq!(name, None);
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_resource_positional_backward_compat() {
        // Old positional format without column names: (id, parent_id, capacity, buffer_after)
        let sql = "INSERT INTO resources VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 20, 1800000)";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertResource { capacity, buffer_after, name, .. } => {
                assert_eq!(capacity, 20);
                assert_eq!(buffer_after, Some(1800000));
                assert_eq!(name, None); // no name in positional format
            }
            _ => panic!("expected InsertResource, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking_with_label() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, 'Team Meeting')"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertBooking { label, start, end, .. } => {
                assert_eq!(label, Some("Team Meeting".to_string()));
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
            }
            _ => panic!("expected InsertBooking, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking_with_null_label() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, NULL)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertBooking { label, .. } => {
                assert_eq!(label, None);
            }
            _ => panic!("expected InsertBooking, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_insert_booking_without_label() {
        // No label column at all, should default to None
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end") VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::InsertBooking { label, .. } => {
                assert_eq!(label, None);
            }
            _ => panic!("expected InsertBooking, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_batch_insert_bookings_with_labels() {
        let sql = r#"INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 1000, 2000, 'Morning'), ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '01ARZ3NDEKTSV4RRFFQ69G5FAV', 3000, 4000, NULL)"#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::BatchInsertBookings { bookings } => {
                assert_eq!(bookings.len(), 2);
                assert_eq!(bookings[0].4, Some("Morning".to_string()));
                assert_eq!(bookings[1].4, None);
            }
            _ => panic!("expected BatchInsertBookings, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_delete_booking() {
        let sql = "DELETE FROM bookings WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::DeleteBooking { .. }));
    }

    #[test]
    fn parse_delete_rule() {
        let sql = "DELETE FROM rules WHERE id = '01ARZ3NDEKTSV4RRFFQ69G5FAV'";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::DeleteRule { .. }));
    }

    #[test]
    fn in_clause_too_many_ids() {
        let ids: Vec<String> = (0..MAX_IN_CLAUSE_IDS + 1)
            .map(|_| format!("'{}'", ulid::Ulid::new()))
            .collect();
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ({}) AND start >= 0 AND \"end\" <= 10000",
            ids.join(", ")
        );
        let result = parse_sql(&sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("IN clause too large"));
    }

    #[test]
    fn listen_fast_path_non_ascii_prefix_does_not_panic() {
        // A multibyte char around the keyword boundary must not byte-slice-panic the fast path.
        for s in ["LISTEN Ωchan", "listen Ω", "Ωlisten x", "listenΩ x", "UNLISTEN Ω", "ßunlisten"] {
            let _ = parse_sql(s);
        }
    }

    #[test]
    fn parse_rule_numeric_blocking_zero_forms_are_false() {
        // The number arm of parse_bool is true only for a nonzero value; "0", "0.0" and "00" are
        // all false (a raw string compare treated "0.0"/"00" as true).
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let r = "01BRZ3NDEKTSV4RRFFQ69G5FAW";
        for (lit, expected) in [("0", false), ("0.0", false), ("00", false), ("1", true), ("2", true)] {
            let sql = format!(
                r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{id}', '{r}', 0, 100, {lit})"#
            );
            match parse_sql(&sql).unwrap() {
                Command::InsertRule { blocking, .. } => assert_eq!(blocking, expected, "lit={lit}"),
                cmd => panic!("expected InsertRule, got {cmd:?}"),
            }
        }
    }

    #[test]
    fn parse_unlisten() {
        let sql = "UNLISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Unlisten { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Unlisten, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unlisten_all() {
        let sql = "UNLISTEN *";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::UnlistenAll));
    }

    #[test]
    fn parse_unlisten_with_semicolon() {
        let sql = "UNLISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV;";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Unlisten { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Unlisten, got {cmd:?}"),
        }
    }

    #[test]
    fn in_clause_at_limit() {
        let ids: Vec<String> = (0..MAX_IN_CLAUSE_IDS)
            .map(|_| format!("'{}'", ulid::Ulid::new()))
            .collect();
        let sql = format!(
            "SELECT * FROM availability WHERE resource_id IN ({}) AND start >= 0 AND \"end\" <= 10000",
            ids.join(", ")
        );
        let result = parse_sql(&sql);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_listen_case_insensitive() {
        let sql = "listen resource_01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_listen_with_semicolon() {
        let sql = "LISTEN resource_01ARZ3NDEKTSV4RRFFQ69G5FAV;";
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unlisten_all_with_semicolon() {
        let sql = "UNLISTEN *;";
        let cmd = parse_sql(sql).unwrap();
        assert!(matches!(cmd, Command::UnlistenAll));
    }

    #[test]
    fn parse_listen_quoted_channel() {
        // postgres.js sends LISTEN "channel_name" with double quotes
        let sql = r#"LISTEN "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV""#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Listen { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Listen, got {cmd:?}"),
        }
    }

    #[test]
    fn parse_unlisten_quoted_channel() {
        let sql = r#"UNLISTEN "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV""#;
        let cmd = parse_sql(sql).unwrap();
        match cmd {
            Command::Unlisten { channel } => {
                assert_eq!(channel, "resource_01ARZ3NDEKTSV4RRFFQ69G5FAV");
            }
            _ => panic!("expected Unlisten, got {cmd:?}"),
        }
    }
}
