use serde::{Deserialize, Serialize};
use sqlparser::ast::{Cte, Query, SetExpr, Statement, TableFactor, TableWithJoins, With};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AstSelector {
    pub operation: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryAstMeta {
    pub operations: Vec<String>,
    pub tables: Vec<String>,
}

impl QueryAstMeta {
    pub fn matches(&self, selector: &AstSelector) -> bool {
        if let Some(target_op) = &selector.operation {
            let has_op = self
                .operations
                .iter()
                .any(|op| op.eq_ignore_ascii_case(target_op));
            if !has_op {
                return false;
            }
        }

        if let Some(target_table) = &selector.table {
            let target_norm = target_table.to_lowercase();
            let matches_table = self.tables.iter().any(|t| {
                t.eq_ignore_ascii_case(&target_norm) || t.ends_with(&format!(".{}", target_norm))
            });
            if !matches_table {
                return false;
            }
        }

        true
    }
}

pub fn extract_ast_metadata(sql: &str) -> Option<QueryAstMeta> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    if statements.is_empty() {
        return None;
    }

    let mut meta = QueryAstMeta::default();

    for stmt in &statements {
        collect_stmt_meta(stmt, &mut meta);
    }

    if meta.operations.is_empty() && meta.tables.is_empty() {
        None
    } else {
        Some(meta)
    }
}

fn collect_stmt_meta(stmt: &Statement, meta: &mut QueryAstMeta) {
    match stmt {
        Statement::Query(query) => {
            collect_query_meta(query, meta);
        }
        Statement::Insert(insert) => {
            meta.operations.push("INSERT".to_string());
            meta.tables
                .push(normalize_object_name(&insert.table.to_string()));
        }
        Statement::Update { table, .. } => {
            meta.operations.push("UPDATE".to_string());
            collect_tables_from_relation(table, &mut meta.tables);
        }
        Statement::Delete(delete) => {
            meta.operations.push("DELETE".to_string());
            match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                    for twj in tables {
                        collect_tables_from_relation(twj, &mut meta.tables);
                    }
                }
            }
        }
        _ => {}
    }
}

fn collect_query_meta(query: &Query, meta: &mut QueryAstMeta) {
    if let Some(ref with) = query.with {
        collect_with_meta(with, meta);
    }

    meta.operations.push("SELECT".to_string());

    if let SetExpr::Select(select) = query.body.as_ref() {
        for table_with_joins in &select.from {
            collect_tables_from_relation(table_with_joins, &mut meta.tables);
        }
    }
}

fn collect_with_meta(with: &With, meta: &mut QueryAstMeta) {
    for cte in &with.cte_tables {
        collect_cte_meta(cte, meta);
    }
}

fn collect_cte_meta(cte: &Cte, meta: &mut QueryAstMeta) {
    collect_query_meta(&cte.query, meta);
}

fn collect_tables_from_relation(twj: &TableWithJoins, tables: &mut Vec<String>) {
    collect_from_factor(&twj.relation, tables);
    for join in &twj.joins {
        collect_from_factor(&join.relation, tables);
    }
}

fn collect_from_factor(factor: &TableFactor, tables: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            tables.push(normalize_object_name(&name.to_string()));
        }
        TableFactor::Derived { subquery, .. } => {
            if let SetExpr::Select(select) = subquery.body.as_ref() {
                for twj in &select.from {
                    collect_tables_from_relation(twj, tables);
                }
            }
        }
        _ => {}
    }
}

fn normalize_object_name(name: &str) -> String {
    name.replace('"', "").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_multi_statement() {
        let sql = "SELECT 1; UPDATE accounts SET balance = balance - 10 WHERE id = 1;";
        let meta = extract_ast_metadata(sql).expect("Failed to parse SQL");
        assert!(meta.matches(&AstSelector {
            operation: Some("UPDATE".to_string()),
            table: Some("accounts".to_string()),
        }));
    }

    #[test]
    fn test_extract_update_with_schema_and_quotes() {
        let sql = r#"UPDATE "public"."accounts" SET balance = balance - 10 WHERE id = 1;"#;
        let meta = extract_ast_metadata(sql).expect("Failed to parse SQL");
        assert!(meta.matches(&AstSelector {
            operation: Some("UPDATE".to_string()),
            table: Some("accounts".to_string()),
        }));
    }

    #[test]
    fn test_extract_cte_query() {
        let sql = "WITH u AS (SELECT * FROM accounts) SELECT * FROM u;";
        let meta = extract_ast_metadata(sql).expect("Failed to parse SQL");
        assert!(meta.matches(&AstSelector {
            operation: Some("SELECT".to_string()),
            table: Some("accounts".to_string()),
        }));
    }
}
