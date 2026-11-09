use serde::{Deserialize, Serialize};
use sqlparser::ast::{SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AstSelector {
    pub operation: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryAstMeta {
    pub operation: Option<String>,
    pub tables: Vec<String>,
}

impl QueryAstMeta {
    pub fn matches(&self, selector: &AstSelector) -> bool {
        if let Some(target_op) = &selector.operation {
            match &self.operation {
                Some(op) if op.eq_ignore_ascii_case(target_op) => {}
                _ => return false,
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
    let stmt = statements.first()?;

    let mut meta = QueryAstMeta::default();

    match stmt {
        Statement::Query(query) => {
            meta.operation = Some("SELECT".to_string());
            if let SetExpr::Select(select) = query.body.as_ref() {
                for table_with_joins in &select.from {
                    collect_tables_from_relation(table_with_joins, &mut meta.tables);
                }
            }
        }
        Statement::Insert(insert) => {
            meta.operation = Some("INSERT".to_string());
            let table_name = normalize_object_name(&insert.table.to_string());
            meta.tables.push(table_name);
        }
        Statement::Update { table, .. } => {
            meta.operation = Some("UPDATE".to_string());
            collect_tables_from_relation(table, &mut meta.tables);
        }
        Statement::Delete(delete) => {
            meta.operation = Some("DELETE".to_string());
            match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                    for twj in tables {
                        collect_tables_from_relation(twj, &mut meta.tables);
                    }
                }
            }
        }
        _ => return None,
    }

    Some(meta)
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
    fn test_extract_update_with_schema_and_quotes() {
        let sql = r#"UPDATE "public"."accounts" SET balance = balance - 10 WHERE id = 1;"#;
        let meta = extract_ast_metadata(sql).expect("Failed to parse SQL");
        assert_eq!(meta.operation.as_deref(), Some("UPDATE"));
        assert!(meta.matches(&AstSelector {
            operation: Some("UPDATE".to_string()),
            table: Some("accounts".to_string()),
        }));
    }

    #[test]
    fn test_extract_insert() {
        let sql = r#"INSERT INTO "users" (id, name) VALUES (1, 'Alice');"#;
        let meta = extract_ast_metadata(sql).expect("Failed to parse SQL");
        assert_eq!(meta.operation.as_deref(), Some("INSERT"));
        assert!(meta.matches(&AstSelector {
            operation: Some("INSERT".to_string()),
            table: Some("users".to_string()),
        }));
    }

    #[test]
    fn test_extract_select_join() {
        let sql = "SELECT * FROM orders JOIN users ON orders.user_id = users.id";
        let meta = extract_ast_metadata(sql).expect("Failed to parse SQL");
        assert_eq!(meta.operation.as_deref(), Some("SELECT"));
        assert!(meta.matches(&AstSelector {
            operation: Some("SELECT".to_string()),
            table: Some("orders".to_string()),
        }));
        assert!(meta.matches(&AstSelector {
            operation: Some("SELECT".to_string()),
            table: Some("users".to_string()),
        }));
        assert!(!meta.matches(&AstSelector {
            operation: Some("SELECT".to_string()),
            table: Some("products".to_string()),
        }));
    }
}
