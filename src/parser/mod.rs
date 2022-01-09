mod macros;
mod patterns;
mod utils;

use self::patterns::capture_command;
use self::utils::*;
use crate::{
    database::{Attribute, Comparison, DataAttributes, DatabaseTable},
    error::UserError,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[non_exhaustive]
#[derive(PartialEq)]
pub enum Command {
    Create { name: String, attributes: Vec<Attribute> },
    Insert { table_name: String, data: DataAttributes },
    Delete { table_name: String, attr_pos: usize, comparison: Comparison },
    Select { table_name: String, attr_pos: usize, comparison: Comparison, selected: Vec<usize> },
    Drop { name: String },
}

const QUOTES: &[char] = &['\'', '"'];

pub async fn get_command(
    input: &str,
    tables: Arc<RwLock<HashMap<String, DatabaseTable>>>,
) -> Result<Command, UserError> {
    let lowercased = capture_command(input)
        .into_iter()
        .map(|s| if s.starts_with(QUOTES) { s.to_owned() } else { s.to_ascii_lowercase() })
        .collect::<Vec<_>>();

    let command = match lowercased.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["create", "table", table, attrs @ ..] => {
            Command::Create { name: table.to_string(), attributes: parse_attributes(attrs)? }
        }
        ["create", "index", _index, "on", _table, _cols @ ..] => todo!(),
        ["select", cols @ .., "from", table] => make_select_command(
            table,
            tables.read().await.get(*table).ok_or(UserError::Other("No such table"))?,
            cols,
            None,
        )?,
        ["select", cols @ .., "from", table, "where", a, cmp, b] => make_select_command(
            table,
            tables.read().await.get(*table).ok_or(UserError::Other("No such table"))?,
            cols,
            Some((a, cmp, b)),
        )?,
        ["insert", "into", table, "where", values @ ..] => Command::Insert {
            table_name: table.to_string(),
            data: DataAttributes {
                attributes: parse_values(
                    values,
                    tables.read().await.get(*table).ok_or(UserError::Other("No such table"))?,
                )?,
            },
        },
        ["delete", "from", table, "where", a, cmp, b] => {
            let (attr_pos, comparison) = parse_comparison(
                a,
                cmp,
                b,
                tables.read().await.get(*table).ok_or(UserError::Other("No such table"))?,
            )?;
            Command::Delete { table_name: table.to_string(), attr_pos, comparison }
        }
        ["drop", "table", table] => Command::Drop { name: table.to_string() },
        [] => return Err(UserError::SyntaxError),
        _ => unreachable!(),
    };
    Ok(command)
}

fn make_select_command(
    table_name: &str,
    table: &DatabaseTable,
    cols: &[&str],
    where_clause: Option<(&str, &str, &str)>,
) -> Result<Command, UserError> {
    let table_name = table_name.to_owned();
    let selected = parse_selected(cols, table)?;
    if let Some((lhs, cmp, rhs)) = where_clause {
        let (attr_pos, comparison) = parse_comparison(lhs, cmp, rhs, table)?;
        Ok(Command::Select { table_name, selected, comparison, attr_pos })
    } else {
        Ok(Command::Select { table_name, selected, comparison: Comparison::All, attr_pos: 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{AttributeType, Database};

    #[tokio::test]
    async fn parse_command() -> Result<(), UserError> {
        let db = Database::default();
        let command = get_command("create table x ( a int, b int primary key )", db.tables).await?;
        let expected = Command::Create {
            name: "x".to_string(),
            attributes: vec![
                Attribute { name: "a".to_string(), attribute_type: AttributeType::Number },
                Attribute { name: "b".to_string(), attribute_type: AttributeType::Id },
            ],
        };
        assert!(command == expected);
        Ok(())
    }
}
