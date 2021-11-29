use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::{database::{Attribute, Comparison, DataAttributes, DatabaseTable}, error::ParseError};

#[non_exhaustive]
pub enum Command {
    Create { name: String, attributes: Vec<Attribute> },
    Insert { table_name: String, data: DataAttributes },
    Delete { table_name: String, attr_pos: usize, comparison: Comparison },
    Select { table_name: String, attr_pos: usize, comparison: Comparison, selected: Vec<usize> },
    Drop { name: String },
}

pub async fn parse(input: &str, tables: Arc<RwLock<HashMap<String, DatabaseTable>>>) -> Result<Command, ParseError> {
    todo!()
}
