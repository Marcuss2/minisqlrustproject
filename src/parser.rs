use crate::database::{Attribute, Comparison, DataAttributes, DatabaseTable};

#[non_exhaustive]
pub enum Command {
    Create { name: String, attributes: Vec<Attribute> },
    Insert { table_name: String, data: DataAttributes },
    Delete { table_name: String, attr_pos: usize, comparison: Comparison },
    Select { table_name: String, attr_pos: usize, comparison: Comparison, selected: Vec<usize> },
    Drop { name: String },
}

pub fn parse(input: &str, tables: &[DatabaseTable]) -> Vec<Command> {
    todo!()
}
