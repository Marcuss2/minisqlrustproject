use tokio::net::TcpStream;

use crate::{error::DatabaseError, parser::Command};

pub enum Comparison {
    All,
    Higher(i64),
    Lower(i64),
    Equal(DataAttribute),
}

pub enum DataAttribute {
    String(String),
    Number(i64),
    Data(Vec<u8>),
}

pub struct DataAttributes {
    pub data: Vec<DataAttribute>,
}

pub enum AttributeType {
    String,
    Number,
    Data,
}

pub struct Attribute {
    pub name: String,
    pub attribute_type: AttributeType,
}

#[derive(Default)]
pub struct DatabaseTable {
    pub name: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Default)]
pub struct Database {
    tables: Vec<DatabaseTable>,
}

impl Database {
    pub async fn create_table(
        &self,
        name: &str,
        attributes: Vec<Attribute>,
    ) -> Result<(), DatabaseError> {
        todo!()
    }

    pub async fn insert(
        &self,
        table_name: &str,
        data: DataAttributes,
    ) -> Result<(), DatabaseError> {
        todo!()
    }

    pub async fn delete(
        &self,
        table_name: &str,
        attr_pos: usize,
        comparison: &Comparison,
    ) -> Result<(), DatabaseError> {
        todo!()
    }

    pub async fn select(
        &self,
        table_name: &str,
        attr_pos: usize,
        comparison: &Comparison,
        selected: Vec<usize>,
    ) -> Result<(), DatabaseError> {
        todo!()
    }

    pub async fn drop(&self, table_name: &str) -> Result<(), DatabaseError> {
        todo!()
    }
}
