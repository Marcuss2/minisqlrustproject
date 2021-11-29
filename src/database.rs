use std::{collections::HashMap, sync::Arc};

use tokio::{join, net::TcpStream, sync::RwLock};

use crate::{error::DatabaseError, parser::Command};

use serde::Serialize;

pub enum Comparison {
    All,
    Higher(i64),
    Lower(i64),
    Equal(DataAttribute),
}

#[derive(Serialize, Debug)]
pub enum DataAttribute {
    String(String),
    Number(i64),
    Data(Vec<u8>),
}

#[derive(Serialize)]
pub struct DataAttributes {
    pub data: Vec<DataAttribute>,
}

pub enum AttributeType {
    Id,
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
    pub attributes: Vec<Attribute>,
}

#[derive(Serialize)]
pub enum DatabaseResponse {
    Nothing,
    Id(i64),
    Data(Vec<DataAttributes>),
}

#[derive(Default, Debug)]
pub struct TableData {
    pub data: RwLock<Vec<DataAttribute>>,
}

#[derive(Default)]
pub struct Database {
    pub tables: Arc<RwLock<HashMap<String, DatabaseTable>>>,
    pub data: Arc<RwLock<HashMap<String, [TableData; 256]>>>,
    
}

fn new_tabledata() -> [TableData; 256] {
    (0..255)
    .map(|_| TableData::default())
    .collect::<Vec<_>>()
    .try_into()
    .unwrap()
}

impl Database {
    pub async fn create_table(
        &self,
        name: &str,
        attributes: Vec<Attribute>,
    ) -> Result<DatabaseResponse, DatabaseError> {
        let (mut db_tables, mut db_data) = join!(self.tables.write(), self.data.write());
        if db_tables.contains_key(name) {
            return Err(DatabaseError::TableExists);
        }
        let table = DatabaseTable { attributes };
        db_tables.insert(name.to_string(), table);
        db_data.insert(name.to_string(), new_tabledata());
        Ok(DatabaseResponse::Nothing)
    }

    pub async fn insert(
        &self,
        table_name: &str,
        data: DataAttributes,
    ) -> Result<DatabaseResponse, DatabaseError> {
        let db_data = self.data.read().await;

    }

    pub async fn delete(
        &self,
        table_name: &str,
        attr_pos: usize,
        comparison: &Comparison,
    ) -> Result<DatabaseResponse, DatabaseError> {
        todo!()
    }

    pub async fn select(
        &self,
        table_name: &str,
        attr_pos: usize,
        comparison: &Comparison,
        selected: Vec<usize>,
    ) -> Result<DatabaseResponse, DatabaseError> {
        todo!()
    }

    pub async fn drop_table(&self, table_name: &str) -> Result<DatabaseResponse, DatabaseError> {
        todo!()
    }
}
