use std::{collections::HashMap, sync::Arc};

use atomic_counter::{AtomicCounter, RelaxedCounter};
use tokio::{join, sync::RwLock};

use crate::error::DatabaseError;

use serde::Serialize;

pub enum Comparison {
    All,
    Higher(i64),
    Lower(i64),
    Equal(DataAttribute),
}

#[derive(Serialize, Debug, PartialEq)]
pub enum DataAttribute {
    String(String),
    Number(i64),
    Data(Vec<u8>),
    NoneId,
    None,
}

#[derive(Serialize, Debug)]
pub struct DataAttributes {
    pub attributes: Vec<DataAttribute>,
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
    pub counter: RelaxedCounter,
}

#[derive(Serialize)]
pub enum DatabaseResponse {
    Nothing,
    Id(i64),
    Data(Vec<DataAttributes>),
}

#[derive(Default, Debug)]
pub struct TableDataChunk {
    pub data: RwLock<Vec<DataAttributes>>,
}

pub struct TableData {
    pub chunks: [TableDataChunk; 256],
    pub counter: RelaxedCounter,
}

impl Default for TableData {
    fn default() -> Self {
        Self { chunks: new_tabledata(), counter: Default::default() }
    }
}

#[derive(Default)]
pub struct Database {
    pub tables: Arc<RwLock<HashMap<String, DatabaseTable>>>,
    pub data: Arc<RwLock<HashMap<String, TableData>>>,
}

fn new_tabledata() -> [TableDataChunk; 256] {
    (0..255).map(|_| TableDataChunk::default()).collect::<Vec<_>>().try_into().unwrap()
}

impl TableData {
    async fn add(&self, mut data: DataAttributes) -> i64 {
        let current_id = self.counter.inc() as i64;
        for id in data.attributes.iter_mut().filter(|att| **att == DataAttribute::NoneId) {
            *id = DataAttribute::Number(current_id);
        }
        let chunk_num = (current_id as u64 & 0b1111_1111) as usize;
        let mut write_lock = self.chunks[chunk_num].data.write().await;
        write_lock.push(data);
        current_id
    }
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
        let table = DatabaseTable { attributes, counter: RelaxedCounter::new(0) };
        db_tables.insert(name.to_string(), table);
        db_data.insert(name.to_string(), TableData::default());
        Ok(DatabaseResponse::Nothing)
    }

    pub async fn insert(
        &self,
        table_name: &str,
        data: DataAttributes,
    ) -> Result<DatabaseResponse, DatabaseError> {
        let read_lock = self.data.read().await;
        let db_data = read_lock.get(table_name);
        if db_data.is_none() {
            return Err(DatabaseError::TableDoesNotExist);
        }
        let id = db_data.unwrap().add(data).await;

        Ok(DatabaseResponse::Id(id))
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
