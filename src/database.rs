use std::{collections::HashMap, sync::Arc};

use atomic_counter::{AtomicCounter, RelaxedCounter};
use tokio::{join, sync::RwLock};

use crate::error::DatabaseError;

use serde::Serialize;

#[derive(PartialEq)]
pub enum Comparison {
    All,
    Higher(DataAttribute),
    Lower(DataAttribute),
    Equal(DataAttribute),
}

#[derive(Serialize, Debug, PartialEq, PartialOrd, Clone)]
pub enum DataAttribute {
    String(String),
    Number(i64),
    Id(i64),
    Data(Vec<u8>),
    NoneId,
    None,
}

#[derive(Serialize, Debug, Default, PartialEq, Clone)]
pub struct DataAttributes {
    pub attributes: Vec<DataAttribute>,
}

#[derive(PartialEq)]
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

#[derive(Serialize, Debug, PartialEq)]
pub enum DatabaseResponse {
    Nothing,
    Id(i64),
    Data(Vec<DataAttributes>),
}

#[derive(Default, Debug)]
pub struct TableDataChunk {
    pub data: Arc<RwLock<Vec<DataAttributes>>>,
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

fn hash_id(id: i64) -> usize {
    (id as u64 & 0b1111_1111) as usize
}

#[derive(Default)]
pub struct Database {
    pub tables: Arc<RwLock<HashMap<String, DatabaseTable>>>,
    pub data: Arc<RwLock<HashMap<String, TableData>>>,
}

fn new_tabledata() -> [TableDataChunk; 256] {
    (0..256).map(|_| TableDataChunk::default()).collect::<Vec<_>>().try_into().unwrap()
}

impl TableData {
    async fn add(&self, mut data: DataAttributes) -> i64 {
        let current_id = self.counter.inc() as i64;
        for id in data.attributes.iter_mut().filter(|att| **att == DataAttribute::NoneId) {
            *id = DataAttribute::Id(current_id);
        }
        let chunk_num = hash_id(current_id);
        let mut write_lock = self.chunks[chunk_num].data.write().await;
        write_lock.push(data);
        current_id
    }

    async fn delete_closure_comp(
        &self,
        attr_pos: usize,
        att: &DataAttribute,
        closure: fn(&DataAttribute, &DataAttribute) -> bool,
    ) {
        // All chunks are handled asynchronously
        let mut futures_vec = vec![];
        for i in 0..256usize {
            let data = self.chunks[i].data.clone();
            let att_clone = (*att).clone();
            futures_vec.push(tokio::spawn(async move {
                data.write().await.retain(|item| !closure(&item.attributes[attr_pos], &att_clone));
            }));
        }
        for handle in futures_vec {
            handle.await.unwrap();
        }
    }

    async fn select_closure_comp(
        &self,
        attr_pos: usize,
        att: &DataAttribute,
        selected: Vec<usize>,
        closure: fn(&DataAttribute, &DataAttribute) -> bool,
    ) -> Vec<DataAttributes> {
        let selected = Arc::new(selected);
        let mut futures_vec = vec![];
        for i in 0..256usize {
            let data = self.chunks[i].data.clone();
            let att_clone = (*att).clone();
            let selected = selected.clone();
            let future = async move {
                let mut ret = vec![];
                for item in data
                    .read()
                    .await
                    .iter()
                    .filter(|elem| closure(&elem.attributes[attr_pos], &att_clone))
                {
                    let mut data_attr = DataAttributes::default();
                    for i in selected.iter() {
                        data_attr.attributes.push(item.attributes[*i].clone());
                    }
                    ret.push(data_attr);
                }
                ret
            };
            futures_vec.push(tokio::spawn(future));
        }
        let mut ret = vec![];
        for handle in futures_vec.iter_mut() {
            ret.append(&mut handle.await.unwrap());
        }

        ret
    }

    async fn delete(&self, attr_pos: usize, comparison: &Comparison) {
        match comparison {
            Comparison::All => {
                self.delete_closure_comp(attr_pos, &DataAttribute::None, |_, _| true)
            }
            Comparison::Higher(attr) => {
                self.delete_closure_comp(attr_pos, attr, |num, att| *num > *att)
            }
            Comparison::Lower(attr) => {
                self.delete_closure_comp(attr_pos, attr, |num, att| *num < *att)
            }
            Comparison::Equal(attr) => {
                self.delete_closure_comp(attr_pos, attr, |num, att| *num == *att)
            }
        }
        .await;
    }

    async fn get(
        &self,
        attr_pos: usize,
        comparison: &Comparison,
        selected: Vec<usize>,
    ) -> Vec<DataAttributes> {
        match comparison {
            Comparison::All => {
                self.select_closure_comp(attr_pos, &DataAttribute::None, selected, |_, _| true)
            }
            Comparison::Higher(attr) => {
                self.select_closure_comp(attr_pos, attr, selected, |num, att| *num > *att)
            }
            Comparison::Lower(attr) => {
                self.select_closure_comp(attr_pos, attr, selected, |num, att| *num < *att)
            }
            Comparison::Equal(attr) => {
                self.select_closure_comp(attr_pos, attr, selected, |num, att| *num == *att)
            }
        }
        .await
    }

    async fn delete_id(&self, attr_pos: usize, id: i64) {
        let chunk_id = hash_id(id);
        let chunk = &self.chunks[chunk_id];
        let mut chunk_lock = chunk.data.write().await;
        let index =
            chunk_lock.iter().position(|item| item.attributes[attr_pos] == DataAttribute::Id(id));
        if let Some(val) = index {
            chunk_lock.remove(val);
        }
    }

    async fn get_by_id(
        &self,
        id: i64,
        attr_pos: usize,
        selected: Vec<usize>,
    ) -> Vec<DataAttributes> {
        let chunk_id = hash_id(id);
        let chunk = &self.chunks[chunk_id];
        let chunk_lock = chunk.data.read().await;
        let item = chunk_lock.iter().find(|i| i.attributes[attr_pos] == DataAttribute::Id(id));
        if item.is_none() {
            return vec![];
        }
        let item = item.unwrap();
        let mut attributes = DataAttributes { attributes: vec![] };
        for i in selected {
            attributes.attributes.push(item.attributes[i].clone());
        }
        vec![attributes]
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

    async fn is_attr_id(&self, table_name: &str, attr_pos: usize) -> bool {
        let schema_lock = self.tables.read().await;
        let table = schema_lock.get(table_name);
        if table.is_none() {
            return false;
        }
        let table = table.unwrap();
        table.attributes[attr_pos].attribute_type == AttributeType::Id
    }

    fn is_eq_comparison(comp: &Comparison) -> bool {
        if let Comparison::Equal(_) = comp {
            return true;
        }
        false
    }

    pub async fn delete(
        &self,
        table_name: &str,
        attr_pos: usize,
        comparison: &Comparison,
    ) -> Result<DatabaseResponse, DatabaseError> {
        let is_id = self.is_attr_id(table_name, attr_pos).await;
        let read_lock = self.data.read().await;
        let db_data = read_lock.get(table_name);
        if db_data.is_none() {
            return Err(DatabaseError::TableDoesNotExist);
        }
        let db_data = db_data.unwrap();
        if is_id && Self::is_eq_comparison(comparison) {
            if let Comparison::Equal(DataAttribute::Id(id)) = comparison {
                db_data.delete_id(attr_pos, *id).await;
            } else {
                panic!("DataAttribute was not Id");
            }
        } else {
            db_data.delete(attr_pos, comparison).await;
        }
        Ok(DatabaseResponse::Nothing)
    }

    pub async fn select(
        &self,
        table_name: &str,
        attr_pos: usize,
        comparison: &Comparison,
        selected: Vec<usize>,
    ) -> Result<DatabaseResponse, DatabaseError> {
        let is_id = self.is_attr_id(table_name, attr_pos).await;
        let read_lock = self.data.read().await;
        let db_data = read_lock.get(table_name);
        if db_data.is_none() {
            return Err(DatabaseError::TableDoesNotExist);
        }
        let db_data = db_data.unwrap();
        if is_id && Self::is_eq_comparison(comparison) {
            if let Comparison::Equal(DataAttribute::Id(id)) = comparison {
                return Ok(DatabaseResponse::Data(
                    db_data.get_by_id(*id, attr_pos, selected).await,
                ));
            } else {
                panic!("DataAttribute was not Id");
            }
        }
        Ok(DatabaseResponse::Data(db_data.get(attr_pos, comparison, selected).await))
    }

    pub async fn drop_table(&self, table_name: &str) -> Result<DatabaseResponse, DatabaseError> {
        self.tables.write().await.remove(table_name);
        self.data.write().await.remove(table_name);
        Ok(DatabaseResponse::Nothing)
    }
}

#[cfg(test)]
mod tests {
    use crate::database::{DataAttribute, DataAttributes, DatabaseResponse};

    use super::{Attribute, AttributeType, Comparison, Database};

    async fn fill_db() -> Database {
        let db = Database::default();
        let mut attributes = vec![];
        attributes.push(Attribute { name: "id".to_string(), attribute_type: AttributeType::Id });
        attributes
            .push(Attribute { name: "name".to_string(), attribute_type: AttributeType::String });
        attributes
            .push(Attribute { name: "age".to_string(), attribute_type: AttributeType::Number });
        attributes.push(Attribute {
            name: "lotto_numbers".to_string(),
            attribute_type: AttributeType::Data,
        });
        assert!(db.create_table("people", attributes).await.is_ok());
        let mut add_data = DataAttributes::default();
        add_data.attributes.push(DataAttribute::NoneId);
        add_data.attributes.push(DataAttribute::String("John Smith".to_string()));
        add_data.attributes.push(DataAttribute::Number(32));
        add_data.attributes.push(DataAttribute::Data(vec![1, 2, 3]));
        assert!(db.insert("people", add_data).await.is_ok());
        db
    }

    #[tokio::test]
    async fn fill_db_test() {
        fill_db().await;
    }

    #[tokio::test]
    async fn delete_data() {
        let db = fill_db().await;
        let attribute = DataAttribute::String("John Smith".to_string());
        assert!(db.delete("people", 1, &Comparison::Equal(attribute)).await.is_ok());
        let attribute = DataAttribute::Id(0);
        let selected = vec![0];
        let res = db.select("people", 0, &Comparison::Equal(attribute), selected).await;
        assert!(res.is_ok());
        let res = res.unwrap();
        let expected_res = DatabaseResponse::Data(vec![]);
        assert_eq!(res, expected_res);
    }

    #[tokio::test]
    async fn get_id() {
        let db = fill_db().await;
        let attribute = DataAttribute::Id(0);
        let selected = vec![0, 1, 2, 3];
        let res = db.select("people", 0, &Comparison::Equal(attribute), selected).await;
        assert!(res.is_ok());
        let res = res.unwrap();
        let mut attrs = vec![];
        attrs.push(DataAttribute::Id(0));
        attrs.push(DataAttribute::String("John Smith".to_string()));
        attrs.push(DataAttribute::Number(32));
        attrs.push(DataAttribute::Data(vec![1, 2, 3]));
        let attrs = DataAttributes { attributes: attrs };
        let db_response = DatabaseResponse::Data(vec![attrs]);
        assert_eq!(res, db_response);
    }

    #[tokio::test]
    async fn get_all() {
        let db = fill_db().await;
        let selected = vec![1, 2, 3];
        let res = db.select("people", 0, &Comparison::All, selected).await;
        assert!(res.is_ok());
        let res = res.unwrap();
        if let DatabaseResponse::Data(data) = res {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0].attributes.len(), 3);
        } else {
            panic!();
        }
    }
}
