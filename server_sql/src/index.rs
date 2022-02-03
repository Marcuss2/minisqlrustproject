use std::{collections::HashMap, io};

use crate::database::DataAttribute;
use std::collections::HashSet;
use std::fs::{create_dir_all, remove_dir_all, File, remove_file};
use std::io::{BufReader, Error, ErrorKind};
use std::iter::FromIterator;
use std::path::Path;

pub async fn create_index(
    table_name: &str,
    attr_name: &str,
    id_vec: &[DataAttribute],
    attr_vec: &[DataAttribute],
) -> io::Result<String> {
    let file_path = format!("./database/{}/{}", table_name, attr_name);
    let path = Path::new(&file_path);
    if path.exists() {
        return Ok(String::from("Index already exists!"));
    }
    create_dir_all(path.parent().unwrap()).unwrap();
    let mut map: HashMap<String, Vec<DataAttribute>> = HashMap::new();
    for i in 0..id_vec.len() {
        let key = format!("{:?}", attr_vec[i].clone());
        map.entry(key).or_insert_with(Vec::new).push(id_vec[i].clone());
    }

    let file = File::create(file_path)?;
    serde_json::to_writer(file, &map)?;
    Ok(String::from("Index successfully created!"))
}

pub async fn index_exists(table_name: &str, attr_name: &str) -> bool {
    let file_path = format!("./database/{}/{}", table_name, attr_name);
    let path = Path::new(&file_path);
    path.exists()
}

pub async fn index_find(
    table_name: &str,
    attr_name: &str,
    item: &DataAttribute,
) -> io::Result<Vec<DataAttribute>> {
    if !index_exists(table_name, attr_name).await {
        return Err(Error::new(ErrorKind::Other, "Index does not exist"));
    }
    let map = get_index_map(table_name, attr_name);
    match map.get(&*format!("{:?}", item)) {
        Some(res) => Ok(res.clone()),
        None => Ok(vec![]),
    }
}

pub async fn table_index_insert(
    table_name: &str,
    attr_names: Vec<&String>,
    values: Vec<&DataAttribute>,
) -> io::Result<()> {
    for i in 1..attr_names.len() {
        if index_exists(table_name, attr_names[i]).await {
            index_insert(table_name, attr_names[i], values[0], values[i]).await?;
        }
    }
    Ok(())
}

async fn index_insert(
    table_name: &str,
    attr_name: &str,
    id: &DataAttribute,
    value: &DataAttribute,
) -> io::Result<()> {
    let file_path = format!("./database/{}/{}", table_name, attr_name);
    let mut map = get_index_map(table_name, attr_name);
    let key = format!("{:?}", value.clone());
    map.entry(key).or_insert_with(Vec::new).push(id.clone());
    serde_json::to_writer(File::create(file_path)?, &map)?;
    Ok(())
}

pub async fn table_index_delete(
    table_name: &str,
    attr_names: Vec<&String>,
    id_vec: &[DataAttribute],
) -> io::Result<()> {
    for name in attr_names[1..].iter() {
        if index_exists(table_name, name).await {
            index_delete(table_name, name, id_vec).await?;
        }
    }
    Ok(())
}

async fn index_delete(
    table_name: &str,
    attr_name: &str,
    id_vec: &[DataAttribute],
) -> io::Result<()> {
    let set: HashSet<&DataAttribute> = HashSet::from_iter(id_vec.iter());
    let file_path = format!("./database/{}/{}", table_name, attr_name);
    let mut map = get_index_map(table_name, attr_name);

    map.values_mut().into_iter().for_each(|v| {
        v.retain(|x| !set.contains(x));
    });

    serde_json::to_writer(File::create(file_path)?, &map)?;
    Ok(())
}

pub async fn index_drop(table_name: &str) -> io::Result<()> {
    let file_path = format!("./database/{}", table_name);
    remove_dir_all(Path::new(&file_path)).unwrap();
    Ok(())
}

pub async fn index_drop_attr(table_name: &str, attr_name: &str) -> io::Result<()> {
    let file_path = format!("./database/{}/{}", table_name, attr_name);
    remove_file(Path::new(&file_path)).unwrap();
    Ok(())
}

fn get_index_map(table_name: &str, attr_name: &str) -> HashMap<String, Vec<DataAttribute>> {
    let file = File::open(Path::new(&format!("./database/{}/{}", table_name, attr_name))).unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).unwrap()
}

#[cfg(test)]
mod tests {
    use crate::database::DataAttribute::*;
    use crate::index::*;
    use std::string::String;

    #[tokio::test]
    async fn test_index() {
        assert!(create_index(
            &String::from("test123"),
            &String::from("second"),
            &vec![Id(1)],
            &vec![DataAttribute::String("smth".parse().unwrap())]
        )
        .await
        .is_ok());

        assert!(index_exists(&String::from("test123"), &String::from("second")).await);

        test_index_find().await;
        test_table_index_insert().await;
        test_table_index_delete().await;
        test_table_index_drop().await;

        assert!(!index_exists(&String::from("test123"), &String::from("second")).await);
    }

    async fn test_index_find() {
        let res = index_find(
            &String::from("test123"),
            &String::from("second"),
            &DataAttribute::String(String::from("nothing")),
        )
        .await;
        match res {
            Ok(vector) => {
                if !vector.is_empty() {
                    panic!("Vector should be empty");
                }
            }
            _ => panic!("Should find index"),
        };

        let res = index_find(
            &String::from("test123"),
            &String::from("second"),
            &DataAttribute::String(String::from("smth")),
        )
        .await;
        match res {
            Ok(vector) => {
                if vector.is_empty() {
                    panic!("Vector should not be empty");
                }
                if vector[0] != DataAttribute::Id(1) {
                    panic!("Vector should contain 'Id(1)' value");
                }
            }
            _ => panic!("Should find index"),
        };
    }

    async fn test_table_index_insert() {
        assert!(table_index_insert(
            &"test123".to_string(),
            vec![&String::from(""), &String::from("second")],
            vec![&Id(2), &String("test_ins".to_string())]
        )
        .await
        .is_ok());

        match index_find(
            &"test123".to_string(),
            &String::from("second"),
            &String(String::from("test_ins")),
        )
        .await
        {
            Ok(res) => {
                if res[0] != Id(2) {
                    panic!("Should be 'Id(2)'")
                }
            }
            _ => panic!("Should be ok (inserted values to index)"),
        }
    }

    async fn test_table_index_delete() {
        assert!(table_index_delete(
            &"test123".to_string(),
            vec![&String::from(""), &String::from("second")],
            &vec![Id(2)]
        )
        .await
        .is_ok());

        match index_find(
            &"test123".to_string(),
            &String::from("second"),
            &String(String::from("test_ins")),
        )
        .await
        {
            Ok(res) => {
                if !res.is_empty() {
                    panic!("Should be empty after deletion")
                }
            }
            _ => panic!("Should be ok (deleted ID from index)"),
        }
    }

    async fn test_table_index_drop() {
        assert!(index_drop(&String::from("test123")).await.is_ok())
    }
}
