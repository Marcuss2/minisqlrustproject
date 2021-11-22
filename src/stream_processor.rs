use std::sync::Arc;

use tokio::net::TcpStream;

use crate::database::Database;

#[derive(Default)]
pub struct StreamProcessor {
    database: crate::database::Database,
}

impl StreamProcessor {
    pub async fn process_stream(
        &self,
        stream: TcpStream,
        database: Arc<Database>,
    ) -> anyhow::Result<()> {
        let mut buffer = [0u8; 1024];
        stream.readable().await?;
        let count = stream.try_read(&mut buffer)?;
        let unparsed = std::str::from_utf8(&buffer[0..count])?;
        let command = crate::parser::parse(unparsed, database.tables.clone()).await?;
        let response = match command {
            crate::parser::Command::Create { name, attributes } => {
                database.create_table(&name, attributes).await?
            }
            crate::parser::Command::Insert { table_name, data } => {
                database.insert(&table_name, data).await?
            }
            crate::parser::Command::Delete { table_name, attr_pos, comparison } => {
                database.delete(&table_name, attr_pos, &comparison).await?
            }
            crate::parser::Command::Select { table_name, attr_pos, comparison, selected } => {
                database.select(&table_name, attr_pos, &comparison, selected).await?
            }
            crate::parser::Command::Drop { name } => database.drop_table(&name).await?,
        };
        todo!()
    }
}
