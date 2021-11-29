use crate::database::Database;
use ron::ser::to_string;
use tokio::net::TcpStream;

#[derive(Default)]
pub struct StreamProcessor {
    database: Database,
}

impl StreamProcessor {
    pub async fn process_stream(&self, stream: TcpStream) -> anyhow::Result<()> {
        let mut buffer = [0u8; 1024];
        stream.readable().await?;
        let count = stream.try_read(&mut buffer)?;
        let unparsed = std::str::from_utf8(&buffer[0..count])?;
        let command = crate::parser::parse(unparsed, self.database.tables.clone()).await?;
        let response = match command {
            crate::parser::Command::Create { name, attributes } => {
                self.database.create_table(&name, attributes).await?
            }
            crate::parser::Command::Insert { table_name, data } => {
                self.database.insert(&table_name, data).await?
            }
            crate::parser::Command::Delete { table_name, attr_pos, comparison } => {
                self.database.delete(&table_name, attr_pos, &comparison).await?
            }
            crate::parser::Command::Select { table_name, attr_pos, comparison, selected } => {
                self.database.select(&table_name, attr_pos, &comparison, selected).await?
            }
            crate::parser::Command::Drop { name } => self.database.drop_table(&name).await?,
        };
        let res = to_string(&response)?;
        stream.try_write(res.as_bytes())?;
        Ok(())
    }
}
