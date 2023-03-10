use crate::database::Database;
use ron::ser::to_string;

use client_sql::Command as Action;
use client_sql::CommandType;

#[derive(Default)]
pub struct StreamProcessor {
    database: Database,
}

impl StreamProcessor {
    pub async fn process_str(&self, str_command: String) -> anyhow::Result<String> {
        let deserialized: Action = serde_json::from_str(&str_command).unwrap();

        match &deserialized.cmd {
            CommandType::Tables => {
                return Ok(format!("{:?}", self.database.tables().await?));
            }
            CommandType::Columns => {
                return Ok(format!(
                    "{:?}",
                    self.database.table_attributes(&deserialized.contents).await?
                ));
            }
            _ => {}
        };
        let command =
            crate::parser::get_command(&deserialized.contents, self.database.tables.clone())
                .await?;
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
            crate::parser::Command::CreateIndex { .. } => DatabaseResponse::Nothing,
        };
        let res = to_string(&response)?;
        Ok(res)
    }
}
