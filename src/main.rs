use std::sync::Arc;

use anyhow::Result;
use database::Database;
use dotenv::dotenv;
use tokio::net::{TcpListener, TcpStream};

pub mod database;
pub mod stream_processor;
pub mod error;
pub mod parser;


#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let url = std::env::var("BIND_URL").expect("BIND_URL must be set");
    let listener = TcpListener::bind(&url).await?;
    let database = Arc::new(Database::default());
    let stream_processor = Arc::new(stream_processor::StreamProcessor::default());

    loop {
        let (stream, _) = listener.accept().await?;
        let sp_handle = stream_processor.clone();
        let db_handle = database.clone();
        tokio::spawn(async move {
            sp_handle.process_stream(stream, db_handle).await;
        });
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn ci_test() {
        assert_eq!(2, 1 + 1);
    }
}
