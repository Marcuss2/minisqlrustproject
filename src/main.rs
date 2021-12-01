use std::sync::Arc;

use anyhow::Result;
use dotenv::dotenv;
use tokio::net::TcpListener;

pub mod database;
pub mod error;
pub mod parser;
pub mod stream_processor;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let url = std::env::var("BIND_URL").expect("BIND_URL must be set");
    let listener = TcpListener::bind(&url).await?;
    let stream_processor = Arc::new(stream_processor::StreamProcessor::default());

    loop {
        let (stream, _) = listener.accept().await?;
        let sp_handle = stream_processor.clone();
        tokio::spawn(async move {
            let res = sp_handle.process_stream(stream).await;
            if let Err(e) = res {
                println!("{:?}", e);
            }
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
