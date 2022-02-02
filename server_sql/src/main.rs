use std::sync::Arc;

use anyhow::Result;
use dotenv::dotenv;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use crate::stream_processor::StreamProcessor;

pub mod allocator;
pub mod data;
pub mod database;
pub mod error;
pub mod parser;
pub mod stream_processor;
pub mod index;

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv().ok();

    //let url = std::env::var("BIND_URL").expect("BIND_URL must be set");
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    let stream_processor = Arc::new(stream_processor::StreamProcessor::default());
    loop {
        let (mut stream, _) = listener.accept().await?;
        let sp_handle = stream_processor.clone();
        tokio::spawn(async move {
            process_stream(&mut stream, &sp_handle).await;
        });
    }
}

/// processes a stream and writes to socket after it has been processed
pub async fn process_stream(stream: &mut TcpStream, processor: &Arc<StreamProcessor>){
    let mut buffer = vec![0; 1024];
    loop {
        let data_result = stream.read(&mut buffer).await;

        let data_length = match data_result {
            Ok(0) => {
                break;
            }
            Ok(size) => size,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(err) => {
                break;
            }
        };

        let data = String::from_utf8(buffer[0..data_length].to_vec()).unwrap();
        match processor.process_str(data).await{
            Ok(s) => {
                stream.write(s.as_bytes()).await;
            },
            Err(e) => {
                let response = format!("{:?}", e);
                stream.write(response.as_bytes()).await;
            },
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn ci_test() {
        assert_eq!(2, 1 + 1);
    }
}
