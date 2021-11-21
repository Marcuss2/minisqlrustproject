use tokio::net::TcpStream;

#[derive(Default)]
pub struct StreamProcessor {
    database: crate::database::Database,
}

impl StreamProcessor {
    pub async fn process_stream(&self, stream: TcpStream) {
        todo!()
    }
}
