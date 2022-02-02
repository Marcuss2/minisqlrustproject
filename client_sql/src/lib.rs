use serde::{Deserialize, Serialize};
use tokio::{
    io::{self, AsyncWriteExt, Interest},
    net::TcpStream,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CommandType {
    Query,
    Tables,
    Columns,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Command {
    pub cmd: CommandType,
    pub contents: String,
}

impl Command {
    pub fn create_command_from(contents: String, cmd: CommandType) -> Command {
        Command { cmd, contents }
    }
}

pub async fn write_command_to_stream(stream: &mut TcpStream, command: Command) -> io::Result<()> {
    let serialized = serde_json::to_string(&command).unwrap();
    stream.write(serialized.as_bytes()).await?;

    Ok(())
}

pub async fn read_from_stream(stream: &mut TcpStream) -> io::Result<()> {
    println!("Message from server:");
    loop {
        let ready = stream.ready(Interest::READABLE).await?;

        if ready.is_readable() {
            let mut buffer = [0u8; 1024];
            match stream.try_read(&mut buffer) {
                Ok(data_length) => {
                    let res = String::from_utf8(buffer[0..data_length].to_vec()).unwrap();
                    print!("{}", res);
                    if data_length == 1024 {
                        continue;
                    }
                    println!();
                    return Ok(());
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }
}
