use client_sql::*;
use client_sql::{read_from_stream, write_command_to_stream};
use std::io::{self as other_io, BufRead};
use tokio::{
    io,
    net::{TcpSocket, TcpStream},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut stream = connect_to_server().await?;
    println!("Welcome to miniSQL server!");

    loop {
        println!("Write if you want to make a query 'q' or to get info about tables 't'.");
        let line = read_input()?;
        let line = &line[..];
        let my_type = match line {
            "q" => true,
            "t" => false,
            "help" => {
                println!("HELP");
                continue;
            }
            _ => {
                println!("'{}' is invalid", line);
                continue;
            }
        };

        //let mut file = String::from("");
        let command = match my_type {
            true => {
                /*println!("Do you want to save result to file? (name of file or ENTER)");
                let line = read_input()?;
                if line.ne(&file){
                    file = line;
                }
                 */
                println!("Write the query:");
                let line = read_input()?;
                Command::create_command_from(line, CommandType::Query)
            }
            false => {
                println!("Get info about database (ENTER) or about a table 'name_of_table'");
                let line = read_input()?;
                let empty = String::from("");
                if line.eq(&empty) {
                    Command::create_command_from(empty, CommandType::Tables)
                } else {
                    Command::create_command_from(line, CommandType::Columns)
                }
            }
        };
        write_command_to_stream(&mut stream, command).await?;
        read_from_stream(&mut stream).await?;
    }
}

fn read_input() -> Result<String, other_io::Error> {
    let stdin = other_io::stdin();
    let mut iterator = stdin.lock().lines();
    let line = iterator.next().unwrap()?;
    Ok(line)
}

async fn connect_to_server() -> io::Result<TcpStream> {
    let addr = "127.0.0.1:8000".parse().unwrap();

    let socket = TcpSocket::new_v4()?;
    let stream = socket.connect(addr).await?;

    Ok(stream)
}
