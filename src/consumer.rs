use std::path::Path;

use tokio::net::UnixStream;

use crate::shared_memory::SharedMemory;
use crate::command::{Command, Message};

pub struct Consumer {
    stream: UnixStream,
    shared_memory: Option<SharedMemory>
}

impl Consumer {
    pub async fn connect(path: &Path) -> tokio::io::Result<Consumer> {
        let stream = UnixStream::connect(path).await?;

        Ok(
            Consumer {
                stream,
                shared_memory: None
            }
        )
    }

    pub async fn create_queue(&mut self, name: &str, auto_delete: bool) -> tokio::io::Result<()> {
        Command::CreateQueue(name.to_owned(), auto_delete).send_command(&mut self.stream).await?;
        Ok(())
    }

    pub async fn bind_queue(&mut self, name: &str, pattern: &str) -> tokio::io::Result<()> {
        Command::BindQueue(name.to_owned(), pattern.to_owned()).send_command(&mut self.stream).await?;
        Ok(())
    }

    pub async fn start_consume_queue(&mut self, name: &str) -> tokio::io::Result<()> {
        Command::StartConsume(name.to_owned()).send_command(&mut self.stream).await?;
        Ok(())
    }

    pub async fn handle_messages<F: FnMut(&mut SharedMemory, Message) -> Option<Command>>(&mut self, mut on_message: F) {
        loop {
            match Command::receive_command(&mut self.stream).await {
                Ok(command) => {
                    match command {
                        Command::SharedMemoryArea(path, size) => {
                            match SharedMemory::read(Path::new(&path), size) {
                                Ok(shared_memory) => {
                                    self.shared_memory = Some(shared_memory);
                                }
                                Err(err) => {
                                    println!("Failed to create shared memory: {:?}", err);
                                }
                            }
                        }
                        Command::Message(message) => {
                            if let Some(shared_memory) = self.shared_memory.as_mut() {
                                if let Some(new_command) = on_message(shared_memory, message) {
                                    new_command.send_command(&mut self.stream).await.unwrap();
                                }
                            } else {
                                println!("Received message without shared memory.");
                            }
                        }
                        _ => {
                            println!("Unhandled command: {:?}", command);
                        }
                    }
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                    break;
                }
            }
        }
    }
}