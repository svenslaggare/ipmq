use std::path::Path;

use tokio::net::UnixStream;

use crate::shared_memory::SharedMemory;
use crate::command::{Command, Message};

/// Represents a consumer of messages
pub struct Consumer {
    stream: UnixStream,
    shared_memory: Option<SharedMemory>
}

impl Consumer {
    /// Tries to connect to the producer at the given path
    pub async fn connect(path: &Path) -> tokio::io::Result<Consumer> {
        let stream = UnixStream::connect(path).await?;

        Ok(
            Consumer {
                stream,
                shared_memory: None
            }
        )
    }

    /// Sends a command to create a new queue in the message queue
    pub async fn create_queue(&mut self, name: &str, auto_delete: bool) -> tokio::io::Result<()> {
        Command::CreateQueue(name.to_owned(), auto_delete).send_command(&mut self.stream).await?;
        Ok(())
    }

    /// Sends a command to bind the given queue to the given pattern
    pub async fn bind_queue(&mut self, name: &str, pattern: &str) -> tokio::io::Result<()> {
        Command::BindQueue(name.to_owned(), pattern.to_owned()).send_command(&mut self.stream).await?;
        Ok(())
    }

    /// Sends a command to start consuming from the given queue
    pub async fn start_consume_queue(&mut self, name: &str) -> tokio::io::Result<()> {
        Command::StartConsume(name.to_owned()).send_command(&mut self.stream).await?;
        Ok(())
    }

    /// Handles messages from the queue.
    /// Callback called on each valid message. Return value is a command to send in response to the server (typically ack).
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