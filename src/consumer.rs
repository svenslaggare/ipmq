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
    pub async fn create_queue(&mut self, name: &str, auto_delete: bool, ttl: Option<f64>) -> tokio::io::Result<()> {
        Command::CreateQueue { name: name.to_owned(), auto_delete, ttl }.send_command(&mut self.stream).await?;
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

    /// Handles messages from the queue using the given callback.
    /// Commands can be sent back to the producer (typically ack) using the first argument to the callback.
    pub async fn handle_messages<F: FnMut(&mut Vec<Command>, &mut SharedMemory, Message) -> Result<(), E>, E>(&mut self, mut on_message: F) -> Result<(), E> {
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
                                    break;
                                }
                            }
                        }
                        Command::Message(message) => {
                            if let Some(shared_memory) = self.shared_memory.as_mut() {
                                let mut commands = Vec::new();
                                on_message(&mut commands, shared_memory, message)?;

                                for new_command in commands {
                                    if new_command.send_command(&mut self.stream).await.is_err() {
                                        break;
                                    }
                                }
                            } else {
                                println!("Received message without shared memory.");
                            }
                        }
                        Command::FailedToStartConsume => {
                            break;
                        }
                        Command::StoppedConsuming => {
                            break;
                        }
                        _ => {
                            println!("Unhandled command: {:?}", command);
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        return Ok(());
    }
}