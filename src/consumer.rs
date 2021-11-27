use std::path::Path;

use log::error;

use tokio::net::UnixStream;

use crate::shared_memory::{SharedMemory, SharedMemoryError};
use crate::command::{Command, Message, StartConsumeError, BindQueueError};
use crate::exchange::QueueId;

#[derive(Debug)]
pub enum ConsumerError {
    IO(tokio::io::Error),
    FailedToCreateSharedMemory(SharedMemoryError),
    FailedToBindToQueue(BindQueueError),
    UnexpectedCommand(Command)
}

impl From<tokio::io::Error> for ConsumerError {
    fn from(err: tokio::io::Error) -> Self {
        ConsumerError::IO(err)
    }
}

pub type ConsumerResult<T> = Result<T, ConsumerError>;

#[derive(Debug)]
pub enum HandleMessageError<T> {
    CallbackError(T),
    IO(tokio::io::Error),
    FailedToStartConsume(StartConsumeError),
    FailedCreatingSharedMemory(SharedMemoryError),
    ReceivedMessageWithoutSharedMemory
}

/// Represents a consumer of messages
pub struct Consumer {
    stream: UnixStream,
    shared_memory: Option<SharedMemory>
}

impl Consumer {
    /// Tries to connect to the producer at the given path
    pub async fn connect(path: &Path) -> ConsumerResult<Consumer> {
        let mut stream = UnixStream::connect(path).await?;

        let response = Command::receive_command(&mut stream).await?;
        let shared_memory = match response {
            Command::SharedMemoryArea(path, size) => {
                match SharedMemory::read(Path::new(&path), size) {
                    Ok(shared_memory) => Some(shared_memory),
                    Err(err) => { return Err(ConsumerError::FailedToCreateSharedMemory(err)); }
                }
            }
            _ => None
        };

        Ok(
            Consumer {
                stream,
                shared_memory
            }
        )
    }

    /// Sends a command to create a new queue in the message queue
    pub async fn create_queue(&mut self, name: &str, auto_delete: bool, ttl: Option<f64>) -> ConsumerResult<()> {
        Command::CreateQueue { name: name.to_owned(), auto_delete, ttl }.send_command(&mut self.stream).await?;
        Ok(())
    }

    /// Sends a command to bind the given queue to the given pattern
    pub async fn bind_queue(&mut self, name: &str, pattern: &str) -> ConsumerResult<QueueId> {
        Command::BindQueue(name.to_owned(), pattern.to_owned()).send_command(&mut self.stream).await?;
        let response = Command::receive_command(&mut self.stream).await?;
        match response {
            Command::BindQueueResult(result) => {
                result.map_err(|err| ConsumerError::FailedToBindToQueue(err))
            }
            _ => { return Err(ConsumerError::UnexpectedCommand(response)); }
        }
    }

    /// Sends a command to start consuming from the given queue
    pub async fn start_consume_queue(&mut self, name: &str) -> ConsumerResult<()> {
        Command::StartConsume(name.to_owned()).send_command(&mut self.stream).await?;
        Ok(())
    }

    /// Handles messages from the queue using the given callback.
    /// Commands can be sent back to the producer (typically ack) using the first argument to the callback.
    pub async fn handle_messages<F, E>(&mut self, mut on_message: F) -> Result<(), HandleMessageError<E>>
        where F : FnMut(&mut Vec<Command>, &SharedMemory, Message) -> Result<(), E> {
        loop {
            match Command::receive_command(&mut self.stream).await {
                Ok(command) => {
                    match command {
                        Command::StartConsumeResult(result) => {
                            if let Err(err) = result {
                                return Err(HandleMessageError::FailedToStartConsume(err));
                            }
                        }
                        Command::SharedMemoryArea(path, size) => {
                            match SharedMemory::read(Path::new(&path), size) {
                                Ok(shared_memory) => {
                                    self.shared_memory = Some(shared_memory);
                                }
                                Err(err) => {
                                    return Err(HandleMessageError::FailedCreatingSharedMemory(err));
                                }
                            }
                        }
                        Command::Message(message) => {
                            if let Some(shared_memory) = self.shared_memory.as_mut() {
                                let mut commands = Vec::new();
                                on_message(&mut commands, shared_memory, message)
                                    .map_err(|err| HandleMessageError::CallbackError(err))?;

                                for new_command in commands {
                                    if new_command.send_command(&mut self.stream).await.is_err() {
                                        break;
                                    }
                                }
                            } else {
                                return Err(HandleMessageError::ReceivedMessageWithoutSharedMemory);
                            }
                        }
                        Command::StoppedConsuming => {
                            break;
                        }
                        _ => {
                            error!("Unhandled command: {:?}", command);
                        }
                    }
                }
                Err(err) => {
                   return Err(HandleMessageError::IO(err));
                }
            }
        }

        return Ok(());
    }
}