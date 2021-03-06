use tokio::io::{AsyncWriteExt, ErrorKind, AsyncReadExt};

use serde::{Deserialize, Serialize};

use crate::queue::MessageId;
use crate::exchange::{QueueId};
use crate::shared_memory::SharedMemory;

/// Represents a command used for communication between the producer and consumer
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    CreateQueue { name: String, auto_delete: bool, ttl: Option<f64> },
    BindQueue(String, String),
    BindQueueResult(Result<QueueId, BindQueueError>),
    StartConsume(String),
    StartConsumeResult(Result<QueueId, StartConsumeError>),
    SharedMemoryArea(String, usize),
    Message(Message),
    Acknowledge(QueueId, MessageId),
    NegativeAcknowledge(QueueId, MessageId),
    StopConsume(QueueId),
    StoppedConsuming
}

impl Command {
    pub async fn receive_command<T: AsyncReadExt + Unpin>(socket: &mut T) -> tokio::io::Result<Command> {
        let num_bytes = socket.read_u64().await? as usize;
        let mut command_bytes = vec![0; num_bytes];
        socket.read_exact(&mut command_bytes[..]).await?;
        bincode::deserialize(&command_bytes).map_err(|_| tokio::io::Error::from(ErrorKind::Other))
    }

    pub async fn send_command<T: AsyncWriteExt + Unpin>(&self, socket: &mut T) -> tokio::io::Result<()> {
        let command_bytes = bincode::serialize(&self).unwrap();
        socket.write_u64(command_bytes.len() as u64).await?;
        socket.write_all(&command_bytes).await?;
        Ok(())
    }
}

/// Bind queue errors
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BindQueueError {
    InvalidBindPattern(String),
    QueueNotFound
}

/// Start consume errors
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StartConsumeError {
    QueueNotFound
}

/// Represents a message received from the producer
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub queue_id: QueueId,
    pub routing_key: String,
    pub id: MessageId,
    pub data: MessageData
}

impl Message {
    pub fn new(queue_id: QueueId, routing_key: String, id: MessageId, data: MessageData) -> Message {
        Message {
            queue_id,
            routing_key,
            id,
            data
        }
    }

    pub fn buffer<'a>(&self, shared_memory: &'a SharedMemory) -> &'a [u8] {
        shared_memory.bytes_from_data(&self.data)
    }

    pub fn acknowledgement(&self) -> Command {
        Command::Acknowledge(self.queue_id, self.id)
    }

    pub fn negative_acknowledgement(&self) -> Command {
        Command::NegativeAcknowledge(self.queue_id, self.id)
    }

    pub fn stop_consume(&self) -> Command {
        Command::StopConsume(self.queue_id)
    }
}

/// Defines where to find the data in the shared memory area
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageData {
    pub offset: usize,
    pub size: usize
}

impl MessageData {
    pub fn new(offset: usize, size: usize) -> MessageData {
        MessageData {
            offset,
            size
        }
    }
}