use tokio::io::{AsyncWriteExt, ErrorKind, AsyncReadExt};

use serde::{Deserialize, Serialize};

use crate::queue::MessageId;
use crate::exchange::{QueueId};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    CreateQueue(String, bool),
    BindQueue(String, String),
    StartConsume(String),
    SharedMemoryArea(String, usize),
    Message(Message),
    Acknowledge(QueueId, MessageId)
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub queue_id: QueueId,
    pub id: MessageId,
    pub data: MessageData
}

impl Message {
    pub fn new(queue_id: QueueId, id: MessageId, data: MessageData) -> Message {
        Message {
            queue_id,
            id,
            data
        }
    }

    pub fn acknowledgement(&self) -> Command {
        Command::Acknowledge(self.queue_id, self.id)
    }
}