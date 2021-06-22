use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::ops::DerefMut;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::net::unix::{OwnedWriteHalf, OwnedReadHalf};
use tokio::sync::{Mutex, mpsc};
use tokio::net::{UnixStream, UnixListener};
use tokio::time;
use tokio::time::Duration;

use crate::queue::{Queue, ClientId};
use crate::command::{Command, Message};
use crate::exchange::{Exchange, QueueId, ExchangeQueue, ExchangeQueueOptions, QueueMessage, QueueMessageData};
use crate::shared_memory::{SmartSharedMemoryAllocator, SmartMemoryAllocation, GenericMemoryAllocation};

pub struct ProducerClient {
    pub sender: UnboundedSender<Command>,
}

/// Represents a producer of messages
pub struct Producer {
    next_client_id: AtomicU64,
    clients: Mutex<HashMap<ClientId, ProducerClient>>,
    exchange: Mutex<Exchange>,
    shared_memory_spec: (PathBuf, usize)
}

impl Producer {
    /// Creates a new producer using the given existing shared memory file
    pub fn new(shared_memory_file: &Path, shared_memory_size: usize) -> Arc<Producer> {
        Arc::new(
            Producer {
                next_client_id: AtomicU64::new(1),
                clients: Mutex::new(HashMap::new()),
                exchange: Mutex::new(Exchange::new()),
                shared_memory_spec: (shared_memory_file.to_owned(), shared_memory_size)
            }
        )
    }

    /// Starts the producer at the given path
    pub async fn start(self: &Arc<Self>, path: &Path) -> tokio::io::Result<()> {
        #[allow(unused_must_use)] {
            std::fs::remove_file(path);
        }

        let listener = UnixListener::bind(path)?;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    self.start_handle_client(stream).await;
                }
                Err(e) => {
                    println!("Failed accepting client: {:?}", e);
                }
            }
        }
    }

    /// Creates a new queue
    pub async fn create_queue(self: &Arc<Self>,
                              name: &str,
                              options: ExchangeQueueOptions) -> Arc<ExchangeQueue> {
        let (queue, created) = self.exchange.lock().await.create(name, options);

        if created {
            let self_clone = self.clone();
            let queue_clone = queue.clone();
            tokio::spawn(async move {
                while queue_clone.is_running() {
                    queue_clone.notified().await;
                    queue_clone.remove_expired().await;
                    self_clone.try_consume_queue(
                        queue_clone.id,
                        queue_clone.queue.lock().await.deref_mut()
                    ).await;
                }
            });

            let queue_clone = queue.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(200));
                while queue_clone.is_running() {
                    interval.tick().await;
                    queue_clone.remove_expired().await;
                }
            });
        }

        queue
    }

    /// Tries to allocate memory from the given shared memory area
    pub async fn allocate(&self,
                          shared_memory_allocator: &SmartSharedMemoryAllocator,
                          size: usize) -> Option<Arc<SmartMemoryAllocation>> {
        let allocate = || SmartMemoryAllocation::new(shared_memory_allocator, size);

        if let Some(allocation) = allocate() {
            Some(allocation)
        } else {
            println!("No free blocks, removing oldest...");

            for queue in self.exchange.lock().await.queues.values() {
                if queue.remove_oldest().await {
                    if let Some(allocation) = allocate() {
                        return Some(allocation);
                    }
                }
            }

            return None;
        }
    }

    /// Publish the given message with the given routing key to the exchange
    pub async fn publish(&self, routing_key: &str, message: QueueMessageData) {
        for queue in self.exchange.lock().await.matching_queues(routing_key).await {
            queue.push(QueueMessage {
                routing_key: routing_key.to_owned(),
                data: message.clone()
            }).await;
        }
    }

    /// Starts tasks to handle the given client
    async fn start_handle_client(self: &Arc<Self>, stream: UnixStream) {
        let client_pid = stream.peer_cred().unwrap().pid().unwrap().to_string();
        let (client_id, client_receiver) = self.create_client().await;

        println!("New client: {} (pid: {})", client_id, client_pid);

        let (reader, mut writer) = stream.into_split();
        let success = Command::SharedMemoryArea(
            self.shared_memory_spec.0.to_str().unwrap().to_owned(),
            self.shared_memory_spec.1
        ).send_command(&mut writer).await.is_ok();

        if !success {
            self.exchange.lock().await.remove_client(client_id).await;
            return;
        }

        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.handle_send_commands(client_id, client_receiver, writer).await
        });

        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.handle_receive_commands(client_id, reader).await
        });
    }

    async fn create_client(&self) -> (ClientId, UnboundedReceiver<Command>) {
        let client_id = self.next_client_id.fetch_add(1, Ordering::SeqCst);

        let (client_sender, client_receiver) = mpsc::unbounded_channel::<Command>();
        self.clients.lock().await.insert(
            client_id,
            ProducerClient {
                sender: client_sender
            }
        );

        (client_id, client_receiver)
    }

    /// Handles sending commands to the given client
    async fn handle_send_commands(&self,
                                  client_id: ClientId,
                                  mut client_receiver: UnboundedReceiver<Command>,
                                  mut writer: OwnedWriteHalf) {
        while let Some(command) = client_receiver.recv().await {
            if command.send_command(&mut writer).await.is_err() {
                self.exchange.lock().await.remove_client(client_id).await;
                break;
            }
        }
    }

    /// Handles receiving commands from the given client
    async fn handle_receive_commands(self: &Arc<Self>, client_id: ClientId, mut reader: OwnedReadHalf) {
        loop {
            match Command::receive_command(&mut reader).await {
                Ok(command) => {
                    match command {
                        Command::CreateQueue { name, auto_delete, ttl } => {
                            self.create_queue(&name, ExchangeQueueOptions { auto_delete, ttl }).await;
                        }
                        Command::BindQueue(queue_name, pattern) => {
                            if let Some(queue) = self.exchange.lock().await.get_queue_by_name(&queue_name) {
                                if let Err(err) = queue.add_binding(&pattern).await {
                                    println!("Error for #{}: Invalid regex pattern: {:?}", client_id, err);
                                }
                            }
                        }
                        Command::StartConsume(queue_name) => {
                            if let Some(queue) = self.exchange.lock().await.get_queue_by_name(&queue_name) {
                                queue.add_client(client_id).await;
                            }
                        }
                        Command::Acknowledge(queue_id, message_id) => {
                            if let Some(queue) = self.exchange.lock().await.get_queue_by_id(queue_id) {
                                queue.acknowledge(client_id, message_id).await;
                            }
                        }
                        _ => {}
                    }
                }
                Err(err) => {
                    println!("Error for #{}: {:?}", client_id, err);
                    self.exchange.lock().await.remove_client(client_id).await;
                    break;
                }
            }
        }
    }

    /// Tries to consume from the given queue
    async fn try_consume_queue(&self, queue_id: QueueId, queue: &mut Queue<QueueMessage>) {
        while queue.len() > 0 {
            let mut clients = self.clients.lock().await;

            if clients.len() > 0 {
                if let Some(client_id) = queue.find_client_to_receive_message() {
                    if let Some(client) = clients.get(&client_id) {
                        let (message_id, message) = queue.pop(client_id).unwrap();
                        let message = Message::new(queue_id, message.routing_key.clone(), message_id, message.data.message_data());
                        if client.sender.send(Command::Message(message)).is_err() {
                            clients.remove(&client_id);
                            queue.remove_client(client_id);
                        }
                    } else {
                        clients.remove(&client_id);
                        queue.remove_client(client_id);
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
}