use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::ops::DerefMut;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::net::unix::{OwnedWriteHalf, OwnedReadHalf};
use tokio::sync::{Mutex, mpsc};
use tokio::net::{UnixStream, UnixListener};

use crate::queue::{Queue, ClientId};
use crate::command::{Command, Message};
use crate::exchange::{Exchange, QueueId, ExchangeQueue, ExchangeQueueOptions, QueueMessage};
use crate::shared_memory::{SmartSharedMemoryAllocator, SmartMemoryAllocation, GenericMemoryAllocation};

pub struct ProducerClient {
    pub sender: UnboundedSender<Command>,
}

pub struct Producer {
    next_client_id: AtomicU64,
    clients: Mutex<HashMap<ClientId, ProducerClient>>,
    exchange: Mutex<Exchange>,
    shared_memory_spec: (PathBuf, usize)
}

impl Producer {
    pub fn new(shared_memory_spec: (&Path, usize)) -> Producer {
        Producer {
            next_client_id: AtomicU64::new(1),
            clients: Mutex::new(HashMap::new()),
            exchange: Mutex::new(Exchange::new()),
            shared_memory_spec: (shared_memory_spec.0.to_owned(), shared_memory_spec.1)
        }
    }

    pub async fn start(self: &Arc<Self>, path: &Path) -> tokio::io::Result<()> {
        #[allow(unused_must_use)] {
            std::fs::remove_file(path);
        }

        let listener = UnixListener::bind(path)?;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    self.handle_client(stream).await;
                }
                Err(e) => {
                    println!("{:?}", e);
                }
            }
        }
    }

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
                    self_clone.try_consume_queue(
                        queue_clone.id,
                        queue_clone.queue.lock().await.deref_mut()
                    ).await;
                }
            });
        }

        queue
    }

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

    pub async fn publish(&self, routing_key: &str, message: QueueMessage) {
        let matching_queues = self.exchange.lock().await.matching_queues(routing_key).await;
        for queue in matching_queues {
            queue.push(message.clone()).await;
        }
    }

    async fn handle_client(self: &Arc<Self>, stream: UnixStream) {
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
            self_clone.start_send_commands(client_id, client_receiver, writer).await
        });

        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.start_receive_commands(client_id, reader).await
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

    async fn start_send_commands(&self,
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

    async fn start_receive_commands(self: &Arc<Self>, client_id: ClientId, mut reader: OwnedReadHalf) {
        loop {
            match Command::receive_command(&mut reader).await {
                Ok(command) => {
                    match command {
                        Command::CreateQueue(queue_name, auto_delete) => {
                            self.create_queue(&queue_name, ExchangeQueueOptions { auto_delete }).await;
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

    async fn try_consume_queue(&self, queue_id: QueueId, queue: &mut Queue<QueueMessage>) {
        while queue.len() > 0 {
            let mut clients = self.clients.lock().await;

            if clients.len() > 0 {
                if let Some(client_id) = queue.find_best_client() {
                    if let Some(client) = clients.get(&client_id) {
                        let (message_id, message) = queue.pop(client_id).unwrap();
                        if client.sender.send(Command::Message(Message::new(queue_id, message_id, message.message_data()))).is_err() {
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