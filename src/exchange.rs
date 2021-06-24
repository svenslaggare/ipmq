use std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use regex::Regex;

use tokio::sync::{Mutex, Notify, RwLock};

use crate::queue::{Queue, ClientId, MessageId};
use crate::shared_memory::SmartMemoryAllocation;

pub type QueueId = u64;

/// Manages the queues
pub struct Exchange {
    next_queue_id: QueueId,
    pub queues: HashMap<QueueId, Arc<ExchangeQueue>>
}

impl Exchange {
    pub fn new() -> Exchange {
        Exchange {
            next_queue_id: 1,
            queues: HashMap::new()
        }
    }

    /// Tries to create a new queue of the given name. If exists, does nothing
    pub fn create(&mut self, name: &str, options: ExchangeQueueOptions) -> (Arc<ExchangeQueue>, bool) {
        match self.get_queue_by_name(name) {
            None => {
                let queue_id = self.next_queue_id;
                println!("Created queue '{}' (id: {})", name, queue_id);
                self.next_queue_id += 1;

                let queue = Arc::new(ExchangeQueue::new(name, queue_id, options));
                self.queues.insert(queue_id, queue.clone());
                (queue, true)
            }
            Some(queue) => {
                (queue, false)
            }
        }
    }

    /// Finds the queues that matches the given routing key
    pub async fn matching_queues(&self, routing_key: &str) -> Vec<Arc<ExchangeQueue>> {
        let mut queues = Vec::new();
        for queue in self.queues.values() {
            if queue.matches(routing_key).await {
                queues.push(queue.clone());
            }
        }

        queues
    }

    /// Removes the given client from the exchange
    pub async fn remove_client(&mut self, client_id: ClientId) {
        let mut should_delete = Vec::new();

        for (id, queue) in self.queues.iter() {
            if queue.remove_client(client_id).await {
                should_delete.push(*id);
            }
        }

        for id in should_delete {
            self.remove_queue(id);
        }
    }

    /// Tries to remove the given queue
    pub fn remove_queue(&mut self, id: QueueId) -> bool {
        if let Some(queue) = self.queues.remove(&id) {
            queue.stop();
            println!("Deleted queue {} (id: {})", queue.name, id);
            true
        } else {
            false
        }
    }

    /// Returns the queue with the given id
    pub fn get_queue_by_id(&mut self, queue_id: QueueId) -> Option<Arc<ExchangeQueue>> {
        self.queues.get(&queue_id).cloned()
    }

    /// Returns the queue with the given name
    pub fn get_queue_by_name(&mut self, name: &str) -> Option<Arc<ExchangeQueue>> {
        self.queues.values().find(|queue| queue.name == name).cloned()
    }
}

/// The options for a queue
pub struct ExchangeQueueOptions {
    pub auto_delete: bool,
    pub ttl: Option<f64>
}

/// Represents a message in the queue
#[derive(Clone)]
pub struct QueueMessage {
    pub routing_key: String,
    pub data: QueueMessageData
}

pub type QueueMessageData = Arc<SmartMemoryAllocation>;

/// Represents a queue in the exchange
pub struct ExchangeQueue {
    pub name: String,
    pub id: QueueId,
    bindings: RwLock<Vec<Regex>>,
    pub queue: Mutex<Queue<QueueMessage>>,
    notify: Notify,
    running: AtomicBool,
    options: ExchangeQueueOptions
}

impl ExchangeQueue {
    pub fn new(name: &str, id: QueueId, options: ExchangeQueueOptions) -> ExchangeQueue {
        ExchangeQueue {
            name: name.to_owned(),
            id,
            bindings: RwLock::new(Vec::new()),
            queue: Mutex::new(Queue::new()),
            notify: Notify::new(),
            running: AtomicBool::new(true),
            options
        }
    }

    /// Future for when the queue gets notified
    pub async fn notified(&self) {
        self.notify.notified().await
    }

    /// Adds a binding for the given pattern
    pub async fn add_binding(&self, pattern: &str) -> Result<(), regex::Error> {
        self.bindings.write().await.push(Regex::new(pattern)?);
        Ok(())
    }

    /// Indicates if the exchange matches the given routing key
    pub async fn matches(&self, routing_key: &str) -> bool {
        self.bindings.read().await.iter().any(|binding| binding.is_match(routing_key))
    }

    /// Adds the given message to the end of the queue
    pub async fn push(&self, message: QueueMessage) {
        self.queue.lock().await.push(message);
        self.notify.notify_one();
    }

    /// Acknowledges the given message (removing it)
    pub async fn acknowledge(&self, client_id: ClientId, message_id: MessageId) {
        self.queue.lock().await.acknowledge(client_id, message_id);
        self.notify.notify_one();
    }

    /// Tries to remove the oldest message from the queue
    pub async fn remove_oldest(&self) -> bool {
        self.queue.lock().await.remove_oldest()
    }

    /// Tries to remove expired messages from the queue
    pub async fn remove_expired(&self) -> bool {
        if let Some(ttl) = self.options.ttl {
            if self.queue.lock().await.remove_expired(ttl) {
                self.notify.notify_one();
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Adds the client with the given id to the queue such that it can receive messages from the queue
    pub async fn add_client(&self, client_id: ClientId) {
        self.queue.lock().await.add_client(client_id);
        self.notify.notify_one();
    }

    /// Tries to remove the given client from the queue
    pub async fn remove_client(&self, client_id: ClientId) -> bool {
        let mut queue = self.queue.lock().await;
        if queue.remove_client(client_id) {
            self.notify.notify_one();
        }

        self.options.auto_delete && queue.num_clients() == 0
    }

    /// Indicates if the queue is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Stops the queue
    pub fn stop(&self) {
        self.notify.notify_one();
        self.running.store(false, Ordering::SeqCst);
    }
}