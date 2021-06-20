use std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use regex::Regex;

use tokio::sync::{Mutex, Notify, RwLock};

use crate::queue::{Queue, ClientId, MessageId};
use crate::shared_memory::SmartMemoryAllocation;

pub type QueueId = u64;

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

    pub async fn matching_queues(&self, routing_key: &str) -> Vec<Arc<ExchangeQueue>> {
        let mut queues = Vec::new();
        for queue in self.queues.values() {
            if queue.matches(routing_key).await {
                queues.push(queue.clone());
            }
        }

        queues
    }

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

    pub fn remove_queue(&mut self, id: QueueId) -> bool {
        if let Some(queue) = self.queues.remove(&id) {
            queue.stop();
            println!("Deleted queue {} (id: {})", queue.name, id);
            true
        } else {
            false
        }
    }

    pub fn get_queue_by_id(&mut self, queue_id: QueueId) -> Option<Arc<ExchangeQueue>> {
        self.queues.get(&queue_id).cloned()
    }

    pub fn get_queue_by_name(&mut self, name: &str) -> Option<Arc<ExchangeQueue>> {
        self.queues.values().find(|queue| queue.name == name).cloned()
    }
}

pub struct ExchangeQueueOptions {
    pub auto_delete: bool
}

pub type QueueMessage = Arc<SmartMemoryAllocation>;

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

    pub async fn notified(&self) {
        self.notify.notified().await
    }

    pub async fn add_binding(&self, pattern: &str) -> Result<(), regex::Error> {
        self.bindings.write().await.push(Regex::new(pattern)?);
        Ok(())
    }

    pub async fn matches(&self, routing_key: &str) -> bool {
        self.bindings.read().await.iter().any(|binding| binding.is_match(routing_key))
    }

    pub async fn push(&self, message: QueueMessage) {
        self.queue.lock().await.push(message);
        self.notify.notify_one();
    }

    pub async fn acknowledge(&self, client_id: ClientId, message_id: MessageId) {
        self.queue.lock().await.acknowledge(client_id, message_id);
        self.notify.notify_one();
    }

    pub async fn add_client(&self, client_id: ClientId) {
        self.queue.lock().await.add_client(client_id);
        self.notify.notify_one();
    }

    pub async fn remove_client(&self, client_id: ClientId) -> bool {
        let mut queue = self.queue.lock().await;
        if queue.remove_client(client_id) {
            self.notify.notify_one();
        }

        println!("{} clients: {}", self.name, queue.num_clients());
        self.options.auto_delete && queue.num_clients() == 0
    }

    pub async fn remove_oldest(&self) -> bool {
        self.queue.lock().await.remove_oldest()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn stop(&self) {
        self.notify.notify_one();
        self.running.store(false, Ordering::SeqCst);
    }
}