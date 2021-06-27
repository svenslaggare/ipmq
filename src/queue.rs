use std::collections::{VecDeque, BTreeSet, BTreeMap, HashMap};

pub type ClientId = u64;
pub type MessageId = u64;

/// Represents a queue entry
pub struct QueueEntry<T> {
    pub value: T,
    pub created: std::time::Instant
}

impl<T> QueueEntry<T> {
    pub fn new(value: T) -> QueueEntry<T> {
        QueueEntry {
            value,
            created: std::time::Instant::now()
        }
    }
}

/// Represents a queue
pub struct Queue<T> {
    next_message_id: MessageId,
    queue_data: HashMap<MessageId, QueueEntry<T>>,
    queue: VecDeque<MessageId>,
    unacknowledged: BTreeMap<ClientId, BTreeSet<MessageId>>,
    last_pop_client_id: Option<ClientId>
}

impl<T> Queue<T> {
    pub fn new() -> Queue<T> {
        Queue {
            next_message_id: 1,
            queue_data: HashMap::new(),
            queue: VecDeque::new(),
            unacknowledged: BTreeMap::new(),
            last_pop_client_id: None
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn num_clients(&self) -> usize {
        self.unacknowledged.len()
    }

    /// Adds a new message to the end of the queue
    pub fn push(&mut self, message: T) -> MessageId {
        let message_id = self.next_message_id;
        self.next_message_id += 1;
        self.queue_data.insert(message_id, QueueEntry::new(message));
        self.queue.push_back(message_id);
        message_id
    }

    /// Pops from the start of the queue
    pub fn pop(&mut self, client_id: ClientId) -> Option<(MessageId, &T)> {
        let message_id = self.queue.pop_front()?;
        self.unacknowledged.entry(client_id).or_insert_with(|| BTreeSet::new()).insert(message_id);
        let data = &self.queue_data[&message_id];
        self.last_pop_client_id = Some(client_id);
        Some((message_id, &data.value))
    }

    /// Acknowledges the given message, removing it completely from the queue
    pub fn acknowledge(&mut self, client_id: ClientId, message_id: MessageId) -> bool {
        if let Some(unacknowledged) = self.unacknowledged.get_mut(&client_id) {
            if unacknowledged.remove(&message_id) {
                self.queue_data.remove(&message_id);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Negative acknowledges the given message, putting it back into the end of the queue
    pub fn negative_acknowledge(&mut self, client_id: ClientId, message_id: MessageId) -> bool {
        if let Some(unacknowledged) = self.unacknowledged.get_mut(&client_id) {
            if unacknowledged.remove(&message_id) {
                self.queue.push_back(message_id);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Removes the oldest message from the queue (including unacknowledged)
    pub fn remove_oldest(&mut self) -> bool {
        let remove_unacknowledged = |self_ref: &mut Self, message_id| {
            println!("Removed: {} (full)", message_id);
            for unacknowledged in self_ref.unacknowledged.values_mut() {
                unacknowledged.remove(&message_id);
            }

            self_ref.queue_data.remove(&message_id);
        };

        let remove_queued = |self_ref: &mut Self, message_id| {
            println!("Removed: {} (full)", message_id);
            self_ref.queue.remove(0);
            self_ref.queue_data.remove(&message_id);
        };

        match (self.oldest_unacknowledged(), self.oldest_queued()) {
            (Some(unack), Some(queued)) => {
                if unack < queued {
                    remove_unacknowledged(self, unack);
                } else {
                    remove_queued(self, queued);
                }

                true
            }
            (None, Some(queued)) => {
                remove_queued(self, queued);
                true
            }
            (Some(unack), None) => {
                remove_unacknowledged(self, unack);
                true
            }
            _ => { false }
        }
    }

    fn oldest_unacknowledged(&self) -> Option<MessageId> {
        let mut oldest: Option<MessageId> = None;
        for unacknowledged in self.unacknowledged.values() {
            if let Some(client_oldest) = unacknowledged.iter().next() {
                match oldest {
                    Some(this_oldest) if this_oldest < *client_oldest => {
                        oldest = Some(*client_oldest);
                    }
                    None => {
                        oldest = Some(*client_oldest);
                    }
                    _ => {}
                }
            }
        }

        oldest
    }

    fn oldest_queued(&self) -> Option<ClientId> {
        self.queue.front().cloned()
    }

    /// Removes messages that have expired
    pub fn remove_expired(&mut self, max_alive: f64) -> bool {
        let mut any_removed = false;
        let now = std::time::Instant::now();
        let get_duration = |queue_data: &HashMap<MessageId, QueueEntry<T>>, message_id: MessageId| {
            (now - queue_data.get(&message_id).unwrap().created).as_micros() as f64 / 1.06E6
        };

        // Remove queued
        let mut tmp_queue = std::mem::take(&mut self.queue);
        tmp_queue.retain(|message_id| {
            if get_duration(&self.queue_data, *message_id) >= max_alive {
                self.queue_data.remove(message_id);
                println!("Removed (TTL): {}", message_id);
                any_removed = true;
                false
            } else {
                true
            }
        });
        std::mem::swap(&mut self.queue, &mut tmp_queue);

        // Removed unacknowledged
        let mut to_remove = Vec::new();
        for unacknowledged in self.unacknowledged.values() {
            for message_id in unacknowledged {
                if get_duration(&self.queue_data, *message_id) >= max_alive {
                    println!("Removed (TTL): {}", message_id);
                    self.queue_data.remove(message_id);
                    to_remove.push(*message_id);
                }
            }
        }

        for unacknowledged in self.unacknowledged.values_mut() {
            for message_id in &to_remove {
                unacknowledged.remove(message_id);
            }
        }

        any_removed
    }

    /// Adds the given client to the queue such that it can receive messages from the queue
    pub fn add_client(&mut self, client_id: ClientId) {
        self.unacknowledged.entry(client_id).or_insert_with(|| BTreeSet::new());
    }

    /// Tries to remove the given client from the queue. If removed, unacknowledged messages are put back into the queue.
    pub fn remove_client(&mut self, client_id: ClientId) -> bool {
        if let Some(unacknowledged) = self.unacknowledged.remove(&client_id) {
            for message_id in unacknowledged.into_iter().rev() {
                self.queue.push_front(message_id);
            }

            true
        } else {
            false
        }
    }

    /// Finds the client to receive the next message
    pub fn find_client_to_receive_message(&mut self) -> Option<ClientId> {
        // self.find_client_least_unacknowledged()
        self.find_client_no_unacknowledged()
    }

    fn find_client_least_unacknowledged(&mut self) -> Option<ClientId> {
        let mut best_client_id = None;
        let mut best_num_unacknowledged = 0;

        for (client_id, unacknowledged) in &self.unacknowledged {
            if unacknowledged.len() < best_num_unacknowledged || best_client_id.is_none() {
                best_client_id = Some(*client_id);
                best_num_unacknowledged = unacknowledged.len();
            } else if unacknowledged.len() == best_num_unacknowledged && self.last_pop_client_id == best_client_id {
                best_client_id = Some(*client_id);
            }
        }

        best_client_id
    }

    fn find_client_no_unacknowledged(&mut self) -> Option<ClientId> {
        let mut best_client_id = None;
        for (client_id, unacknowledged) in &self.unacknowledged {
            if unacknowledged.is_empty() {
                if best_client_id.is_none() || self.last_pop_client_id == best_client_id {
                    best_client_id = Some(*client_id);
                }
            }
        }

        best_client_id
    }
}

#[test]
fn test_remove_oldest1() {
    let mut queue = Queue::<i32>::new();
    queue.push(1);
    queue.push(2);
    queue.push(3);

    assert!(queue.remove_oldest());
    assert_eq!(2, queue.pop(1).unwrap().0);
    assert_eq!(3, queue.pop(1).unwrap().0);
}

#[test]
fn test_remove_oldest2() {
    let mut queue = Queue::<i32>::new();
    queue.push(1);
    queue.push(2);
    queue.push(3);

    let id1 = queue.pop(1).unwrap().0;
    let id2 = queue.pop(1).unwrap().0;
    let id3 = queue.pop(1).unwrap().0;

    assert!(queue.acknowledge(1, id2));
    assert!(queue.remove_oldest());

    assert_eq!(None, queue.queue_data.get(&id1).map(|x| &x.value));
    assert_eq!(Some(&3), queue.queue_data.get(&id3).map(|x| &x.value));
}