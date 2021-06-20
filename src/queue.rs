use std::collections::{VecDeque, BTreeSet, BTreeMap, HashMap};

pub type ClientId = u64;
pub type MessageId = u64;

pub struct Queue<T> {
    next_message_id: MessageId,
    queue_data: HashMap<MessageId, T>,
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

    pub fn push(&mut self, message: T) -> MessageId {
        let message_id = self.next_message_id;
        self.next_message_id += 1;
        self.queue_data.insert(message_id, message);
        self.queue.push_back(message_id);
        message_id
    }

    pub fn pop(&mut self, client_id: ClientId) -> Option<(MessageId, &T)> {
        let message_id = self.queue.pop_front()?;
        self.unacknowledged.entry(client_id).or_insert_with(|| BTreeSet::new()).insert(message_id);
        let data = &self.queue_data[&message_id];
        self.last_pop_client_id = Some(client_id);
        Some((message_id, data))
    }

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

    pub fn remove_oldest(&mut self) -> bool {
        let remove_unacknowledged = |self_ref: &mut Self, message_id| {
            for unacknowledged in self_ref.unacknowledged.values_mut() {
                unacknowledged.remove(&message_id);
            }

            self_ref.queue_data.remove(&message_id);
        };

        let remove_queued = |self_ref: &mut Self, message_id| {
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

    pub fn add_client(&mut self, client_id: ClientId) {
        self.unacknowledged.entry(client_id).or_insert_with(|| BTreeSet::new());
    }

    pub fn find_best_client(&mut self) -> Option<ClientId> {
        self.find_best_client_least_unacknowledged()
        // self.find_best_client_no_unacknowledged()
    }

    fn find_best_client_least_unacknowledged(&mut self) -> Option<ClientId> {
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

    fn find_best_client_no_unacknowledged(&mut self) -> Option<ClientId> {
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

    assert_eq!(None, queue.queue_data.get(&id1));
    assert_eq!(Some(&3), queue.queue_data.get(&id3));
}