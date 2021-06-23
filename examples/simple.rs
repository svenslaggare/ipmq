use std::path::Path;

use tokio::time;
use tokio::time::Duration;

use ipmq::producer::{Producer};
use ipmq::shared_memory::{SharedMemory, SharedMemoryAllocator};
use ipmq::shared_memory::GenericMemoryAllocation;
use ipmq::consumer::Consumer;

#[tokio::main]
async fn main() {
    let mut arguments = std::env::args().collect::<Vec<_>>();
    let mode = arguments.remove(1);

    match mode.as_str() {
        "producer" => {
            main_producer().await;
        }
        "consumer" => {
            main_consumer(arguments.get(1).cloned()).await;
        }
        _ => {}
    }
}

async fn main_producer() {
    println!("producer");

    let shared_memory = SharedMemory::write(Path::new("/dev/shm/test.data"), 2048).unwrap();

    let producer = Producer::new(Path::new("test.queue"), shared_memory.path(), shared_memory.size());

    let producer_clone = producer.clone();
    tokio::spawn(async move {
        let shared_memory_allocator = SharedMemoryAllocator::new_smart(shared_memory);

        let mut interval = time::interval(Duration::from_millis(250));
        let mut number = 1;

        loop {
            interval.tick().await;

            let tmp_buffer = format!("Hello, World #{:04}!", number).into_bytes();
            let allocation = {
                let allocation = producer_clone.allocate(&shared_memory_allocator, tmp_buffer.len()).await.unwrap();
                allocation.bytes_mut().copy_from_slice(&tmp_buffer);
                allocation
            };

            number += 1;
            producer_clone.publish("test", allocation).await;
        }
    });

    producer.start().await.unwrap();
}

async fn main_consumer(queue: Option<String>) {
    let queue = queue.unwrap_or("test".to_owned());
    println!("consumer: {}", queue);

    let mut consumer = Consumer::connect(Path::new("test.queue")).await.unwrap();
    consumer.create_queue(&queue, true, Some(1.0)).await.unwrap();
    consumer.bind_queue(&queue, ".*").await.unwrap();
    consumer.start_consume_queue(&queue).await.unwrap();

    consumer.handle_messages::<_, ()>(|shared_memory, message| {
        let buffer = shared_memory.bytes_from_data(&message.data);
        let data_str = std::str::from_utf8(buffer).unwrap();
        println!("{}: {}", message.id, data_str);
        Ok(Some(message.acknowledgement()))
        // if message.id % 10 == 0 {
        //     None
        // } else {
        //     Some(message.acknowledgement())
        // }
    }).await.unwrap();
}