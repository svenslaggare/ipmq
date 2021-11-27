use std::path::Path;

use tokio::time;
use tokio::time::Duration;

use ipmq::producer::{Producer};
use ipmq::shared_memory::{SharedMemory};
use ipmq::shared_memory::GenericMemoryAllocation;
use ipmq::consumer::Consumer;
use ipmq::helpers;

#[tokio::main]
async fn main() {
    helpers::setup_logging().unwrap();

    let mut arguments = std::env::args().collect::<Vec<_>>();
    let mode = arguments.remove(1);

    match mode.as_str() {
        "producer" => { main_producer1().await; }
        "producer2" => { main_producer2().await; }
        "consumer" => { main_consumer1(arguments.get(1).cloned()).await; }
        "consumer2" => { main_consumer2(arguments.get(1).cloned()).await; }
        "consumer3" => { main_consumer3(arguments.get(1).cloned()).await; }
        _ => {}
    }
}

async fn main_producer1() {
    println!("producer");

    let shared_memory = SharedMemory::write(Path::new("/dev/shm/test.data"), 2048).unwrap();
    let producer = Producer::new(Path::new("test.queue"), shared_memory);

    let producer_clone = producer.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(250));
        let mut number = 1;

        loop {
            interval.tick().await;

            let message = format!("Hello, World #{:04}!", number).into_bytes();
            number += 1;
            producer_clone.publish_bytes("test", &message).await;
        }
    });

    producer.start().await.unwrap();
}

async fn main_producer2() {
    println!("producer");

    let shared_memory = SharedMemory::write(Path::new("/dev/shm/test.data"), 2048).unwrap();
    let producer = Producer::new(Path::new("test.queue"), shared_memory);

    let producer_clone = producer.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(250));
        let mut number = 1;

        loop {
            interval.tick().await;

            let tmp_buffer = format!("Hello, World #{:04}!", number).into_bytes();
            let allocation = {
                let allocation = producer_clone.allocate(tmp_buffer.len()).await.unwrap();
                allocation.bytes_mut().copy_from_slice(&tmp_buffer);
                allocation
            };

            number += 1;
            producer_clone.publish("test", allocation).await;
        }
    });

    producer.start().await.unwrap();
}

async fn main_consumer1(queue: Option<String>) {
    let queue = queue.unwrap_or("test".to_owned());
    println!("consumer: {}", queue);

    let mut consumer = Consumer::connect(Path::new("test.queue")).await.unwrap();
    consumer.create_queue(&queue, true, Some(1.0)).await.unwrap();
    consumer.bind_queue(&queue, ".*").await.unwrap();
    consumer.start_consume_queue(&queue).await.unwrap();

    consumer.handle_messages::<_, ()>(|commands, shared_memory, message| {
        let data_str = std::str::from_utf8(message.buffer(shared_memory)).unwrap();
        println!("{}: {}", message.id, data_str);

        commands.push(message.acknowledgement());

        Ok(())
    }).await.unwrap();
}

async fn main_consumer2(queue: Option<String>) {
    let queue = queue.unwrap_or("test".to_owned());
    println!("consumer: {}", queue);

    let mut consumer = Consumer::connect(Path::new("test.queue")).await.unwrap();
    consumer.create_queue(&queue, true, Some(1.0)).await.unwrap();
    consumer.bind_queue(&queue, ".*").await.unwrap();
    consumer.start_consume_queue(&queue).await.unwrap();

    consumer.handle_messages::<_, ()>(|commands, shared_memory, message| {
        let data_str = std::str::from_utf8(message.buffer(shared_memory)).unwrap();
        println!("{}: {}", message.id, data_str);

        commands.push(message.acknowledgement());

        if message.id == 10 {
            commands.push(message.stop_consume());
        }

        Ok(())
    }).await.unwrap();
}

async fn main_consumer3(queue: Option<String>) {
    let queue = queue.unwrap_or("test".to_owned());
    println!("consumer: {}", queue);

    let mut consumer = Consumer::connect(Path::new("test.queue")).await.unwrap();
    consumer.create_queue(&queue, true, Some(1.0)).await.unwrap();
    consumer.bind_queue(&queue, ".*").await.unwrap();
    consumer.start_consume_queue(&queue).await.unwrap();

    let mut number = 0;
    consumer.handle_messages::<_, ()>(|commands, shared_memory, message| {
        let data_str = std::str::from_utf8(message.buffer(shared_memory)).unwrap();
        println!("{}: {}", message.id, data_str);

        commands.push(message.acknowledgement());

        number += 1;
        if number % 10 == 0 {
            commands.push(message.negative_acknowledgement());
        } else {
            commands.push(message.acknowledgement());
        }

        Ok(())
    }).await.unwrap();
}