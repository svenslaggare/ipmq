use std::sync::{Arc};
use std::path::Path;

use opencv::videoio::VideoCapture;
use opencv::prelude::{VideoCaptureTrait};
use opencv::prelude::MatTrait;
use opencv::core::{Mat, Size2i};

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
#[derive(Clone)]
pub struct CameraMetadata {
    pub width: i32,
    pub height: i32,
    pub typ: i32
}

async fn main_producer() {
    println!("producer");

    let mut video_capture = VideoCapture::new(0, opencv::videoio::CAP_V4L2).unwrap();
    if !video_capture.is_opened().unwrap() {
        panic!("Unable to open camera!");
    }

    video_capture.set(opencv::videoio::CAP_PROP_FOURCC, opencv::videoio::VideoWriter::fourcc('M' as i8, 'J' as i8, 'P' as i8, 'G' as i8).unwrap() as f64).unwrap();
    video_capture.set(opencv::videoio::CAP_PROP_FRAME_WIDTH, 1280.0).unwrap();
    video_capture.set(opencv::videoio::CAP_PROP_FRAME_HEIGHT, 720.0).unwrap();
    video_capture.set(opencv::videoio::CAP_PROP_FPS, 30.0).unwrap();

    let mut ref_frame = Mat::default();
    video_capture.read(&mut ref_frame).unwrap();
    let frame_data_size = ref_frame.elem_size().unwrap() * ref_frame.total().unwrap() as usize;
    let fps = video_capture.get(opencv::videoio::CAP_PROP_FPS).unwrap();
    println!("{}x{} @ {} FPS", ref_frame.cols(), ref_frame.rows(), fps);

    let metadata_size = std::mem::size_of::<CameraMetadata>();
    let frame_size = frame_data_size + metadata_size;
    let shared_memory = SharedMemory::write(Path::new("/dev/shm/test.data"), frame_size * 3).unwrap();

    let producer = Producer::new(shared_memory.path(), shared_memory.size());

    let producer_clone = producer.clone();
    tokio::spawn(async move {
        let shared_memory_allocator = SharedMemoryAllocator::new_smart(shared_memory);

        let mut frame_count = 0;
        let mut last_measurement = std::time::Instant::now();

        loop {
            let allocation = producer_clone.allocate(&shared_memory_allocator, frame_size).await.unwrap();
            let mut frame = unsafe {
                let frame_metadata = CameraMetadata {
                    width: ref_frame.cols(),
                    height: ref_frame.rows(),
                    typ: ref_frame.typ().unwrap()
                };
                std::ptr::write(allocation.bytes_mut().as_mut_ptr() as *mut _, frame_metadata);

                Mat::new_size_with_data(
                    Size2i::new(ref_frame.cols(), ref_frame.rows()),
                    ref_frame.typ().unwrap(),
                    allocation.bytes_mut()[metadata_size..].as_mut_ptr() as *mut _,
                    opencv::core::Mat_AUTO_STEP
                ).unwrap()
            };
            tokio::task::block_in_place(|| video_capture.read(&mut frame).unwrap());

            producer_clone.publish("test", allocation).await;

            let elapsed = (std::time::Instant::now() - last_measurement).as_micros() as f64 / 1.0E6;
            frame_count += 1;
            if elapsed >= 1.0 {
                println!("FPS: {:.2}", frame_count as f64 / elapsed);
                frame_count = 0;
                last_measurement = std::time::Instant::now();
            }
        }
    });

    producer.start(Path::new("test.queue")).await.unwrap();
}

async fn main_consumer(queue: Option<String>) {
    let queue = queue.unwrap_or("test".to_owned());
    println!("consumer: {}", queue);

    let mut consumer = Consumer::connect(Path::new("test.queue")).await.unwrap();
    consumer.create_queue(&queue, true, None).await.unwrap();
    consumer.bind_queue(&queue, ".*").await.unwrap();
    consumer.start_consume_queue(&queue).await.unwrap();

    consumer.handle_messages::<_, ()>(|shared_memory, message| {
        let metadata_size = std::mem::size_of::<CameraMetadata>();
        let buffer = shared_memory.bytes_mut_from_data(&message.data);
        let frame_metadata = unsafe { std::ptr::read(buffer[..metadata_size].as_ptr() as *const CameraMetadata) };

        println!("Got: {}, {}x{}", message.id, frame_metadata.width, frame_metadata.height);
        let frame = unsafe {
            Mat::new_size_with_data(
                Size2i::new(frame_metadata.width, frame_metadata.height),
                frame_metadata.typ,
                (&mut buffer[metadata_size..]).as_mut_ptr() as *mut _,
                opencv::core::Mat_AUTO_STEP
            ).unwrap()
        };
        opencv::highgui::imshow(format!("Queue: {}", queue).as_str(), &frame).unwrap();
        opencv::highgui::wait_key(1).unwrap();

        Ok(Some(message.acknowledgement()))
    }).await.unwrap();
}