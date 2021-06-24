use std::os::raw::c_char;
use std::path::Path;
use std::sync::Arc;

use tokio::runtime::Runtime;
use std::ffi::{CStr, CString};

use crate::consumer::Consumer as ConsumerImpl;
use crate::producer::Producer as ProducerImpl;
use crate::shared_memory::{SmartSharedMemoryAllocator, SharedMemory, SharedMemoryAllocator, GenericMemoryAllocation};

#[repr(C)]
pub struct Consumer {
    tokio_runtime: Runtime,
    consumer: ConsumerImpl
}

#[no_mangle]
pub extern fn ipmq_create_consumer(path_ptr: *const c_char) -> Option<Box<Consumer>> {
    let path = unsafe { CStr::from_ptr(path_ptr).to_str().unwrap() };

    let tokio_runtime = Runtime::new().unwrap();
    if let Ok(consumer) = tokio_runtime.block_on(ConsumerImpl::connect(Path::new(path))) {
        Some(
            Box::new(
                Consumer {
                    tokio_runtime,
                    consumer
                }
            )
        )
    } else {
        None
    }
}

#[no_mangle]
pub extern fn ipmq_destroy_consumer(consumer_ptr: *mut Consumer) {
    unsafe {
        Box::from_raw(consumer_ptr);
    }
}

#[no_mangle]
pub extern fn ipmq_consumer_create_queue(consumer: &mut Consumer, name_ptr: *const c_char, auto_delete: bool, ttl: f64) -> i32 {
    let ttl = if ttl >= 0.0 {Some(ttl)} else {None};
    let name = unsafe { CStr::from_ptr(name_ptr).to_str().unwrap() };

    if let Err(_) = consumer.tokio_runtime.block_on(consumer.consumer.create_queue(name, auto_delete, ttl)) {
        -1
    } else {
        0
    }
}

#[no_mangle]
pub extern fn ipmq_consumer_bind_queue(consumer: &mut Consumer, name_ptr: *const c_char, pattern_ptr: *const c_char) -> i32 {
    let name = unsafe { CStr::from_ptr(name_ptr).to_str().unwrap() };
    let pattern = unsafe { CStr::from_ptr(pattern_ptr).to_str().unwrap() };

    if let Err(_) = consumer.tokio_runtime.block_on(consumer.consumer.bind_queue(name, pattern)) {
        -1
    } else {
        0
    }
}

#[no_mangle]
pub extern fn ipmq_consumer_start_consume_queue(consumer: &mut Consumer, name_ptr: *const c_char, callback: extern fn(u64, *const c_char, u64, *const u8, usize)) -> i32 {
    let name = unsafe { CStr::from_ptr(name_ptr).to_str().unwrap() };

    if let Err(_) = consumer.tokio_runtime.block_on(consumer.consumer.start_consume_queue(name)) {
        return -1;
    }

    let result = consumer.tokio_runtime.block_on(
        consumer.consumer.handle_messages::<_, ()>(|shared_memory, message| {
            let buffer = shared_memory.bytes_from_data(&message.data);

            let routing_key: CString = CString::new(message.routing_key.clone()).unwrap();
            callback(
                message.queue_id,
                routing_key.as_ptr(),
                message.id,
                buffer.as_ptr(),
                buffer.len()
            );

            Ok(Some(message.acknowledgement()))
        })
    );

    if result.is_ok() {0} else {-1}
}

#[repr(C)]
pub struct Producer {
    tokio_runtime: Arc<Runtime>,
    producer: Arc<ProducerImpl>,
    shared_memory_allocator: SmartSharedMemoryAllocator
}

#[no_mangle]
pub extern fn ipmq_create_producer(path_ptr: *const c_char,
                                   shared_memory_path_ptr: *const c_char,
                                   shared_memory_size: usize) -> Option<Box<Producer>> {
    let path = unsafe { CStr::from_ptr(path_ptr).to_str().unwrap() };
    let shared_memory_path = unsafe { CStr::from_ptr(shared_memory_path_ptr).to_str().unwrap() };

    let tokio_runtime = Arc::new(Runtime::new().unwrap());

    let shared_memory = SharedMemory::write(Path::new(shared_memory_path), shared_memory_size);
    if shared_memory.is_err() {
        return None;
    }
    let shared_memory = shared_memory.unwrap();

    let producer = ProducerImpl::new(Path::new(path), shared_memory.path(), shared_memory.size());
    let shared_memory_allocator = SharedMemoryAllocator::new_smart(shared_memory);

    let tokio_runtime_clone = tokio_runtime.clone();
    let producer_clone = producer.clone();
    std::thread::spawn(move || {
        tokio_runtime_clone.block_on(producer_clone.start()).unwrap();
    });

    Some(
        Box::new(
            Producer {
                producer,
                tokio_runtime,
                shared_memory_allocator
            }
        )
    )
}

#[no_mangle]
pub extern fn ipmq_destroy_producer(producer_ptr: *mut Producer) {
    unsafe {
        Box::from_raw(producer_ptr);
    }
}

#[no_mangle]
pub extern fn ipmq_producer_publish_bytes(producer: &Producer, routing_key_ptr: *const c_char, message_ptr: *const u8, message_size: usize) -> i32 {
    let routing_key = unsafe { CStr::from_ptr(routing_key_ptr).to_str().unwrap() };

    let allocation = producer.tokio_runtime.block_on(producer.producer.allocate(&producer.shared_memory_allocator, message_size));
    if allocation.is_none() {
        return -1;
    }
    let allocation = allocation.unwrap();

    let message = unsafe { std::slice::from_raw_parts(message_ptr, message_size) };
    allocation.bytes_mut().copy_from_slice(message);

    producer.tokio_runtime.block_on(producer.producer.publish(routing_key, allocation));
    0
}
