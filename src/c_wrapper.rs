use std::os::raw::c_char;
use std::path::Path;
use std::sync::Arc;
use std::ffi::{CStr, CString, c_void};

use tokio::runtime::Runtime;

use crate::consumer::{Consumer as ConsumerImpl};
use crate::producer::Producer as ProducerImpl;
use crate::shared_memory::{SmartSharedMemoryAllocator, SharedMemory, SharedMemoryAllocator, GenericMemoryAllocation, SmartMemoryAllocation};
use crate::command::Command;
use crate::exchange::QueueId;
use crate::queue::MessageId;

pub struct IPMQConsumer {
    tokio_runtime: Runtime,
    consumer: ConsumerImpl
}

#[no_mangle]
pub extern fn ipmq_consumer_create(path_ptr: *const c_char,
                                   error_msg_ptr: *mut c_char, error_msg_max_length: usize) -> *mut IPMQConsumer {
    let path = unsafe { CStr::from_ptr(path_ptr).to_str().unwrap() };

    let tokio_runtime = Runtime::new().unwrap();
    match tokio_runtime.block_on(ConsumerImpl::connect(Path::new(path))) {
        Ok(consumer) => {
            heap_allocate(
                IPMQConsumer {
                    tokio_runtime,
                    consumer
                }
            )
        }
        Err(err) => {
            if error_msg_ptr != std::ptr::null_mut() {
                copy_str(format!("{:?}", err), error_msg_ptr, error_msg_max_length);
            }

            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern fn ipmq_consumer_destroy(consumer_ptr: *mut IPMQConsumer) {
    unsafe {
        Box::from_raw(consumer_ptr);
    }
}

#[no_mangle]
pub extern fn ipmq_consumer_create_queue(consumer: &mut IPMQConsumer,
                                         name_ptr: *const c_char, auto_delete: bool, ttl: f64,
                                         error_msg_ptr: *mut c_char, error_msg_max_length: usize) -> i32 {
    let ttl = if ttl >= 0.0 {Some(ttl)} else {None};
    let name = unsafe { CStr::from_ptr(name_ptr).to_str().unwrap() };

    match consumer.tokio_runtime.block_on(consumer.consumer.create_queue(name, auto_delete, ttl)) {
        Ok(()) => 0,
        Err(err) => {
            if error_msg_ptr != std::ptr::null_mut() {
                copy_str(format!("{:?}", err), error_msg_ptr, error_msg_max_length);
            }

            -1
        }
    }
}

#[no_mangle]
pub extern fn ipmq_consumer_bind_queue(consumer: &mut IPMQConsumer,
                                       name_ptr: *const c_char, pattern_ptr: *const c_char,
                                       error_msg_ptr: *mut c_char, error_msg_max_length: usize) -> i32 {
    let name = unsafe { CStr::from_ptr(name_ptr).to_str().unwrap() };
    let pattern = unsafe { CStr::from_ptr(pattern_ptr).to_str().unwrap() };

    match consumer.tokio_runtime.block_on(consumer.consumer.bind_queue(name, pattern)) {
        Ok(()) => 0,
        Err(err) => {
            if error_msg_ptr != std::ptr::null_mut() {
                copy_str(format!("{:?}", err), error_msg_ptr, error_msg_max_length);
            }

            -1
        }
    }
}

pub struct IPMQCommands<'a>(&'a mut Vec<Command>);

#[no_mangle]
pub extern fn ipmq_consumer_start_consume_queue(consumer: &mut IPMQConsumer,
                                                name_ptr: *const c_char,
                                                callback: extern fn(*mut IPMQCommands, u64, *const c_char, u64, *const u8, usize, *mut c_void),
                                                user_ptr: *mut c_void,
                                                error_msg_ptr: *mut c_char, error_msg_max_length: usize) -> i32 {
    let name = unsafe { CStr::from_ptr(name_ptr).to_str().unwrap() };

    if let Err(err) = consumer.tokio_runtime.block_on(consumer.consumer.start_consume_queue(name)) {
        if error_msg_ptr != std::ptr::null_mut() {
            copy_str(format!("{:?}", err), error_msg_ptr, error_msg_max_length);
        }

        return -1;
    }

    let result = consumer.tokio_runtime.block_on(
        consumer.consumer.handle_messages::<_, ()>(|commands, shared_memory, message| {
            let buffer = shared_memory.bytes_from_data(&message.data);

            let mut commands_wrapper = IPMQCommands(commands);
            let routing_key: CString = CString::new(message.routing_key.clone()).unwrap();
            callback(
                &mut commands_wrapper as *mut _,
                message.queue_id,
                routing_key.as_ptr(),
                message.id,
                buffer.as_ptr(),
                buffer.len(),
                user_ptr
            );

            Ok(())
        })
    );

    if result.is_ok() {
        0
    } else {
        if error_msg_ptr != std::ptr::null_mut() {
            copy_str("Failed to consume messages.".to_owned(), error_msg_ptr, error_msg_max_length);
        }

        -1
    }
}

#[no_mangle]
pub extern fn ipmq_consumer_add_ack_command(commands: &mut IPMQCommands, queue_id: QueueId, message_id: MessageId) {
    commands.0.push(Command::Acknowledge(queue_id, message_id));
}

#[no_mangle]
pub extern fn ipmq_consumer_add_nack_command(commands: &mut IPMQCommands, queue_id: QueueId, message_id: MessageId) {
    commands.0.push(Command::NegativeAcknowledge(queue_id, message_id));
}

#[no_mangle]
pub extern fn ipmq_consumer_add_stop_consume_command(commands: &mut IPMQCommands, queue_id: QueueId) {
    commands.0.push(Command::StopConsume(queue_id));
}

pub struct IPMQProducer {
    tokio_runtime: Arc<Runtime>,
    producer: Arc<ProducerImpl>,
    shared_memory_allocator: SmartSharedMemoryAllocator
}

#[no_mangle]
pub extern fn ipmq_producer_create(path_ptr: *const c_char,
                                   shared_memory_path_ptr: *const c_char,
                                   shared_memory_size: usize,
                                   error_msg_ptr: *mut c_char, error_msg_max_length: usize) -> *mut IPMQProducer {
    let path = unsafe { CStr::from_ptr(path_ptr).to_str().unwrap() };
    let shared_memory_path = unsafe { CStr::from_ptr(shared_memory_path_ptr).to_str().unwrap() };

    let tokio_runtime = Arc::new(Runtime::new().unwrap());

    let shared_memory = match SharedMemory::write(Path::new(shared_memory_path), shared_memory_size) {
        Ok(shared_memory) => shared_memory,
        Err(err) => {
            if error_msg_ptr != std::ptr::null_mut() {
                copy_str(format!("{:?}", err), error_msg_ptr, error_msg_max_length);
            }

            return std::ptr::null_mut();
        }
    };

    let producer = ProducerImpl::new(Path::new(path), &shared_memory);
    let shared_memory_allocator = SharedMemoryAllocator::new_smart(shared_memory);

    let tokio_runtime_clone = tokio_runtime.clone();
    let producer_clone = producer.clone();
    std::thread::spawn(move || {
        tokio_runtime_clone.block_on(producer_clone.start()).unwrap();
    });

    heap_allocate(
        IPMQProducer {
            producer,
            tokio_runtime,
            shared_memory_allocator
        }
    )
}

#[no_mangle]
pub extern fn ipmq_producer_destroy(producer_ptr: *mut IPMQProducer) {
    unsafe {
        Box::from_raw(producer_ptr);
    }
}

#[no_mangle]
pub extern fn ipmq_producer_stop(producer: &IPMQProducer) {
    producer.producer.stop();
}

pub struct IPMQMemoryAllocation {
    allocation: Arc<SmartMemoryAllocation>,
}

#[no_mangle]
pub extern fn ipmq_producer_allocate(producer: &IPMQProducer,
                                     size: usize,
                                     error_msg_ptr: *mut c_char, error_msg_max_length: usize) -> *mut IPMQMemoryAllocation {
    let allocation = match producer.tokio_runtime.block_on(producer.producer.allocate(&producer.shared_memory_allocator, size)) {
        Some(allocation) => allocation,
        None => {
            if error_msg_ptr != std::ptr::null_mut() {
                copy_str(format!("Failed to allocate shared memory."), error_msg_ptr, error_msg_max_length);
            }

            return std::ptr::null_mut();
        }
    };

    heap_allocate(
        IPMQMemoryAllocation {
            allocation
        }
    )
}

#[no_mangle]
pub extern fn ipmq_producer_allocation_get_ptr(allocation: &IPMQMemoryAllocation) -> *mut u8 {
    allocation.allocation.ptr()
}

#[no_mangle]
pub extern fn ipmq_producer_return_allocation(allocation_ptr: *mut IPMQMemoryAllocation) {
    unsafe {
        Box::from_raw(allocation_ptr);
    }
}

#[no_mangle]
pub extern fn ipmq_producer_publish_bytes(producer: &IPMQProducer,
                                          routing_key_ptr: *const c_char, message_ptr: *const u8, message_size: usize,
                                          error_msg_ptr: *mut c_char, error_msg_max_length: usize) -> i32 {
    let routing_key = unsafe { CStr::from_ptr(routing_key_ptr).to_str().unwrap() };

    let allocation = match producer.tokio_runtime.block_on(producer.producer.allocate(&producer.shared_memory_allocator, message_size)) {
        Some(allocation) => allocation,
        None => {
            if error_msg_ptr != std::ptr::null_mut() {
                copy_str(format!("Failed to allocate shared memory."), error_msg_ptr, error_msg_max_length);
            }

            return -1;
        }
    };

    let message = unsafe { std::slice::from_raw_parts(message_ptr, message_size) };
    allocation.bytes_mut().copy_from_slice(message);

    producer.tokio_runtime.block_on(producer.producer.publish(routing_key, allocation));
    0
}

#[no_mangle]
pub extern fn ipmq_producer_publish(producer: &IPMQProducer,
                                    routing_key_ptr: *const c_char, allocation: &IPMQMemoryAllocation,
                                    _error_msg_ptr: *mut c_char, _error_msg_max_length: usize) -> i32 {
    let routing_key = unsafe { CStr::from_ptr(routing_key_ptr).to_str().unwrap() };

    producer.tokio_runtime.block_on(producer.producer.publish(routing_key, allocation.allocation.clone()));
    0
}

fn heap_allocate<T>(x: T) -> *mut T {
    Box::leak(Box::new(x)) as *mut _
}

fn copy_str(str: String, destination_ptr: *mut c_char, destination_max_length: usize) {
    let str = str.into_bytes();
    let length = str.len().min(destination_max_length - 1);

    unsafe {
        for i in 0..length {
            *destination_ptr.offset(i as isize) = str[i] as i8;
        }

        *destination_ptr.offset(length as isize) = 0;
    }
}