use std::path::Path;
use std::sync::Arc;
use std::ffi::{c_void, CString};

use tokio::runtime::Runtime;

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::{PyBufferProtocol};
use pyo3::ffi::{Py_buffer, Py_INCREF};
use pyo3::AsPyPointer;
use pyo3::types::PyList;

use crate::consumer::{Consumer};
use crate::producer::Producer;
use crate::shared_memory::{SharedMemory, SharedMemoryAllocator, SmartSharedMemoryAllocator, SmartMemoryAllocation, GenericMemoryAllocation};
use crate::exchange::QueueId;
use crate::queue::MessageId;
use crate::command::Command;

#[pyclass(name="Producer")]
struct ProducerWrapper {
    producer: Arc<Producer>,
    tokio_runtime: Arc<Runtime>,
    shared_memory_allocator: SmartSharedMemoryAllocator
}

#[pymethods]
impl ProducerWrapper {
    #[new]
    fn new(path: &str, shared_memory_path: &str, shared_memory_size: usize) -> PyResult<Self> {
        let tokio_runtime = Arc::new(Runtime::new().unwrap());

        let shared_memory = SharedMemory::write(Path::new(shared_memory_path), shared_memory_size)
            .map_err(|err| PyValueError::new_err(format!("Failed to create shared memory: {:?}.", err)))?;

        let producer = Producer::new(Path::new(path), &shared_memory);
        let shared_memory_allocator = SharedMemoryAllocator::new_smart(shared_memory);

        let tokio_runtime_clone = tokio_runtime.clone();
        let producer_clone = producer.clone();
        std::thread::spawn(move || {
            tokio_runtime_clone.block_on(producer_clone.start()).unwrap();
        });

        Ok(
            ProducerWrapper {
                producer,
                tokio_runtime,
                shared_memory_allocator
            }
        )
    }

    fn allocate(&mut self, size: usize) -> PyResult<MemoryAllocationWrapper> {
        let allocation = self.tokio_runtime.block_on(self.producer.allocate(&self.shared_memory_allocator, size))
            .ok_or_else(|| PyValueError::new_err("Failed to allocate."))?;

        let shape = vec![allocation.size() as isize];
        Ok(
            MemoryAllocationWrapper {
                allocation,
                buffer_shape: shape,
                buffer_format: CString::new("B").unwrap()
            }
        )
    }

    fn publish(&self, routing_key: String, message: MemoryAllocationWrapper) {
        let producer_clone = self.producer.clone();
        self.tokio_runtime.block_on(async move {
            producer_clone.publish(&routing_key, message.allocation).await;
        });
    }

    fn publish_bytes(&mut self, routing_key: String, message: &[u8]) -> PyResult<()> {
        let mut allocation = self.allocate(message.len())?;
        allocation.copy_from(0, message);
        self.publish(routing_key, allocation);
        Ok(())
    }
}

#[pyclass(name="MemoryAllocation")]
#[derive(Clone)]
struct MemoryAllocationWrapper {
    allocation: Arc<SmartMemoryAllocation>,
    buffer_shape: Vec<isize>,
    buffer_format: CString
}

#[pymethods]
impl MemoryAllocationWrapper {
    fn bytes(&self) -> &[u8] {
        self.allocation.bytes()
    }

    fn copy_from(&mut self, offset: usize, data: &[u8]) {
        self.allocation.bytes_mut()[offset..offset + data.len()].copy_from_slice(data)
    }
}

#[pyproto]
impl PyBufferProtocol for MemoryAllocationWrapper {
    fn bf_getbuffer(mut slf: PyRefMut<Self>, view: *mut Py_buffer, _flags: i32) -> PyResult<()> {
        unsafe {
            (*view).obj = slf.as_ptr();
            (*view).buf = slf.allocation.bytes_mut().as_mut_ptr() as *mut c_void;
            (*view).len = slf.allocation.bytes_mut().len() as isize;
            (*view).readonly = 0;
            (*view).itemsize = 1;
            (*view).format = slf.buffer_format.as_ptr() as *mut _;
            (*view).ndim = 1;
            (*view).shape = slf.buffer_shape.as_mut_ptr();
            (*view).strides = (&mut (*view).itemsize) as *mut isize;
            (*view).suboffsets = std::ptr::null_mut();
            (*view).internal = std::ptr::null_mut();

            Py_INCREF(slf.as_ptr());
        }

        Ok(())
    }

    fn bf_releasebuffer(_slf: PyRefMut<Self>, _view: *mut Py_buffer) -> PyResult<()> {
        Ok(())
    }
}

#[pyclass(name="Consumer")]
struct ConsumerWrapper {
    tokio_runtime: Runtime,
    consumer: Consumer
}

#[pymethods]
impl ConsumerWrapper {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let tokio_runtime = Runtime::new().unwrap();
        let consumer = tokio_runtime.block_on(Consumer::connect(Path::new(path)))
            .map_err(|err| PyValueError::new_err(format!("{:?}", err)))?;

        Ok(
            ConsumerWrapper {
                tokio_runtime,
                consumer
            }
        )
    }

    fn create_queue(&mut self, name: &str, auto_delete: bool, ttl: Option<f64>) -> PyResult<()> {
        self.tokio_runtime.block_on(self.consumer.create_queue(name, auto_delete, ttl))
            .map_err(|err| PyValueError::new_err(format!("{:?}", err)))?;
        Ok(())
    }

    fn bind_queue(&mut self, name: &str, pattern: &str) -> PyResult<()>  {
        self.tokio_runtime.block_on(self.consumer.bind_queue(name, pattern))
            .map_err(|err| PyValueError::new_err(format!("{:?}", err)))?;
        Ok(())
    }

    fn start_consume_queue(&mut self, py: Python, name: &str, callback: PyObject) -> PyResult<()>  {
        self.tokio_runtime.block_on(self.consumer.start_consume_queue(name))
            .map_err(|err| PyValueError::new_err(format!("{:?}", err)))?;

        self.tokio_runtime.block_on(
            self.consumer.handle_messages::<_, PyErr>(|commands, shared_memory, message| {
                let buffer = shared_memory.bytes_from_data(&message.data);
                let callback_commands = PyList::empty(py);

                callback.call1(
                    py,
                    (callback_commands, message.queue_id, &message.routing_key, message.id, buffer)
                )?;

                for command in callback_commands {
                    let command: CommandWrapper = command.extract()?;
                    commands.push(command.command().ok_or_else(|| PyValueError::new_err("invalid command"))?);
                }

                Ok(())
            })
        )
    }
}

#[pyclass(name="Command")]
#[derive(Clone)]
struct CommandWrapper {
    command_id: u64,
    queue_id: QueueId,
    message_id: MessageId
}

#[pymethods]
impl CommandWrapper {
    #[staticmethod]
    fn acknowledgement(queue_id: QueueId, message_id: MessageId) -> CommandWrapper {
        CommandWrapper {
            command_id: 1,
            queue_id,
            message_id
        }
    }

    #[staticmethod]
    fn negative_acknowledgement(queue_id: QueueId, message_id: MessageId) -> CommandWrapper {
        CommandWrapper {
            command_id: 2,
            queue_id,
            message_id
        }
    }

    #[staticmethod]
    fn stop_consume(queue_id: QueueId) -> CommandWrapper {
        CommandWrapper {
            command_id: 3,
            queue_id,
            message_id: 0
        }
    }
}

impl CommandWrapper {
    fn command(&self) -> Option<Command> {
        match self.command_id {
            1 => Some(Command::Acknowledge(self.queue_id, self.message_id)),
            2 => Some(Command::NegativeAcknowledge(self.queue_id, self.message_id)),
            3 => Some(Command::StopConsume(self.queue_id)),
            _ => None
        }
    }
}

#[pymodule]
fn libipmq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ProducerWrapper>()?;
    m.add_class::<MemoryAllocationWrapper>()?;
    m.add_class::<ConsumerWrapper>()?;
    m.add_class::<CommandWrapper>()?;
    Ok(())
}
