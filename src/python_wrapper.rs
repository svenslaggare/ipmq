use std::path::Path;
use std::sync::Arc;

use tokio::runtime::Runtime;

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;

use crate::consumer::Consumer;
use crate::producer::Producer;
use crate::shared_memory::{SharedMemory, SharedMemoryAllocator, SmartSharedMemoryAllocator, SmartMemoryAllocation, GenericMemoryAllocation};

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

        let producer = Producer::new(Path::new(path), shared_memory.path(), shared_memory.size());
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

        Ok(
            MemoryAllocationWrapper {
                allocation
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
    allocation: Arc<SmartMemoryAllocation>
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
            .map_err(|err| PyValueError::new_err(err))?;

        Ok(
            ConsumerWrapper {
                tokio_runtime,
                consumer
            }
        )
    }

    fn create_queue(&mut self, name: &str, auto_delete: bool, ttl: Option<f64>) -> PyResult<()> {
        self.tokio_runtime.block_on(self.consumer.create_queue(name, auto_delete, ttl))
            .map_err(|err| PyValueError::new_err(err))?;
        Ok(())
    }

    fn bind_queue(&mut self, name: &str, pattern: &str) -> PyResult<()>  {
        self.tokio_runtime.block_on(self.consumer.bind_queue(name, pattern))
            .map_err(|err| PyValueError::new_err(err))?;
        Ok(())
    }

    fn start_consume_queue(&mut self, py: Python, name: &str, callback: PyObject) -> PyResult<()>  {
        self.tokio_runtime.block_on(self.consumer.start_consume_queue(name))
            .map_err(|err| PyValueError::new_err(err))?;

        self.tokio_runtime.block_on(
            self.consumer.handle_messages::<_, PyErr>(|shared_memory, message| {
                let buffer = shared_memory.bytes_from_data(&message.data);

                callback.call1(
                    py,
                    (message.queue_id, &message.routing_key, message.id, buffer)
                )?;

                Ok(Some(message.acknowledgement()))
            })
        )
    }
}

#[pymodule]
fn libipmq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ProducerWrapper>()?;
    m.add_class::<MemoryAllocationWrapper>()?;
    m.add_class::<ConsumerWrapper>()?;
    Ok(())
}
