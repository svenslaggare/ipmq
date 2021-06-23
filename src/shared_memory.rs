use std::path::{Path, PathBuf};
use std::os::unix::io::AsRawFd;
use std::os::raw::c_void;
use std::ffi::OsStr;
use std::fs::{OpenOptions};
use std::sync::{Arc, Mutex};

use crate::command::MessageData;

/// Represents a memory allocation
#[derive(Clone)]
pub struct MemoryAllocation {
    offset: usize,
    ptr: *mut u8,
    size: usize,
    full_size: usize
}

impl MemoryAllocation {
    pub fn new(offset: usize, ptr: *mut u8, size: usize) -> MemoryAllocation {
        MemoryAllocation {
            offset,
            ptr,
            size,
            full_size: size
        }
    }

    pub fn is_next_to(&self, other: &MemoryAllocation) -> bool {
        (self.offset + self.full_size) == other.offset
    }
}

impl GenericMemoryAllocation for MemoryAllocation {
    fn offset(&self) -> usize {
        self.offset
    }

    fn ptr(&self) -> *mut u8 {
        self.ptr
    }

    fn size(&self) -> usize {
        self.size
    }
}

unsafe impl Send for MemoryAllocation {}
unsafe impl Sync for MemoryAllocation {}

pub type SmartSharedMemoryAllocator = Arc<Mutex<SharedMemoryAllocator>>;

/// Represents a memory allocator for shared memory
pub struct SharedMemoryAllocator {
    shared_memory: SharedMemory,
    current_offset: usize,
    free_allocations: Vec<MemoryAllocation>
}

impl SharedMemoryAllocator {
    /// Creates a new allocator using the given memory area
    pub fn new(shared_memory: SharedMemory) -> SharedMemoryAllocator {
        SharedMemoryAllocator {
            shared_memory,
            current_offset: 0,
            free_allocations: Vec::new()
        }
    }

    /// Creates a new "smart" memory allocator
    pub fn new_smart(shared_memory: SharedMemory) -> SmartSharedMemoryAllocator {
        Arc::new(Mutex::new(SharedMemoryAllocator::new(shared_memory)))
    }

    /// Tries to allocate memory of the given size
    pub fn allocate(&mut self, size: usize) -> Option<MemoryAllocation> {
        if let Some(allocation) = self.allocate_from_free_store(size) {
            return Some(allocation);
        }

        // Try allocate new
        let offset = self.current_offset;
        if offset + size > self.shared_memory.size() {
            self.merge_free_allocations();
            return self.allocate_from_free_store(size);
        }

        self.current_offset += size;
        Some(MemoryAllocation::new(offset, self.create_ptr(offset), size))
    }

    fn allocate_from_free_store(&mut self, size: usize) -> Option<MemoryAllocation> {
        for allocation_index in 0..self.free_allocations.len() {
            if size <= self.free_allocations[allocation_index].full_size {
                let mut allocation = self.free_allocations.remove(allocation_index);
                allocation.size = size;
                return Some(allocation);
            }
        }

        return None
    }

    fn merge_free_allocations(&mut self) {
        self.free_allocations.sort_by_key(|allocation| allocation.offset);
        let mut index1 = 0;
        while index1 < self.free_allocations.len() {
            let mut index2 = index1 + 1;
            while index2 < self.free_allocations.len() {
                if self.free_allocations[index1].is_next_to(&self.free_allocations[index2]) {
                    self.free_allocations[index1].full_size += self.free_allocations.remove(index2).full_size;
                } else {
                    index2 += 1;
                }
            }

            index1 += 1;
        }
    }

    /// Deallocate the given allocation
    pub fn deallocate(&mut self, allocation: MemoryAllocation) {
        self.free_allocations.push(allocation);
    }

    fn create_ptr(&mut self, offset: usize) -> *mut u8 {
        unsafe { self.shared_memory.ptr_mut().offset(offset as isize) }
    }
}

/// Represents a "smart" memory allocation that removes itself from the allocator when dropped
pub struct SmartMemoryAllocation {
    allocator: Arc<Mutex<SharedMemoryAllocator>>,
    allocation: Option<MemoryAllocation>
}

impl SmartMemoryAllocation {
    /// Tries to allocate using the given allocator
    pub fn new(allocator: &SmartSharedMemoryAllocator, size: usize) -> Option<Arc<SmartMemoryAllocation>> {
        let allocation = allocator.lock().unwrap().allocate(size)?;
        Some(
            Arc::new(
                SmartMemoryAllocation {
                    allocator: allocator.clone(),
                    allocation: Some(allocation)
                }
            )
        )
    }
}

impl GenericMemoryAllocation for SmartMemoryAllocation {
    fn offset(&self) -> usize {
        self.allocation.as_ref().unwrap().offset()
    }

    fn ptr(&self) -> *mut u8 {
        self.allocation.as_ref().unwrap().ptr()
    }

    fn size(&self) -> usize {
        self.allocation.as_ref().unwrap().size()
    }
}

impl Drop for SmartMemoryAllocation {
    fn drop(&mut self) {
        self.allocator.lock().unwrap().deallocate(self.allocation.take().unwrap())
    }
}

#[derive(Debug)]
pub enum SharedMemoryError {
    FailedToAllocate,
    FailedToMap,
    IO(std::io::Error)
}

pub struct SharedMemory {
    path: PathBuf,
    address: *mut c_void,
    size: usize
}

/// Represents a shared memory area
impl SharedMemory {
    /// Create a new writable shared memory area in /dev/shm
    pub fn write_auto(size: usize) -> Result<SharedMemory, SharedMemoryError>{
        let named_tempfile = tempfile::Builder::new()
            .prefix("")
            .suffix(".data")
            .tempfile().unwrap();

        let filename = named_tempfile
            .path()
            .file_name().and_then(OsStr::to_str).unwrap().to_owned();

        let path = Path::new("/dev/shm").to_path_buf().join(Path::new(&filename));
        SharedMemory::write(&path, size)
    }

    /// Creates a new writable shared memory area in the given location
    pub fn write(path: &Path, size: usize) -> Result<SharedMemory, SharedMemoryError> {
        SharedMemory::full(path, size, true)
    }

    /// Readable shared memory area from the given location
    pub fn read(path: &Path, size: usize) -> Result<SharedMemory, SharedMemoryError> {
        SharedMemory::full(path, size, false)
    }

    fn full(path: &Path, size: usize, writable: bool) -> Result<SharedMemory, SharedMemoryError> {
        let file = if writable {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .map_err(|err| SharedMemoryError::IO(err))?
        } else {
            OpenOptions::new()
                .read(true)
                .open(path)
                .map_err(|err| SharedMemoryError::IO(err))?
        };

        let fd = file.as_raw_fd();

        if writable {
            let result = unsafe {
                libc::fallocate(fd, 0, 0, size as i64)
            };

            if result == -1 {
                return Err(SharedMemoryError::FailedToAllocate);
            }
        }

        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                if writable {libc::PROT_READ | libc::PROT_WRITE} else {libc::PROT_READ},
                libc::MAP_SHARED,
                fd,
                0
            )
        };

        if address == libc::MAP_FAILED {
            return Err(SharedMemoryError::FailedToMap);
        }

        Ok(
            SharedMemory {
                path: path.to_owned(),
                address,
                size
            }
        )
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn bytes_from_data(&self, data: &MessageData) -> &[u8] {
        &self.bytes()[data.offset..(data.offset + data.size)]
    }

    pub fn bytes_mut_from_data(&mut self, data: &MessageData) -> &mut [u8] {
        &mut self.bytes_mut()[data.offset..(data.offset + data.size)]
    }

    pub fn bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.address as *const u8, self.size)
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.address as *mut u8, self.size)
        }
    }

    pub fn ptr_mut(&mut self) -> *mut u8 {
        self.address as *mut u8
    }
}

unsafe impl Send for SharedMemory {}
unsafe impl Sync for SharedMemory {}

impl Drop for SharedMemory {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.address, self.size);
        }
    }
}

pub trait GenericMemoryAllocation {
    fn offset(&self) -> usize;
    fn ptr(&self) -> *mut u8;
    fn size(&self) -> usize;

    fn message_data(&self) -> MessageData {
        MessageData {
            offset: self.offset(),
            size: self.size()
        }
    }

    fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr(), self.size()) }
    }

    fn bytes_mut(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.size()) }
    }
}

fn get_tempfile_name() -> String {
    let named_tempfile = tempfile::Builder::new()
        .suffix(".shm")
        .tempfile().unwrap();

    let filename = named_tempfile
        .path()
        .file_name().and_then(OsStr::to_str).unwrap().to_owned();

    "/tmp/".to_owned() + &filename
}

#[test]
fn test_create1() {
    let filename = get_tempfile_name();
    assert!(SharedMemory::write(Path::new(&filename), 1024).is_ok());
    std::fs::remove_file(filename).unwrap();
}

#[test]
fn test_create2() {
    let filename = get_tempfile_name();
    let mut shared_memory = SharedMemory::write(Path::new(&filename), 1024).unwrap();

    shared_memory.bytes_mut()[44] = 133;
    assert_eq!(133, shared_memory.bytes_mut()[44]);

    std::fs::remove_file(filename).unwrap();
}

#[test]
fn test_create_existing() {
    let filename = get_tempfile_name();
    let mut shared_memory1 = SharedMemory::write(Path::new(&filename), 1024).unwrap();
    let mut shared_memory2 = SharedMemory::read(Path::new(&filename), 1024).unwrap();

    shared_memory1.bytes_mut()[44] = 133;
    assert_eq!(133, shared_memory1.bytes_mut()[44]);
    assert_eq!(shared_memory1.bytes_mut()[44], shared_memory2.bytes_mut()[44]);

    std::fs::remove_file(filename).unwrap();
}