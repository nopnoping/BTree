use std::ffi::c_void;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroUsize;
use std::os::fd::AsFd;
use std::ptr::NonNull;
use std::slice::from_raw_parts_mut;

use nix::libc::{munmap, off_t};
use nix::sys::mman::{MapFlags, mmap, MsFlags, msync, ProtFlags};

use crate::common::{BTREE_PAGE_SIZE, SYS_PAGE_SIZE};
use crate::little_endian::LittleEndian;

pub struct FileMap {
    ptr: NonNull<c_void>,
    size: usize,
    offset: usize,
    dirty: bool,
}

impl FileMap {
    pub fn new(file: &File, size: usize, offset: usize) -> Self {
        assert_eq!(offset % *SYS_PAGE_SIZE, 0);
        assert_eq!((offset + size) % *SYS_PAGE_SIZE, 0);
        if offset + size > file.metadata().unwrap().len() as usize {
            file.set_len((offset + size) as u64).unwrap();
        }
        let ptr = unsafe {
            mmap(None, NonZeroUsize::new(size).unwrap(), ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                 MapFlags::MAP_SHARED, file.as_fd(), offset as off_t).unwrap()
        };
        FileMap {
            ptr,
            size,
            offset,
            dirty: false,
        }
    }

    pub fn n_pages(&self) -> usize {
        self.size / BTREE_PAGE_SIZE
    }

    pub fn write(&mut self, pages_num: usize, data: &[u8]) {
        assert!(pages_num < self.n_pages());
        assert!(data.len() <= BTREE_PAGE_SIZE);
        let mut file_data = unsafe {
            &mut from_raw_parts_mut(self.ptr.as_ptr() as *mut u8, self.size)[pages_num * BTREE_PAGE_SIZE..(pages_num + 1) * BTREE_PAGE_SIZE]
        };
        file_data.write_all(data).unwrap();
        self.dirty = true;
    }
    pub fn read(&self, pages_num: usize) -> &[u8] {
        assert!(pages_num < self.n_pages());
        unsafe {
            &from_raw_parts_mut(self.ptr.as_ptr() as *mut u8, self.size)[pages_num * BTREE_PAGE_SIZE..(pages_num + 1) * BTREE_PAGE_SIZE]
        }
    }
    pub fn flush(&mut self) {
        if self.dirty {
            unsafe {
                msync(self.ptr, self.size, MsFlags::MS_SYNC).unwrap();
            }
            self.dirty = false;
        }
    }
}

impl Drop for FileMap {
    fn drop(&mut self) {
        unsafe {
            munmap(self.ptr.as_ptr(), self.size);
        }
    }
}

impl LittleEndian for FileMap {
    fn read_u16(&self, start: usize) -> u16 {
        let data = unsafe {
            &mut from_raw_parts_mut(self.ptr.as_ptr() as *mut u8, self.size)[start..start + 2]
        };
        data[0] as u16 | ((data[0] as u16) << 8)
    }

    fn write_u16(&mut self, start: usize, data: u16) {
        let low = (data & 0xff) as u8;
        let hi = (data >> 8) as u8;
        let data = unsafe {
            &mut from_raw_parts_mut(self.ptr.as_ptr() as *mut u8, self.size)[start..start + 2]
        };
        data[0] = low;
        data[1] = hi;
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use super::*;

    fn file_create() -> File {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("test").unwrap();
        file
    }

    #[test]
    fn test_file_map() {
        let f = file_create();
        let mut file_map = FileMap::new(&f, *SYS_PAGE_SIZE, 0);
        file_map.write(0, &[0xac, 0xac]);
        file_map.write(1, &[0xab, 0xab]);
        file_map.write(2, &[0xee, 0xee]);
        file_map.write(3, &[0xfe, 0xfe]);
        assert_eq!(file_map.read(0)[..2], [0xac, 0xac]);
        assert_eq!(file_map.read(1)[..2], [0xab, 0xab]);
        assert_eq!(file_map.read(2)[..2], [0xee, 0xee]);
        assert_eq!(file_map.read(3)[..2], [0xfe, 0xfe]);
        assert_eq!(file_map.n_pages(), 4);
        file_map.flush();
    }
}