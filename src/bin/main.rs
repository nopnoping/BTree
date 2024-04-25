use std::fs::OpenOptions;
use std::io::Write;
use std::num::NonZeroUsize;
use std::os::fd::AsFd;
use std::slice::from_raw_parts_mut;

use nix::sys::mman::{MapFlags, mmap, MsFlags, msync, ProtFlags};

fn main() {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("test").unwrap();
    file.set_len(0x10).unwrap();
    // file.set_len(0x2000).unwrap();
    // let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
    // println!("Page size: {} bytes", page_size);
    // // file.set_len(126).unwrap();
    //
    // let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    //
    // // mmap.copy_from_slice(b"hello mmap!");
    // // mmap.as_mut().;
    // (&mut mmap[..]).write_all(b"hdwadaw").expect("TODO: panic message");
    // mmap.flush().unwrap();

    println!("{}", file.metadata().unwrap().len());

    let m = unsafe {
        mmap(None, NonZeroUsize::new(0x4000).unwrap(), ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
             MapFlags::MAP_SHARED, file.as_fd(), 0).unwrap()
    };

    let mut slice = unsafe {
        from_raw_parts_mut(m.as_ptr() as *mut u8, 0x4000)
    };

    slice[0xfff] = 0x4a;
    unsafe { msync(m, 0x4000, MsFlags::MS_SYNC).unwrap() };

    // file.set_len(0x8000).unwrap();
    let m1 = unsafe {
        mmap(None, NonZeroUsize::new(0x4000).unwrap(), ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
             MapFlags::MAP_SHARED, file.as_fd(), 0x4000).unwrap()
    };

    let mut slice2 = unsafe {
        from_raw_parts_mut(m1.as_ptr() as *mut u8, 0x4000)
    };

    slice2[3] = 0x4c;
    unsafe { msync(m1, 0x4000, MsFlags::MS_SYNC).unwrap() };
}