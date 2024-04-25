use std::ffi::c_void;
use std::fs::File;
use std::io::Write;
use std::os::fd::AsFd;
use std::ptr::NonNull;

use crate::b_node::BNode;
use crate::common::Persist;

mod file_map;

struct KV {
    path: String,

    file: File,
    size: usize,
    // file map
    file_maps: Vec<NonNull<c_void>>,
    // stored in file pages
    flushed: u64,
    // temp BNode, in mem, no disk
    temp: Vec<BNode>,

    sys_page_size: usize,
}

impl Persist for KV {
    fn get(&self, ptr: u64) -> BNode {
        todo!()
    }

    fn new(&mut self, node: &BNode) -> u64 {
        todo!()
    }

    fn del(&mut self, ptr: u64) {
        // Todo
    }

    fn len(&self) -> usize {
        todo!()
    }
}

impl KV {}