use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::AsFd;

use crate::b_node::BNode;
use crate::common::{BTREE_PAGE_SIZE, Persist, SYS_PAGE_SIZE};
use crate::kv::file_map::FileMap;
use crate::little_endian::LittleEndian;

mod file_map;

const DB_SIG: &str = "BuildYourOwnDB05";

struct KV {
    path: String,

    file: File,
    map_size: usize,
    // file map
    file_maps: Vec<FileMap>,
    // temp BNode, in mem, no disk
    flushed: u64,
    temp: Vec<BNode>,
    root: u64,
}

impl Persist for KV {
    fn get(&self, ptr: u64) -> BNode {
        let row = (ptr / (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE)) as usize;
        let col = (ptr % (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE)) as usize;
        assert!(row < self.file_maps.len());
        let x = self.file_maps[row].read(col);
        BNode::new_with_data(x.to_vec())
    }

    fn new(&mut self, node: &BNode) -> u64 {
        let ptr = self.flushed + self.temp.len() as u64;
        self.temp.push(node.clone());
        ptr
    }

    fn del(&mut self, ptr: u64) {
        // Todo
    }

    fn len(&self) -> usize {
        self.flushed as usize - 1
    }

    fn get_root(&self) -> u64 {
        self.root
    }

    fn set_root(&mut self, root: u64) {
        self.root = root;
        self.file_maps[0].write_u64(16, root);
        self.file_maps[0].flush();
    }

    fn flush(&mut self) {
        self.write_temp_to_map();
        self.flushed
    }
}

impl KV {
    pub fn new(path: String) -> Result<KV, String> {
        // file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path).unwrap();

        // file_map
        let n = file.metadata().unwrap().len() / *SYS_PAGE_SIZE + 1;
        let mut v = Vec::new();
        for i in 0..n {
            v.push(FileMap::new(&file, *SYS_PAGE_SIZE, (i * (*SYS_PAGE_SIZE)) as usize));
        }

        let master = v[0].read(0);
        let sig = &master[..16];
        let root = v[0].read_u64(16);
        let used = v[0].read_u64(24);

        if sig != DB_SIG.as_bytes() {
            return Err(String::from("db sgi err"));
        }

        let kv = KV {
            path,
            file,
            map_size: (n * (*SYS_PAGE_SIZE)) as usize,
            file_maps: v,
            temp: Vec::new(),
            root,
            flushed: used,
        };
        Ok(kv)
    }

    pub fn write_temp_to_map(&mut self) {
        let n = ((self.flushed + self.temp.len()) / 4) as usize;
        if n > self.file_maps.len() {
            let offset = self.file_maps.len();
            let new_map = self.file_maps.len() - n;
            for i in 0..new_map {
                self.file_maps.push(FileMap::new(&self.file, *SYS_PAGE_SIZE, (i + offset) * (*SYS_PAGE_SIZE)));
            }
        }

        for _ in 0..self.temp.len() {
            let ptr = self.flushed;
            let row = (ptr / (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE)) as usize;
            let col = (ptr % (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE)) as usize;
            let node = self.temp.pop().unwrap();
            self.file_maps[row].write(col, node.get_bytes(0, node.n_bytes()));
            self.flushed += 1;
        }
    }

    pub fn flush_map(&mut self) {
        for file_map in self.file_maps {
            file_map.flush();
        }
    }
}