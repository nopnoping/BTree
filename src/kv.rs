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
    flushed: u64,
    // temp BNode, in mem, no disk
    temp: Vec<BNode>,

    root: u64,
}

impl Persist for KV {
    fn get_node(&self, ptr: u64) -> BNode {
        let row = ptr as usize / (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE);
        let col = ptr as usize % (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE);
        assert!(row < self.file_maps.len());
        let x = self.file_maps[row].read(col);
        BNode::new_with_data(x.to_vec())
    }

    fn new_node(&mut self, node: &BNode) -> u64 {
        let ptr = self.flushed + self.temp.len() as u64;
        self.temp.push(node.clone());
        ptr
    }

    fn del_node(&mut self, ptr: u64) {
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
    }

    fn flush(&mut self) {
        self.write_temp_to_map();
        self.flush_map();
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
        let n_sys_pages = file.metadata().unwrap().len() as usize / *SYS_PAGE_SIZE + 1;
        let mut file_maps = Vec::new();
        for i in 0..n_sys_pages {
            file_maps.push(FileMap::new(&file, *SYS_PAGE_SIZE, i * (*SYS_PAGE_SIZE)));
        }

        let master = file_maps[0].read(0);
        let sig = &master[..16];
        let root = file_maps[0].read_u64(16);
        let used = file_maps[0].read_u64(24);

        if sig != DB_SIG.as_bytes() {
            return Err(String::from("db sgi err"));
        }

        let kv = KV {
            path,
            file,
            map_size: n_sys_pages * (*SYS_PAGE_SIZE),
            file_maps,
            temp: Vec::new(),
            root,
            flushed: used,
        };
        Ok(kv)
    }

    pub fn write_temp_to_map(&mut self) {
        // new file map
        let n_sys_pages = (self.flushed as usize + self.temp.len()) / ((*SYS_PAGE_SIZE) / BTREE_PAGE_SIZE);
        if n_sys_pages > self.file_maps.len() {
            let offset = self.file_maps.len();
            let new_sys_pages = self.file_maps.len() - n_sys_pages;
            for i in 0..new_sys_pages {
                self.file_maps.push(FileMap::new(&self.file, *SYS_PAGE_SIZE, (i + offset) * (*SYS_PAGE_SIZE)));
            }
        }

        // copy to file
        for _ in 0..self.temp.len() {
            let ptr = self.flushed;
            let row = ptr as usize / (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE);
            let col = ptr as usize % (*SYS_PAGE_SIZE / BTREE_PAGE_SIZE);
            let node = self.temp.pop().unwrap();
            self.file_maps[row].write(col, node.get_bytes(0, node.n_bytes()));
            self.flushed += 1;
        }

        self.file_maps[0].write_u64(24, self.flushed);
    }

    pub fn flush_map(&mut self) {
        for mut file_map in &mut self.file_maps {
            file_map.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use crate::b_node::BNode;
    use crate::common::{BTREE_PAGE_SIZE, Persist};
    use crate::kv::{DB_SIG, KV};

    fn init() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("test").unwrap();
        file.set_len(BTREE_PAGE_SIZE as u64).unwrap();
        file.write_all(DB_SIG.as_bytes()).unwrap();
        file.write_all(&[0x00; 8]).unwrap();
        file.write_all(&[0x01]).unwrap();
        file.flush().unwrap();
    }

    fn node_data() -> Vec<u8> {
        vec![0x01, 0x00, // type
             0x01, 0x00, // n key
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ptr
             0x06, 0x00, // offset
             0x01, 0x00, 0x01, 0x00, 0xac, 0xac, // kv
        ]
    }

    #[test]
    fn test_new() {
        init();
        let mut kv = KV::new(String::from("test")).unwrap();
        assert_eq!(kv.get_root(), 0);
        assert_eq!(kv.flushed, 1);
    }

    #[test]
    fn test_new_node() {
        init();
        let mut kv = KV::new(String::from("test")).unwrap();
        let ptr = kv.new_node(&BNode::new_with_data(node_data()));
        assert_eq!(ptr, 1);
        kv.flush();
        assert_eq!(kv.flushed, 2);
        assert_eq!(kv.get_node(ptr).get_key(0), &[0xac]);
    }

    #[test]
    fn test_root() {
        init();
        let mut kv = KV::new(String::from("test")).unwrap();
        kv.set_root(1);
        kv.flush();
        assert_eq!(kv.get_root(), 1);
    }
}