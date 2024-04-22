pub const HEADER: usize = 4;
pub const BTREE_PAGE_SIZE: usize = 4096;
pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;

pub enum BType {
    Node = 1,
    LEAF = 2,
}

#[derive(Clone)]
pub struct BNode {
    data: Vec<u8>,
}

// Basic
impl BNode {
    pub fn new(size: usize) -> BNode {
        BNode {
            data: Vec::with_capacity(size)
        }
    }

    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }

    pub fn n_type(&self) -> BType {
        if self.read_u16(0) == 1 {
            BType::Node
        } else {
            BType::LEAF
        }
    }

    pub fn n_keys(&self) -> u16 {
        self.read_u16(2)
    }

    fn n_bytes(&self) -> u16 {
        self.kv_pos(self.n_keys())
    }

    pub fn set_header(&mut self, n_type: BType, n_keys: u16) {
        self.write_u16(0, n_type as u16);
        self.write_u16(2, n_keys);
    }

    pub fn get_ptr(&self, idx: u16) -> u64 {
        assert!(idx < self.n_keys());
        let pos = HEADER + idx as usize * 8;
        self.read_u64(pos)
    }

    pub fn set_ptr(&mut self, idx: u16, val: u64) {
        assert!(idx < self.n_keys());
        let pos = HEADER + idx as usize * 8;
        self.write_u64(pos, val)
    }

    fn offset_pos(&self, idx: u16) -> u16 {
        assert!(idx >= 0 && idx <= self.n_keys());
        HEADER as u16 + 8 * self.n_keys() + 2 * (idx - 1)
    }

    pub fn get_offset(&self, idx: u16) -> u16 {
        if idx == 0 {
            0
        } else {
            self.read_u16(self.offset_pos(idx) as usize)
        }
    }

    pub fn set_offset(&mut self, idx: u16, offset: u16) {
        self.write_u16(self.offset_pos(idx) as usize, offset)
    }

    pub fn kv_pos(&self, idx: u16) -> u16 {
        assert!(idx <= self.n_keys());
        HEADER as u16 + 8 & self.n_keys() + 2 * self.n_keys() + self.get_offset(idx)
    }

    pub fn get_key(&self, idx: u16) -> &[u8] {
        assert!(idx < self.n_keys());
        let pos = self.kv_pos(idx);
        let k_len = self.read_u16(pos as usize);
        &self.data[pos as usize + 4..][..k_len as usize]
    }

    fn get_val(&self, idx: u16) -> &[u8] {
        assert!(idx < self.n_keys());
        let pos = self.kv_pos(idx);
        let k_len = self.read_u16(pos as usize);
        let v_len = self.read_u16(pos as usize + 2);
        &self.data[(pos + 4 + k_len) as usize..][..v_len as usize]
    }

    fn byte_copy(&mut self, start: u16, val: &[u8]) {
        assert!(start as usize + val.len() <= self.data.len());
        for i in 0..val.len() {
            self.data[start as usize + i] = val[i];
        }
    }

    fn get_bytes(&self, start: u16, end: u16) -> &[u8] {
        assert!(end as usize <= self.data.len());
        &self.data[start as usize..end as usize]
    }
}

// Domain
impl BNode {
    pub fn lookup_le(&self, key: &[u8]) -> u16 {
        let mut found = 0;
        for i in 0..self.n_keys() {
            let r = self.get_key(i).cmp(key);
            if r.is_le() {
                found = i;
            }
            if r.is_ge() {
                break;
            }
        }
        found
    }

    pub fn copy_range(&mut self, old: &BNode, dest_new: u16, src_old: u16, n: u16) {
        assert!(src_old + n <= old.n_keys());
        assert!(dest_new + n <= self.n_keys());
        if n == 0 {
            return;
        }
        // pointer copy
        for i in 0..n {
            self.set_ptr(dest_new + i, old.get_ptr(src_old + i));
        }
        // offsets copy
        let dest_begin = self.get_offset(dest_new);
        let src_begin = old.get_offset(src_old);
        for i in 1..=n {
            let offset = old.get_offset(src_old + i) - src_begin + dest_begin;
            self.set_offset(dest_new + 1, offset);
        }
        // kv copy
        let start = old.kv_pos(src_old);
        let end = old.kv_pos(src_old + n);
        self.byte_copy(self.kv_pos(dest_new), old.get_bytes(start, end));
    }

    pub fn insert_kv(&mut self, idx: u16, ptr: u64, key: &[u8], val: &[u8]) {
        // ptr
        self.set_ptr(idx, ptr);
        // kvs
        let pos = self.kv_pos(idx);
        self.write_u32(pos as usize, key.len() as u32);
        self.write_u32((pos + 2) as usize, val.len() as u32);
        self.byte_copy(pos + 4, key);
        self.byte_copy(pos + 4 + key.len() as u16, val);
        // offset
        self.set_offset(idx + 1, self.get_offset(idx) + 4 + (key.len() + val.len()) as u16)
    }

    pub fn split(&mut self) -> Vec<BNode> {
        if self.n_bytes() <= BTREE_PAGE_SIZE as u16 {
            self.resize(BTREE_PAGE_SIZE);
            return vec![self.clone()];
        }
        let (mut left, right) = self.split2();
        if left.n_keys() <= BTREE_PAGE_SIZE as u16 {
            left.resize(0);
            return vec![left, right];
        }
        let (left, middle) = left.split2();
        assert!(left.n_bytes() <= BTREE_PAGE_SIZE as u16);
        vec![left, middle, right]
    }
    fn split2(&mut self) -> (BNode, BNode) {
        let mut left = BNode::new(2 * BTREE_PAGE_SIZE);
        let mut right = BNode::new(BTREE_PAGE_SIZE);

        let mut idx = self.n_keys() - 1;
        loop {
            let nk = self.n_keys() - idx;
            let kv_size = self.get_offset(self.n_keys()) - self.get_offset(idx);
            let size = HEADER as u16 + 8 * nk + 2 * nk + kv_size;
            if size >= BTREE_PAGE_SIZE as u16 { break; }
            idx -= 1;
        }

        right.set_header(self.n_type(), self.n_keys() - idx);
        right.copy_range(self, 0, idx, self.n_keys() - idx);

        left.set_header(self.n_type(), idx - 1);
        left.copy_range(self, 0, 0, idx - 1);

        (left, right)
    }
}

// little endian
impl BNode {
    fn read_u16(&self, start: usize) -> u16 {
        let low = self.data[start] as u16;
        let hi = self.data[start + 1] as u16;
        low | (hi << 8)
    }
    fn read_u32(&self, start: usize) -> u32 {
        let low = self.read_u16(start) as u32;
        let hi = self.read_u16(start + 2) as u32;
        low | (hi << 16)
    }
    fn read_u64(&self, start: usize) -> u64 {
        let low = self.read_u32(start) as u64;
        let hi = self.read_u32(start + 4) as u64;
        low | (hi << 32)
    }
    fn write_u16(&mut self, start: usize, data: u16) {
        self.data[start] = (data & 0xff) as u8;
        self.data[start + 1] = (data >> 8) as u8;
    }
    fn write_u32(&mut self, start: usize, data: u32) {
        self.write_u16(start, (data & 0xffff) as u16);
        self.write_u16(start + 2, (data >> 16) as u16);
    }
    fn write_u64(&mut self, start: usize, data: u64) {
        self.write_u32(start, (data & 0xffffffff) as u32);
        self.write_u32(start + 2, (data >> 32) as u32);
    }
}