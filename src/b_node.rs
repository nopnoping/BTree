const HEADER: usize = 4;

pub struct BNode {
    data: Vec<u8>,
}

// Basic
impl BNode {
    fn n_type(&self) -> u16 {
        self.read_u16(0)
    }

    pub fn n_keys(&self) -> u16 {
        self.read_u16(2)
    }

    fn n_bytes(&self) -> u16 {
        self.kv_pos(self.n_keys())
    }

    pub fn set_header(&mut self, n_type: u16, n_keys: u16) {
        self.write_u16(0, n_type);
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

    fn get_key(&self, idx: u16) -> &[u8] {
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