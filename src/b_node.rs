use crate::common::{BTREE_PAGE_SIZE, HEADER};
use crate::little_endian::LittleEndian;

#[derive(Eq, PartialEq, Debug)]
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
    pub fn new_with_cap(size: usize) -> BNode {
        let mut v = Vec::new();
        v.resize(size, 0);
        BNode {
            data: v,
        }
    }
    pub fn new_with_data(data: Vec<u8>) -> BNode {
        BNode { data }
    }

    // basic info
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
    pub fn n_bytes(&self) -> u16 {
        self.kv_pos(self.n_keys())
    }

    // header
    pub fn set_header(&mut self, n_type: BType, n_keys: u16) {
        self.write_u16(0, n_type as u16);
        self.write_u16(2, n_keys);
    }

    // ptr
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

    // pos
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
    fn offset_pos(&self, idx: u16) -> u16 {
        assert!(idx > 0 && idx <= self.n_keys());
        HEADER as u16 + 8 * self.n_keys() + 2 * (idx - 1)
    }

    // kv
    pub fn get_key(&self, idx: u16) -> &[u8] {
        assert!(idx < self.n_keys());
        let pos = self.kv_pos(idx);
        let k_len = self.read_u16(pos as usize);
        &self.data[pos as usize + 4..][..k_len as usize]
    }
    pub fn get_val(&self, idx: u16) -> &[u8] {
        assert!(idx < self.n_keys());
        let pos = self.kv_pos(idx);
        let k_len = self.read_u16(pos as usize);
        let v_len = self.read_u16(pos as usize + 2);
        &self.data[(pos + 4 + k_len) as usize..][..v_len as usize]
    }
    pub fn kv_pos(&self, idx: u16) -> u16 {
        assert!(idx <= self.n_keys());
        HEADER as u16 + 8 * self.n_keys() + 2 * self.n_keys() + self.get_offset(idx)
    }

    // data
    fn resize(&mut self, size: usize) {
        self.data.resize(size, 0);
    }
    fn byte_copy(&mut self, start: u16, val: &[u8]) {
        assert!(start as usize + val.len() <= self.data.len());
        for i in 0..val.len() {
            self.data[start as usize + i] = val[i];
        }
    }
    pub fn get_bytes(&self, start: u16, end: u16) -> &[u8] {
        assert!(end as usize <= self.data.len());
        &self.data[start as usize..end as usize]
    }
}

// Domain
impl BNode {
    // lookup key
    pub fn lookup_le(&self, key: &[u8]) -> u16 {
        let mut found = 0;
        for i in 1..self.n_keys() {
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

    // copy node from range
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
            self.set_offset(dest_new + i, offset);
        }
        // kv copy
        let start = old.kv_pos(src_old);
        let end = old.kv_pos(src_old + n);
        self.byte_copy(self.kv_pos(dest_new), old.get_bytes(start, end));
    }

    // insert a kv
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

    // split the node to [1,2,3] nodes
    pub fn split(&mut self) -> Vec<BNode> {
        if self.n_bytes() <= BTREE_PAGE_SIZE as u16 {
            self.resize(BTREE_PAGE_SIZE);
            return vec![self.clone()];
        }
        let (mut left, right) = self.split2();
        if left.n_bytes() <= BTREE_PAGE_SIZE as u16 {
            left.resize(BTREE_PAGE_SIZE);
            return vec![left, right];
        }
        let (left, middle) = left.split2();
        assert!(left.n_bytes() <= BTREE_PAGE_SIZE as u16);
        vec![left, middle, right]
    }
    fn split2(&mut self) -> (BNode, BNode) {
        let mut left = BNode::new_with_cap(2 * BTREE_PAGE_SIZE);
        let mut right = BNode::new_with_cap(BTREE_PAGE_SIZE);

        let mut idx = self.n_keys() - 1;
        loop {
            let nk = self.n_keys() - idx;
            let kv_size = self.get_offset(self.n_keys()) - self.get_offset(idx);
            let size = HEADER as u16 + 8 * nk + 2 * nk + kv_size;
            if size >= BTREE_PAGE_SIZE as u16 { break; }
            idx -= 1;
        }

        // [0, idx]
        left.set_header(self.n_type(), idx + 1);
        left.copy_range(self, 0, 0, idx + 1);

        // [idx+1, n_keys-1]
        right.set_header(self.n_type(), self.n_keys() - idx - 1);
        right.copy_range(self, 0, idx + 1, self.n_keys() - idx - 1);

        (left, right)
    }

    // merge a node
    pub fn merge(&mut self, left: &Self, right: &Self) {
        self.resize((left.n_bytes() + right.n_bytes()) as usize);
        self.set_header(left.n_type(), left.n_keys() + right.n_keys());
        self.copy_range(left, 0, 0, left.n_keys());
        self.copy_range(right, left.n_keys(), 0, right.n_keys());
    }
}

// little endian
impl LittleEndian for BNode {
    fn read_u16(&self, start: usize) -> u16 {
        let low = self.data[start] as u16;
        let hi = self.data[start + 1] as u16;
        low | (hi << 8)
    }
    fn write_u16(&mut self, start: usize, data: u16) {
        self.data[start] = (data & 0xff) as u8;
        self.data[start + 1] = (data >> 8) as u8;
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE};

    use super::*;

    /* Basic Test */
    fn basic_data() -> Vec<u8> {
        vec![0x01, 0x00, // type
             0x01, 0x00, // n key
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ptr
             0x06, 0x00, // offset
             0x01, 0x00, 0x01, 0x00, 0xac, 0xac, // kv
        ]
    }

    #[test]
    fn test_basic_info() {
        let data = basic_data();
        let node = BNode::new_with_data(data);
        assert_eq!(node.n_type(), BType::Node);
        assert_eq!(node.n_keys(), 1);
        assert_eq!(node.get_offset(1), 6);
        assert_eq!(node.n_bytes(), 20);
    }

    #[test]
    fn test_header() {
        let mut node = BNode::new_with_cap(4);
        node.set_header(BType::LEAF, 2);
        assert_eq!(node.n_type(), BType::LEAF);
        assert_eq!(node.n_keys(), 2);
    }

    #[test]
    fn test_ptr() {
        let mut node = BNode::new_with_data(basic_data());
        node.set_ptr(0, 0xffff);
        assert_eq!(node.get_ptr(0), 0xffff);
    }

    #[test]
    fn test_offset() {
        let mut node = BNode::new_with_data(basic_data());
        node.set_offset(1, 0xcafe);
        assert_eq!(node.get_offset(0), 0);
        assert_eq!(node.get_offset(1), 0xcafe);
        let t = [0xfe, 0xca];
        assert_eq!(node.get_bytes(12, 14), &t)
    }

    #[test]
    fn test_kv() {
        let mut node = BNode::new_with_data(basic_data());
        assert_eq!(node.get_key(0), &[0xac]);
        assert_eq!(node.get_val(0), &[0xac]);
    }

    #[test]
    fn test_data() {
        let mut node = BNode::new_with_data(basic_data());
        node.byte_copy(0, &[0x02, 00]);
        assert_eq!(node.n_type(), BType::LEAF);
        assert_eq!(node.get_bytes(0, 2), &[0x02, 00]);
    }

    /* Domain test */
    fn domain_data() -> Vec<u8> {
        vec![0x01, 0x00, // type
             0x02, 0x00, // n key
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ptr
             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
             0x06, 0x00, // offset
             0x0c, 0x00,
             0x01, 0x00, 0x01, 0x00, 0x9c, 0xac, // kv
             0x01, 0x00, 0x01, 0x00, 0xac, 0xac,
        ]
    }

    #[test]
    fn test_look_up() {
        let mut node = BNode::new_with_data(domain_data());
        assert_eq!(node.n_keys(), 2);
        assert_eq!(node.lookup_le(&[0x90]), 0);
        assert_eq!(node.lookup_le(&[0x9c]), 0);
        assert_eq!(node.lookup_le(&[0xa0]), 0);
        assert_eq!(node.lookup_le(&[0xac]), 1);
        assert_eq!(node.lookup_le(&[0xad]), 1);
    }

    #[test]
    fn test_copy_range() {
        let mut old = BNode::new_with_data(domain_data());
        let mut new = BNode::new_with_cap(old.n_bytes() as usize);
        new.set_header(old.n_type(), old.n_keys());
        new.copy_range(&old, 0, 0, old.n_keys());
        assert_eq!(new.get_bytes(0, new.n_bytes()), old.get_bytes(0, old.n_bytes()));

        let mut new2 = BNode::new_with_cap(basic_data().len());
        new2.set_header(old.n_type(), 1);
        new2.copy_range(&old, 0, 1, 1);
        assert_eq!(new2.get_bytes(0, new2.n_bytes()), basic_data());
    }

    #[test]
    fn test_insert_kv() {
        let mut node = BNode::new_with_data(domain_data());
        node.insert_kv(1, 0xff, &[0xbb], &[0xbb]);
        assert_eq!(node.get_ptr(1), 0xff);
        assert_eq!(node.get_key(1), &[0xbb]);
        assert_eq!(node.get_val(1), &[0xbb]);
    }

    #[test]
    fn test_split() {
        let mut node = BNode::new_with_cap(3 * BTREE_PAGE_SIZE);
        node.set_header(BType::LEAF, 3);
        node.insert_kv(0, 0, &[0x11; BTREE_MAX_KEY_SIZE], &[0x11; BTREE_MAX_VAL_SIZE]);
        node.insert_kv(1, 0, &[0x22; BTREE_MAX_KEY_SIZE], &[0x22; BTREE_MAX_VAL_SIZE]);
        node.insert_kv(2, 0, &[0x33; BTREE_MAX_KEY_SIZE], &[0x33; BTREE_MAX_VAL_SIZE]);
        let v = node.split();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].get_key(0), &[0x11; BTREE_MAX_KEY_SIZE]);
        assert_eq!(v[0].get_val(0), &[0x11; BTREE_MAX_VAL_SIZE]);
        assert_eq!(v[1].get_key(0), &[0x22; BTREE_MAX_KEY_SIZE]);
        assert_eq!(v[1].get_val(0), &[0x22; BTREE_MAX_VAL_SIZE]);
        assert_eq!(v[2].get_key(0), &[0x33; BTREE_MAX_KEY_SIZE]);
        assert_eq!(v[2].get_val(0), &[0x33; BTREE_MAX_VAL_SIZE]);
    }

    #[test]
    fn test_merge() {
        let mut node1 = BNode::new_with_data(domain_data());
        let mut node2 = BNode::new_with_data(domain_data());
        let mut node3 = BNode::new_with_cap(0);
        node3.merge(&node1, &node2);
        assert_eq!(node3.n_keys(), node1.n_keys() + node2.n_keys());
        assert_eq!(node3.get_key(0), node1.get_key(0));
        assert_eq!(node3.get_key(1), node1.get_key(1));
        assert_eq!(node3.get_key(0), node2.get_key(0));
        assert_eq!(node3.get_key(1), node2.get_key(1));
    }
}

