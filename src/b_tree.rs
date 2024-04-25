use crate::b_node::{BNode, BType};
use crate::common::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE, BTREE_PAGE_SIZE, HEADER, Persist};

struct BTree {
    root: u64,
    persist: Box<dyn Persist>,
}

impl BTree {
    pub fn new(persist: Box<dyn Persist>) -> Self {
        BTree {
            root: persist.get_root(),
            persist,
        }
    }
    // delete a key from root
    pub fn delete(&mut self, key: &[u8]) -> bool {
        assert_ne!(key.len(), 0);
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);
        if self.root == 0 {
            return false;
        }

        let k_node = self.persist.get(self.root);
        let r = self.tree_delete(&k_node, key);
        match r {
            None => false,
            Some(node) => {
                self.persist.del(self.root);
                if node.n_type() == BType::Node && node.n_keys() == 1 {
                    self.root = node.get_ptr(0);
                } else {
                    self.root = self.persist.new(&node);
                }
                true
            }
        }
    }
    // insert a key from root
    pub fn insert(&mut self, key: &[u8], val: &[u8]) {
        assert_ne!(key.len(), 0);
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);
        assert!(val.len() <= BTREE_MAX_VAL_SIZE);

        if self.root == 0 {
            let mut root_node = BNode::new_with_cap(BTREE_PAGE_SIZE);
            root_node.set_header(BType::LEAF, 2);
            root_node.insert_kv(0, 0, &[], &[]);
            root_node.insert_kv(1, 0, key, val);
            self.root = self.persist.new(&root_node);
            return;
        }

        let old = self.persist.get(self.root);
        self.persist.del(self.root);

        let childs = self.tree_insert(&old, key, val).split();
        if childs.len() > 1 {
            let mut root_node = BNode::new_with_cap(BTREE_PAGE_SIZE);
            root_node.set_header(BType::Node, childs.len() as u16);
            for i in 0..childs.len() as u16 {
                let key = childs[i as usize].get_key(0);
                let ptr = self.persist.new(&childs[i as usize]);
                root_node.insert_kv(i, ptr, key, &[]);
            }
            self.root = self.persist.new(&root_node);
        } else {
            self.root = self.persist.new(&childs[0]);
        }
    }

    // insert a kv from a node
    fn tree_insert(&mut self, node: &BNode, key: &[u8], val: &[u8]) -> BNode {
        let mut new_node = BNode::new_with_cap(2 * BTREE_PAGE_SIZE);

        let idx = node.lookup_le(key);
        match node.n_type() {
            BType::LEAF => {
                if key.cmp(node.get_key(idx)).is_eq() {
                    self.leaf_update(&mut new_node, node, idx, key, val);
                } else {
                    self.leaf_insert(&mut new_node, node, idx + 1, key, val);
                }
            }
            BType::Node => self.node_insert(&mut new_node, node, idx, key, val),
        };

        new_node
    }
    // delete a kv from a node
    fn tree_delete(&mut self, node: &BNode, key: &[u8]) -> Option<BNode> {
        let idx = node.lookup_le(key);
        match node.n_type() {
            BType::LEAF => {
                if key.cmp(node.get_key(idx)).is_ne() {
                    None
                } else {
                    let mut new = BNode::new_with_cap(BTREE_PAGE_SIZE);
                    self.leaf_delete(&mut new, node, idx);
                    Some(new)
                }
            }
            BType::Node => self.node_delete(node, idx, key)
        }
    }

    // leaf
    fn leaf_insert(&self, new: &mut BNode, old: &BNode, idx: u16, key: &[u8], val: &[u8]) {
        new.set_header(BType::LEAF, old.n_keys() + 1);
        new.copy_range(old, 0, 0, idx);
        new.insert_kv(idx, 0, key, val);
        new.copy_range(old, idx + 1, idx, old.n_keys() - idx);
    }
    fn leaf_update(&self, new: &mut BNode, old: &BNode, idx: u16, key: &[u8], val: &[u8]) {
        new.set_header(BType::LEAF, old.n_keys());
        new.copy_range(old, 0, 0, idx);
        new.insert_kv(idx, 0, key, val);
        new.copy_range(old, idx + 1, idx + 1, old.n_keys() - idx - 1);
    }
    fn leaf_delete(&self, new: &mut BNode, old: &BNode, idx: u16) {
        new.set_header(BType::LEAF, old.n_keys() - 1);
        new.copy_range(old, 0, 0, idx);
        new.copy_range(old, idx, idx + 1, old.n_keys() - idx - 1);
    }

    // node
    fn node_insert(&mut self, new: &mut BNode, old: &BNode, idx: u16, key: &[u8], val: &[u8]) {
        // get next level node
        let k_ptr = old.get_ptr(idx);
        let mut k_node = self.persist.get(k_ptr);
        self.persist.del(k_ptr);
        // insert
        k_node = self.tree_insert(&mut k_node, key, val);
        // split
        let childs = k_node.split();
        // update
        self.node_replace_n_kid(new, old, idx, &childs);
    }
    fn node_delete(&mut self, node: &BNode, idx: u16, key: &[u8]) -> Option<BNode> {
        let k_ptr = node.get_ptr(idx);
        let k_node = self.persist.get(k_ptr);
        let update_node = self.tree_delete(&k_node, key)?;

        self.persist.del(k_ptr);

        let mut new = BNode::new_with_cap(BTREE_PAGE_SIZE);
        match self.should_merge(node, &update_node, idx) {
            Some((dir, sibling)) => {
                let mut merged_child = BNode::new_with_cap(BTREE_PAGE_SIZE);
                if dir < 0 {
                    merged_child.merge(&sibling, &update_node);
                    self.persist.del(node.get_ptr(idx - 1));
                    let ptr = self.persist.new(&merged_child);
                    self.node_replace_2_kid(&mut new, node, idx - 1, ptr, merged_child.get_key(0));
                } else {
                    merged_child.merge(&update_node, &sibling);
                    self.persist.del(node.get_ptr(idx + 1));
                    let ptr = self.persist.new(&merged_child);
                    self.node_replace_2_kid(&mut new, node, idx, ptr, merged_child.get_key(0));
                }
            }
            None => {
                assert!(update_node.n_keys() > 0);
                self.node_replace_n_kid(&mut new, node, idx, &vec![update_node]);
            }
        }
        Some(new)
    }
    fn node_replace_n_kid(&mut self, new: &mut BNode, old: &BNode, idx: u16, childs: &Vec<BNode>) {
        new.set_header(BType::Node, old.n_keys() + childs.len() as u16 - 1);
        new.copy_range(old, 0, 0, idx);
        for i in 0..childs.len() as u16 {
            new.insert_kv(idx + i, self.persist.new(&childs[i as usize]), childs[i as usize].get_key(0), &[]);
        }
        new.copy_range(old, idx + childs.len() as u16, idx + 1, old.n_keys() - (idx + 1));
    }
    fn node_replace_2_kid(&self, new: &mut BNode, old: &BNode, idx: u16, ptr: u64, key: &[u8]) {
        new.set_header(BType::Node, old.n_keys() - 1);
        new.copy_range(old, 0, 0, idx);
        new.insert_kv(idx, ptr, key, &[]);
        new.copy_range(old, idx + 1, idx + 2, old.n_keys() - (idx + 2));
    }

    // help
    fn should_merge(&self, parent: &BNode, child: &BNode, idx: u16) -> Option<(i8, BNode)> {
        if child.n_bytes() > BTREE_PAGE_SIZE as u16 / 4 {
            return None;
        }

        if idx > 0 {
            let sibling = self.persist.get(parent.get_ptr(idx - 1));
            if sibling.n_bytes() + child.n_bytes() - HEADER as u16 <= BTREE_PAGE_SIZE as u16 {
                return Some((-1, sibling));
            }
        }

        if idx + 1 < parent.n_keys() {
            let sibling = self.persist.get(parent.get_ptr(idx + 1));
            if sibling.n_bytes() + child.n_bytes() - HEADER as u16 <= BTREE_PAGE_SIZE as u16 {
                return Some((1, sibling));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    // mock persist
    struct MockPersist {
        pages: HashMap<u64, BNode>,
        incr: u64,
    }

    impl MockPersist {
        pub fn new() -> Self {
            MockPersist {
                pages: HashMap::new(),
                incr: 0,
            }
        }
    }

    impl Persist for MockPersist {
        fn get(&self, ptr: u64) -> BNode {
            let node = self.pages.get(&ptr).unwrap();
            node.clone()
        }

        fn new(&mut self, node: &BNode) -> u64 {
            self.incr += 1;
            self.pages.insert(self.incr, node.clone());
            self.incr
        }

        fn del(&mut self, ptr: u64) {
            self.pages.remove(&ptr).unwrap();
        }

        fn len(&self) -> usize {
            self.pages.len()
        }
    }

    // Mock db
    struct MockDB {
        tree: BTree,
    }

    impl MockDB {
        pub fn new() -> Self {
            let persist = Box::new(MockPersist::new());
            let tree = BTree::new(persist);
            MockDB {
                tree,
            }
        }

        pub fn add(&mut self, key: &[u8], val: &[u8]) {
            self.tree.insert(key, val);
        }

        pub fn del(&mut self, key: &[u8]) {
            self.tree.delete(key);
        }
    }

    // insert test
    #[test]
    fn test_insert() {
        let mut mock = MockDB::new();
        mock.add("cafe".as_bytes(), "cafe_val".as_bytes());
        mock.add("cafe1".as_bytes(), "cafe_val1".as_bytes());
        assert_eq!(mock.tree.persist.get(2).get_key(1), "cafe".as_bytes());
        assert_eq!(mock.tree.persist.get(2).get_val(1), "cafe_val".as_bytes());
        assert_eq!(mock.tree.persist.get(2).get_key(2), "cafe1".as_bytes());
        assert_eq!(mock.tree.persist.get(2).get_val(2), "cafe_val1".as_bytes());
    }

    #[test]
    fn test_split() {
        let mut mock = MockDB::new();
        mock.add(&[0xca; BTREE_MAX_KEY_SIZE], &[0xca; BTREE_MAX_VAL_SIZE]);
        mock.add(&[0xff; BTREE_MAX_KEY_SIZE], &[0xff; BTREE_MAX_VAL_SIZE]);
        mock.add(&[0xdf; BTREE_MAX_KEY_SIZE], &[0xdf; BTREE_MAX_VAL_SIZE]);
        assert_eq!(mock.tree.persist.len(), 4);
        assert_eq!(mock.tree.persist.get(5).get_val(1), &[0xca; BTREE_MAX_VAL_SIZE]);
        assert_eq!(mock.tree.persist.get(6).get_val(0), &[0xdf; BTREE_MAX_VAL_SIZE]);
        assert_eq!(mock.tree.persist.get(3).get_val(0), &[0xff; BTREE_MAX_VAL_SIZE]);


        assert_eq!(mock.tree.persist.get(5).n_type(), BType::LEAF);
        assert_eq!(mock.tree.persist.get(6).n_type(), BType::LEAF);
        assert_eq!(mock.tree.persist.get(3).n_type(), BType::LEAF);
        assert_eq!(mock.tree.persist.get(7).n_type(), BType::Node);

        assert_eq!(mock.tree.persist.get(7).get_ptr(0), 5);
        assert_eq!(mock.tree.persist.get(7).get_ptr(1), 6);
        assert_eq!(mock.tree.persist.get(7).get_ptr(2), 3);
    }

    // delete test
    #[test]
    fn test_delete() {
        let mut mock = MockDB::new();
        mock.add(&[0xca; BTREE_MAX_KEY_SIZE], &[0xca; BTREE_MAX_VAL_SIZE]);
        mock.add(&[0xdf; BTREE_MAX_KEY_SIZE - 0x100], &[0xdf; BTREE_MAX_VAL_SIZE - 0x100]);
        mock.add(&[0xff], &[0xff]);
        mock.del(&[0xff]);
        assert_eq!(mock.tree.persist.len(), 3);
        assert_eq!(mock.tree.persist.get(7).n_keys(), 1);
        assert_eq!(mock.tree.persist.get(7).get_key(0), &[0xdf; BTREE_MAX_KEY_SIZE - 0x100]);
        assert_eq!(mock.tree.persist.get(8).n_type(), BType::Node);
    }

    #[test]
    fn test_right_merge() {
        let mut mock = MockDB::new();
        mock.add(&[0xca; BTREE_MAX_KEY_SIZE], &[0xca; BTREE_MAX_VAL_SIZE]);
        mock.add(&[0xff; BTREE_MAX_KEY_SIZE], &[0xff; BTREE_MAX_VAL_SIZE]);
        mock.del(&[0xff; BTREE_MAX_KEY_SIZE]);
        assert_eq!(mock.tree.persist.len(), 1);
        assert_eq!(mock.tree.persist.get(5).n_type(), BType::LEAF);
        assert_eq!(mock.tree.persist.get(5).get_key(1), &[0xca; BTREE_MAX_KEY_SIZE]);
    }

    #[test]
    fn test_left_merge() {
        let mut mock = MockDB::new();
        mock.add(&[0xca; BTREE_MAX_KEY_SIZE], &[0xca; BTREE_MAX_VAL_SIZE]);
        mock.add(&[0xff; BTREE_MAX_KEY_SIZE], &[0xff; BTREE_MAX_VAL_SIZE]);
        mock.del(&[0xca; BTREE_MAX_KEY_SIZE]);
        assert_eq!(mock.tree.persist.len(), 1);
        assert_eq!(mock.tree.persist.get(5).n_type(), BType::LEAF);
        assert_eq!(mock.tree.persist.get(5).get_key(1), &[0xff; BTREE_MAX_KEY_SIZE]);
    }
}

