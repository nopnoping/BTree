use crate::b_node::{BNode, BType};
use crate::common::{BTREE_MAX_KEY_SIZE, BTREE_MAX_VAL_SIZE, BTREE_PAGE_SIZE, HEADER};

struct BTree {
    root: u64,
    get: fn(u64) -> BNode,
    new: fn(&BNode) -> u64,
    del: fn(u64),
}

impl BTree {
    // delete a key from root
    pub fn delete(&mut self, key: &[u8]) -> bool {
        assert_ne!(key.len(), 0);
        assert!(key.len() <= BTREE_MAX_KEY_SIZE);
        if self.root == 0 {
            return false;
        }

        let r = self.tree_delete(&(self.get)(self.root), key);
        match r {
            None => false,
            Some(node) => {
                (self.del)(self.root);
                if node.n_type() == BType::Node && node.n_keys() == 1 {
                    self.root = node.get_ptr(0);
                } else {
                    self.root = (self.new)(&node);
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
            self.root = (self.new)(&root_node);
            return;
        }

        let old = (self.get)(self.root);
        (self.del)(self.root);

        let childs = self.tree_insert(&old, key, val).split();
        if childs.len() > 1 {
            let mut root_node = BNode::new_with_cap(BTREE_PAGE_SIZE);
            root_node.set_header(BType::Node, childs.len() as u16);
            for i in 0..childs.len() as u16 {
                let key = childs[i as usize].get_key(0);
                let ptr = (self.new)(&childs[i as usize]);
                root_node.insert_kv(i, ptr, key, &[]);
            }
            self.root = (self.new)(&root_node);
        } else {
            self.root = (self.new)(&childs[0]);
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
    fn tree_delete(&self, node: &BNode, key: &[u8]) -> Option<BNode> {
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
        let mut k_node = (self.get)(k_ptr);
        (self.del)(k_ptr);
        // insert
        k_node = self.tree_insert(&mut k_node, key, val);
        // split
        let childs = k_node.split();
        // update
        self.node_replace_n_kid(new, old, idx, &childs);
    }
    fn node_delete(&self, node: &BNode, idx: u16, key: &[u8]) -> Option<BNode> {
        let k_ptr = node.get_ptr(idx);
        let k_node = (self.get)(k_ptr);
        let update_node = self.tree_delete(&k_node, key)?;

        (self.del)(k_ptr);

        let mut new = BNode::new_with_cap(BTREE_PAGE_SIZE);
        match self.should_merge(node, &update_node, idx) {
            Some((dir, sibling)) => {
                let mut merged_child = BNode::new_with_cap(BTREE_PAGE_SIZE);
                if dir < 0 {
                    merged_child.merge(&sibling, &update_node);
                    (self.del)(node.get_ptr(idx - 1));
                    self.node_replace_2_kid(&mut new, node, idx - 1, (self.new)(&merged_child), merged_child.get_key(0));
                } else {
                    merged_child.merge(&update_node, &sibling);
                    (self.del)(node.get_ptr(idx + 1));
                    self.node_replace_2_kid(&mut new, node, idx, (self.new)(&merged_child), merged_child.get_key(0));
                }
            }
            None => {
                assert!(update_node.n_keys() > 0);
                self.node_replace_n_kid(&mut new, node, idx, &vec![update_node]);
            }
        }
        Some(new)
    }
    fn node_replace_n_kid(&self, new: &mut BNode, old: &BNode, idx: u16, childs: &Vec<BNode>) {
        new.set_header(BType::Node, old.n_keys() + childs.len() as u16 - 1);
        new.copy_range(old, 0, 0, idx);
        for i in 0..childs.len() as u16 {
            new.insert_kv(idx + i, (self.new)(&childs[0]), childs[0].get_key(0), &[]);
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
            let sibling = (self.get)(parent.get_ptr(idx - 1));
            if sibling.n_bytes() + child.n_bytes() - HEADER as u16 <= BTREE_PAGE_SIZE as u16 {
                return Some((-1, sibling));
            }
        }

        if idx + 1 < parent.n_keys() {
            let sibling = (self.get)(parent.get_ptr(idx + 1));
            if sibling.n_bytes() + child.n_bytes() - HEADER as u16 <= BTREE_PAGE_SIZE as u16 {
                return Some((1, sibling));
            }
        }

        None
    }
}