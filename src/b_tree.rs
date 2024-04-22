use crate::b_node::{BNode, BTREE_PAGE_SIZE, BType};

struct BTree {
    root: u64,
    get: fn(u64) -> BNode,
    new: fn(&BNode) -> u64,
    del: fn(u64),
}

impl BTree {
    fn insert(&mut self, node: &BNode, key: &[u8], val: &[u8]) -> BNode {
        let mut new_node = BNode::new(2 * BTREE_PAGE_SIZE);

        let idx = node.lookup_le(key);
        match node.n_type() {
            BType::LEAF => {
                if key.cmp(node.get_key(idx)).is_eq() {
                    self.leaf_update(&mut new_node, node, idx, key, val);
                } else {
                    self.leaf_insert(&mut new_node, node, idx, key, val);
                }
            }
            BType::Node => self.node_insert(&mut new_node, node, idx, key, val),
        };

        new_node
    }


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

    fn node_insert(&mut self, new: &mut BNode, node: &BNode, idx: u16, key: &[u8], val: &[u8]) {
        // get next level node
        let k_ptr = node.get_ptr(idx);
        let mut k_node = (self.get)(k_ptr);
        (self.del)(k_ptr);
        // insert
        k_node = self.insert(&mut k_node, key, val);
        // split
        let vec = k_node.split();
        // update
    }
}