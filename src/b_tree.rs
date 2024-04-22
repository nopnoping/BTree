use crate::b_node::BNode;

const BTREE_PAGE_SIZE: usize = 4096;
const BTREE_MAX_KEY_SIZE: usize = 1000;
const BTREE_MAX_VAL_SIZE: usize = 3000;
const BNODE_NODE: u16 = 1;
// internal nodes without values
const BNODE_LEAF: u16 = 2; // leaf nodes with values

struct BTree {}

impl BTree {
    fn leaf_insert(&self, new: &mut BNode, old: &BNode, idx: u16, key: &[u8], val: &[u8]) {
        new.set_header(BNODE_LEAF, old.n_keys() + 1);
        self.node_append_range(new, old, 0, 0, idx);
        self.insert_kv(idx, 0, key, val);
        self.node_append_range(new, old, idx + 1, idx, old.n_keys() - idx);
    }
}