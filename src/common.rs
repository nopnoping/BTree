use lazy_static::lazy_static;
use nix::libc::{_SC_PAGESIZE, sysconf};

use crate::b_node::BNode;

pub const HEADER: usize = 4;
pub const BTREE_PAGE_SIZE: usize = 4096;
pub const BTREE_MAX_KEY_SIZE: usize = 1000;
pub const BTREE_MAX_VAL_SIZE: usize = 3000;

lazy_static! {
    pub static ref SYS_PAGE_SIZE: usize = get_page_size();
}

pub trait Persist {
    fn get_node(&self, ptr: u64) -> BNode;
    fn new_node(&mut self, node: &BNode) -> u64;
    fn del_node(&mut self, ptr: u64);
    fn len(&self) -> usize;
    fn get_root(&self) -> u64;
    fn set_root(&mut self, root: u64);
    fn flush(&mut self);
}

fn get_page_size() -> usize {
    unsafe { sysconf(_SC_PAGESIZE) as usize }
}