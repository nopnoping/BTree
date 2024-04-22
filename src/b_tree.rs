const HEADER:usize = 4;

struct BNode {
    data: Vec<u8>
}

impl BNode {
    fn n_type(&self) -> u16 {
        self.read_u16(0)
    }

    fn n_keys(&self) -> u16 {
        self.read_u16(2)
    }

    fn n_bytes(&self) -> u16 {
        self.kv_pos(self.n_keys())
    }

    fn set_header(&mut self,n_type: u16, n_keys:u16) {
        self.write_u16(0, n_type);
        self.write_u16(2, n_keys);
    }

    fn get_ptr(&self,idx:u16) -> u64 {
        assert!(idx < self.n_keys());
        let pos = HEADER + idx as usize*8;
        self.read_u64(pos)
    }

    fn set_ptr(&mut self, idx:u16, val:u64) {
        assert!(idx < self.n_keys());
        let pos = HEADER + idx as usize*8;
        self.write_u64(pos, val)
    }

    fn offset_pos(&self, idx: u16) -> u16 {
        assert!(idx>=0 &&idx <= self.n_keys());
        HEADER as u16 + 8 * self.n_keys() + 2*(idx-1)
    }

    fn get_offset(&self, idx:u16) -> u16 {
        if idx == 0 {
            0
        } else {
            self.read_u16(self.offset_pos(idx) as usize)
        }
    }

    fn set_offset(&mut self, idx:u16, offset:u16) {
        self.write_u16(self.offset_pos(idx) as usize, offset)
    }

    fn kv_pos(&self, idx:u16) -> u16 {
        assert!(idx <= self.n_keys());
        HEADER as u16 + 8&self.n_keys() + 2*self.n_keys() + self.get_offset(idx)
    }

    fn get_key(&self, idx:u16) -> &[u8] {
        assert!(idx < self.n_keys());
        let pos = self.kv_pos(idx);
        let k_len = self.read_u16(pos as usize);
        &self.data[pos as usize+4..][..k_len as usize]
    }

    fn get_val(&self, idx:u16) -> &[u8] {
        assert!(idx < self.n_keys());
        let pos = self.kv_pos(idx);
        let k_len = self.read_u16(pos as usize);
        let v_len = self.read_u16(pos as usize + 2);
        &self.data[(pos + 4 + k_len)as usize..][..v_len as usize]
    }


}

impl BNode {
    fn read_u16(&self, start:usize) -> u16{
        let low = self.data[start] as u16;
        let hi = self.data[start + 1] as u16;
        low | (hi << 8)
    }
    fn read_u32(&self, start:usize) -> u32 {
        let low = self.read_u16(start) as u32;
        let hi = self.read_u16(start + 2) as u32;
        low | (hi << 16)
    }
    fn read_u64(&self, start:usize) -> u64 {
        let low = self.read_u32(start) as u64;
        let hi = self.read_u32(start+4) as u64;
        low | (hi << 32)
    }
    fn write_u16(&mut self, start:usize, data:u16) {
        self.data[start] = (data & 0xff) as u8;
        self.data[start + 1] = (data >> 8) as u8;
    }
    fn write_u32(&mut self, start:usize, data:u32) {
        self.write_u16(start, (data & 0xffff) as u16);
        self.write_u16(start+2, (data >> 16) as u16);
    }
    fn write_u64(&mut self, start:usize, data:u64) {
        self.write_u32(start, (data & 0xffffffff) as u32);
        self.write_u32(start+2, (data >> 32) as u32);
    }
}