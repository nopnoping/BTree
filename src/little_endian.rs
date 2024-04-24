pub trait LittleEndian {
    fn read_u16(&self, start: usize) -> u16;
    fn write_u16(&mut self, start: usize, data: u16);
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
    fn write_u32(&mut self, start: usize, data: u32) {
        self.write_u16(start, (data & 0xffff) as u16);
        self.write_u16(start + 2, (data >> 16) as u16);
    }
    fn write_u64(&mut self, start: usize, data: u64) {
        self.write_u32(start, (data & 0xffffffff) as u32);
        self.write_u32(start + 2, (data >> 32) as u32);
    }
}