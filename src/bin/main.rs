use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};

fn main() {
    let rdr = vec![2, 3, 4, 5];
    let mut buf = Cursor::new(&rdr);
    buf.read_u16::<BigEndian>().unwrap();
    println!("{}", buf.position());
}
