fn main() {
    let rdr = vec![2, 3, 4, 5];
    let b1 = &rdr[0..2];
    let b2 = &rdr[..];
    if b1 <= b2 {
        println!("ok")
    }
}
