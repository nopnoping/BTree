fn main() {
    let mut s = String::from("");
    {
        let st = String::from("wdad");
        s = st;
    }
    println!("{}", s)
}

struct A {
    callback: fn() -> (),
}

struct B {}

impl B {
    fn test() {}
}