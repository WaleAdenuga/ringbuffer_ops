use std::println;

pub mod buffer;

fn main() {
    println!("Hello, world!");
    let mut count = 0;
    for i in 0..11 {
        println!("i: {:?}", i);
        count += 1;        
    }

    println!("Count = {:?}", count);
}
