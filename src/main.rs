fn main() {
    println!("Hello, world!");
}


#[cfg(test)]
mod tests {
    #[test]
    fn ci_test() {
        assert_eq!(2, 1 + 1);
    }
}
