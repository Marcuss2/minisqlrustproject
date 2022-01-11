use cap::Cap;
use std::alloc;

#[global_allocator]
pub static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::max_value());

// Sets 16 Mb as the maximum available heap memory for the database
const DEFAULT_MEMORY_LIMIT: usize = 16_000_000;

pub fn out_of_memory() -> bool {
    ALLOCATOR.allocated() > get_memory_limit()
}

fn get_memory_limit() -> usize {
    if let Ok(limit) = std::env::var("MEMORY_LIMIT") {
        return limit.parse::<usize>().expect("MEMORY_LIMIT must be an integer");
    }
    DEFAULT_MEMORY_LIMIT
}
