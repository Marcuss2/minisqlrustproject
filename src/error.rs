use std::error::Error;


#[non_exhaustive]
pub enum DatabaseError {
    TableExists,
    DataConflict,
    TableDoesNotExist,
    NoDataFound,
}