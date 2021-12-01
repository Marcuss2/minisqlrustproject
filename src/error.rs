use std::{
    error::Error,
    fmt::{Display},
};

#[non_exhaustive]
#[derive(Debug)]
pub enum DatabaseError {
    TableExists,
    DataConflict,
    TableDoesNotExist,
    NoDataFound,
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            DatabaseError::TableExists => "Table Already Exists",
            DatabaseError::DataConflict => "Data Conflict",
            DatabaseError::TableDoesNotExist => "Table Does Not Exist",
            DatabaseError::NoDataFound => "No Data Found",
            _ => "Unknown error",
        };

        f.write_str(message)
    }
}

impl Error for DatabaseError {}

#[non_exhaustive]
#[derive(Debug)]
pub enum ParseError {
    SyntaxError,
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            ParseError::SyntaxError => "SyntaxError",
            _ => "Unknown error",
        };
        f.write_str(message)
    }
}

impl Error for ParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}
