use std::{error::Error, fmt::Display};

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
        };

        f.write_str(message)
    }
}

impl Error for DatabaseError {}

#[non_exhaustive]
#[derive(Debug)]
pub enum UserError {
    SyntaxError,
    Other(&'static str),
}

impl Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match &self {
            UserError::SyntaxError => "SyntaxError",
            UserError::Other(msg) => msg,
        };
        write!(f, "UserError: {}", message)
    }
}

impl Error for UserError {
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
