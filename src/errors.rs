use std::error::Error;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug)]
pub struct CliError(String);

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CliError: {}", self.0)
    }
}

impl CliError {
    pub fn new (message: String) -> CliError {
        CliError(message)
    }
}

impl Error for CliError {}
