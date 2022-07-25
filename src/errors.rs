use std::error::Error;
use std::fmt;
use confy::ConfyError;
use http::uri::InvalidUri;
use tonic::Status;

#[derive(Debug)]
pub enum CliError {
    NoServerAddressError,
    ConfyError(ConfyError),
    InvalidUri(InvalidUri),
    TonicTransportError(tonic::transport::Error),
    HttpStatusError(Status)
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CliError::NoServerAddressError => write!(f, "No Server Address Provided!"),
            CliError::ConfyError(e) => write!(f, "Confy Error: {:?}", e),
            CliError::InvalidUri(e) => write!(f, "Invalid URI Error: {:?}", e),
            CliError::TonicTransportError(e) => write!(f, "Transport Error: {:?}", e),
            CliError::HttpStatusError(e) => write!(f, "HTTP Status Error: {:?}", e)
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(_err: std::io::Error) -> CliError {
        CliError::NoServerAddressError
    }
}

impl From<ConfyError> for CliError {
    fn from(err: ConfyError) -> CliError {
        CliError::ConfyError(err)
    }
}

impl From<InvalidUri> for CliError {
    fn from(err: InvalidUri) -> CliError {
        CliError::InvalidUri(err)
    }
}

impl From<tonic::transport::Error> for CliError {
    fn from(err: tonic::transport::Error) -> CliError {
        CliError::TonicTransportError(err)
    }
}

impl From<Status> for CliError {
    fn from(err: Status) -> CliError {
        CliError::HttpStatusError(err)
    }
}

impl Error for CliError {}