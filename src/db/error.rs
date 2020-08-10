use failure::Fail;

use failure::_core::num::ParseIntError;
use std::io;
use std::result;

pub type Result<T> = result::Result<T, StatusError>;

#[derive(Fail, Debug)]
pub enum StatusError {
    #[fail(display = "{}", _0)]
    NotFound(String),

    #[fail(display = "{}", _0)]
    Corruption(String),

    #[fail(display = "{}", _0)]
    NotSupported(String),

    #[fail(display = "{}", _0)]
    InvalidArgument(String),

    #[fail(display = "{}", _0)]
    IOError(io::Error),

    #[fail(display = "{}", _0)]
    Eof(String),
}

impl From<io::Error> for StatusError {
    fn from(err: io::Error) -> Self {
        StatusError::IOError(err)
    }
}

impl From<ParseIntError> for StatusError {
    fn from(err: ParseIntError) -> Self {
        StatusError::Corruption(err.to_string())
    }
}
