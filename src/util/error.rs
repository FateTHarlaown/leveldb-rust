use failure::Fail;

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
}

impl From<io::Error> for StatusError {
    fn from(err: io::Error) -> Self {
        StatusError::IOError(err)
    }
}
