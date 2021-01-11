use failure::Fail;

use crossbeam_channel::RecvError;
use failure::_core::num::ParseIntError;
use std::fmt;
use std::io;
use std::result;
use std::string::FromUtf8Error;

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

    #[fail(display = "{}", _0)]
    RecvError(RecvError),

    #[fail(display = "{}", _0)]
    UTF8Error(FromUtf8Error),

    #[fail(display = "{}", _0)]
    DBClose(String),

    #[fail(display = "{}", _0)]
    Customize(String),
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

impl From<RecvError> for StatusError {
    fn from(err: RecvError) -> Self {
        StatusError::RecvError(err)
    }
}

impl From<FromUtf8Error> for StatusError {
    fn from(err: FromUtf8Error) -> Self {
        StatusError::UTF8Error(err)
    }
}
