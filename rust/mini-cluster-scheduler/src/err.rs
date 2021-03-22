// Errors implementation mostly copied from `mini-cluster-worker/err.rs`.

use std::error::Error;
use std::fmt;
use std::result;
use std::io;

#[derive(Debug)]
pub enum SchedulerError {
    NetworkError(io::Error),
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchedulerError::NetworkError(err) => {
                write!(f, "NetworkError when trying to connect to the worker: {}", err)
            },
        }
    }
}

impl Error for SchedulerError {}

pub type Result<T> = result::Result<T, Box<dyn Error>>;

#[derive(Debug)]
pub enum ErrKind {
    NetworkError,
}

impl SchedulerError {
    pub fn new(kind: ErrKind, msg: &str) -> SchedulerError {
        match kind {
            ErrKind::NetworkError => {
                SchedulerError::NetworkError(io::Error::new(io::ErrorKind::Other, msg))
            },
        }
    }
}
