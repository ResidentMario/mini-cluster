use std::fmt;
use std::option::Option;

use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::err::{Result, SchedulerError, ErrKind};

pub struct WorkerProxy {
    pub port: u16,
    pub connection: Option<TcpStream>,
}

impl fmt::Display for WorkerProxy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<WorkerProxy port:{}>", self.port.to_string())
    }
}

impl WorkerProxy {
    pub fn new(port: u16) -> WorkerProxy {
        WorkerProxy { port, connection: Option::None }
    }

    /// Connects to the remote worker process.
    pub async fn connect(&mut self) -> Result<()> {
        // Interestingly enough, you can return a closure in Rust, but only if the closure is
        // async. The following code has type impl Future. Leave out the async, and the type
        // becomes the unit type `()`, because the closure is simply executed immediately.
        //
        // Rust supports creating and returning closures (`impl Fn`), but this was feature was
        // not added until 2016 or so, resulting in this interesting syntactic quirk.
        //
        // Cf. https://stackoverflow.com/questions/25445761/returning-a-closure-from-a-function
        let conn = TcpStream::connect(format!("localhost:{}", self.port)).await?;
        self.connection = Some(conn);
        Ok(())
    }
    
    /// Closes the connection.
    pub async fn close(&mut self) -> Result<()> {
        // Oddly enough, it doesn't appear to be possible to call `TcpStream.shutdown()` unless
        // you use the `io-util` feature of `tokio`, which adds this function as part of the
        // `tokio::io::AsyncWriteExt` trait.
        //
        // See the following discussion for the reason why this is true:
        // https://discord.com/channels/442252698964721669/448238009733742612/823263598771961857
        self.connection
            .take()
            .ok_or_else(|| SchedulerError::new(
                ErrKind::NetworkError,
                "Cannot close a connection that is not currently open.",
            ))?
            .shutdown()
            .await?;
        Ok(())
    }
}