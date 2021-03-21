use std::fmt;
use std::future::Future;
use std::option::Option;
use std::net::TcpStream;

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

    pub async fn init(&self) -> impl Future<Output=()> {
        let init_fn = self.connect().await;
        init_fn
    }

    pub async fn connect(&self) -> impl Future<Output=()> {
        let connect_fn = async {
            println!("Trying to connect...");
        };
        connect_fn
    }
}