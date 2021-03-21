use std::fmt;
use sqlx::{Column, Row, sqlite::SqliteRow};
use tokio::net::{TcpStream, TcpListener};
use err::Result;
use protobuf::Message;

pub mod err;
pub mod workload;
pub mod file;
pub mod fixtures;
pub mod db;
pub mod job;

use err::{WorkerError,ErrKind};
use job::Job;
use file::create_new_s3_client;

pub struct Worker {
    pub port: u16,
    pub listener: TcpListener
}

impl fmt::Display for Worker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<Worker port:{}>", self.port.to_string())
    }
}

impl Worker {
    pub async fn new(port: u16) -> Result<Worker> {
        let addr = format!("127.0.0.1:{port}", port=port.to_string());
        let listener = TcpListener::bind(addr).await?;
        Ok(Worker { port, listener })
    }

    // This asynchronous listener courtesy of
    // https://docs.rs/tokio/1.3.0/tokio/net/struct.TcpListener.html.
    pub async fn listen(&self) -> Result<()> {
        loop {
            let (mut socket, _) = self.listener.accept().await?;
            self.handle_connection(&mut socket).await?;
        }
    }

    async fn read_metadata_bytes(stream: &mut TcpStream) -> Result<Option<[u8; 3]>> {
        // `read` is inherited from the `Read` trait, with a `buf: &mut [u8]` signature. Here,
        // `&mut` means a mutable pointer reference, and `[u8]` specifies an array of unsigned
        // 8-bit ints.
        //
        // To build such a thing we need to create a mutable value reference with the right type.
        // `[u8; n]` is the syntax for an array containing n `u8` values. `[0]` is not
        // correct on its own because Rust won't cast the type for you automatically. You have
        // to cast it yourself--done so here using `as`.
        //
        // Protocol buffers are arbitrarily sized, but the array TcpStream reads into needs to be
        // of a fixed size, because Rust. So we'll split the job across two buffers. The first
        // buffer reads three bytes of metadata from the header: a one-byte signal, and two bytes
        // describing the incoming protocol buffer's size.
        let mut scheduler_request_metadata_buffer: [u8; 3] = [0 as u8; 3];

        // `read` will pull a number of bytes into `stream` in the range (0, usize). Reading zero
        // bytes indicates that the buffer recieved was zero bytes in length, or that the reader
        // has reached EOF (e.g. because the connection got closed).
        //
        // However, `read` is not guaranteed to pull exactly `usize` bytes either. The docstring
        // lists two scenarios where a smaller buffer will be received instead. The first is
        // a less-than-usize buffer followed by an EOF (e.g. the client closing the connection).
        // In this case, the set of bytes returned will be the set recieved prior to the EOF. The
        // second occurs "when read() was interrupted by a signal". What this means is not
        // immediately apparent to me, but asking on Discord got this cleared up:
        // https://discord.com/channels/442252698964721669/448238009733742612/814602496676462592.
        //
        // What this means. Suppose the usage pattern is that the client sends some data, then
        // closed the connection without expecting a response. In this case we wait until the
        // return size (`rsize`) is 0; that is our indicator that the client is done writing
        // data (closing the connection always triggers an EOF).
        //
        // Suppose the usage pattern is that the client sends some data, and expects to get some
        // other data in response (read-write), after which the client closes the connection. In
        // this case we wait until the sum of all segments received is at least `usize` in length
        // before proceeding forward.
        //
        // In this case we want to hold until we have successfully read three bytes from the
        // stream. If we see a nil read, indicating the client closed the connection, we close
        // the socket and yield.
        loop {
            let mut total_bytes_received: usize = 0;

            stream.readable().await?;
            let rsize = stream.try_read(&mut scheduler_request_metadata_buffer)?;
            if rsize == 0 {
                println!("Client sent empty (nil) input before closing the connection.");
                return Ok(None);
            } else {
                total_bytes_received += rsize;
                if total_bytes_received == 3 {
                    return Ok(Some(scheduler_request_metadata_buffer));
                }
            }
        }
    }

    async fn read_protobuf_bytes(
        stream: &mut TcpStream, buffer_length: usize
    ) -> Result<Option<workload::Workload>> {
        // Allocate a fixed-size buffer matching the to-be-received size.
        // Rust differentiates between capacity and length. Setting capacity with_capacity
        // reserves the underlying memory, but it doesn't actually assign that length to
        // the vector, it's still length 0!
        let mut scheduler_request_buffer = Vec::<u8>::with_capacity(buffer_length);
        scheduler_request_buffer.resize(buffer_length, 0);
        let scheduler_request_buffer = &mut scheduler_request_buffer[0..buffer_length];
        loop {
            let mut total_bytes_received: usize = 0;

            stream.readable().await?;
            let rsize = stream.try_read(scheduler_request_buffer)?;
            if rsize == 0 {
                println!("Client closed the connection.");
                return Ok(None);
            } else {
                total_bytes_received += rsize;
                if total_bytes_received == buffer_length { break; }
            }
        }
        println!("Received work buffer with length {:?}.", buffer_length);
        let workload = workload::Workload::parse_from_bytes(scheduler_request_buffer)?;
        Ok(Some(workload))
    }

    /// Displays the result of a computation.
    pub fn print_result(rows: Vec<SqliteRow>) -> Result<()> {
        if rows.len() == 0 { return Ok(()) }
        let first_result_row = &rows[0];

        let mut out = "|".to_owned();
        for column in first_result_row.columns() {
            let column_name = column.name();
            out += column_name;
            out += "|";
        }
        
        // Because Rust doesn't allow us to pass a type object as a parameter, this code is
        // really weird.
        //
        // The basic issue is that `sqlx` does not know the exact type of its row eleemnts. I
        // don't really understand why this is the case; certainly each row has a well-defined
        // type in the SQL table, and you'd think that `sqlx` would convert that type's value
        // to an equivalent Rust type for you, but it doesn't. Must be a Rust "always-be-safe"
        // kind of thing.
        //
        // This makes piping the results to output super painful. Every SQLite type has one and
        // only one Rust type that SQLx can map it to. SQLx provides a `get`; this panics if the
        // type of the variable being assigned to is not exactly the type that SQLx expects. SQLx
        // provides a `try_get`; this returns a `Result`, which will be a DecodeError if the
        // type is not an exact match.
        //
        // So basically, as far as I can tell, decoding the values requires EITHER knowing the
        // type ahead of time, OR just trying every possible output type in sequence. This
        // seems...super shitty. =(
        //
        // Here is the conversion chart: https://docs.rs/sqlx/0.5.1/sqlx/sqlite/types/index.html.
        for row in rows.iter() {
            out += "\n";
            out += "|";
            for i in 0..row.len() {
                // Is it an INTEGER?
                let v: std::result::Result<i64, sqlx::Error> = row.try_get(i);
                if v.is_ok() {
                    out += &v.unwrap().to_string();
                    out += "|";
                    continue;
                }

                // Is it a TEXT?
                let v: std::result::Result<String, sqlx::Error> = row.try_get(i);
                if v.is_ok() {
                    out += &v.unwrap();
                    out += "|";
                    continue;
                }

                // TODO: add more possible types. Or, investigate online if there's a better way
                // to do this, because this seems kind of mental.

                Err(WorkerError::new(
                    ErrKind::DatabaseError, 
                    "Result set has output type not understood by the worker."
                ))?
            }
        }
        println!("{}", out);
        Ok(())
    }

    /// Handles a connections into the worker's socket listener.
    pub async fn handle_connection(&self, stream: &mut TcpStream) -> Result<()> {
        // read_metadata_bytes handles reading the first three bytes of the stream. It returns
        // Result<Option<[u8, 3]>>. Possible return values are: an error, if the stream reader
        // throws one; an Ok([u8, 3]), if all is successful; or a None, if the stream is closed,
        // probably by the client, before three bytes are successfully read.
        let scheduler_request_metadata_buffer =
            match Worker::read_metadata_bytes(stream).await {
                Ok(v) => match v {
                    Some(v) => v,
                    None => return Ok(()),
                },
                Err(e) => return Err(e),
            };

        // The first byte describes the signal type: PING, WORK, or SHUTDOWN. When a PING or
        // SHUTDOWN is received, all of the other bytes are ignored.
        match scheduler_request_metadata_buffer[0] {
            0 => println!("Scheduler sent PING signal (first byte 0)."),
            1 => {
                println!("Scheduler sent WORK signal (first byte 1).");
                // The second and third byte describe the protocol buffer size (in bytes).
                // The maximum size is 2**16=65636 bytes, e.g. ~65kB. This should be sufficient.
                let buffer_length: usize = 256 *
                    (scheduler_request_metadata_buffer[1] as usize) +
                    (scheduler_request_metadata_buffer[2] as usize);

                // read_protobuf_bytes handles reading the protobuf message out of the stream. Its
                // return type and usage notes are the same as the ones for
                // scheduler_request_metadata_buffer just above.
                let workload = match
                    Worker::read_protobuf_bytes(stream, buffer_length).await {
                        Ok(v) => match v {
                            Some(v) => v,
                            None => return Ok(()),    
                        },
                        Err(e) => return Err(e),
                    };

                println!("Workload plaintext representation is: {:?}", workload);
                let job = Job::new(workload).await?;
                job.build(create_new_s3_client()).await?;
                let result = job.run().await?;
                println!("Workload computation result is:");
                Worker::print_result(result)?;
                println!("Done processing workload!");
            },
            2 => {
                println!("Scheduler sent SHUTDOWN signal (first byte 2).")
            }
            _ => panic!(
                format!(
                    "Received invalid signal (first byte {:?}).",
                    &scheduler_request_metadata_buffer[0..1]
                )
            )
        }
        Ok(())
    }
}
