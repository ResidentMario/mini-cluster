use std::error::Error;
use std::fmt;
use std::result;
use std::io;

#[derive(Debug)]
pub enum WorkerError {
    NetworkError(io::Error),
    AWSError(io::Error),
    DatabaseError(io::Error),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Because `&self` is declared in the function signature, it is a pointer. As a result,
        // `match self` would require match arms prefixed with `&`: `&WorkerError::IOError`
        // etcetera.
        //
        // Haha JK. This used to be the case, however Rust changed how this works via RFC back in
        // 2016. The document is here: https://rust-lang.github.io/rfcs/2005-match-ergonomics.html.
        // Basically the Rust community decided that this was one of those cases where it was worth
        // having the compiler "do the right thing for you". As a result, in the following code,
        // `&` is not needed, Rust just converts the value type to a pointer type on the fly.
        //
        // Notably, the code sample in the blog post I derived this from seems to predate this RFC.
        // It uses `*self` and `ref WorkerError::[...]`
        // (https://blog.burntsushi.net/rust-error-handling/).
        //
        // Historical context derived from https://stackoverflow.com/a/51111291/1993206.
        match self {
            // The wrapped error types already implement the `Display` trait, which we lean on.
            WorkerError::NetworkError(err) => {
                write!(f, "NetworkError when trying to connect to the scheduler: {}", err)
            }
            WorkerError::AWSError(err) => {
                write!(f, "AWSError when trying to communicate with AWS: {}", err)
            }
            WorkerError::DatabaseError(err) => {
                write!(f, "DatabaseError when trying to communicate with the DB: {}", err)
            }
        }
    }
}

// Implementing the Error trait for a struct used to mean providing an `impl Error` block with
// `description` and `cause` implementations. AFAICT, `description` was deprecated in favor of
// using `fmt` from the `Display` trait (https://github.com/rust-lang/rust/pull/66919), and `cause`
// is not required and I guess drawn from the default implementation now.
impl Error for WorkerError {}

// A lot is going on here.
//
// First of all, we are aliasing the stdlib `result::Result` type with our own `Result` type, one
// which curries the generic's error value (to `Box<dyn Error>`). This is done for convenience: it
// allows us to use a standard uniform `Result` throughout our code that doesn't make us type out
// `WorkerError` every time we need it. E.g. otherwise every time we want to provide a `Result` we
// would have to type out the full `Result<T, WorkerError>` in our signature.
//
// Overwriting the default Result type with a library-specific verison is standard practice when
// building a Rust library, attested to in various places on the web.
//
// Ok, next question, why Box<dyn Error> instead of the simpler WorkerError?
//
// To review the syntax: `dyn Error` indicates a trait object, e.g. an object that implements
// `std::error::Error`. Recall that the `dyn` keyword refers to _dynamic compilation_, a
// duck typing alternative to static compilation. Further reading from the book:
// https://doc.rust-lang.org/book/ch17-02-trait-objects.html#trait-objects-perform-dynamic-dispatch
//
// The advantage of using dynamic compilation here is that is lets us pack _any_ error object
// into our result type. If we were to use `result::Result<T, WorkerError>` instead, any time
// internal code throws a different error, we'd have to convert that other error into a
// `WorkerError`; otherwise we wouldn't be able to use `Result<T, WorkerError>` as our return
// signature. I did this in my earlier aleksey-writes project, and it led to some hairiness:
// https://github.com/ResidentMario/aleksey-writes/tree/master/rust/html2documents/src.
//
// This pattern of using a totally generic type instead is what is recommended by the book:
// https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html. See also
// https://stackoverflow.com/a/62273886/1993206, which is how I found this pattern.
//
// This means that we can use the sweet `?` syntax without any problems.
pub type Result<T> = result::Result<T, Box<dyn Error>>;

// Because implementing the Error trait automatically gives you the From trait; the From trait
// allows automatic conversion from T to Box<T> (see
// https://blog.burntsushi.net/rust-error-handling/#the-from-trait); and ? automatically performs
// any conversions that the From trait supports that are needed to match the return type signature
// (? is super-powerful); our Result type works out-of-the-box with errors we bubble up from other
// libraries.
//
// However, defining our own errors is clunky. We can reuse the ? trick to avoid having to declare
// the boxing manually, but we'd still need to construct the WorkerError object, with e.g.:
// Err(WorkerError::NetworkError(io::Error::new(io::ErrorKind::Other, msg)))?
//
// We can make this process a little less painful by defining a `new`. However, Rust does not
// allow you to create a concrete type object:
// `WorkerError::new(WorkerError::IOError(io::Error::new(...)))`  // ok
// `WorkerError::new(WorkerError::IOError)  // not ok
//
// Compare this with Python:
// err_object = IOError("foo")
// err_type_object = IOError
//
// As a result, to implement our `new` we have to declare a whole other enum, `ErrKind`, solely
// to be used as input to `new`. We can then `match` on this `enum` to craft the suitable
// `WorkerError` for our purposes.
//
// This has the disadvantage of requiring an additional import in any code module that needs to
// construct its own errors: `import crate::ErrKind`. It's technically possible to work around
// having such an import by building a macro, but this is not considered good practice -- you're
// better off just being explicit and declaring the import.
//
// This thread summarizes the following comment stream from Rust Discord #beginners:
// https://discord.com/channels/442252698964721669/448238009733742612/820019764528939028
#[derive(Debug)]
pub enum ErrKind {
    NetworkError,
    AWSError,
    DatabaseError,
}

impl WorkerError {
    pub fn new(kind: ErrKind, msg: &str) -> WorkerError {
        match kind {
            ErrKind::NetworkError => {
                WorkerError::NetworkError(io::Error::new(io::ErrorKind::Other, msg))
            },
            ErrKind::AWSError => {
                WorkerError::AWSError(io::Error::new(io::ErrorKind::Other, msg))
            },
            ErrKind::DatabaseError => {
                WorkerError::DatabaseError(io::Error::new(io::ErrorKind::Other, msg))
            }
        }
    }
}
