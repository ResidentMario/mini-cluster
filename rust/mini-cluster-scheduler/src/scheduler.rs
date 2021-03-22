// use std::fmt;
// use std::future::Future;

// use crate::worker_proxy::WorkerProxy;

// pub struct Scheduler {
//     pub port: u16,
//     pub workers: Vec<WorkerProxy>
// }

// impl fmt::Display for Scheduler {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "<Scheduler port:{}>", self.port.to_string())
//     }
// }

// impl Scheduler {
//     pub fn new(port: u16) -> Scheduler {
//         Scheduler { port, workers: vec![] }
//     }

//     // pub fn scale(nworkers: u16)

//     pub async fn init(&self) {
//         let processes: Vec<&dyn Future<Output=()>> = vec![];
//         for worker in &self.workers {
//             let init_fn: Future<Output=()> = worker.init().await;
//             processes.push(init_fn);
//         }
//         // future::join!(processes);
//     }
// }