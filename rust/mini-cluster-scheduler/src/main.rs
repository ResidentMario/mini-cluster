// use mini_cluster_scheduler::scheduler::Scheduler;
use mini_cluster_scheduler::worker_proxy::WorkerProxy;

fn main() {
    // let sched = Scheduler::new(8080);
    let worker_proxy = WorkerProxy::new(8081);
    // println!("{}", sched);
    println!("{}", worker_proxy);
}
