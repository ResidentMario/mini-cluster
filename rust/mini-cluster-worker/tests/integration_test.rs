use serial_test::serial;
use protobuf::RepeatedField;
// use tokio::{io::AsyncWriteExt, net::{TcpStream, TcpListener}};

use mini_cluster_worker::file::{
    localize_file, create_new_s3_client
};
use mini_cluster_worker::job::Job;
use mini_cluster_worker::fixtures::{
    craft_file_message, craft_workload_message, craft_op_message
};
// use mini_cluster_worker::Worker;

#[tokio::test]
#[serial]
async fn test_localize_file() {
    let f = craft_file_message(
        Some(1), Some("s3://mini-cluster-tests/simple-csv.csv".to_owned())
    );
    let client = create_new_s3_client();
    let result = localize_file(&f, &client).await;

    assert!(result.is_ok());
}

// Why am I using the multithreaded flavor of testing here? For whatever reason this test fails
// with a "blocking thread" error if I run it in a single-threaded manner. AFAICT, from looking
// at the stack trace, the source of the error is internal to sqlx.connect somehow? IDK what is
// going on here exactly. but swapping to multithreaded testing resolves the issue, so I'm just
// going to be lazy and band-aid over the problem that way. `sqlx` does claim to by full async...
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_build_job() {
    let f = craft_file_message(
        Some(1), Some("s3://mini-cluster-tests/simple-csv.csv".to_owned())
    );
    let op = craft_op_message(
        Some(RepeatedField::from_vec(vec![f])),
        Some("SELECT * FROM dataset_1".to_owned()),
        Some(1),
    );
    let workload = craft_workload_message(
        Some(RepeatedField::from_vec(vec![op])),
    );

    let job = Job::new(workload).await;
    assert!(job.is_ok());
    let job = job.unwrap();

    let result = job.build(create_new_s3_client()).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_run_job() {
    let f1 = craft_file_message(
        Some(1), Some("s3://mini-cluster-tests/simple-csv.csv".to_owned())
    );
    let f2 = craft_file_message(
        Some(1), Some("s3://mini-cluster-tests/simple-csv.csv".to_owned())
    );
    let op1 = craft_op_message(
        Some(RepeatedField::from_vec(vec![f1])),
        Some("SELECT 1 + 1".to_owned()),
        Some(1),
    );
    let op2 = craft_op_message(
        Some(RepeatedField::from_vec(vec![f2])),
        Some("SELECT * FROM dataset_1".to_owned()),
        Some(2),
    );
    let workload = craft_workload_message(
        Some(RepeatedField::from_vec(vec![op1, op2])),
    );

    let job = Job::new(workload).await;
    assert!(job.is_ok());
    let job = job.unwrap();

    let build_result = job.build(create_new_s3_client()).await;
    assert!(build_result.is_ok());

    let run_result = job.run().await;
    assert!(run_result.is_ok());
    let result = run_result.unwrap();

    assert!(result.len() == 1);
}

// TODO: integration test for the handle_connection in lib.rs.
// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// #[serial]
// async fn test_handle_connection() {
//     let worker = Worker::new(5001).await;
//     assert!(worker.is_ok());
//     let worker = worker.unwrap();

//     let stream = TcpStream::connect("localhost:5000").await;
//     assert!(stream.is_ok());
//     let mut stream = stream.unwrap();
    
//     let stream_write = stream.write(b"\x00").await;
//     assert!(stream_write.is_ok());
//     let handle_connection_fut = worker.handle_connection(&mut stream);

//     let result = handle_connection_fut.await;
//     assert!(result.is_ok());
// }