use mini_cluster_worker::Worker;

/// This helper function generates the raw bytes of a valid Protobuf message. You can copy-paste
/// the output into `nc` input in order to test that the process actually works:
///
/// ```bash
/// $ echo -e "$1" | nc localhost 8080
/// ```
#[allow(dead_code)]
fn generate_test_buffer_bytes() {
    use mini_cluster_worker::fixtures::{
        craft_workload_buffer, craft_workload_message, craft_op_message, craft_file_message
    };
    use protobuf::RepeatedField;

    let file1 = craft_file_message(
        Some(1), Some("s3://mini-cluster-tests/simple-csv.csv".to_owned())
    );
    let op1 = craft_op_message(
        Some(RepeatedField::from_vec(vec![file1])), 
        Some("SELECT 1 + 1".to_owned()),
        Some(1)
    );
    let file2 = craft_file_message(
        Some(1), Some("s3://mini-cluster-tests/simple-csv.csv".to_owned())
    );
    let op2 = craft_op_message(
        Some(RepeatedField::from_vec(vec![file2])), 
        Some("SELECT * FROM dataset_1".to_owned()),
        Some(2)
    );
    let workload = craft_workload_message(
        Some(RepeatedField::from_vec(vec![op1, op2])),
    );
    let workload_buffer = craft_workload_buffer(Some(workload));
    let mut workload_buffer_hex = "".to_owned();
    for byte_u8 in workload_buffer {
        let byte_hex = format!("\\x{:X}", byte_u8);
        workload_buffer_hex += &byte_hex;
    }
    println!("{:?}", workload_buffer_hex);
}

#[tokio::main]
async fn main() {
    // generate_test_buffer_bytes();
    let worker = Worker::new(8080).await.unwrap();
    worker.listen().await.unwrap();
}
