/// Test fixtures.

use crate::workload::{File, Op, Workload};
use protobuf::{Message, RepeatedField};
use std::option::Option;

pub fn craft_file_message(id: Option<i32>, path: Option<String>) -> File {
    let mut file = File::new();
    let id = id.unwrap_or(1);
    let path = path.unwrap_or(String::from("s3://foo/bar"));
    file.set_id(id);
    file.set_path(path);
    file
}

pub fn craft_op_message(
    targets: Option<RepeatedField<File>>, statement: Option<String>,
    op_sequence_number: Option<i32>
) -> Op {
    let mut op = Op::new();
    let targets = targets.unwrap_or(
        RepeatedField::from_vec(vec![craft_file_message(None, None)])
    );
    let statement = statement.unwrap_or(String::from("SELECT * FROM table"));
    let op_sequence_number = op_sequence_number.unwrap_or(1);
    op.set_targets(targets);
    op.set_statement(statement);
    op.set_op_sequence_num(op_sequence_number);
    op
}

pub fn craft_workload_message(ops: Option<RepeatedField<Op>>) -> Workload {
    let ops = ops.unwrap_or(
        RepeatedField::from_vec(vec![
            craft_op_message(None, None, None)
        ])
    );
    let mut workload = Workload::new();
    workload.set_ops(ops);
    workload
}

pub fn craft_workload_buffer(workload: Option<Workload>) -> Vec<u8> {
    let workload = workload.unwrap_or(craft_workload_message(None));

    let operation_byte: u8 = 1;
    let workload_bytesize = workload.compute_size() as usize;
    let size_bytes: [u8; 2] = [
        (workload_bytesize / 256) as u8, (workload_bytesize % 256) as u8
    ];
    let mut workload_bytes = workload.write_to_bytes().unwrap();

    let mut buffer: Vec<u8> = Vec::with_capacity(workload_bytes.len() + 3);
    buffer.push(operation_byte);
    buffer.push(size_bytes[0]);
    buffer.push(size_bytes[1]);
    buffer.append(&mut workload_bytes);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
        
    #[test]
    /// Asserts that the default workload buffer created by our test fixture has correct bytes.
    fn test_workload_buffer_default_size() {
        let buffer = craft_workload_buffer(None);
        assert_eq!(buffer[0], 1);
        assert_eq!(buffer[1], 0);
        assert_eq!(buffer[2], 43);
        assert_eq!(buffer.len(), 46);
    }

    #[test]
    /// Asserts that a non-default workload buffer created by our test fixture has correct bytes.
    fn test_workload_buffer_nondefault_size() {
        let file = craft_file_message(Some(42), Some(String::from("s3://hello/world")));
        let op = craft_op_message(
            Some(RepeatedField::from_vec(vec![file])),
            Some(
                String::from(
                    "This is just a really long string, not even valid SQL, that I'm creating here 
                     to push the size of the buffer overall over 256 bytes, so that the second 
                     byte of the size bytes in the buffer metadata actually gets used."
            )),
            Some(1),
        );
        let workload = craft_workload_message(Some(RepeatedField::from_vec(vec![op])));
        let buffer = craft_workload_buffer(Some(workload));
        assert_eq!(buffer[0], 1);
        assert_eq!(buffer[1], 1);
        assert_eq!(buffer[2], 35);
    }
}
