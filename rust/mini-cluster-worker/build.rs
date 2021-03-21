extern crate protobuf_codegen_pure;

fn main() {
    protobuf_codegen_pure::Codegen::new()
    .out_dir("src/")
    .inputs(&["../protos/workload.proto"])
    .include("../protos/")
    .run()
    .expect("Codegen failed.");
}
