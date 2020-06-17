fn main() {
    prost_build::compile_protos(&["src/hypertrie_pb.proto"], &["src"]).unwrap();
    prost_build::compile_protos(&["src/mount_pb.proto"], &["src"]).unwrap();
}
