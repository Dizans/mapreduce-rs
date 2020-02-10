fn main() {
    tonic_build::compile_protos("proto/master.proto").unwrap();
}

