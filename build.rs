fn main() {
    tonic_build::compile_protos("proto/mr.proto").unwrap();
}

