use std::fs;
use std::io::prelude::*;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use crate::utils::*;
use serde_json;

pub fn do_map(
    job_name: &str,
    map_task: usize,
    in_file: &str,
    n_reduce: usize,
    map_f: fn(&str, &str) -> Vec<KeyValue>,
) {
    let contents = fs::read_to_string(&in_file).expect(&format!("can't read file {}", in_file));

    let mut reduce_files: Vec<fs::File> = Vec::with_capacity(n_reduce);

    let in_file_path = PathBuf::from(in_file);
    let in_file_dir = in_file_path.parent().unwrap();

    for i in 0..n_reduce {
        let filename = reduce_name(job_name, map_task, i);
        let mut out_file = PathBuf::from(&in_file_dir);
        out_file.push(filename);
        let file =
            fs::File::create(&out_file).expect(&format!("create file {:?} failed", out_file));
        reduce_files.push(file)
    }

    let kvs: Vec<KeyValue> = map_f(in_file, &contents);

    for kv in kvs {
        let r = hash_key(&kv.k) % n_reduce;
        let mut inter_file = &reduce_files[r];
        let json_string = serde_json::to_string(&kv).expect("failed to conver kv to string");
        inter_file
            .write_all(&json_string.into_bytes())
            .expect("write string to file failed");
        inter_file
            .write_all("\n".as_bytes())
            .expect("write \\n failed");
    }
}

fn hash_key(k: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    k.hash(&mut hasher);
    hasher.finish() as usize
}
