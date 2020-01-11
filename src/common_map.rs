use std::fs;
use std::io::prelude::*;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::utils::*;
use serde_json;

pub fn do_map(
    job_name: &str,
    map_task: usize,
    in_file: &str,
    n_reduce: usize,
    map_f: fn(&str, &str) -> Vec<KeyValue>,
) {
    let contents = fs::read_to_string(in_file)
        .expect(&format!("can't read file {}", in_file));

    let mut reduce_files: Vec<fs::File> = Vec::with_capacity(n_reduce);

    for i in 0..n_reduce {
        let filename = reduce_name(job_name, map_task, i);
        let mut file = fs::File::create(&filename)
            .expect(&format!("create file {} failed", filename));
        reduce_files.push(file)
    }

    let kvs: Vec<KeyValue> = map_f(in_file, &contents);

    for kv in kvs {
        let r = hash_key(&kv.k) % n_reduce;
        let mut inter_file = &reduce_files[r];
        let json_string = serde_json::to_string(&kv)
            .expect("failed to conver kv to string");
        inter_file.write_all(&json_string.into_bytes())
            .expect("write string to file failed");
    }
}

fn hash_key(k: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    k.hash(&mut hasher);
    hasher.finish() as usize
}
