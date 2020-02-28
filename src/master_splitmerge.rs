use std::collections::HashMap;
use std::fs;
use std::io::{prelude::*, BufReader};

use serde_json;

use crate::utils::*;

pub fn merge(job_name: &str, n_reduce: usize) {
    let mut kvs: HashMap<String, String> = HashMap::new();
    for i in 0..n_reduce {
        let filename = merge_name(job_name, i);
        let file = fs::File::open(&filename).expect("read file failed");
        let file = BufReader::new(file);
        for line in file.lines() {
            let line = line.unwrap();
            let kv: KeyValue = serde_json::from_str(&line).expect("deserialize failed");
            kvs.insert(kv.k.clone(), kv.v.clone());
        }
    }

    let mut keys: Vec<&str> = Vec::new();
    for k in kvs.keys() {
        keys.push(k);
    }
    keys.sort();

    let mut file = fs::File::create(&format!("mrtmp.{}", job_name)).expect("create file failed");
    for k in keys {
        file.write_all(&format!("{}: {}\n", k, kvs.get(k).unwrap()).into_bytes())
            .expect("failed to write");
    }
}
