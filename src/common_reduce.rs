use std::fs;
use std::path::PathBuf;
use std::env;

use std::io::{prelude::*, BufReader};

use crate::utils::{reduce_name, KeyValue};
use serde_json;


pub fn do_reduce(
    job_name: &str,
    reduce_task: usize,
    outfile: &str,
    n_map: usize,
    reduce_f: fn(&str, &Vec<String>) -> String,
) {
    let mut inter_files: Vec<fs::File> = Vec::with_capacity(n_map);

    // let in_file_path = PathBuf::from(in_file);
    // let in_file_dir = in_file_path.parent().unwrap();

    for i in 0..n_map {
        let filename = reduce_name(job_name, i, reduce_task);
        let mut current_dir = env::current_dir().unwrap();
        current_dir.push("data");
        current_dir.push(filename);

        let file = fs::File::open(current_dir).expect("open file failed");
        inter_files.push(file);
    }

    let mut kvs: Vec<KeyValue> = Vec::new();
    for file in inter_files {
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let kv: KeyValue =
                serde_json::from_str(&line.unwrap()).expect("parse string to kv failed");
            kvs.push(kv);
        }
    }

    kvs.sort_by(|l, r| l.k.partial_cmp(&r.k).unwrap());

    let mut out_file_path = PathBuf::from("./data");
    out_file_path.push(outfile);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(out_file_path)
        .expect("append/create file failed");

    let mut i = 0;
    while i < kvs.len() {
        let mut vals: Vec<String> = Vec::new();
        let mut j = i;
        while j < kvs.len() && kvs[i].k == kvs[j].k {
            vals.push(kvs[j].v.clone());
            j += 1;
        }

        let result_kv = KeyValue {
            k: kvs[i].k.clone(),
            v: reduce_f(&kvs[i].k, &vals),
        };

        file.write_all(&serde_json::to_string(&result_kv).unwrap().into_bytes())
            .expect("write failed");
        file.write_all("\n".as_bytes()).expect("write failed");
        i = j;
    }
}
