mod common_map;
mod master;
mod utils;
mod wc;

use std::env;
use std::fs::File;
use std::io::prelude::*;

use clap::{Arg, App, SubCommand};

use common_map::do_map;

fn main(){
    let matches = App::new("mapreduce")
                    .author("dizansyu dizansyu@gmail.com")
                    .arg(Arg::with_name("files")
                         .required(true)
                         .multiple(true)
                         .help("files waiting to mapreduce"))
                    .get_matches();

    let filenames: Vec<_> = matches.values_of("files")
        .unwrap().collect();

    for filename in filenames{
	println!("processing: {}", filename);
	do_map("first_time_map", 1, filename, 2, wc::map);
    }
    
}

// pub fn do_map(
//     job_name: &str,
//     map_task: usize,
//     in_file: &str,
//     n_reduce: usize,
//     map_f: fn(&str, &str) -> Vec<KeyValue>,



// fn main() {
//     let args: Vec<String> = env::args().collect();
//     if args.len() < 3 {
//         panic!("Usage: mrsequential ../mrapps/xxx.so inputfiles...");
//     }

//     let mut intermediate: Vec<utils::KeyValue> = Vec::new();

//     for filename in args[2..].iter() {
//         let file = File::open(filename);
//         if let Ok(mut file) = file {
//             let mut content = String::new();
//             file.read_to_string(&mut content)
//                 .expect(&format!("read {} contents failed", filename));
//             let mut kva = wc::map(&filename, &content);
//             intermediate.append(&mut kva);
//         }
//     }

//     intermediate.sort_by(|a, b| a.k.cmp(&b.k));

//     let mut i = 0;
//     while i < intermediate.len() {
//         let mut j = i + 1;

//         while j < intermediate.len() && intermediate[j].k == intermediate[i].k {
//             j += 1;
//         }

//         let mut values: Vec<String> = Vec::new();

//         for k in i..j {
//             values.push(intermediate[k].v.clone());
//         }

//         let output = wc::reduce(&intermediate[i].k, &values);
//         println!("{} {:?}", intermediate[i].k, output);
//         i = j;
//     }
// }
