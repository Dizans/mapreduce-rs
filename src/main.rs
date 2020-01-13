mod common_map;
mod master;
mod utils;
mod wc;
mod common_reduce;

use std::env;
use std::fs::File;
use std::io::prelude::*;

use clap::{App, Arg, SubCommand};

use common_map::do_map;
use common_reduce::do_reduce;

fn main() {
    let matches = App::new("mapreduce")
        .author("dizansyu dizansyu@gmail.com")
        .arg(
            Arg::with_name("files")
                .required(true)
                .multiple(true)
                .help("files waiting to mapreduce"),
        )
        .get_matches();

    let filenames: Vec<_> = matches.values_of("files").unwrap().collect();

    let n_map = 1;
    let n_reduce = 2;
    for filename in filenames {
        println!("processing: {}", filename);
        for n in 0..n_map{
            do_map("word_count", n, filename, n_reduce, wc::map);
        }
    }

    for n in 0..n_reduce{
        do_reduce("word_count", n, "final_result", n_map, wc::reduce);
    }
}
