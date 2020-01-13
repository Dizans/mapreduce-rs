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

    for filename in filenames {
        println!("processing: {}", filename);
        do_map("first_time_map", 1, filename, 2, wc::map);
    }
}