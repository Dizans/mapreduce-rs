mod common_map;
mod common_reduce;
mod master;
mod master_rpc;
mod master_splitmerge;
mod utils;
mod wc;

use std::env;
use std::fs::File;
use std::io::prelude::*;

use clap::{App, Arg, SubCommand};

use common_map::do_map;
use common_reduce::do_reduce;
use master_rpc::start_server;
use master_splitmerge::merge;
use utils::merge_name;

#[tokio::main]
async fn main() {
    // let matches = App::new("mapreduce")
    //     .author("dizansyu dizansyu@gmail.com")
    //     .arg(
    //         Arg::with_name("files")
    //             .required(true)
    //             .multiple(true)
    //             .help("files waiting to mapreduce"),
    //     )
    //     .get_matches();

    // let filenames: Vec<_> = matches.values_of("files").unwrap().collect();

    // let n_map = filenames.len();
    // let n_reduce = 2;
    // let job_name = "world_count";
    // for n in 0..n_map {
    //     do_map(job_name, n, filenames[n], n_reduce, wc::map);
    // }

    // for n in 0..n_reduce {
    //     do_reduce(job_name, n, &merge_name(job_name, n), n_map, wc::reduce);
    // }

    // merge(job_name, n_reduce);
    let server = start_server().await;
}
