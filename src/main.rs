mod common_map;
mod common_reduce;
mod common_rpc;
mod master;
mod master_rpc;
mod master_splitmerge;
mod schedule;
mod utils;
mod wc;
mod worker;

use std::env;

use master::sequential;
use master_rpc::distribucted;
use worker::run_worker;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    for arg in &args{
        println!("{}", arg);
    }
    if args.len() < 4{
       usage(); 
    }else if &args[1]=="master"{
        let (args ,files) = args.split_at(3);
        let files = files.to_owned();
        let addr = args[2].to_owned();
        
        println!("{:?}", addr);

        if &args[2] == "sequential"{
            sequential("wcseq".to_owned(), files, 3, wc::map, wc::reduce);
        }else{
            distribucted("wcseq".to_owned(), files, 3, addr).await.unwrap();
        }
    }else{
        run_worker(args[2].clone(), args[3].clone()).await;
    }
}


fn usage(){
    let usage = r"
Can be run in 3 ways:
1) Sequential (e.g., cargo run -- master sequential x1.txt .. xN.txt)
2) Master (e.g., cargo run -- master 127.0.0.1:7777 x1.txt .. xN.txt)
3) Worker (e.g., cargo run -- worker 127.0.0.1:7777 127.0.0.1:8888 &)
";
    println!("{}", usage);
}
