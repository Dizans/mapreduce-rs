use crate::utils::*;
use tokio::sync::{mpsc, broadcast};
use std::sync::Arc;
use futures::future::join_all;

#[allow(dead_code)]
pub async fn schedule(job_name: &str, 
                map_files: Vec<String>, 
                n_reduce: usize, 
                phase: JobPhase,
                register_rx:mpsc::Receiver<String>
                ){
    let mut n_tasks: usize;
    let mut n_other: usize;
    let mut register_rx = register_rx;
    
    match phase{
        JobPhase::MapPhase => {
            n_tasks = map_files.len();
            n_other = n_reduce;
        },
        JobPhase::ReducePhase => {
            n_tasks = n_reduce;
            n_other = map_files.len();
        }
    }

    println!("Schedule: {} {:?} tasks ({} I/Os)", n_tasks, phase, n_other);

    let free_workers: Vec<String> = vec![];
    let (mut tx, mut rx) = broadcast::channel(10);
    
    let waitting_tx = tx.clone();
    tokio::spawn(async move {
        loop{
            println!("waitting register");
            let address = register_rx.recv().await.unwrap();
            println!("got a new address: {}", address);
            waitting_tx.send(address).unwrap();
        }
    });
    
    let mut handles = vec![];
    for i in 0..n_tasks{
       let map_file = map_files[i].clone();
       let phase = phase.clone();
       let tx = tx.clone();
       let mut rx = tx.subscribe();

       let handle = tokio::spawn(async move{
            println!("waitting for free worker");
            let w = rx.recv().await.unwrap();
            println!("processing {}", w);

            let file: String;
            match phase{
                JobPhase::MapPhase => file = map_file,
                JobPhase::ReducePhase => file = "".to_owned(),
            }
            // TODO: do_task 
            println!("scheduling {} task to workers", file);
            tx.send(w).unwrap();
       });
       handles.push(handle);
    }

    join_all(handles).await;
}


