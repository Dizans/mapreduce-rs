use crate::common_rpc::{worker_do_task, TaskArg};
use crate::utils::*;
use futures::future::join_all;
use tokio::sync::broadcast;

#[allow(dead_code)]
pub async fn schedule(
    job_name: String,
    map_files: Vec<String>,
    n_reduce: usize,
    phase: JobPhase,
    register_rx: broadcast::Receiver<String>,
) {
    let n_tasks: usize;
    let n_other: usize;

    match phase {
        JobPhase::MapPhase => {
            n_tasks = map_files.len();
            n_other = n_reduce;
        }
        JobPhase::ReducePhase => {
            n_tasks = n_reduce;
            n_other = map_files.len();
        }
    }
    
    println!("Schedule: {} {:?} tasks ({} I/Os)", n_tasks, phase, n_other);

    let (tx, _) = broadcast::channel(10);

    let waitting_tx = tx.clone();

    let mut register_rx = register_rx;
    let handle = tokio::spawn(async move {
         loop {
            println!("waitting register");
            let address = register_rx.recv().await.unwrap();
            println!("got a new address: {}", address);
            match waitting_tx.send(address){
                Ok(_) => {},
                Err(_) => break,
            }
            break;
        }
    });
    println!("n task: {}", n_tasks);
    let mut handles = vec![];
    for i in 0..n_tasks {
        let phase = phase.clone();

        let phase_string;
        let file;
        match phase {
            JobPhase::MapPhase => {
                phase_string = String::from("map_phase");
                file = map_files[i].clone();
            }
            JobPhase::ReducePhase => {
                phase_string =String::from("reduce_phase");
                file = "".to_owned();
            }
        }

        let tx = tx.clone();
        let mut rx = tx.subscribe();

        let job_name = job_name.clone();

        let handle = tokio::spawn(async move {
            println!("waitting for free worker");
            let w = rx.recv().await.unwrap();
            println!("processing {}", w);

            println!("scheduling {} task to workers", file);
            let arg = TaskArg {
                job_name,
                file,
                phase: phase_string,
                task_number: i as i32,
                num_other_phase: n_other as i32,
            };
            worker_do_task(&w, arg)
                .await
                .expect("worker do task failed");
            tx.send(w).unwrap();
            println!("handle a work");
        });
        handles.push(handle);
    }
    
    join_all(handles).await;
    println!("schdule finished");
    // handle.await.expect("send worker task failed");
}
