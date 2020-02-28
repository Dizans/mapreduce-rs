use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::delay_for;

use crate::common_rpc::{worker_do_task, TaskArg};
use crate::utils::*;
use futures::future::join_all;

#[allow(dead_code)]
pub async fn schedule(
    job_name: String,
    map_files: Vec<String>,
    n_reduce: usize,
    phase: JobPhase,
    free_workers: Arc<Mutex<Vec<String>>>,
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

        let job_name = job_name.clone();

        let shared_workers = free_workers.clone();
        let handle = tokio::spawn(async move {
            
            let worker;
            loop{    
                let mut arr = shared_workers.lock().await;
                let len = arr.len();
                if len < 1{
                    drop(arr);
                    delay_for(Duration::from_secs(2)).await;
                    continue;
                }
                worker = arr.remove(len - 1);
                break;         
            }
            println!("processing {}", worker);

            println!("scheduling {} task to workers", file);
            let arg = TaskArg {
                job_name,
                file,
                phase: phase_string,
                task_number: i as i32,
                num_other_phase: n_other as i32,
            };
            worker_do_task(&worker, arg)
                .await
                .expect("worker do task failed");

            let mut arr = shared_workers.lock().await;
            arr.push(worker);
            println!("handle a work");
        });
        handles.push(handle);
    }
    
    join_all(handles).await;
    println!("schdule finished");
    // handle.await.expect("send worker task failed");
}
