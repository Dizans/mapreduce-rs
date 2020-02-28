use std::time::Duration;

use tokio::time::delay_for;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_server::{Master, MasterServer};
use mr::master_client::MasterClient;
use mr::worker_client::WorkerClient;
use mr::{Empty, WorkerAddr};
use crate::schedule::schedule;
use crate::utils::*;
use crate::master_splitmerge::merge;


struct MasterService {
    workers: Mutex<Vec<String>>,
    sender: broadcast::Sender<String>,
}

#[tonic::async_trait]
impl Master for MasterService {
    async fn register(&self, request: Request<WorkerAddr>) -> Result<Response<Empty>, Status> {
        println!("got a registr request from {:?}", request);
        let mut workers = self.workers.lock().await;
        let addr = request.into_inner().addr;
        workers.push(addr.clone());
        println!("current workders: {:?}", workers);
        self.sender.send(addr).unwrap();
        Ok(Response::new(Empty::default()))
    }

    async fn shutdown(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        println!("shuting down master server");
        let workders = self.workers.lock().await;
        for worker in workders.iter(){
            let mut client = WorkerClient::connect(format!("{}", worker)).await.unwrap();
            match client.shutdown(Request::new(Empty::default())).await{
                Ok(_) => {},
                Err(_) => {},
            };
        }
        std::process::exit(0x0111);
    }
}

impl MasterService {
    pub fn new(sender: broadcast::Sender<String>) -> Self {
        MasterService {
            workers: Mutex::new(Vec::new()),
            sender,
        }
    }
}

#[allow(non_snake_case)]
pub async fn distribucted(
    job_name: String,
    files: Vec<String>,
    n_reduce: usize,
    master_addr: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let (tx, _rx) = broadcast::channel(3);

    let t = tx.clone();

    let addr_for_finish = master_addr.clone();
    let handle = tokio::spawn(async move{
        schedule(
                job_name.clone(), 
                files.clone(), 
                n_reduce, 
                JobPhase::MapPhase, 
                t.subscribe()).await;

        schedule(
                job_name.clone(), 
                files.clone(), 
                n_reduce, 
                JobPhase::ReducePhase, 
                t.subscribe()).await;
        finish(addr_for_finish).await;
        merge(&job_name, n_reduce);
    });
    let route_guide = MasterService::new(tx);

    let addr = master_addr.parse().unwrap();
    let svc = MasterServer::new(route_guide);
    tokio::spawn(async move {
        Server::builder().add_service(svc).serve(addr).await
            .expect("start master server failed");
    });
    
    delay_for(Duration::from_secs(10)).await;
    handle.await.expect("run job failed");
    Ok(())
}


async fn finish(addr: String){
    // let mut client = MasterClient::connect(addr).await
    //                 .expect("connect master server failed");
    
    // let _ = client.shutdown(Request::new(Empty::default())).await
    //             .expect("shutdown server failed");
    println!("finished")
}
