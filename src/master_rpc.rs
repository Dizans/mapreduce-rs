use std::time::Duration;
use std::sync::Arc;

use tokio::time::delay_for;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use log::info;
use futures::future::join_all;

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_server::{Master, MasterServer};
use mr::worker_client::WorkerClient;
use mr::master_client::MasterClient;
use mr::{Empty, WorkerAddr};
use crate::schedule::schedule;
use crate::utils::*;
use crate::master_splitmerge::merge;
use crate::common_rpc::validate_uri;


struct MasterService {
    workers: Arc<Mutex<Vec<String>>>,
}

#[tonic::async_trait]
impl Master for MasterService {
    async fn register(&self, request: Request<WorkerAddr>) -> Result<Response<Empty>, Status> {
        info!("got a registr request from {:?}", request);
        let mut workers = self.workers.lock().await;
        let addr = request.into_inner().addr;
        workers.push(addr.clone());
        info!("current workders: {:?}", workers);
        Ok(Response::new(Empty::default()))
    }

    async fn shutdown(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        info!("shuting down master server");
        let workders = self.workers.lock().await;
        for worker in workders.iter(){
            let mut worker_addr = format!("{}", worker);
            validate_uri(&mut worker_addr);
            let mut client = WorkerClient::connect(worker_addr).await.unwrap();
            match client.shutdown(Request::new(Empty::default())).await{
                Ok(_) => {},
                Err(_) => {},
            };
        }
        std::process::exit(1);
    }
}

impl MasterService {
    pub fn new(workers: Arc<Mutex<Vec<String>>>) -> Self {
        MasterService {
            workers,
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
    let free_workers = Arc::new(Mutex::new(vec![]));
    let workers = free_workers.clone();

    let addr_for_finish = master_addr.clone();
    let handle = tokio::spawn(async move{
        delay_for(Duration::from_secs(5)).await;
        schedule(
                job_name.clone(), 
                files.clone(), 
                n_reduce, 
                JobPhase::MapPhase, 
                free_workers.clone()).await;

        schedule(
                job_name.clone(), 
                files.clone(), 
                n_reduce, 
                JobPhase::ReducePhase, 
                free_workers.clone()).await;
        merge(&job_name, n_reduce);
        finish(addr_for_finish).await;  
    });
    let route_guide = MasterService::new(workers);

    let addr = master_addr.parse().unwrap();
    let svc = MasterServer::new(route_guide);
    let server_handle = tokio::spawn(async move {
        Server::builder().add_service(svc).serve(addr).await
            .expect("start master server failed");
    });
    
    join_all(vec![handle, server_handle]).await;
    Ok(())
}


async fn finish(addr: String){
    let mut addr = addr;
    validate_uri(&mut addr);
    info!(" finish {}", addr);
    
    let mut client = MasterClient::connect(addr).await
                    .expect("connect master server failed");
    
    let _ = client.shutdown(Request::new(Empty::default())).await
                .expect("shutdown server failed");
    info!("finished")
}
