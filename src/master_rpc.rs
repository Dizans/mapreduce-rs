use std::sync::Arc;

use tokio::sync::{mpsc, broadcast};
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_server::{Master, MasterServer};
use mr::worker_client::WorkerClient;
use mr::{Empty, WorkerAddr};

use crate::master::run;
use crate::schedule::schedule;
use crate::utils::*;

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
        workers.push(addr);
        println!("current workders: {:?}", workers);
        Ok(Response::new(Empty::default()))
    }

    async fn shutdown(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        println!("shuting down master server");
        let workders = self.workers.lock().await;
        for worker in workders.iter(){
            let mut client = WorkerClient::connect(format!("{}", worker)).await.unwrap();
            client.shutdown(Request::new(Empty::default())).await;
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
) -> Result<(), Box<dyn std::error::Error>> {
    let (tx, _rx) = broadcast::channel(3);

    let t = tx.clone();
    let handle = tokio::spawn(async move{
        run(
            job_name.clone(),
            &files,
            n_reduce,
            |phase| {
                schedule(
                    job_name.clone(), 
                    files.clone(), n_reduce, phase, t.subscribe());
            },
            seq_finish,
        )
    });
    let route_guide = MasterService::new(tx);

    let addr = "[::1]:10000".parse().unwrap();
    let svc = MasterServer::new(route_guide);
    Server::builder().add_service(svc).serve(addr).await?;
    handle.await;
    Ok(())
}

fn seq_finish() {}
