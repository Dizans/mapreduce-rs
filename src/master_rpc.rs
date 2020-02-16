use tonic::{Request, Response, Status};
use tonic::transport::Server;
use tokio::sync::Mutex;

use std::thread;

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_server::{Master, MasterServer};
use mr::{WorkerAddr, Empty};

struct MasterService{
    workers: Mutex<Vec<String>>,
    job_name: String,
    files: Mutex<Vec<String>>,
    n_reduce: usize,
}

#[tonic::async_trait]
impl Master for MasterService {
    async fn register(&self, request: Request<WorkerAddr>) -> Result<Response<Empty>,Status>{
        println!("got a registr request from {:?}", request);
        let mut workers = self.workers.lock().await;
        let addr = request.into_inner().addr;
        workers.push(addr);
        println!("current workders: {:?}", workers);
        Ok(Response::new(Empty::default()))
    }

    async fn shutdown(&self, _: Request<Empty>) -> Result<Response<Empty>,Status>{
        println!("shuting down master server");
        std::process::exit(0x0111);
    }
}


impl MasterService{
    pub fn default() -> Self{
        MasterService{
            workers: Mutex::new(Vec::new()),
            job_name: String::default(),
            files: Mutex::new(Vec::new()),
            n_reduce: 0,
        }
    }
    
}

pub async fn start_server() -> Result<(), Box<dyn std::error::Error>>{
    let addr = "[::1]:10000".parse().unwrap();
    
    println!("MasterServer listening on: {}", addr);

    let route_guide = MasterService::default();

    let svc = MasterServer::new(route_guide);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
