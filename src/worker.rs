use master::master_client::MasterClient;
use master::WorkerAddr;

pub mod master {
    tonic::include_proto!("master");
}

use worker::worker_server::{Worker,WorkerServer};
use worker::{DoTaskArg,DoTaskResponse};

use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod worker {
    tonic::include_proto!("worker");
}

#[derive(Debug)]
pub struct WorkerService{
}

#[tonic::async_trait]
impl Worker for WorkerService{
    async fn do_task(&self, request: Request<DoTaskArg>) -> Result<Response<DoTaskResponse>, Status>{
        Ok(Response::new(DoTaskResponse::default()))
    }
}

async fn register() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MasterClient::connect("http://[::1]:10000").await?;
    
    let response = client
        .register(Request::new(WorkerAddr{
             addr: "127.0.0.1".to_owned()
        }))
        .await?;
    println!("RESPONSE = {:?}", response);
    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let addr = "[::1]:9999".parse().unwrap();
    
    println!("Worker listening on: {}", addr);


    let route_guide = WorkerService{};

    let svc = WorkerServer::new(route_guide);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

