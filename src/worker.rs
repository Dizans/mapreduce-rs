use tonic::transport::{Server, Channel};
use tonic::{Request, Response, Status};

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_client::MasterClient;
use mr::worker_server::{Worker,WorkerServer};
use mr::{DoTaskArg,WorkerAddr,Empty};


#[derive(Debug)]
pub struct WorkerService{
}

#[tonic::async_trait]
impl Worker for WorkerService{
    async fn do_task(&self, request: Request<DoTaskArg>) -> Result<Response<Empty>, Status>{
        // TODO
        Ok(Response::new(Empty::default()))
    }
}

async fn register(client: &mut MasterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .register(Request::new(WorkerAddr{
             addr: "127.0.0.1".to_owned()
        }))
        .await?;
    println!("RESPONSE = {:?}", response);
    Ok(())
}

async fn shutdown(client:&mut MasterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .shutdown(Request::new(Empty::default()))
        .await?;
    println!("RESPONSE = {:?}", response);
    Ok(())
}

async fn run_worker(addr: &str) -> Result<(), Box<dyn std::error::Error>>{
    // let addr = "[::1]:9999".parse().unwrap();
    let addr = addr.parse().expect("Invalid worker addr");
    println!("Worker listening on: {}", addr);

    let route_guide = WorkerService{};

    let svc = WorkerServer::new(route_guide);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

