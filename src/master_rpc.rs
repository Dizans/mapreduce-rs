use tonic::{Request, Response, Status};
use tonic::transport::Server;
use tokio::sync::Mutex;

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_server::{Master, MasterServer};
use mr::{WorkerAddr, Empty};

struct MasterService{
    workers: Mutex<Vec<String>>,
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

pub async fn start_server() -> Result<(), Box<dyn std::error::Error>>{
    let addr = "[::1]:10000".parse().unwrap();
    
    println!("MasterServer listening on: {}", addr);

    let route_guide = MasterService{
        workers: Mutex::new(Vec::new()),
    };

    let svc = MasterServer::new(route_guide);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
