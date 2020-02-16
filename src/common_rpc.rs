use tonic::transport::{Server, Channel};
use tonic::{Request, Response, Status};

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_client::MasterClient;

use mr::worker_client::WorkerClient;
use mr::worker_server::{Worker,WorkerServer};
use mr::{DoTaskArg,WorkerAddr,Empty};


pub async fn master_shutdown(client:&mut MasterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .shutdown(Request::new(Empty::default()))
        .await?;
    println!("RESPONSE = {:?}", response);
    Ok(())
}

pub async fn worker_do_task(addr: &str, arg: DoTaskArg) -> Result<(), Box<dyn std::error::Error>>{
    let mut client = WorkerClient::connect("http://[::1]:10000").await?;
    client.do_task(arg).await?;
    Ok(())
}
