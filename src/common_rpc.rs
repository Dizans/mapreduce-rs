use tonic::transport::{Server, Channel};
use tonic::{Request, Response, Status};

pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_client::MasterClient;
use mr::worker_server::{Worker,WorkerServer};
use mr::{DoTaskArg,WorkerAddr,Empty};


async fn master_shutdown(client:&mut MasterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .shutdown(Request::new(Empty::default()))
        .await?;
    println!("RESPONSE = {:?}", response);
    Ok(())
}
