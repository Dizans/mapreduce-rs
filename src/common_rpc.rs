#![allow(dead_code)]
use tonic::transport::Channel;
use tonic::Request;

use std::net::SocketAddr;
pub mod mr {
    tonic::include_proto!("mr");
}

use mr::master_client::MasterClient;

use mr::worker_client::WorkerClient;
use mr::{DoTaskArg, Empty};

pub use mr::DoTaskArg as TaskArg;


pub async fn worker_do_task(addr: &str, arg: DoTaskArg) -> Result<(), Box<dyn std::error::Error>> {
    let mut addr: String = addr.to_owned();
    if ! addr.starts_with(&"http"){
        addr = format!("http://{}", addr);
    }
    
    let mut client = WorkerClient::connect(addr).await?;
    client.do_task(arg).await?;
    Ok(())
}

pub async fn worker_shutdown(
    client: &mut WorkerClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.shutdown(Request::new(Empty::default())).await?;
    println!("RESPONSE = {:?}", response);
    Ok(())
}

pub fn validate_uri(addr: &mut String){
    if !addr.starts_with("http"){
        *addr = format!("http://{}", addr);
    }
}
