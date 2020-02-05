use crate::common_rpc::Master;
use futures::{
    future::{self, Ready},
    prelude::*,
};
use std::{
    io,
    net::{IpAddr, SocketAddr},
};
use tarpc::{
    context,
    server::{self, Channel, Handler},
};
use tokio::sync::mpsc;
use std::sync::{Mutex, Arc};
use tokio_serde::formats::Json;

#[derive(Clone)]
struct MasterServer{
    socket_addr: SocketAddr,
    workers: Arc<Mutex<Vec<String>>>,
}

impl Master for MasterServer {
    type RegisterFut = Ready<()>;

    fn register(self, _: context::Context, addr: String) -> Self::RegisterFut{
        let mut v = self.workers.lock().unwrap();
        v.push(addr);
        println!("{:?}", v);
        future::ready(())
    }
}

pub async fn start_server() -> io::Result<()> {
    let server_addr = (IpAddr::from([0, 0, 0, 0]), 8383);
    tarpc::serde_transport::tcp::listen(&server_addr, Json::default)
        .await?
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.as_ref().peer_addr().unwrap().ip())
        .map(|channel| {
            let server = MasterServer{ 
                socket_addr: channel.as_ref().as_ref().peer_addr().unwrap(),
                workers: Arc::new(Mutex::new(vec![])),
            };
            channel.respond_with(server.serve()).execute()
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}
