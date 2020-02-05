
#[tarpc::service]
pub trait Master {
    async fn register(addr: String);
}
