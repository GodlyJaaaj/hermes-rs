mod grpc;

use hermes_broker::router::{Router, RouterConfig};
use hermes_proto::broker_server::BrokerServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let (router, router_tx) = Router::new(RouterConfig::default(), 8192);
    tokio::spawn(router.run());

    let service = grpc::BrokerService::new(router_tx);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(hermes_proto::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    println!("hermes-broker listening on {addr}");
    Server::builder()
        .add_service(reflection)
        .add_service(BrokerServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
