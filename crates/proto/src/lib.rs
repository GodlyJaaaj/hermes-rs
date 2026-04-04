pub mod hermes {
    pub mod broker {
        pub mod v1 {
            tonic::include_proto!("hermes.broker.v1");
        }
    }
}

pub use hermes::broker::v1::*;

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("broker_descriptor");
