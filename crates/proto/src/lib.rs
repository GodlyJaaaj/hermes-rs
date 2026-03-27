pub mod scylla {
    pub mod broker {
        pub mod v1 {
            tonic::include_proto!("scylla.broker.v1");
        }
    }
}

pub use scylla::broker::v1::*;

pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/scylla_broker_descriptor.bin"));
