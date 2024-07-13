use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize)]
pub enum Message {
    Request { client_id: u64, request_number: u64, operation: Vec<u8> },
    Prepare { view_number: u64, op_number: u64, operation: Operation },
    PrepareOk { view_number: u64, op_number: u64, replica_id: u64 },
    Commit { view_number: u64, op_number: u64 },
    StartViewChange { new_view_number: u64 },
    DoViewChange { view_number: u64, log: Vec<Operation>, replica_id: u64 },
    StartView { view_number: u64, log: Vec<Operation> },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Operation {
    pub client_id: u64,
    pub request_number: u64,
    pub operation: Vec<u8>,
}
