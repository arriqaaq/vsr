use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use serde::{Serialize, Deserialize};

mod message;

use message::{Message, Operation};

#[derive(Clone, Serialize, Deserialize)]
struct ReplicaState {
    view_number: u64,
    op_number: u64,
    log: Vec<Operation>,
    commit_number: u64,
    client_table: HashMap<u64, ClientTableEntry>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ClientTableEntry {
    last_op_number: u64,
    result: Vec<u8>,
}

#[derive(Clone)]
struct ReplicaConfig {
    replicas: Vec<SocketAddr>,
    f: usize, // maximum number of faulty replicas
}

struct Replica {
    id: u64,
    state: Arc<Mutex<ReplicaState>>,
    config: ReplicaConfig,
    socket: Arc<UdpSocket>,
}

impl Replica {
    async fn new(id: u64, config: ReplicaConfig, addr: SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Replica {
            id,
            state: Arc::new(Mutex::new(ReplicaState {
                view_number: 0,
                op_number: 0,
                log: Vec::new(),
                commit_number: 0,
                client_table: HashMap::new(),
            })),
            config,
            socket: Arc::new(socket),
        })
    }

    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = [0u8; 65536];
        loop {
            let (size, src) = self.socket.recv_from(&mut buf).await?;
            let message: Message = serde_json::from_slice(&buf[..size])?;
            self.handle_message(message).await?;
        }
    }

    async fn handle_message(&self, message: Message) -> Result<(), Box<dyn std::error::Error>> {
        match message {
            Message::Request { client_id, request_number, operation } => {
                self.handle_request(client_id, request_number, operation).await?;
            }
            Message::Prepare { view_number, op_number, operation } => {
                self.handle_prepare(view_number, op_number, operation).await?;
            }
            Message::PrepareOk { view_number, op_number, replica_id } => {
                self.handle_prepare_ok(view_number, op_number, replica_id).await?;
            }
            Message::Commit { view_number, op_number } => {
                self.handle_commit(view_number, op_number).await?;
            }
            Message::StartViewChange { new_view_number } => {
                self.start_view_change(new_view_number).await?;
            }
            Message::DoViewChange { view_number, log, replica_id } => {
                self.handle_do_view_change(view_number, log, replica_id).await?;
            }
            Message::StartView { view_number, log } => {
                self.handle_start_view(view_number, log).await?;
            }
        }
        Ok(())
    }

    async fn handle_request(&self, client_id: u64, request_number: u64, operation: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().unwrap();
        
        if self.is_primary(state.view_number) {
            if let Some(entry) = state.client_table.get(&client_id) {
                if entry.last_op_number >= request_number {
                    return Ok(());
                }
            }

            state.op_number += 1;
            let op = Operation {
                client_id,
                request_number,
                operation,
            };
            state.log.push(op.clone());

            self.send_prepare(state.view_number, state.op_number, op).await?;
        } else {
            self.forward_to_primary(client_id, request_number, operation).await?;
        }
        Ok(())
    }

    async fn handle_prepare(&self, view_number: u64, op_number: u64, operation: Operation) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().unwrap();

        if view_number == state.view_number && op_number == state.op_number + 1 {
            state.op_number = op_number;
            state.log.push(operation);

            self.send_prepare_ok(view_number, op_number).await?;
        } else if view_number > state.view_number {
            self.start_view_change(view_number).await?;
        }
        Ok(())
    }

    async fn handle_prepare_ok(&self, view_number: u64, op_number: u64, replica_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let state = self.state.lock().unwrap();

        if view_number == state.view_number && self.is_primary(view_number) {
            self.send_commit(view_number, op_number).await?;
        }
        Ok(())
    }

    async fn handle_commit(&self, view_number: u64, op_number: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().unwrap();

        if view_number == state.view_number && op_number > state.commit_number {
            state.commit_number = op_number;

            while state.commit_number > 0 && state.commit_number <= state.log.len() as u64 {
                let op = &state.log[(state.commit_number - 1) as usize].clone();
                state.client_table.insert(op.client_id, ClientTableEntry {
                    last_op_number: op.request_number,
                    result: Vec::new(),
                });
                state.commit_number += 1;
            }
        }
        Ok(())
    }

    async fn start_view_change(&self, new_view_number: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().unwrap();

        if new_view_number > state.view_number {
            state.view_number = new_view_number;
            self.send_do_view_change(new_view_number, state.log.clone(), self.id).await?;
        }
        Ok(())
    }

    async fn handle_do_view_change(&self, view_number: u64, log: Vec<Operation>, replica_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().unwrap();

        if view_number == state.view_number && self.is_primary(view_number) {
            state.log = log;
            state.op_number = state.log.len() as u64;
            state.commit_number = state.op_number;

            self.send_start_view(view_number, state.log.clone()).await?;
        }
        Ok(())
    }

    async fn handle_start_view(&self, view_number: u64, log: Vec<Operation>) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.lock().unwrap();

        if view_number >= state.view_number {
            state.view_number = view_number;
            state.log = log;
            state.op_number = state.log.len() as u64;
            state.commit_number = state.op_number;
        }
        Ok(())
    }

    fn is_primary(&self, view_number: u64) -> bool {
        (view_number % self.config.replicas.len() as u64) == self.id
    }

    async fn send_prepare(&self, view_number: u64, op_number: u64, operation: Operation) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::Prepare { view_number, op_number, operation };
        self.broadcast(message).await
    }

    async fn send_prepare_ok(&self, view_number: u64, op_number: u64) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::PrepareOk { view_number, op_number, replica_id: self.id };
        self.send_to_primary(message).await
    }

    async fn send_commit(&self, view_number: u64, op_number: u64) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::Commit { view_number, op_number };
        self.broadcast(message).await
    }

    async fn send_do_view_change(&self, view_number: u64, log: Vec<Operation>, replica_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::DoViewChange { view_number, log, replica_id };
        self.send_to_primary(message).await
    }

    async fn send_start_view(&self, view_number: u64, log: Vec<Operation>) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::StartView { view_number, log };
        self.broadcast(message).await
    }

    async fn forward_to_primary(&self, client_id: u64, request_number: u64, operation: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::Request { client_id, request_number, operation };
        self.send_to_primary(message).await
    }

    async fn broadcast(&self, message: Message) -> Result<(), Box<dyn std::error::Error>> {
        let encoded = serde_json::to_vec(&message)?;
        for &addr in &self.config.replicas {
            self.socket.send_to(&encoded, addr).await?;
        }
        Ok(())
    }

    async fn send_to_primary(&self, message: Message) -> Result<(), Box<dyn std::error::Error>> {
        let encoded = serde_json::to_vec(&message)?;
        let state = self.state.lock().unwrap();
        let primary_id = state.view_number % self.config.replicas.len() as u64;
        let primary_addr = self.config.replicas[primary_id as usize];
        self.socket.send_to(&encoded, primary_addr).await?;
        Ok(())
    }
}


// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let config = ReplicaConfig {
//         replicas: vec![
//             "127.0.0.1:8000".parse()?,
//             "127.0.0.1:8001".parse()?,
//             "127.0.0.1:8002".parse()?,
//         ],
//         f: 1,
//     };

//     let replica = Replica::new(0, config.clone(), "127.0.0.1:8000".parse()?).await?;
//     replica.run().await?;

//     Ok(())
// }