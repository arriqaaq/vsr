use crate::{message::*, types::ReplicaID};
use std::collections::VecDeque;

pub enum Request {
    Prepare(Prepare),
    PrepareOk(PrepareOk),
    Commit(Commit),
    GetState(GetState),
    NewState(NewState),
    StartViewChange(StartViewChange),
    DoViewChange(DoViewChange),
    NewView(NewView),
}

#[derive(Default)]
pub struct Responses {
    send: VecDeque<(ReplicaID, Request)>,
    broadcast: VecDeque<Request>,
}

impl Responses {
    // Queues a `Prepare` message for broadcast to all replicas.
    // This is part of the normal operation phase where a primary replica
    // proposes a new operation to be executed.
    pub fn broadcast_prepare(&mut self, message: Prepare) {
        self.broadcast.push_back(Request::Prepare(message));
    }

    // Queues a `PrepareOk` message to be sent to a specific replica.
    // This message is a response from a backup replica acknowledging
    // the receipt and logging of a `Prepare` message.
    pub fn send_prepare_ok(&mut self, replica_id: ReplicaID, message: PrepareOk) {
        self.send
            .push_back((replica_id, Request::PrepareOk(message)));
    }

    // Queues a `Commit` message for broadcast to all replicas.
    // This message indicates that an operation can be executed as it
    // has been logged and prepared by a quorum of replicas.
    pub fn broadcast_commit(&mut self, message: Commit) {
        self.broadcast.push_back(Request::Commit(message));
    }

    // Queues a `GetState` message to be sent to a specific replica.
    // This is used during recovery to request the current state from
    // another replica.
    pub fn send_get_state(&mut self, replica_id: ReplicaID, message: GetState) {
        self.send
            .push_back((replica_id, Request::GetState(message)));
    }

    // Queues a `NewState` message to be sent to a specific replica.
    // This is used during recovery to send the current state to another
    // replica that requested it.
    pub fn send_new_state(&mut self, replica_id: ReplicaID, message: NewState) {
        self.send
            .push_back((replica_id, Request::NewState(message)));
    }

    // Queues a `StartViewChange` message for broadcast to all replicas.
    // This message is used to initiate a view change process.
    pub fn broadcast_start_view_change(&mut self, message: StartViewChange) {
        self.broadcast.push_back(Request::StartViewChange(message));
    }

    // Queues a `DoViewChange` message to be sent to a specific replica.
    // This message is sent in response to a `StartViewChange` message,
    // indicating agreement to move to a new view.
    pub fn send_do_view_change(&mut self, replica_id: ReplicaID, message: DoViewChange) {
        self.send
            .push_back((replica_id, Request::DoViewChange(message)));
    }

    // Queues a `NewView` message for broadcast to all replicas.
    // This message is used to finalize the view change process, indicating
    // the start of operations in the new view.
    pub fn broadcast_new_view(&mut self, message: NewView) {
        self.broadcast.push_back(Request::NewView(message));
    }
}
