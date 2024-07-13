use std::cmp::Ordering;
use std::collections::VecDeque;

use crate::types::{ClientID, OpNumber, ReplicaID, RequestNumber, ViewNumber};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Prepare {
    pub view_number: ViewNumber,
    pub op_number: OpNumber,
    pub operation: Operation,
    pub commit_number: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareOk {
    pub view_number: ViewNumber,
    pub op_number: OpNumber,
    pub replica_id: ReplicaID,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Commit {
    pub view_number: ViewNumber,
    pub commit_number: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartViewChange {
    pub new_view_number: ViewNumber,
    pub replica_id: ReplicaID,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DoViewChange {
    pub view_number: ViewNumber,
    pub log: Log,
    pub replica_id: ReplicaID,
    pub commit_number: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewView {
    pub view_number: ViewNumber,
    pub log: Log,
    pub commit_number: OpNumber,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetState {
    pub view_number: ViewNumber,
    pub op_number: OpNumber,
    pub replica_id: ReplicaID,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewState {
    pub view_number: ViewNumber,
    pub log: Log,
    pub commit_number: OpNumber,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation {
    pub client_id: ClientID,
    pub request_number: RequestNumber,
    pub operation: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct Log {
    view_number: ViewNumber,
    pub start_op_number: OpNumber,
    pub end_op_number: OpNumber,
    pub entries: VecDeque<Operation>,
}

impl Log {
    pub fn after(&self, after: OpNumber) -> Self {
        Self {
            view_number: self.view_number,
            start_op_number: after + 1,
            end_op_number: self.end_op_number,
            entries: self.entries.iter().skip(after).cloned().collect(),
        }
    }

    pub fn contains(&self, op_number: &OpNumber) -> bool {
        *op_number >= self.start_op_number && *op_number <= self.end_op_number
    }

    pub fn append(&mut self, view_number: ViewNumber, op: Operation) -> OpNumber {
        self.view_number = view_number;
        self.end_op_number += 1;
        if self.entries.is_empty() {
            self.start_op_number = self.end_op_number;
        }

        self.entries.push_back(op);
        self.end_op_number
    }

    pub fn merge(&mut self, other: Self) {
        self.view_number = other.view_number;
        self.end_op_number = other.end_op_number;
        self.entries.extend(other.entries);
    }
}

impl Ord for Log {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.view_number, self.end_op_number).cmp(&(other.view_number, other.end_op_number))
    }
}

impl PartialOrd for Log {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
