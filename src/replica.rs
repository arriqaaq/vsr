use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Rem;

use crate::message::*;
use crate::replies::Responses;
use crate::types::{CommitNumber, OpNumber, ReplicaID, ViewNumber};
use rand::Rng;

// The Status enum represents the two main states a replica can be in
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Status {
    Normal,     // Normal operation
    ViewChange, // View change in progress
}

pub trait StateMachine {
    fn apply(&self, op: Operation);
}

// ReplicaState holds the core state of a replica
#[derive(Clone)]
struct ReplicaState {
    view_number: ViewNumber,     // Current view number
    commit_number: CommitNumber, // Highest committed operation number
    op_number: OpNumber,         // Highest operation number
    log: Log,                    // Log of operations
    status: Status,              // Current status (Normal or ViewChange)
}

impl ReplicaState {
    fn append_log(&mut self, view_number: ViewNumber, op: Operation) -> OpNumber {
        self.log.append(view_number, op)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplicaConfig {
    replicas: Vec<ReplicaID>,
}

impl ReplicaConfig {
    fn replicas(&self) -> usize {
        self.replicas.len()
    }

    pub fn f(&self) -> usize {
        (self.replicas.len() - 1) / 2
    }

    pub fn quorum(&self) -> usize {
        self.f() + 1
    }

    pub fn primary_id(&self, view_number: ViewNumber) -> ReplicaID {
        self.replicas[view_number % self.replicas.len()]
    }
}

impl Rem<ViewNumber> for ReplicaConfig {
    type Output = usize;

    fn rem(self, rhs: ViewNumber) -> Self::Output {
        rhs % (self.replicas())
    }
}

pub struct Replica {
    id: ReplicaID,
    config: ReplicaConfig,
    state: ReplicaState,
    state_machine: Box<dyn StateMachine>,
    acks: BTreeMap<OpNumber, HashSet<ReplicaID>>,
    start_view_changes: HashSet<usize>,
    do_view_changes: HashMap<usize, DoViewChange>,
}

impl Replica {
    pub fn new(id: u64, config: ReplicaConfig, state_machine: Box<dyn StateMachine>) -> Self {
        Replica {
            id: id as usize,
            state: ReplicaState {
                view_number: ViewNumber::default(),
                op_number: OpNumber::default(),
                log: Log::default(),
                commit_number: CommitNumber::default(),
                status: Status::Normal,
            },
            state_machine,
            config,
            acks: BTreeMap::new(),
            start_view_changes: HashSet::new(),
            do_view_changes: HashMap::new(),
        }
    }

    // The client sends a hREQUEST op, c, si message to
    // the primary, where op is the operation (with its arguments)
    // the client wants to run, c is the client-id,
    // and s is the request-number assigned to the request.
    pub fn on_request(&mut self, op: Operation, responses: &mut Responses) {
        assert!(!self.is_backup());

        // The primary advances op-number, adds the request
        // to the end of the log, and updates the information
        // for this client in the client-table to contain the new
        // request number, s. Then it sends a hPREPARE v, m,
        // n, ki message to the other replicas, where v is the
        // current view-number, m is the message it received
        // from the client, n is the op-number it assigned to
        // the request, and k is the commit-number.
        let last_op_number = self.state.log.append(self.state.view_number, op.clone());
        responses.broadcast_prepare(Prepare {
            view_number: self.state.view_number,
            op_number: last_op_number,
            operation: op,
            commit_number: self.state.commit_number,
        });
    }

    /// The primary sends a `Prepare` message to replicate an operation to backup
    /// nodes. The nodes that receive a `Prepare` message will reply with `PrepareOk`
    /// when they have appended `op` to their logs. The message also contains the
    /// commit number of the primary, so that the backups can commit their logs up
    /// to that point.
    pub fn on_prepare(
        &mut self,
        view_number: ViewNumber,
        op_number: OpNumber,
        op: Operation,
        commit_number: CommitNumber,
        responses: &mut Responses,
    ) {
        // When a backup receives a PREPARE message, it
        // checks that the message comes from the primary of its view.
        assert!(self.is_backup());

        // Backups process PREPARE messages in order: a
        // backup won’t accept a prepare with op-number n
        // until it has entries for all earlier requests in its log.
        // When a backup i receives a PREPARE message, it
        // waits until it has entries in its log for all earlier requests
        // (doing state transfer if necessary to get the
        // missing information). Then it increments its op number,
        // adds the request to the end of its log, updates the client’s
        // information in the client-table, andsends a hPREPAREOK v, n, ii
        // message to the primary to indicate that this operation and all
        // earlier ones have prepared locally.
        if self.need_state_transfer(view_number, op_number, commit_number) {
            self.state_transfer(responses);
            return;
        }

        // If all checks succeed, the backup adds the
        // request to its log and sends a PREPAREOK message to the primary.
        self.state.append_log(self.state.view_number, op);
        self.commit_ops(commit_number, responses);

        // Acknowledge the prepare message to the primary.
        // TODO: implement network layer to send theese messages.
        // Currently just caching responses for easy testing
        responses.send_prepare_ok(
            self.config.primary_id(self.state.view_number),
            PrepareOk {
                view_number: self.state.view_number,
                op_number,
                replica_id: self.id,
            },
        );
    }

    // Handle PREPAREOK messages (primary)
    pub fn on_prepare_ok(
        &mut self,
        op_number: OpNumber,
        replica_id: ReplicaID,
        responses: &mut Responses,
    ) {
        assert!(!self.is_backup());
        if op_number <= self.state.commit_number {
            return;
        }

        // The primary collects PREPAREOK messages until
        // it has a quorum (including itself).
        let acks = self.acks.entry(op_number).or_default();
        acks.insert(replica_id);

        // When the primary has a quorum for operation number n,
        // it considers the operation committed.
        if acks.len() >= self.config.quorum() {
            // Retain only acknowledgements for operations beyond the current operation number
            self.acks
                .retain(|&ack_op_number, _| ack_op_number > op_number);

            // Commit the operations up to the current operation number
            self.commit_ops(op_number, responses);
        }
    }

    pub fn on_commit(&mut self, commit_number: CommitNumber, responses: &mut Responses) {
        if commit_number > self.state.op_number {
            self.state_transfer(responses);
            return;
        }

        self.commit_ops(commit_number, responses);
    }

    pub fn on_get_state(
        &mut self,
        op_number: OpNumber,
        replica_id: ReplicaID,
        responses: &mut Responses,
    ) {
        // A replica responds to a GETSTATE message only if its
        // status is normal and it is currently in view v. In this case
        // it sends a hNEWSTATE v, l, n, ki message, where v is its
        // view-number, l is its log after n, n is its op-number, and
        // k is its commit-number.
        assert_eq!(self.state.status, Status::Normal);

        responses.send_new_state(
            replica_id,
            NewState {
                view_number: self.state.view_number,
                log: self.state.log.after(op_number),
                commit_number: self.state.commit_number,
            },
        );
    }

    pub fn on_new_state(&mut self, new_state: NewState, responses: &mut Responses) {
        // When replica i receives the NEWSTATE message, it
        // appends the log in the message to its log and updates its
        // state using the other information in the message.

        self.state.view_number = new_state.view_number;
        self.state.log.merge(new_state.log);
        self.commit_ops(new_state.commit_number, responses);
    }

    pub fn on_start_view_change(
        &mut self,
        new_view_number: ViewNumber,
        replica_id: ReplicaID,
        responses: &mut Responses,
    ) {
        // A replica i that notices the need for a view change
        // advances its view-number, sets its status to viewchange, and sends a hTARTVIEWCHANGE
        // message to the all other replicas, where v identifies the new view.
        if self.state.view_number < new_view_number {
            self.state.view_number = new_view_number;
            self.state.status = Status::ViewChange;
            responses.broadcast_start_view_change(StartViewChange {
                new_view_number,
                replica_id: self.id,
            });
        }

        // Track view change for the replica
        self.start_view_changes.insert(replica_id);

        // When replica i receives STARTVIEWCHANGE messages for its view-number from f other replicas, it
        // sends a DOVIEWCHANGE message
        // to the node that will be the primary in the new view.
        if self.start_view_changes.len() >= self.config.f() {
            responses.send_do_view_change(
                self.config.primary_id(self.state.view_number),
                DoViewChange {
                    view_number: self.state.view_number,
                    log: self.state.log.clone(),
                    commit_number: self.state.commit_number,
                    replica_id: self.id,
                },
            )
        }
    }

    // Handle DOVIEWCHANGE messages (new primary)
    pub fn on_do_view_change(&mut self, view_change: DoViewChange, responses: &mut Responses) {
        // When the new primary receives f + 1
        // DOVIEWCHANGE messages from different
        // replicas (including itself), it sets its view-number
        // to that in the messages and selects as the new log
        // the one contained in the message with the largest
        // v; if several messages have the same v, it selects
        // the one among them with the largest n. It sets its
        // op-number to that of the topmost entry in the new
        // log, sets its commit-number to the largest such
        // number it received in the DOVIEWCHANGE messages,
        // changes its status to normal, and informs the
        // other replicas of the completion of the view change
        // by sending STARTVIEW messages to
        // the other replicas, where l is the new log, n is the
        // op-number, and k is the commit-number.

        // Collect DOVIEWCHANGE messages from f+1 replicas (including itself)
        self.do_view_changes
            .insert(view_change.replica_id, view_change);

        // Proceed only if the new primary has f+1 DOVIEWCHANGE messages and includes its own
        if self.has_quorum_for_view_change() {
            {
                // Determine the most recent commit number and corresponding log among the DOVIEWCHANGE messages.
                let latest_view = self.find_latest_view();

                // If a log is selected, update the replica's state and notify others.
                if let Some((do_view, latest_commit_number)) = latest_view {
                    self.update_local_state(do_view.log, do_view.view_number);
                    responses.broadcast_new_view(NewView {
                        view_number: self.state.view_number,
                        log: self.state.log.clone(),
                        commit_number: latest_commit_number,
                    });

                    // Commit any operations up to the latest commit number and process pending log entries.
                    self.commit_ops(latest_commit_number, responses);
                    self.broadcast_prepares(responses);
                }
            }
        }
    }

    pub fn on_new_view(&mut self, new_view: NewView, responses: &mut Responses) {
        // When other replicas receive the STARTVIEW message, they replace
        // their log with the one in the message, set their op-number to that
        // of the latest entry in the log, set their view-number to the view number
        // in the message, change their status to normal,
        // and update the information in their client-table. If
        // there are non-committed operations in the log, they
        // send a hPREPAREOK v, n, ii message to the primary;
        // here n is the op-number. Then they execute all operations known to be
        // committed that they haven’t executed previously, advance their commit-number,
        // and update the information in their client-table.

        if self.state.view_number > new_view.view_number {
            return;
        }

        self.state.view_number = new_view.view_number;
        self.state.log = new_view.log;

        self.state.status = Status::Normal;
        self.commit_ops(new_view.commit_number, responses);

        // Use the updated state to send a prepare_ok message
        let prepare_ok = PrepareOk {
            view_number: self.state.view_number,
            op_number: self.state.log.end_op_number,
            replica_id: self.id,
        };
        responses.send_prepare_ok(self.config.primary_id(self.state.view_number), prepare_ok);
    }
}

// Helper functions
impl Replica {
    fn commit_ops(&mut self, commit_number: CommitNumber, responses: &mut Responses) {
        while self.state.commit_number < commit_number {
            // Increment the commit number after processing
            self.state.commit_number += 1;

            // Apply to state machine
            let op = &self.state.log.entries[self.state.commit_number];
            self.state_machine.apply(op.clone())
        }

        responses.broadcast_commit(Commit {
            view_number: self.state.view_number,
            commit_number: self.state.commit_number,
        })
    }

    fn broadcast_prepares(&mut self, responses: &mut Responses) {
        let start_op_number = self.state.commit_number + 1;
        let view_number = self.state.view_number;

        for op_number in start_op_number..=self.state.log.end_op_number {
            let operation = &self.state.log.entries[op_number];
            responses.broadcast_prepare(Prepare {
                view_number: view_number,
                op_number,
                operation: operation.clone(),
                commit_number: (op_number - 1),
            });
        }
    }

    // Finds the log entry with the highest commit number among the DOVIEWCHANGE messages.
    fn find_latest_view(&self) -> Option<(DoViewChange, OpNumber)> {
        let latest_commit_number = self
            .do_view_changes
            .values()
            .map(|v| v.commit_number)
            .max()
            .unwrap_or(self.state.commit_number);

        let latest_view = self
            .do_view_changes
            .values()
            .max_by(|x, y| x.log.cmp(&y.log));

        if latest_view.is_some() {
            Some((latest_view.unwrap().clone(), latest_commit_number))
        } else {
            None
        }
    }

    // Updates the replica's state for the new view.
    fn update_local_state(&mut self, log: Log, view_number: ViewNumber) {
        self.state.log = log;
        self.state.view_number = view_number;
        self.state.status = Status::Normal;
    }

    pub fn is_primary(&self) -> bool {
        self.config.primary_id(self.state.view_number) == self.id
    }

    pub fn is_backup(&self) -> bool {
        !self.is_primary()
    }

    fn need_state_transfer(
        &self,
        view_number: ViewNumber,
        op_number: OpNumber,
        commit_number: CommitNumber,
    ) -> bool {
        let next = self.state.log.end_op_number + 1;
        self.state.status == Status::Normal
            && view_number > self.state.view_number
            && (next < op_number || next < commit_number)
    }

    // Checks if received DOVIEWCHANGE messages meet the quorum requirement.
    fn has_quorum_for_view_change(&self) -> bool {
        self.do_view_changes.contains_key(&self.id)
            && self.do_view_changes.len() >= self.config.quorum()
    }

    fn state_transfer(&mut self, responses: &mut Responses) {
        let total_replicas = self.config.replicas();
        // Ensure the selected replica is not the current one without using a loop.
        let selected_replica =
            (self.id + rand::thread_rng().gen_range(1..total_replicas)) % total_replicas;

        responses.send_get_state(
            selected_replica,
            GetState {
                view_number: self.state.view_number,
                op_number: self.state.op_number,
                replica_id: self.id,
            },
        );
    }
}
