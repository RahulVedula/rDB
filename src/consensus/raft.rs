use crate::core::{NodeId, Result, KvError, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use chrono::{DateTime, Utc};

/// Raft log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub timestamp: DateTime<Utc>,
}

/// Commands that can be applied to the state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    NoOp,
}

/// Raft node state
#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    pub id: NodeId,
    pub state: Arc<RwLock<NodeState>>,
    pub current_term: Arc<RwLock<u64>>,
    pub voted_for: Arc<RwLock<Option<NodeId>>>,
    pub log: Arc<RwLock<Vec<LogEntry>>>,
    pub commit_index: Arc<RwLock<u64>>,
    pub last_applied: Arc<RwLock<u64>>,
    
    // LEader  state
    pub next_index: Arc<RwLock<HashMap<NodeId, u64>>>,
    pub match_index: Arc<RwLock<HashMap<NodeId, u64>>>,
    
    // Election timeout
    pub last_heartbeat: Arc<RwLock<Instant>>,
    pub election_timeout: Duration,
    
    // Cluster config
    pub peers: Arc<RwLock<Vec<NodeId>>>,
}

// Raft node implementation (RC alg helper)
impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            state: Arc::new(RwLock::new(NodeState::Follower)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(Vec::new())),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            next_index: Arc::new(RwLock::new(HashMap::new())),
            match_index: Arc::new(RwLock::new(HashMap::new())),
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            election_timeout: Duration::from_millis(150 + (id.0 % 150)), // Randomized timeout
            peers: Arc::new(RwLock::new(peers)),
        }
    }

    /// Start the Raft node
    pub async fn start(&self) -> Result<()> {
        loop {
            let state = self.state.read().map_err(|e| KvError::InternalError(e.to_string()))?;
            
            match *state {
                NodeState::Follower => {
                    self.run_follower().await?;
                }
                NodeState::Candidate => {
                    self.run_candidate().await?;
                }
                NodeState::Leader => {
                    self.run_leader().await?;
                }
            }
            
            // Small delay to prevent busy waiting
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Run as a follower
    async fn run_follower(&self) -> Result<()> {
        let last_heartbeat = *self.last_heartbeat.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        if last_heartbeat.elapsed() > self.election_timeout {
            // Convert to candidate
            let mut state = self.state.write().map_err(|e| KvError::InternalError(e.to_string()))?;
            *state = NodeState::Candidate;
        }
        
        Ok(())
    }

    /// Run as a candidate
    async fn run_candidate(&self) -> Result<()> {
        {
            let mut term = self.current_term.write().map_err(|e| KvError::InternalError(e.to_string()))?;
            *term += 1;
        }
        
        // Vote for self
        {
            let mut voted_for = self.voted_for.write().map_err(|e| KvError::InternalError(e.to_string()))?;
            *voted_for = Some(self.id);
        }
        
        let votes = self.request_votes().await?;
        let peers = self.peers.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let majority = (peers.len() + 1) / 2 + 1; // +1 for self
        
        if votes >= majority {
            // Become leader
            let mut state = self.state.write().map_err(|e| KvError::InternalError(e.to_string()))?;
            *state = NodeState::Leader;
            self.initialize_leader_state().await?;
        } else {
            let mut state = self.state.write().map_err(|e| KvError::InternalError(e.to_string()))?;
            *state = NodeState::Follower;
        }
        
        Ok(())
    }

    /// Run as a leader
    async fn run_leader(&self) -> Result<()> {
        self.send_heartbeats().await?;
        
        sleep(Duration::from_millis(50)).await;
        
        Ok(())
    }

    /// Request votes from peers
    async fn request_votes(&self) -> Result<u64> {
        let mut votes = 1; // Vote for self
        let term = *self.current_term.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let log = self.log.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let last_log_index = log.len() as u64;
        let last_log_term = if last_log_index > 0 {
            log[last_log_index as usize - 1].term
        } else {
            0
        };
        
        let peers = self.peers.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        for peer in peers.iter() {e
            if self.simulate_vote_request(*peer, term, last_log_index, last_log_term).await? {
                votes += 1;
            }
        }
        
        Ok(votes)
    }

    /// Simulate a vote request 
    async fn simulate_vote_request(&self, peer: NodeId, term: u64, last_log_index: u64, last_log_term: u64) -> Result<bool> {
        sleep(Duration::from_millis(10)).await;
        
        Ok(peer.0 % 2 == 0)
    }

    /// Initialize leader state
    async fn initialize_leader_state(&self) -> Result<()> {
        let peers = self.peers.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let log = self.log.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let next_log_index = log.len() as u64;
        
        // Initialize next_index and match_index for each peer
        let mut next_index = self.next_index.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        let mut match_index = self.match_index.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        for peer in peers.iter() {
            next_index.insert(*peer, next_log_index);
            match_index.insert(*peer, 0);
        }
        
        Ok(())
    }

    /// Send heartbeats to all peers
    async fn send_heartbeats(&self) -> Result<()> {
        let term = *self.current_term.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let peers = self.peers.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        for peer in peers.iter() {
            // In a real implementation, this would send AppendEntries RPC
            self.send_append_entries(*peer, term).await?;
        }
        
        Ok(())
    }

    /// Send AppendEntries RPC to a peer
    async fn send_append_entries(&self, peer: NodeId, term: u64) -> Result<()> {
        sleep(Duration::from_millis(5)).await;
    
        Ok(())
    }

    /// Append a new log entry
    pub fn append_log_entry(&self, command: Command) -> Result<u64> {
        let term = *self.current_term.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let mut log = self.log.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        let index = log.len() as u64;
        let entry = LogEntry {
            term,
            index,
            command,
            timestamp: Utc::now(),
        };
        
        log.push(entry);
        Ok(index)
    }

    /// Apply committed entries to the state machine
    pub fn apply_committed_entries(&self) -> Result<Vec<Command>> {
        let mut applied_commands = Vec::new();
        let mut last_applied = self.last_applied.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        let commit_index = *self.commit_index.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let log = self.log.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        while *last_applied < commit_index {
            *last_applied += 1;
            if let Some(entry) = log.get(*last_applied as usize - 1) {
                applied_commands.push(entry.command.clone());
            }
        }
        
        Ok(applied_commands)
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> Result<bool> {
        let state = self.state.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        Ok(*state == NodeState::Leader)
    }

    /// Get the current term
    pub fn get_current_term(&self) -> Result<u64> {
        let term = self.current_term.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        Ok(*term)
    }

    /// Get the commit index
    pub fn get_commit_index(&self) -> Result<u64> {
        let index = self.commit_index.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        Ok(*index)
    }
}
