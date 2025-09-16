use crate::core::{NodeId, NodeAddress, ClusterConfig, Result, KvError};
use crate::consensus::RaftNode;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::RwLock as AsyncRwLock;

/// Cluster manager for handling distributed operations
pub struct ClusterManager {
    pub node_id: NodeId,
    pub config: Arc<AsyncRwLock<ClusterConfig>>,
    pub raft_node: Arc<RaftNode>,
    pub peers: Arc<AsyncRwLock<HashMap<NodeId, NodeAddress>>>,
}

impl ClusterManager {
    pub fn new(node_id: NodeId, config: ClusterConfig) -> Self {
        let peers: Vec<NodeId> = config.nodes.keys().cloned().collect();
        let raft_node = Arc::new(RaftNode::new(node_id, peers));
        
        Self {
            node_id,
            config: Arc::new(AsyncRwLock::new(config)),
            raft_node,
            peers: Arc::new(AsyncRwLock::new(HashMap::new())),
        }
    }

    /// Start the cluster manager
    pub async fn start(&self) -> Result<()> {
        let raft_node = self.raft_node.clone();
        tokio::spawn(async move {
            if let Err(e) = raft_node.start().await {
                eprintln!("Raft node error: {}", e);
            }
        });
        
        Ok(())
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> Result<bool> {
        self.raft_node.is_leader()
    }

    /// Get the current leader
    pub async fn get_leader(&self) -> Result<Option<NodeId>> {
        let config = self.config.read().await;
        Ok(config.leader)
    }

    /// Set the leader
    pub async fn set_leader(&self, leader: NodeId) -> Result<()> {
        let mut config = self.config.write().await;
        config.set_leader(leader);
        Ok(())
    }

    /// Add a new node to the cluster
    pub async fn add_node(&self, node_id: NodeId, address: NodeAddress) -> Result<()> {
        let mut config = self.config.write().await;
        config.add_node(node_id, address);
        
        let mut peers = self.peers.write().await;
        peers.insert(node_id, address);
        
        Ok(())
    }

    // Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> Result<()> {
        let mut config = self.config.write().await;
        config.nodes.remove(&node_id);
        
        let mut peers = self.peers.write().await;
        peers.remove(&node_id);
        
        Ok(())
    }

    /// Get cluster configuration
    pub async fn get_config(&self) -> Result<ClusterConfig> {
        let config = self.config.read().await;
        Ok(config.clone())
    }

    /// Update cluster configuration
    pub async fn update_config(&self, new_config: ClusterConfig) -> Result<()> {
        let mut config = self.config.write().await;
        *config = new_config;
        Ok(())
    }

    /// Get all peer addresses
    pub async fn get_peer_addresses(&self) -> Result<HashMap<NodeId, NodeAddress>> {
        let peers = self.peers.read().await;
        Ok(peers.clone())
    }

    // Get the address of a specific node
    pub async fn get_node_address(&self, node_id: NodeId) -> Result<Option<NodeAddress>> {
        let peers = self.peers.read().await;
        Ok(peers.get(&node_id).cloned())
    }

    // Check if a node is alive (in a real implementation, this would ping the node)
    pub async fn is_node_alive(&self, node_id: NodeId) -> Result<bool> {
        Ok(true)
    }

    /// Get cluster health status
    pub async fn get_cluster_health(&self) -> Result<ClusterHealth> {
        let config = self.config.read().await;
        let mut healthy_nodes = 0;
        let mut total_nodes = config.nodes.len();
        
        for node_id in config.nodes.keys() {
            if self.is_node_alive(*node_id).await? {
                healthy_nodes += 1;
            }
        }
        
        let is_healthy = healthy_nodes > total_nodes / 2;
        let leader = config.leader;
        
        Ok(ClusterHealth {
            is_healthy,
            total_nodes,
            healthy_nodes,
            leader,
            current_term: self.raft_node.get_current_term()?,
            commit_index: self.raft_node.get_commit_index()?,
        })
    }

    /// Propose a configuration change
    pub async fn propose_config_change(&self, new_config: ClusterConfig) -> Result<()> {
        self.update_config(new_config).await?;
        Ok(())
    }

    /// Handle node failure
    pub async fn handle_node_failure(&self, failed_node: NodeId) -> Result<()> {
        self.remove_node(failed_node).await?;
        
        let mut config = self.config.write().await;
        config.nodes.remove(&failed_node);
        
        // If the failed node was the leader, trigger a new election
        if config.leader == Some(failed_node) {
            config.leader = None;
        }
        
        Ok(())
    }

    /// Get cluster statistics
    pub async fn get_cluster_stats(&self) -> Result<ClusterStats> {
        let config = self.config.read().await;
        let health = self.get_cluster_health().await?;
        
        Ok(ClusterStats {
            total_nodes: config.nodes.len(),
            healthy_nodes: health.healthy_nodes,
            leader: config.leader,
            current_term: health.current_term,
            commit_index: health.commit_index,
            is_healthy: health.is_healthy,
        })
    }
}

/// Cluster health information
#[derive(Debug, Clone)]
pub struct ClusterHealth {
    pub is_healthy: bool,
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub leader: Option<NodeId>,
    pub current_term: u64,
    pub commit_index: u64,
}

/// Cluster statistics
#[derive(Debug, Clone)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub leader: Option<NodeId>,
    pub current_term: u64,
    pub commit_index: u64,
    pub is_healthy: bool,
}
