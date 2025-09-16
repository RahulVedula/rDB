use crate::core::{Key, Value, TransactionId, Result, KvError, ConsistencyLevel, WriteConsistency};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tokio::time::{timeout, Duration};

/// Client for interacting with the distributed key-value store
pub struct KvClient {
    pub servers: Vec<String>,
    pub current_server: Arc<RwLock<usize>>,
    pub timeout: Duration,
}

impl KvClient {
    /// Create a new client
    pub fn new(servers: Vec<String>) -> Self {
        Self {
            servers,
            current_server: Arc::new(RwLock::new(0)),
            timeout: Duration::from_secs(30),
        }
    }

    /// Create a new client with assigned timeout
    pub fn with_timeout(servers: Vec<String>, timeout: Duration) -> Self {
        Self {
            servers,
            current_server: Arc::new(RwLock::new(0)),
            timeout,
        }
    }

    /// Connect to the cluster
    pub async fn connect(&self) -> Result<()> {
        // Try to connect to each server until one succeeds
        for (i, server) in self.servers.iter().enumerate() {
            if let Ok(_) = self.connect_to_server(server).await {
                let mut current = self.current_server.write().map_err(|e| KvError::InternalError(e.to_string()))?;
                *current = i;
                return Ok(());
            }
        }
        
        Err(KvError::ClusterNotAvailable)
    }

    /// Connect to a specific server
    async fn connect_to_server(&self, server: &str) -> Result<()> {
        let url = format!("http://{}", server);
        let client = reqwest::Client::new();
        
        let response = timeout(self.timeout, client.get(&format!("{}/health", url)).send()).await
            .map_err(|_| KvError::TimeoutError("Connection timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;
        
        if response.status().is_success() {
            Ok(())
        } else {
            Err(KvError::NetworkError("Server not available".to_string()))
        }
    }

    /// Begin a new transaction
    pub async fn begin_transaction(&self) -> Result<TransactionId> {
        let server = self.get_current_server().await?;
        let url = format!("http://{}/transaction/begin", server);
        let client = reqwest::Client::new();
        
        let response = timeout(
            self.timeout,
            client.post(&url).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: BeginTransactionResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(result.transaction_id)
            } else {
                Err(KvError::TransactionAborted("Failed to begin transaction".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Get a value
    pub async fn get(&self, key: &str, transaction_id: TransactionId, consistency: Option<ConsistencyLevel>) -> Result<Option<String>> {
        let server = self.get_current_server().await?;
        let mut url = format!("http://{}/key/{}?transaction_id={}", server, key, transaction_id.0);
        
        if let Some(consistency) = consistency {
            url.push_str(&format!("&consistency={:?}", consistency));
        }

        let client = reqwest::Client::new();
        let response = timeout(
            self.timeout,
            client.get(&url).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: GetResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(result.value.map(|v| String::from_utf8_lossy(&v.data).to_string()))
            } else {
                Err(KvError::KeyNotFound(key.to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Put a value
    pub async fn put(&self, key: &str, value: &str, transaction_id: TransactionId, consistency: Option<WriteConsistency>) -> Result<()> {
        let server = self.get_current_server().await?;
        let request = PutRequest {
            value: value.to_string(),
            transaction_id,
            consistency,
        };

        let client = reqwest::Client::new();
        let response = timeout(
            self.timeout,
            client.put(&format!("http://{}/key/{}", server, key))
                .json(&request)
                .send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: PutResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(())
            } else {
                Err(KvError::TransactionAborted("Failed to put value".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Delete a key
    pub async fn delete(&self, key: &str, transaction_id: TransactionId, consistency: Option<WriteConsistency>) -> Result<()> {
        let server = self.get_current_server().await?;
        let mut url = format!("http://{}/key/{}?transaction_id={}", server, key, transaction_id.0);
        
        if let Some(consistency) = consistency {
            url.push_str(&format!("&consistency={:?}", consistency));
        }

        let client = reqwest::Client::new();
        let response = timeout(
            self.timeout,
            client.delete(&url).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: DeleteResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(())
            } else {
                Err(KvError::TransactionAborted("Failed to delete key".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Commit a transaction
    pub async fn commit(&self, transaction_id: TransactionId) -> Result<()> {
        let server = self.get_current_server().await?;
        let request = CommitTransactionRequest { transaction_id };

        let client = reqwest::Client::new();
        let response = timeout(
            self.timeout,
            client.post(&format!("http://{}/transaction/commit", server))
                .json(&request)
                .send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: TransactionResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(())
            } else {
                Err(KvError::TransactionAborted("Failed to commit transaction".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Get cluster health
    pub async fn get_cluster_health(&self) -> Result<ClusterHealth> {
        let server = self.get_current_server().await?;
        let client = reqwest::Client::new();
        
        let response = timeout(
            self.timeout,
            client.get(&format!("http://{}/health", server)).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let health: ClusterHealth = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            Ok(health)
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Get storage statistics
    pub async fn get_stats(&self) -> Result<StorageStats> {
        let server = self.get_current_server().await?;
        let client = reqwest::Client::new();
        
        let response = timeout(
            self.timeout,
            client.get(&format!("http://{}/stats", server)).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let stats: StorageStats = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            Ok(stats)
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Get the current server
    async fn get_current_server(&self) -> Result<String> {
        let current = self.current_server.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        if let Some(server) = self.servers.get(*current) {
            Ok(server.clone())
        } else {
            Err(KvError::ClusterNotAvailable)
        }
    }

    /// Check if the client is connected
    pub async fn is_connected(&self) -> bool {
        self.connect().await.is_ok()
    }
}


#[derive(Debug, Serialize)]
struct PutRequest {
    value: String,
    transaction_id: TransactionId,
    consistency: Option<WriteConsistency>,
}

#[derive(Debug, Deserialize)]
struct BeginTransactionResponse {
    transaction_id: TransactionId,
    success: bool,
}

#[derive(Debug, Deserialize)]
struct GetResponse {
    value: Option<Value>,
    success: bool,
}

#[derive(Debug, Deserialize)]
struct PutResponse {
    success: bool,
}

#[derive(Debug, Deserialize)]
struct DeleteResponse {
    success: bool,
}

#[derive(Debug, Serialize)]
struct CommitTransactionRequest {
    transaction_id: TransactionId,
}

#[derive(Debug, Deserialize)]
struct TransactionResponse {
    success: bool,
}

/// Cluster health information
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterHealth {
    pub status: String,
    pub total_keys: usize,
}

/// Storage statistics
#[derive(Debug, Clone, Deserialize)]
pub struct StorageStats {
    pub total_keys: usize,
    pub total_versions: usize,
    pub committed_versions: usize,
    pub uncommitted_versions: usize,
    pub active_transactions: usize,
}