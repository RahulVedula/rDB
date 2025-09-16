use crate::core::{NodeId, Result, KvError};
use crate::storage::StorageEngine;
use crate::api::HttpApiServer;
use std::path::Path;
use std::sync::Arc;

/// Main server that coordinates all components
#[derive(Clone)]
pub struct KvServer {
    pub node_id: NodeId,
    pub storage_engine: Arc<StorageEngine>,
    pub http_server: Arc<HttpApiServer>,
}

impl KvServer {
    /// Create a new server instance
    pub async fn new(node_id: NodeId, http_port: u16, wal_path: &Path, persistent_path: Option<&Path>) -> Result<Self> {
        // Initialize storage engine with WAL and persistent storage
        let storage_engine = Arc::new(StorageEngine::new(wal_path, persistent_path)?);
        storage_engine.initialize()?;
        
        // Initialize HTTP API server
        let http_server = Arc::new(HttpApiServer::new(storage_engine.clone(), http_port));
        
        Ok(Self {
            node_id,
            storage_engine,
            http_server,
        })
    }

    /// Start the server
    pub async fn start(&self) -> Result<()> {
        println!("Starting KV server with node ID: {}", self.node_id.0);
        
        // Start HTTP API server
        let http_server = self.http_server.clone();
        tokio::spawn(async move {
            if let Err(e) = http_server.start().await {
                eprintln!("HTTP server error: {}", e);
            }
        });
        
        println!("KV server started successfully");
        println!("Node ID: {}", self.node_id.0);
        println!("HTTP API port: {}", self.http_server.port);
        
        Ok(())
    }

    /// Get the storage engine
    pub fn get_storage_engine(&self) -> Arc<StorageEngine> {
        self.storage_engine.clone()
    }
}