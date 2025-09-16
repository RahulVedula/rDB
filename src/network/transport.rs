use crate::core::{NodeId, Result, KvError};
use crate::network::{Message, MessageWrapper, MessageSerializer};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use std::net::SocketAddr;

/// Network transport layer for inter-node communication
pub struct NetworkTransport {
    pub node_id: NodeId,
    pub address: String,
    pub port: u16,
    pub connections: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
    pub message_sender: mpsc::UnboundedSender<MessageWrapper>,
    pub message_receiver: Arc<RwLock<mpsc::UnboundedReceiver<MessageWrapper>>>,
}

impl NetworkTransport {
    pub fn new(node_id: NodeId, address: String, port: u16) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        Self {
            node_id,
            address,
            port,
            connections: Arc::new(RwLock::new(HashMap::new())),
            message_sender: sender,
            message_receiver: Arc::new(RwLock::new(receiver)),
        }
    }

    /// Start the network transport
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("{}:{}", self.address, self.port)).await
            .map_err(|e| KvError::NetworkError(e.to_string()))?;
        
        println!("Network transport listening on {}:{}", self.address, self.port);
        
        // Start accepting connections
        let connections = self.connections.clone();
        let message_sender = self.message_sender.clone();
        let node_id = self.node_id;
        
        tokio::spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                println!("New connection from {}", addr);
                
                let connections = connections.clone();
                let message_sender = message_sender.clone();
                let node_id = node_id;
                
                tokio::spawn(async move {
                    if let Err(e) = Self::handle_connection(stream, addr, connections, message_sender, node_id).await {
                        eprintln!("Connection error: {}", e);
                    }
                });
            }
        });
        
        Ok(())
    }

    /// Handle a new connection
    async fn handle_connection(
        mut stream: TcpStream,
        addr: SocketAddr,
        connections: Arc<RwLock<HashMap<NodeId, TcpStream>>>,
        message_sender: mpsc::UnboundedSender<MessageWrapper>,
        node_id: NodeId,
    ) -> Result<()> {
        let mut buffer = [0u8; 4096];
        
        loop {
            // Read message length
            let mut len_bytes = [0u8; 4];
            if stream.read_exact(&mut len_bytes).await.is_err() {
                break;
            }
            
            let len = u32::from_le_bytes(len_bytes) as usize;
            if len > buffer.len() {
                return Err(KvError::NetworkError("Message too large".to_string()));
            }
            
            // Read message data
            if stream.read_exact(&mut buffer[..len]).await.is_err() {
                break;
            }
            
            // Deserialize message
            let message_wrapper = MessageSerializer::deserialize(&buffer[..len])?;
            
            // Send to message handler
            if message_sender.send(message_wrapper).is_err() {
                break;
            }
        }
        
        Ok(())
    }

    /// Send a message to a specific node
    pub async fn send_message(&self, to: NodeId, message: Message) -> Result<()> {
        let message_wrapper = MessageWrapper::new(message, self.node_id, to);
        let serialized = MessageSerializer::serialize(&message_wrapper)?;
        
        // Get or create connection to the target node
        let mut connections = self.connections.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        if let Some(stream) = connections.get_mut(&to) {
            // Send message length
            let len = serialized.len() as u32;
            stream.write_all(&len.to_le_bytes()).await
                .map_err(|e| KvError::NetworkError(e.to_string()))?;
            
            // Send message data
            stream.write_all(&serialized).await
                .map_err(|e| KvError::NetworkError(e.to_string()))?;
            
            stream.flush().await
                .map_err(|e| KvError::NetworkError(e.to_string()))?;
        } else {
            // Create new connection
            let address = self.get_node_address(to).await?;
            let mut stream = TcpStream::connect(&address).await
                .map_err(|e| KvError::NetworkError(e.to_string()))?;
            
            // Send message length
            let len = serialized.len() as u32;
            stream.write_all(&len.to_le_bytes()).await
                .map_err(|e| KvError::NetworkError(e.to_string()))?;
            
            // Send message data
            stream.write_all(&serialized).await
                .map_err(|e| KvError::NetworkError(e.to_string()))?;
            
            stream.flush().await
                .map_err(|e| KvError::NetworkError(e.to_string()))?;
            
            connections.insert(to, stream);
        }
        
        Ok(())
    }

    /// Get the address of a node (in a real implementation, this would come from cluster config)
    async fn get_node_address(&self, node_id: NodeId) -> Result<String> {
        // For simplicity, we'll use a simple addressing scheme
        // In a real implementation, this would query the cluster configuration
        Ok(format!("127.0.0.1:{}", 8000 + node_id.0))
    }

    /// Broadcast a message to all connected nodes
    pub async fn broadcast_message(&self, message: Message) -> Result<()> {
        let connections = self.connections.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        for &node_id in connections.keys() {
            if node_id != self.node_id {
                if let Err(e) = self.send_message(node_id, message.clone()).await {
                    eprintln!("Failed to send message to node {}: {}", node_id.0, e);
                }
            }
        }
        
        Ok(())
    }

    /// Receive a message
    pub async fn receive_message(&self) -> Result<Option<MessageWrapper>> {
        let mut receiver = self.message_receiver.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        Ok(receiver.recv().await)
    }

    /// Check if a node is connected
    pub fn is_connected(&self, node_id: NodeId) -> bool {
        let connections = self.connections.read().map_err(|_| KvError::InternalError("Lock error".to_string())).unwrap_or_default();
        connections.contains_key(&node_id)
    }

    /// Get list of connected nodes
    pub fn get_connected_nodes(&self) -> Vec<NodeId> {
        let connections = self.connections.read().map_err(|_| KvError::InternalError("Lock error".to_string())).unwrap_or_default();
        connections.keys().cloned().collect()
    }

    /// Disconnect from a node
    pub async fn disconnect(&self, node_id: NodeId) -> Result<()> {
        let mut connections = self.connections.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        connections.remove(&node_id);
        Ok(())
    }

    /// Close all connections
    pub async fn close_all(&self) -> Result<()> {
        let mut connections = self.connections.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        connections.clear();
        Ok(())
    }
}
