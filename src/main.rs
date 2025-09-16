use distributed_kv_store::api::KvServer;
use distributed_kv_store::core::NodeId;
use clap::{Parser, Subcommand};
use tokio;

#[derive(Parser)]
#[command(name = "distributed-kv-store")]
#[command(about = "A distributed transactional key-value store")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a server node
    Server {
        /// Node ID
        #[arg(short, long)]
        node_id: u64,
        /// HTTP API port
        #[arg(long, default_value = "9000")]
        http_port: u16,
    },
    Client {
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        servers: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            node_id,
            http_port,
        } => {
            println!("Starting server with node ID: {}", node_id);
            
            let node_id = NodeId::new(node_id);
            let wal_path_str = format!("./wal_node_{}", node_id.0);
            let data_path_str = format!("./data_node_{}", node_id.0);
            let wal_path = std::path::Path::new(&wal_path_str);
            let persistent_path = Some(std::path::Path::new(&data_path_str));
            
            let server = KvServer::new(node_id, http_port, wal_path, persistent_path).await?;
            
            // Handle Ctrl+C gracefully
            let server_clone = server.clone();
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
                println!("\nShutting down server...");
                std::process::exit(0);
            });
            
            server.start().await?;
            
            // Keep the server running
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
        Commands::Client { servers } => {
            println!("Starting client");
            
            let server_list: Vec<String> = servers.split(',').map(|s| s.trim().to_string()).collect();
            let client = distributed_kv_store::client::KvClient::new(server_list);
            
            // Connect to the cluster
            client.connect().await?;
            println!("Connected to cluster");
            
            // Example usage
            let transaction_id = client.begin_transaction().await?;
            println!("Started transaction: {}", transaction_id.0);
            
            // Put some values
            client.put("key1", "value1", transaction_id, None).await?;
            client.put("key2", "value2", transaction_id, None).await?;
            println!("Put values");
            
            // Get values
            let value1 = client.get("key1", transaction_id, None).await?;
            let value2 = client.get("key2", transaction_id, None).await?;
            println!("Got values: {:?}, {:?}", value1, value2);
            
            // Commit transaction
            client.commit(transaction_id).await?;
            println!("Committed transaction");
            
            // Get cluster health
            let health = client.get_cluster_health().await?;
            println!("Cluster health: {:?}", health);
            
            // Get storage stats
            let stats = client.get_stats().await?;
            println!("Storage stats: {:?}", stats);
        }
    }

    Ok(())
}