use distributed_kv_store::client::KvClient;
use distributed_kv_store::core::{ConsistencyLevel, WriteConsistency};
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create a client
    let servers = vec!["127.0.0.1:9000".to_string()];
    let client = KvClient::new(servers);

    // Connect to the cluster
    client.connect().await?;
    println!("Connected to cluster");

    // Benchmark parameters
    let num_operations = 1000;
    let batch_size = 100;

    // Benchmark 1: Write performance
    println!("\n=== Write Performance Benchmark ===");
    let start = Instant::now();
    
    for batch in 0..(num_operations / batch_size) {
        let transaction_id = client.begin_transaction().await?;
        
        for i in 0..batch_size {
            let key = format!("benchmark:write:{}:{}", batch, i);
            let value = format!("value_{}_{}", batch, i);
            client.put(&key, &value, transaction_id, Some(WriteConsistency::Any)).await?;
        }
        
        client.commit(transaction_id).await?;
    }
    
    let write_duration = start.elapsed();
    let write_ops_per_sec = num_operations as f64 / write_duration.as_secs_f64();
    println!("Write benchmark completed:");
    println!("  Operations: {}", num_operations);
    println!("  Duration: {:?}", write_duration);
    println!("  Operations/sec: {:.2}", write_ops_per_sec);

    // Benchmark 2: Read performance
    println!("\n=== Read Performance Benchmark ===");
    let start = Instant::now();
    
    for batch in 0..(num_operations / batch_size) {
        let transaction_id = client.begin_transaction().await?;
        
        for i in 0..batch_size {
            let key = format!("benchmark:write:{}:{}", batch, i);
            let _value = client.get(&key, transaction_id, Some(ConsistencyLevel::Eventual)).await?;
        }
        
        client.commit(transaction_id).await?;
    }
    
    let read_duration = start.elapsed();
    let read_ops_per_sec = num_operations as f64 / read_duration.as_secs_f64();
    println!("Read benchmark completed:");
    println!("  Operations: {}", num_operations);
    println!("  Duration: {:?}", read_duration);
    println!("  Operations/sec: {:.2}", read_ops_per_sec);

    // Benchmark 3: Mixed workload
    println!("\n=== Mixed Workload Benchmark ===");
    let start = Instant::now();
    
    for i in 0..num_operations {
        let transaction_id = client.begin_transaction().await?;
        
        // 70% reads, 30% writes
        if i % 10 < 7 {
            // Read operation
            let key = format!("benchmark:write:{}:{}", i % (num_operations / batch_size), i % batch_size);
            let _value = client.get(&key, transaction_id, Some(ConsistencyLevel::Eventual)).await?;
        } else {
            // Write operation
            let key = format!("benchmark:mixed:{}", i);
            let value = format!("mixed_value_{}", i);
            client.put(&key, &value, transaction_id, Some(WriteConsistency::Any)).await?;
        }
        
        client.commit(transaction_id).await?;
    }
    
    let mixed_duration = start.elapsed();
    let mixed_ops_per_sec = num_operations as f64 / mixed_duration.as_secs_f64();
    println!("Mixed workload benchmark completed:");
    println!("  Operations: {}", num_operations);
    println!("  Duration: {:?}", mixed_duration);
    println!("  Operations/sec: {:.2}", mixed_ops_per_sec);

    // Benchmark 4: Consistency level comparison
    println!("\n=== Consistency Level Comparison ===");
    
    // Test different write consistency levels
    let consistency_levels = [
        ("Any", WriteConsistency::Any),
        ("Majority", WriteConsistency::Majority),
        ("All", WriteConsistency::All),
    ];
    
    for (name, consistency) in consistency_levels.iter() {
        let start = Instant::now();
        let test_ops = 100;
        
        for i in 0..test_ops {
            let transaction_id = client.begin_transaction().await?;
            let key = format!("consistency:{}:{}", name, i);
            let value = format!("value_{}", i);
            client.put(&key, &value, transaction_id, Some(*consistency)).await?;
            client.commit(transaction_id).await?;
        }
        
        let duration = start.elapsed();
        let ops_per_sec = test_ops as f64 / duration.as_secs_f64();
        println!("Write consistency '{}': {:.2} ops/sec", name, ops_per_sec);
    }
    
    // Test different read consistency levels
    let read_consistency_levels = [
        ("Eventual", ConsistencyLevel::Eventual),
        ("Majority", ConsistencyLevel::Majority),
        ("Strong", ConsistencyLevel::Strong),
    ];
    
    for (name, consistency) in read_consistency_levels.iter() {
        let start = Instant::now();
        let test_ops = 100;
        
        for i in 0..test_ops {
            let transaction_id = client.begin_transaction().await?;
            let key = format!("consistency:Any:{}", i);
            let _value = client.get(&key, transaction_id, Some(*consistency)).await?;
            client.commit(transaction_id).await?;
        }
        
        let duration = start.elapsed();
        let ops_per_sec = test_ops as f64 / duration.as_secs_f64();
        println!("Read consistency '{}': {:.2} ops/sec", name, ops_per_sec);
    }

    // Benchmark 5: Transaction size impact
    println!("\n=== Transaction Size Impact ===");
    let transaction_sizes = [1, 10, 50, 100];
    
    for size in transaction_sizes.iter() {
        let start = Instant::now();
        let num_transactions = 100;
        
        for txn in 0..num_transactions {
            let transaction_id = client.begin_transaction().await?;
            
            for i in 0..*size {
                let key = format!("txn_size:{}:{}:{}", size, txn, i);
                let value = format!("value_{}_{}_{}", size, txn, i);
                client.put(&key, &value, transaction_id, Some(WriteConsistency::Any)).await?;
            }
            
            client.commit(transaction_id).await?;
        }
        
        let duration = start.elapsed();
        let total_ops = num_transactions * size;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        let txns_per_sec = num_transactions as f64 / duration.as_secs_f64();
        println!("Transaction size {}: {:.2} ops/sec, {:.2} txns/sec", size, ops_per_sec, txns_per_sec);
    }

    // Get final statistics
    println!("\n=== Final Statistics ===");
    let health = client.get_cluster_health().await?;
    let stats = client.get_stats().await?;
    
    println!("Cluster health: {:?}", health);
    println!("Storage stats: {:?}", stats);

    println!("\nBenchmark completed successfully!");
    Ok(())
}
