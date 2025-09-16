use crate::core::{Key, Value, TransactionId, Result, KvError};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc};

/// Write-Ahead Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    BeginTransaction { transaction_id: TransactionId, timestamp: DateTime<Utc> },
    Put { transaction_id: TransactionId, key: Key, value: Value, timestamp: DateTime<Utc> },
    Delete { transaction_id: TransactionId, key: Key, timestamp: DateTime<Utc> },
    Commit { transaction_id: TransactionId, timestamp: DateTime<Utc> },
    Abort { transaction_id: TransactionId, timestamp: DateTime<Utc> },
}

impl WalEntry {
    pub fn transaction_id(&self) -> TransactionId {
        match self {
            WalEntry::BeginTransaction { transaction_id, .. } => *transaction_id,
            WalEntry::Put { transaction_id, .. } => *transaction_id,
            WalEntry::Delete { transaction_id, .. } => *transaction_id,
            WalEntry::Commit { transaction_id, .. } => *transaction_id,
            WalEntry::Abort { transaction_id, .. } => *transaction_id,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            WalEntry::BeginTransaction { timestamp, .. } => *timestamp,
            WalEntry::Put { timestamp, .. } => *timestamp,
            WalEntry::Delete { timestamp, .. } => *timestamp,
            WalEntry::Commit { timestamp, .. } => *timestamp,
            WalEntry::Abort { timestamp, .. } => *timestamp,
        }
    }
}

/// Write-Ahead Log implementation
pub struct WriteAheadLog {
    file: Arc<Mutex<BufWriter<File>>>,
    path: String,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
            path: path_str,
        })
    }

    pub fn append(&self, entry: WalEntry) -> Result<()> {
        let serialized = bincode::serialize(&entry)?;
        let mut file = self.file.lock().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        // Write length prefix
        file.write_all(&(serialized.len() as u32).to_le_bytes())?;
        // Write entry data
        file.write_all(&serialized)?;
        file.flush()?;
        
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<WalEntry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut buffer = Vec::new();

        loop {
            // Read length prefix
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    buffer.resize(len, 0);
                    
                    // Read entry data
                    reader.read_exact(&mut buffer)?;
                    
                    // Deserialize entry
                    let entry: WalEntry = bincode::deserialize(&buffer)?;
                    entries.push(entry);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(entries)
    }

    pub fn truncate(&self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        
        let mut wal_file = self.file.lock().map_err(|e| KvError::InternalError(e.to_string()))?;
        *wal_file = BufWriter::new(file);
        
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock().map_err(|e| KvError::InternalError(e.to_string()))?;
        file.flush()?;
        Ok(())
    }
}