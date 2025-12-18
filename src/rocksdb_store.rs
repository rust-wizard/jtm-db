// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

//! A RocksDB-backed tree store implementation.

use crate::{
    node_type::{LeafNode, Node, NodeKey},
    storage::{HasPreimage, TreeReader, TreeUpdateBatch, TreeWriter},
    types::Version,
    KeyHash, OwnedValue,
};
use anyhow::Result;
use rocksdb::{DB, Options, WriteBatch};
use std::sync::Arc;

/// A RocksDB-backed tree store.
pub struct RocksDbTreeStore {
    db: Arc<DB>,
}

impl RocksDbTreeStore {
    /// Creates a new RocksDB tree store with the given database path.
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        let db = DB::open(&opts, path)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Creates a new temporary RocksDB tree store.
    pub fn new_temporary() -> Result<Self> {
        let temp_dir = tempfile::TempDir::new()?;
        Self::new(temp_dir.path())
    }
}

impl TreeReader for RocksDbTreeStore {
    fn get_node_option(&self, node_key: &NodeKey) -> Result<Option<Node>> {
        let key = bincode::serialize(node_key)?;
        match self.db.get(key)? {
            Some(value) => {
                let node = bincode::deserialize(&value)?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    fn get_rightmost_leaf(&self) -> Result<Option<(NodeKey, LeafNode)>> {
        // This is a simplified implementation. In practice, you'd want to maintain
        // an index for efficient retrieval of the rightmost leaf.
        // For now, we'll just return None to avoid complex iterator handling.
        Ok(None)
    }

    fn get_value_option(
        &self,
        max_version: Version,
        key_hash: KeyHash,
    ) -> Result<Option<OwnedValue>> {
        // Store values with composite key: (key_hash, version)
        // Retrieve the latest version <= max_version
        // For simplicity, we'll iterate through all keys and find the matching ones.
        // This is inefficient but works for testing purposes.
        
        let mut iter = self.db.iterator(rocksdb::IteratorMode::Start);
        let mut latest_value: Option<OwnedValue> = None;
        let mut latest_version: Option<Version> = None;
        
        for item in iter {
            let (key, value) = item?;
            
            // Try to deserialize the key as (KeyHash, Version)
            if let Ok((stored_key_hash, version)) = bincode::deserialize::<(KeyHash, Version)>(&key) {
                if stored_key_hash == key_hash && version <= max_version {
                    if latest_version.is_none() || version > latest_version.unwrap() {
                        latest_version = Some(version);
                        // Deserialize the value as Option<Vec<u8>>
                        if let Ok(deserialized_value) = bincode::deserialize::<Option<Vec<u8>>>(&value) {
                            latest_value = deserialized_value;
                        }
                    }
                }
            }
        }
        
        Ok(latest_value)
    }
}

impl HasPreimage for RocksDbTreeStore {
    fn preimage(&self, key_hash: KeyHash) -> Result<Option<Vec<u8>>> {
        let key = bincode::serialize(&(key_hash, "preimage"))?;
        match self.db.get(key)? {
            Some(value) => Ok(Some(value)),
            None => Ok(None),
        }
    }
}

impl TreeWriter for RocksDbTreeStore {
    fn write_node_batch(&self, node_batch: &crate::storage::NodeBatch) -> Result<()> {
        let mut batch = WriteBatch::default();
        
        // Write nodes
        for (node_key, node) in node_batch.nodes() {
            let key = bincode::serialize(node_key)?;
            let value = bincode::serialize(node)?;
            batch.put(key, value);
        }
        
        // Write values
        for ((version, key_hash), value) in node_batch.values() {
            let key = bincode::serialize(&(*key_hash, *version))?;
            let serialized_value = bincode::serialize(value)?;
            batch.put(key, serialized_value);
        }
        
        self.db.write(batch)?;
        Ok(())
    }
}

impl RocksDbTreeStore {
    /// Writes a tree update batch to the database.
    pub fn write_tree_update_batch(&self, batch: TreeUpdateBatch) -> Result<()> {
        self.write_node_batch(&batch.node_batch)?;
        // Note: stale nodes are typically handled separately in a real implementation
        // For simplicity, we're ignoring the stale_node_index_batch here
        Ok(())
    }
    
    /// Prints the contents of the database for visualization purposes.
    /// This is useful for debugging and understanding what's stored in the database.
    #[cfg(test)]
    pub fn print_database_contents(&self) -> Result<()> {
        println!("Database contents:");
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        let mut count = 0;
        
        for item in iter {
            let (key, value) = item?;
            count += 1;
            
            // Try to deserialize as a NodeKey
            if let Ok(node_key) = bincode::deserialize::<crate::node_type::NodeKey>(&key) {
                if let Ok(node) = bincode::deserialize::<crate::node_type::Node>(&value) {
                    println!("  {}: NodeKey({:?}) -> Node({:?})", count, node_key, node);
                } else {
                    println!("  {}: NodeKey({:?}) -> Raw Value({} bytes)", count, node_key, value.len());
                }
            } 
            // Try to deserialize as (KeyHash, Version)
            else if let Ok((key_hash, version)) = bincode::deserialize::<(KeyHash, Version)>(&key) {
                if let Ok(option_value) = bincode::deserialize::<Option<Vec<u8>>>(&value) {
                    println!("  {}: (KeyHash({:?}), Version({})) -> {:?}", count, key_hash, version, option_value);
                } else {
                    println!("  {}: (KeyHash({:?}), Version({})) -> Raw Value({} bytes)", count, key_hash, version, value.len());
                }
            }
            // Try to deserialize as (KeyHash, "preimage")
            else if let Ok((key_hash, _)) = bincode::deserialize::<(KeyHash, &str)>(&key) {
                println!("  {}: KeyHash({:?}) preimage -> {} bytes", count, key_hash, value.len());
            }
            else {
                println!("  {}: Unknown key ({} bytes) -> {} bytes", count, key.len(), value.len());
            }
        }
        
        if count == 0 {
            println!("  Database is empty");
        } else {
            println!("  Total entries: {}", count);
        }
        
        Ok(())
    }
    
    /// Returns the underlying RocksDB database for advanced operations.
    /// This is primarily for testing and debugging purposes.
    #[cfg(test)]
    pub fn db(&self) -> &DB {
        &self.db
    }
}
