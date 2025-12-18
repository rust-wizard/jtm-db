// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the Jellyfish Merkle Tree using RocksDB as backing storage.

use crate::{
    rocksdb_store::RocksDbTreeStore,
    JellyfishMerkleTree, KeyHash, SPARSE_MERKLE_PLACEHOLDER_HASH,
};
use sha2::Sha256;

fn hash_leaf(key: KeyHash, value_hash: crate::ValueHash) -> [u8; 32] {
    use crate::types::proof::SparseMerkleLeafNode;
    SparseMerkleLeafNode::new(key, value_hash).hash::<Sha256>()
}

#[test]
fn test_rocksdb_basic_operations() -> anyhow::Result<()> {
    // Create a temporary RocksDB store
    let db = RocksDbTreeStore::new_temporary()?;
    
    // Create a Jellyfish Merkle Tree with the RocksDB store
    let tree: JellyfishMerkleTree<RocksDbTreeStore, Sha256> = JellyfishMerkleTree::new(&db);
    
    // Put some values
    let key1 = KeyHash([1u8; 32]);
    let value1 = vec![0x01, 0x02, 0x03];
    let _value_hash1 = crate::ValueHash::with::<Sha256>(&value1);
    
    let key2 = KeyHash([2u8; 32]);
    let value2 = vec![0x04, 0x05, 0x06];
    let _value_hash2 = crate::ValueHash::with::<Sha256>(&value2);
    
    let values = vec![(key1, Some(value1.clone())), (key2, Some(value2.clone()))];
    let (new_root, batch) = tree.put_value_set(values, 0 /* version */)?;
    
    // Write the batch to the database
    db.write_tree_update_batch(batch)?;
    
    // Print database contents for visualization
    println!("\n=== Database contents after put_value_set ===");
    db.print_database_contents()?;
    
    // Verify the root hash
    // For a simple two-leaf tree, the root hash would be:
    // hash_internal(leaf_hash1, leaf_hash2)
    // But we'll just check that it's not the placeholder hash
    assert_ne!(new_root.0, SPARSE_MERKLE_PLACEHOLDER_HASH);
    
    // Get values back
    let retrieved_value1 = tree.get_with_proof(key1, 0)?.0;
    assert_eq!(retrieved_value1, Some(value1));
    
    let retrieved_value2 = tree.get_with_proof(key2, 0)?.0;
    assert_eq!(retrieved_value2, Some(value2));
    
    Ok(())
}

#[test]
fn test_rocksdb_multiple_versions() -> anyhow::Result<()> {
    // Create a temporary RocksDB store
    // let db = RocksDbTreeStore::new_temporary()?;
    use tempfile::TempDir;
    
    // Create a persistent RocksDB store in a temporary directory
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("jmt_test");
    
    // Create the RocksDB store
    let db = RocksDbTreeStore::new(&db_path)?;
    
    // Create a Jellyfish Merkle Tree with the RocksDB store
    let tree: JellyfishMerkleTree<RocksDbTreeStore, Sha256> = JellyfishMerkleTree::new(&db);
    
    // Version 0: Insert key1=value1
    let key1 = KeyHash([1u8; 32]);
    let value1_v0 = vec![0x01];
    let values_v0 = vec![(key1, Some(value1_v0.clone()))];
    let (root_v0, batch_v0) = tree.put_value_set(values_v0, 0)?;
    db.write_tree_update_batch(batch_v0)?;
    
    // Print database contents after version 0
    println!("\n=== Database contents after version 0 ===");
    db.print_database_contents()?;
    
    // Version 1: Update key1=value1_updated
    let value1_v1 = vec![0x02];
    let values_v1 = vec![(key1, Some(value1_v1.clone()))];
    let (root_v1, batch_v1) = tree.put_value_set(values_v1, 1)?;
    db.write_tree_update_batch(batch_v1)?;
    
    // Print database contents after version 1
    println!("\n=== Database contents after version 1 ===");
    db.print_database_contents()?;
    
    // Version 2: Insert key2=value2
    let key2 = KeyHash([2u8; 32]);
    let value2_v2 = vec![0x03];
    let values_v2 = vec![(key1, None), (key2, Some(value2_v2.clone()))]; // Delete key1, insert key2
    let (root_v2, batch_v2) = tree.put_value_set(values_v2, 2)?;
    db.write_tree_update_batch(batch_v2)?;
    
    // Print database contents after version 2
    println!("\n=== Database contents after version 2 ===");
    db.print_database_contents()?;
    
    // Verify roots are different
    assert_ne!(root_v0.0, root_v1.0);
    assert_ne!(root_v1.0, root_v2.0);
    
    // Check values at different versions
    let value_at_v0 = tree.get_with_proof(key1, 0)?.0;
    assert_eq!(value_at_v0, Some(value1_v0));
    
    let value_at_v1 = tree.get_with_proof(key1, 1)?.0;
    assert_eq!(value_at_v1, Some(value1_v1));
    
    let value1_at_v2 = tree.get_with_proof(key1, 2)?.0;
    assert_eq!(value1_at_v2, None);
    
    let value2_at_v2 = tree.get_with_proof(key2, 2)?.0;
    assert_eq!(value2_at_v2, Some(value2_v2));
    
    Ok(())
}

#[test]
fn test_rocksdb_visualization_example() -> anyhow::Result<()> {
    use std::fs;
    use tempfile::TempDir;
    
    // Create a persistent RocksDB store in a temporary directory
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("jmt_visualization_test");
    
    // Print database path for external inspection
    println!("Database path: {:?}", db_path);
    
    // Create the RocksDB store
    let db = RocksDbTreeStore::new(&db_path)?;
    
    // Create a Jellyfish Merkle Tree with the RocksDB store
    let tree: JellyfishMerkleTree<RocksDbTreeStore, Sha256> = JellyfishMerkleTree::new(&db);
    
    // Insert some sample data
    let key1 = KeyHash([1u8; 32]);
    let value1 = vec![0x01, 0x02, 0x03];
    
    let key2 = KeyHash([2u8; 32]);
    let value2 = vec![0x04, 0x05, 0x06];
    
    let values = vec![
        (key1, Some(value1.clone())),
        (key2, Some(value2.clone()))
    ];
    
    let (_new_root, batch) = tree.put_value_set(values, 0 /* version */)?;
    db.write_tree_update_batch(batch)?;
    
    // Print database contents
    println!("\n=== Visualization Example: Database Contents ===");
    db.print_database_contents()?;
    
    // Show how to use RocksDB's property API to get statistics
    println!("\n=== RocksDB Statistics ===");
    let db_ref = db.db();
    
    // Get some basic properties
    if let Some(stats) = db_ref.property_value("rocksdb.stats")? {
        println!("RocksDB Stats:\n{}", stats);
    }
    
    if let Some(sst_count) = db_ref.property_int_value("rocksdb.num-files-at-level0")? {
        println!("Number of SST files at level 0: {}", sst_count);
    }
    
    // Clean up
    drop(tree);  // Drop tree first since it borrows db
    drop(db);
    fs::remove_dir_all(&db_path)?;
    
    Ok(())
}
