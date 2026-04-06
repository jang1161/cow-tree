## 1) cow_gtx_cache
- A COW B-tree implementation with multi-producer, single-writer

## 2) cow_gtx_cache_p
- A 'pwrite() version' of `cow_gtx_cache`  

## 3) cow_zfs
- A ZFS-style transaction pipeline implementation  
- Latch-crabbing based parallel B-tree
- Separates sync, commit, and flusher for asynchronous processing  

## 4) cow_zfs_shard
- Extends the ZFS model with shard-based parallelism  
- Distributes inserts using per-shard roots and locks  

## 5) cow_zfs_shard_cache
- Combines the sharded structure with global cache