# Cow Tree Experiment Timeline and Results

This document summarizes the implementation timeline and benchmark outcomes in chronological order.

## 1) cow_final (On-demand Paging)
- Model: On-demand Paging system using a 4-Way Set Associative Global Cache
- Behavior:
  - Fixed-size global cache, loading pages from disk only when necessary
  - Batch Flush

## 2) cow_zfs (Parallel Update & Asynchronous Pipeline)
- Model: Full ZFS-style transaction model featuring Parallel Latch-Crabbing Tree Updates and a dedicated I/O committer
- Behavior:
  - Parallel Updates: Multiple worker threads perform concurrent B-tree inserts using per-node RW locks (latch-crabbing)
  - Parallel Serialization: Workers collaboratively convert modified RAM nodes into disk-ready pages in parallel
  - Asynchronous Batch Commit: A background committer handles vectorized NVMe appends, decoupling storage latency from the parallel update process

## 3) cow_ram_stage2_bf (Stage2 + Batch Flush)
- Mode: Stage2 + Batch Flush

## 4) cow_ram_stage2 (Pipeline-optimized RAM Table)
- Model: Implements a decoupled 2-stage asynchronous pipeline on top of the RAM-resident page table
- Behavior:
  - Stage 1 (Sync): A dedicated thread drains the insertion queue, assigns sequence numbers, and performs Key Sorting to optimize the tree traversal pattern
  - Stage 2 (Commit): A background committer merges multiple batch jobs from the Stage 2 queue to perform a combined Overlay Apply and Vectorized Flush

## 5) cow_btrfs1 (TX State Machine & Commit Winner)
- Model: Implements a Btrfs-style transaction state machine (RUNNING → COMMIT_PREP → COMMIT_DOING → COMPLETED)
- Behavior:
  - Allows multiple threads to join the same "Running" transaction simultaneously to improve concurrency
  - Commit Winner Mechanism: Once all writers in a group finish, exactly one thread (the winner) executes the actual commit while others wait for completion

## 6) cow_shard (Shard-based parallel writer)
- Model: Partitioned MPSC (Multi-Producer Single-Consumer) request queues with 16 shards (QUEUE_SHARD_COUNT=16)
- Behavior:
  - Uses a shard_id_for_key(key) function to automatically distribute insertion requests across shards
  - The background writer performs round-robin polling across all shards to collect batches

## 7) cow_ram (RAM-first)
- Model: RAM-resident page table

## 8) cow_v3_multi_cache
- Model: 8K set x 4-way global cache with instrumentation

## 9) cow_v3 baseline
- Model: single writer with request queue and batching