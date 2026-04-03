/*
 * cow_zfs_shard_cache.h — Type reference for cow_zfs_shard_cache.c
 *
 * This file is NOT included by the implementation (which is standalone).
 * It documents the key types and build command for reference.
 *
 * Build:
 *   gcc -O2 -g -Wall -Wextra -std=c11 -pthread -Iinclude -Iinclude/variants -I. \
 *       src/variants/cow_zfs_shard_cache.c \
 *       -o build/bin/cow-bench-zfs-shard-cache -lzbd -lnvme -lpthread
 */

#pragma once

#include <libnvme.h>
#include <libzbd/zbd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#define PAGE_SIZE         4096
#define N_SHARDS          12
#define SHARD_MAX_SERIAL_NODES 256
#define GLOBAL_MAX_SERIAL_NODES (N_SHARDS * SHARD_MAX_SERIAL_NODES)  /* 3072 */
#define LEAF_ORDER        32
#define INTERNAL_ORDER    249
#define COMMIT_Q_DEPTH    16
#define CACHE_NUM_SETS    (4096 * 4)   /* 16384 sets */
#define CACHE_WAYS        4
#define TL_CACHE_SLOTS    64
#define NODE_LOCK_SLOTS   65536
#define MAX_HEIGHT        32
#define TX_TIMEOUT_NS     (2000000ULL)
#define SERIAL_BARRIER_TIMEOUT_NS (2000000000ULL)

#define SB_MAGIC          0x5A534853434143484ULL  /* "ZSHCACH" + version */
#define META_ZONE         0
#define DATA_ZONE_START   2

typedef uint64_t pgn_t;
#define PGN_NULL          ((pgn_t)0xFFFFFFFFFFFFFFFFull)
#define IS_TEMP_ID(id)    (((id) & (1ULL << 63)) != 0)
#define MAKE_TEMP_ID(n)   ((pgn_t)(1ULL << 63) | (pgn_t)(n))

#define LEAF_MAX   (LEAF_ORDER - 1)       /* 31  */
#define INT_MAX_K  (INTERNAL_ORDER - 1)   /* 248 */

/* ── Disk page layout (4096 B) ────────────────────────────────────────────── */
typedef struct { uint64_t key; char value[120]; } leaf_ent;
typedef struct { uint64_t key; pgn_t child; }     int_ent;

typedef struct
{
    uint32_t is_leaf;
    uint32_t num_keys;
    pgn_t    pointer;    /* rightmost child (internal) or PGN_NULL (leaf) */
    uint8_t  hpad[112];
    union
    {
        leaf_ent leaf[LEAF_MAX];
        int_ent  internal[INT_MAX_K];
    };
} disk_page;

/* ── FIX-5: Multi-shard superblock — stores all N_SHARDS roots ───────────── */
typedef struct
{
    uint64_t magic;
    uint64_t seq_no;
    uint32_t num_shards;
    uint32_t leaf_order;
    uint32_t internal_order;
    uint32_t _pad;
    pgn_t    root_pn[N_SHARDS];
    uint8_t  tail[PAGE_SIZE - 8 - 8 - 4 - 4 - 4 - 4 - N_SHARDS * 8];
} multi_superblock_t;

/* ── 4-way set-associative global page cache ─────────────────────────────── */
typedef struct
{
    uint8_t   valid;
    uint64_t  lru_counter;
    pgn_t     tag;
    disk_page data;
} cache_way;

typedef struct
{
    pthread_mutex_t lock;
    cache_way       ways[CACHE_WAYS];
} cache_set;

typedef struct
{
    uint8_t   valid;
    pgn_t     tag;
    disk_page data;
} tl_cache_entry;

/* ── Overlay: per-shard COW layer (FIX-2: pre-allocated, never realloced) ── */
typedef struct overlay_node
{
    pgn_t     id;           /* disk pgn or MAKE_TEMP_ID(n) */
    disk_page node;
    uint8_t   dirty;
    pgn_t     flushed_pn;   /* assigned by closer at commit time */
} overlay_node;

typedef struct overlay_state
{
    overlay_node   *arr;      /* pre-allocated, fixed capacity */
    size_t          cap;
    size_t          len;
    uint64_t        next_temp;
    size_t         *idx_table; /* open-addressing hash: id → arr index */
    size_t          idx_cap;
    size_t          idx_used;
    pthread_mutex_t lock;
} overlay_state;

/* ── Global TX machine ───────────────────────────────────────────────────── */
typedef struct
{
    uint64_t        epoch;
    int             expected;
    int             joined;
    int             done;
    bool            closed;
    bool            closed_by_count;
    bool            flushing;
    bool            serializing;
    uint64_t        open_ns;
    pthread_mutex_t lock;
    pthread_cond_t  cond;

    _Atomic int     serial_claimed;
    _Atomic int     serial_done;
    int             serial_total;
    int             serial_buf_slot;
    int             serial_batch_size;
    int             serial_joined_snap;   /* snapshot set by closer, stable */
} global_tx_t;

/* ── Commit batch (pipeline slot) ───────────────────────────────────────── */
typedef struct
{
    int      n_pages;
    int      buf_slot;
    uint32_t zone_id;
    pgn_t    root_pn[N_SHARDS];
    bool     needs_zone_finish;
    uint32_t old_zone_id;
    uint64_t batch_open_ns;
    uint64_t batch_enqueue_ns;
    uint64_t seq_no;
    bool     ready;
} commit_batch_t;

/* ── Shard ────────────────────────────────────────────────────────────────── */
typedef struct shard_t
{
    int                shard_id;
    pthread_rwlock_t  *node_locks;
    pgn_t              root;
    pthread_rwlock_t   root_lock;
    overlay_state      overlay;
    pgn_t              dfs_order[GLOBAL_MAX_SERIAL_NODES * 2];
    int                dfs_count;
    uint8_t            _pad[64];
} shard_t;

/* ── Per-run metrics ─────────────────────────────────────────────────────── */
typedef struct
{
    _Atomic uint64_t tx_count;
    _Atomic uint64_t tx_joined_sum;
    _Atomic uint64_t tx_closed_by_count;
    _Atomic uint64_t tx_closed_by_timeout;
    _Atomic uint64_t nvme_ns_total;
    _Atomic uint64_t insert_ns_total;
    _Atomic uint64_t entry_wait_ns_total;
    _Atomic uint64_t epoch_wait_ns_total;
    _Atomic uint64_t dirty_nodes_total;
    _Atomic uint64_t zone_advance_count;
    _Atomic uint64_t serialize_ns_total;
    _Atomic uint64_t commit_ns_total;
    _Atomic uint64_t commit_wait_ns_total;
    _Atomic uint64_t batch_formation_ns_sum;
    _Atomic uint64_t batch_formation_count;
    _Atomic uint64_t committer_stall_ns_sum;
    _Atomic uint64_t committer_stall_count;
    _Atomic uint64_t req_e2e_ns_sum;
    _Atomic uint64_t req_e2e_count;
    _Atomic uint64_t cache_hit_count;
    _Atomic uint64_t cache_miss_count;
} run_metrics_t;
