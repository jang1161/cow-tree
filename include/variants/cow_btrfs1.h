#pragma once

#include <libnvme.h>
#include <libzbd/zbd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define PAGE_SIZE 4096
#define META_ZONE_0 0
#define META_ZONE_1 1
#define DATA_ZONE_START 2

#define ZH_MAGIC 0x5A4E535A48445200ULL
#define SB_MAGIC 0x434F574250545245ULL
#define ZH_ACTIVE 0x01

#define LEAF_ORDER 32
#define INTERNAL_ORDER 249

#define FLUSH_INTERVAL_MS 10

typedef uint64_t pagenum_t;
#define INVALID_PGN ((pagenum_t) - 1)

typedef struct record
{
    char value[120];
} record;

typedef struct leaf_entity
{
    uint64_t key;
    record record;
} leaf_entity;

typedef struct internal_entity
{
    uint64_t key;
    pagenum_t child;
} internal_entity;

typedef struct
{
    pagenum_t pn;
    uint32_t is_leaf;
    uint32_t num_keys;
    pagenum_t pointer;
    uint8_t pad[128 - (sizeof(pagenum_t) * 2 +
                       sizeof(uint32_t) * 2)];

    union
    {
        leaf_entity leaf[LEAF_ORDER - 1];
        internal_entity internal[INTERNAL_ORDER - 1];
    };
} page;

typedef struct
{
    uint64_t magic;
    uint8_t state;
    uint64_t version;
    uint8_t pad[PAGE_SIZE - 8 - 1 - 8];
} zone_header;

typedef struct
{
    uint64_t magic;
    uint64_t seq_no;
    pagenum_t root_pn;
    uint32_t leaf_order;
    uint32_t internal_order;
    uint8_t pad[PAGE_SIZE - 8 * 3 - 4 * 2];
} superblock_entry;

typedef struct
{
    _Atomic(pagenum_t) root_pn;
    _Atomic(uint64_t) seq_no;
} atomic_superblock;

typedef struct insert_req
{
    int64_t key;
    char value[120];
    int done;

    pthread_mutex_t done_lock;
    pthread_cond_t done_cv;

    struct insert_req *next;
} insert_req;

/* Transaction states for Btrfs-style transaction machine */
typedef enum
{
    TX_RUNNING,
    TX_COMMIT_PREP,
    TX_COMMIT_DOING,
    TX_COMPLETED
} tx_state_t;

typedef struct
{
    uint64_t tx_id;
    tx_state_t state;
    uint64_t num_writers; // Number of threads joined
    pthread_mutex_t state_lock;
    pthread_cond_t commit_cv;   // Waiters for commit winner
    pthread_cond_t observer_cv; // Waiters for commit completion
} transaction_t;

typedef struct
{
    int fd;
    __u32 nsid;
    int direct_fd;
    struct zbd_info info;
    struct zbd_zone *zones;
    _Atomic(uint32_t) current_zone;

    _Atomic(uint64_t) *zone_wp_bytes;
    _Atomic(uint8_t) *zone_full;

    superblock_entry durable_sb;
    uint32_t active_zone;
    uint64_t meta_wp;
    uint64_t version;

    atomic_superblock volatile_sb;

    pthread_t flusher_tid;
    _Atomic(bool) flusher_stop;
    _Atomic(bool) dirty;
    pthread_mutex_t flush_lock;

    /* Transaction state machine (Btrfs-style) */
    _Atomic(uint64_t) current_tx_id;
    transaction_t *current_tx;  // Current running/commit transaction
    pthread_mutex_t tx_lock;    // Protects transaction state changes
    pthread_cond_t tx_state_cv; // Signals state transitions

    /* Legacy writer/queue path (disabled for direct tx mode) */
    pthread_t writer_tid;
    pthread_mutex_t q_lock;
    pthread_cond_t q_cv;
    insert_req *q_head;
    insert_req *q_tail;
    _Atomic(bool) stop_writer;

    /* Direct tx session shared by main workers */
    pthread_mutex_t direct_tx_lock;
    pthread_cond_t direct_tx_cv;
    uint64_t direct_tx_epoch;
    uint64_t direct_tx_committed_epoch;
    uint64_t direct_tx_participants;
    bool direct_tx_active;
    bool direct_tx_committing;
    uint64_t direct_tx_root_id;
    void *direct_tx_overlay;
    int expected_workers;

    /* Throughput & Latency Stats */
    _Atomic(uint64_t) stat_batches;
    _Atomic(uint64_t) stat_batch_items;
    _Atomic(uint64_t) stat_q_lock_wait_ns_insert;
    _Atomic(uint64_t) stat_q_lock_wait_samples_insert;
    _Atomic(uint64_t) stat_q_lock_wait_ns_writer;
    _Atomic(uint64_t) stat_q_lock_wait_samples_writer;
    _Atomic(uint64_t) stat_writer_empty_waits;

    /* Stage-specific latency */
    _Atomic(uint64_t) stat_append_latency_ns_samples;
    _Atomic(uint64_t) stat_append_latency_ns_sum;
    _Atomic(uint64_t) stat_batch_latency_ns_samples;
    _Atomic(uint64_t) stat_batch_latency_ns_sum;
    _Atomic(uint64_t) stat_sort_latency_ns_samples;
    _Atomic(uint64_t) stat_sort_latency_ns_sum;
    _Atomic(uint64_t) stat_apply_latency_ns_samples;
    _Atomic(uint64_t) stat_apply_latency_ns_sum;
    _Atomic(uint64_t) stat_flush_latency_ns_samples;
    _Atomic(uint64_t) stat_flush_latency_ns_sum;

    /* Bottleneck Analysis Stats */
    _Atomic(uint64_t) stat_overlay_nodes_sum;
    _Atomic(uint64_t) stat_overlay_nodes_max;
    _Atomic(uint64_t) stat_queue_depth_current;
    _Atomic(uint64_t) stat_queue_depth_sum;
    _Atomic(uint64_t) stat_queue_depth_max;
    _Atomic(uint64_t) stat_queue_depth_samples;

    /* RWLock Contention */
    _Atomic(uint64_t) stat_rwlock_try_failed;
    _Atomic(uint64_t) stat_rwlock_try_total;

    /* Fallback Mutex Contention (serial apply mode) */
    _Atomic(uint64_t) stat_fallback_lock_waits_ns;
    _Atomic(uint64_t) stat_fallback_lock_waits_count;

    /* Apply Stage Lock Wait (within parallel apply) */
    _Atomic(uint64_t) stat_apply_lock_waits_ns;
    _Atomic(uint64_t) stat_apply_lock_waits_count;

    /* Concurrent Writer Activity */
    _Atomic(uint64_t) stat_concurrent_writers_max;

    /* TX State Machine Stats */
    _Atomic(uint64_t) stat_tx_starts;
    _Atomic(uint64_t) stat_tx_joins;
    _Atomic(uint64_t) stat_tx_commit_winners;
    _Atomic(uint64_t) stat_tx_commit_waiters;

    /* Winner Selection & Waiter Blocking */
    _Atomic(uint64_t) stat_winner_wait_ns_sum;
    _Atomic(uint64_t) stat_winner_wait_samples;

    void *ram_slots;
    size_t ram_cap;
    size_t ram_used;
} cow_tree;

cow_tree *cow_open(const char *path);
void cow_close(cow_tree *t);

record *cow_find(cow_tree *t, int64_t key);
void cow_insert(cow_tree *t, int64_t key, const char *value);
void cow_insert_direct(cow_tree *t, int64_t key, const char *value);
void cow_set_expected_workers(cow_tree *t, int workers);
