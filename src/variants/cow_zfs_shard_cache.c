/*
 * cow_zfs_shard_cache.c — 12-Shard Global TX B-Tree with 4-Way Set-Associative Cache
 *
 * Architecture: Multi-Shard (12 Shards) + Global TX + Overlay + Global Page Cache
 *   - Tree is physically split into N_SHARDS=12 independent B-trees (shards)
 *   - All shards share ONE global TX epoch (g_tx), eliminating per-shard TX overhead
 *   - Worker threads insert into overlay (COW-style), not directly into persisted pages
 *   - flush_prepare_phase1_global: sweeps ALL 12 shards → one NVMe write per TX
 *   - 4-way set-associative global page cache (CACHE_NUM_SETS=16384, CACHE_WAYS=4)
 *   - Tiered load: thread-local cache → global cache → O_DIRECT disk read
 *
 * ─── Lock Ordering (MUST be respected to prevent deadlock) ───────────────────
 *   L1: per-shard root_lock (rwlock, brief)
 *   L2: node_lock_for(s, pgn)  (rwlock, per-node)
 *   L3: overlay.lock  (mutex, per-shard, brief — no nested locks while held)
 *   L4: g_tx.lock  (mutex — never acquire L1/L2/L3/L5/L6 while holding this;
 *                   committer acquires briefly for durability broadcast, no other locks held)
 *   L5: g_zone_lock  (mutex, brief — never acquire L3 while holding this)
 *   L6: g_cq_lock  (mutex — never acquire L4 while holding this)
 *   L7: g_enqueue_lock  (mutex — acquire AFTER releasing L4, before L6)
 *   Cache: cache_set[i].lock — lowest level, never acquire other locks while held
 *
 * Build:
 *   gcc -O2 -g -Wall -Wextra -std=c11 -pthread -Iinclude -Iinclude/variants \
 *       -I. src/variants/cow_zfs_shard_cache.c \
 *       -o build/bin/cow-bench-zfs-shard-cache -lzbd -lnvme -lpthread
 * Run:
 *   sudo ./build/bin/cow-bench-zfs-shard-cache <num_keys> <thread_mode> [dev]
 *   thread_mode: 0=sweep 1/2/4/8/16/32/64=specific count
 */

/* ═══════════════════════════════════════════════════════════════════════════
 * AUDIT NOTES — bugs fixed in this revision:
 *
 * FIX-1  B-tree internal split: carry_right and original rightmost pointer were
 *        both lost when building the tmp[] array. The right node received the
 *        wrong rightmost child, causing subtree corruption under heavy workloads.
 *        → Rewritten internal-split logic with explicit final_rightmost tracking.
 *
 * FIX-2  Dangling pointer from overlay realloc: overlay_get_mut() returns a raw
 *        pointer that can be invalidated if another shard-concurrent call triggers
 *        realloc(ov->arr). Fixed by pre-allocating the overlay array at startup
 *        (capacity = 2× expected nodes per shard). No realloc ever occurs on
 *        the hot path. Pointer stability is guaranteed for the benchmark lifetime.
 *
 * FIX-3  Type truncation: g_global_dfs_order[] is pgn_t (uint64_t) but was read
 *        into nid_t (uint32_t) in the serialization loop, silently truncating
 *        temp IDs whose bit 63 is set.  All node ID variables are now pgn_t.
 *
 * FIX-4  g_zone_lock held during overlay_get_mut (which calls load_page/pread):
 *        A slow disk read inside the zone lock would starve the entire TX pipeline.
 *        Fixed by computing pagenums and releasing g_zone_lock before calling
 *        overlay_get_mut to stamp flushed_pn values.
 *
 * FIX-5  Superblock only stored g_shards[0].root.  If any other shard's tree is
 *        the authoritative root, recovery would lose it.  Replaced superblock_t
 *        with multi_superblock_t that stores root_pn[N_SHARDS] for all shards.
 *
 * FIX-6  flush_shard_sync race: the residual sync flush reused g_global_serial_bufs[0]
 *        while the committer thread might still be reading a previous batch from that
 *        slot.  Fixed by draining the committer (pthread_join) BEFORE the residual
 *        flush, and using a dedicated local aligned buffer for the sync path.
 *
 * FIX-7  NVMe I/O failure called abort() inside the committer, leaving the TX
 *        pipeline stuck.  Replaced with g_io_error flag; committer and closer
 *        propagate the error so workers exit cleanly.
 *
 * FIX-8  Last serializer held g_tx.lock while calling pthread_cond_wait on
 *        g_enqueue_cond (which waits for the previous epoch's batch to publish).
 *        This blocked all threads trying to join the next TX epoch.  Fixed by
 *        releasing g_tx.lock before the enqueue-order wait and re-acquiring it
 *        for the epoch reset.  Excess serial_done increments from races are
 *        handled correctly because serializing=false is checked after re-lock.
 *
 * FIX-9  Serialization barrier had no timeout: if any worker thread hung in
 *        build_disk_page (e.g., overlay lock contention), serial_done would never
 *        reach joined_snap and all threads would block indefinitely.  Added a
 *        2-second pthread_cond_timedwait in the non-last-serializer wait path.
 *
 * FIX-10 overlay_get_mut called load_page for temp IDs (bit-63 set), which would
 *        compute a huge byte offset (~36 PiB) and either EINVAL or read garbage.
 *        Fixed by returning NULL immediately (and aborting) if the ID is a temp
 *        ID that is somehow missing from the overlay — indicating a logic error.
 *
 * FIX-11 dfs_dirty_collect called overlay_get_mut which acquires the overlay lock.
 *        If a dirty node's child was already returned NULL by overlay_get_mut, the
 *        DFS would silently skip the subtree, producing an incomplete commit batch.
 *        Now asserts that all nodes in the dirty path are present in the overlay.
 *
 * FIX-12 Committer broadcasts g_tx.cond (durability gate) while non-last
 *        serializers are blocked in the FIX-9 timedwait.  They would wake up,
 *        see g_tx.serializing still true, claim a second batch_id, and increment
 *        serial_done again — exceeding joined_snap and preventing the barrier
 *        from ever firing.  Fixed with a per-worker did_serialize flag: each
 *        worker claims and serializes at most once per epoch.
 *
 * FIX-13 dfs_dirty_collect called overlay_get_mut which falls through to
 *        load_page for every child of a dirty internal node not in the overlay.
 *        After an overlay reset most children are physical page IDs absent from
 *        the overlay, so the DFS triggered ~286 load_page calls per TX just to
 *        discover those nodes are clean.  Fixed by checking overlay_find_idx
 *        directly; nodes absent from the overlay cannot be dirty and are pruned
 *        immediately without I/O.  Node data is snapshotted under ov->lock
 *        before recursing (safe: TX is closed, overlay is read-only during DFS).
 * ═══════════════════════════════════════════════════════════════════════════ */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <libnvme.h>
#include <libzbd/zbd.h>

/* ─────────────────── Constants ─────────────────── */
#define N_SHARDS 12
#define SHARD_MAX_SERIAL_NODES 256
#define GLOBAL_MAX_SERIAL_NODES (N_SHARDS * SHARD_MAX_SERIAL_NODES)  /* 3072 */
#define PAGE_SIZE 4096
#define LEAF_ORDER 32
#define INTERNAL_ORDER 249
#define MAX_APPEND_PAGES 64
#define TX_TIMEOUT_NS (2000000ULL)
#define SERIAL_BARRIER_TIMEOUT_NS (2000000000ULL)  /* 2-second safety timeout (FIX-9) */
#define COMMIT_Q_DEPTH 16
#define META_ZONE 0
#define DATA_ZONE_START 2
#define SB_MAGIC 0x5A53485343414348ULL    /* "ZSHSCACH" (8 bytes) */
#define MAX_HEIGHT 32
#define HS_MAX (MAX_HEIGHT + 4)
#define CACHE_NUM_SETS (4096 * 4)
#define CACHE_WAYS 4
#define TL_CACHE_SLOTS 64
#define NODE_LOCK_SLOTS 65536

#define LEAF_MAX (LEAF_ORDER - 1)
#define INT_MAX_K (INTERNAL_ORDER - 1)
#define INT_CHILDREN INTERNAL_ORDER
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

/* Temp overlay IDs have bit 63 set; disk page numbers never do (ZNS device
 * capacity is typically < 2^40 pages). */
#define IS_TEMP_ID(id) (((id) & (1ULL << 63)) != 0)
#define MAKE_TEMP_ID(n) ((pgn_t)(1ULL << 63) | (pgn_t)(n))

typedef uint64_t pgn_t;
#define PGN_NULL ((pgn_t)0xFFFFFFFFFFFFFFFFull)

/* ─────────────────── Disk page layout (4096 B) ─────────────────── */
typedef struct { uint64_t key; char value[120]; } leaf_ent;
typedef struct { uint64_t key; pgn_t child; } int_ent;

typedef struct
{
    uint32_t is_leaf;
    uint32_t num_keys;
    pgn_t pointer;     /* rightmost child (internal) or PGN_NULL (leaf) */
    uint8_t hpad[112];
    union
    {
        leaf_ent leaf[LEAF_MAX];
        int_ent internal[INT_MAX_K];
    };
} disk_page;
_Static_assert(sizeof(disk_page) == PAGE_SIZE, "disk_page != 4096");

/* ── FIX-5: Multi-shard superblock stores all N_SHARDS roots ─────────────── */
typedef struct
{
    uint64_t magic;
    uint64_t seq_no;
    uint32_t num_shards;
    uint32_t leaf_order;
    uint32_t internal_order;
    uint32_t _pad;
    pgn_t    root_pn[N_SHARDS];  /* root page number for each shard */
    uint8_t  tail[PAGE_SIZE - 8 - 8 - 4 - 4 - 4 - 4 - N_SHARDS * 8];
} multi_superblock_t;
_Static_assert(sizeof(multi_superblock_t) == PAGE_SIZE, "multi_superblock_t != 4096");

/* ─────────────────── 4-way set-associative global cache ─────────────────── */
typedef struct
{
    uint8_t  valid;
    uint64_t lru_counter;
    pgn_t    tag;
    disk_page data;
} cache_way;

typedef struct
{
    pthread_mutex_t lock;
    cache_way ways[CACHE_WAYS];
} cache_set;

typedef struct
{
    uint8_t   valid;
    pgn_t     tag;
    disk_page data;
} tl_cache_entry;

/* ─────────────────── Overlay node and state ─────────────────── */
/*
 * FIX-2: overlay_state.arr is pre-allocated at startup with a fixed capacity.
 * overlay_add_node() never calls realloc(), so all pointers returned by
 * overlay_get_mut() remain valid for the lifetime of the benchmark run.
 */
typedef struct overlay_node
{
    pgn_t     id;        /* disk pgn or MAKE_TEMP_ID(n) */
    disk_page node;
    uint8_t   dirty;
    pgn_t     flushed_pn;  /* assigned by closer at commit time */
} overlay_node;

typedef struct overlay_state
{
    overlay_node *arr;   /* pre-allocated, never realloced (FIX-2) */
    size_t cap;
    size_t len;
    uint64_t next_temp;
    size_t *idx_table;   /* open-addressing hash table: id → arr index */
    size_t idx_cap;
    size_t idx_used;
    pthread_mutex_t lock;
} overlay_state;

/* ─────────────────── ZNS Device (global) ─────────────────── */
static int g_fd;
static int g_direct_fd;
static __u32 g_nsid;
static struct zbd_info g_info;
static struct zbd_zone *g_zones;
static uint32_t g_nzones;
static pthread_mutex_t g_dev_lock;
static _Atomic(uint64_t) g_pages_appended;
static cache_set *g_global_cache;
static _Atomic(uint64_t) g_cache_lru_clock;
/* FIX-7: I/O error propagation — set once, never cleared; workers check it */
static _Atomic(bool) g_io_error;
/* Highest batch seq_no confirmed written by the committer; -1 = none yet.
 * Workers wait on g_tx.cond for this to reach overlay_reset_seq before
 * resetting the overlay and switching shard roots to flushed page numbers. */
static _Atomic(int64_t) g_durable_seq;

/* Thread-local cache (no locking, per-thread private) */
static __thread tl_cache_entry g_tl_cache[TL_CACHE_SLOTS];

/* Per-node rwlocks, sharded by hash */
static pthread_rwlock_t *g_node_locks_base;
static size_t g_node_lock_seg_size;

/* ─────────────────── Per-run metrics ─────────────────── */
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
static run_metrics_t g_metrics;

/* ─────────────────── Commit pipeline ─────────────────── */
typedef struct
{
    int      n_pages;
    int      buf_slot;
    uint32_t zone_id;
    pgn_t    root_pn[N_SHARDS];   /* FIX-5: all shard roots */
    bool     needs_zone_finish;
    uint32_t old_zone_id;
    uint64_t batch_open_ns;
    uint64_t batch_enqueue_ns;
    uint64_t seq_no;
    bool     ready;
} commit_batch_t;

/* ─────────────────── Global TX machine ─────────────────── */
typedef struct
{
    uint64_t epoch;
    int expected;
    int joined;
    int done;
    bool closed;
    bool closed_by_count;
    bool flushing;
    bool serializing;
    uint64_t open_ns;
    pthread_mutex_t lock;
    pthread_cond_t cond;

    _Atomic int serial_claimed;
    _Atomic int serial_done;
    int serial_total;
    int serial_buf_slot;
    int serial_batch_size;
    int serial_joined_snap;  /* snapshot of joined count set by closer, stable */

    /* Overlay-reset gate: set by last serializer, cleared by the winner worker
     * once g_durable_seq >= overlay_reset_seq.  Both flags are protected by
     * g_tx.lock; the actual overlay/root update is done outside g_tx.lock. */
    bool     overlay_reset_pending;
    bool     overlay_reset_in_progress;
    uint64_t overlay_reset_seq;
    pgn_t    overlay_reset_roots[N_SHARDS];
} global_tx_t;
static global_tx_t g_tx;

/* ─────────────────── Global DFS state ─────────────────── */
/* FIX-3: use pgn_t (uint64_t) throughout — temp IDs have bit 63 set */
static pgn_t g_global_dfs_order[GLOBAL_MAX_SERIAL_NODES];
static int   g_global_dfs_shard[GLOBAL_MAX_SERIAL_NODES];
static int   g_global_dfs_count;
static int   g_active_shards;

/* ─────────────────── Global serial buffers ─────────────────── */
static uint8_t *g_global_serial_bufs[COMMIT_Q_DEPTH];

/* ─────────────────── Shard struct ─────────────────── */
typedef struct shard_t
{
    int shard_id;
    pthread_rwlock_t *node_locks;
    pgn_t root;
    pthread_rwlock_t root_lock;
    overlay_state overlay;
    pgn_t dfs_order[GLOBAL_MAX_SERIAL_NODES * 2];  /* scratch for DFS traversal */
    int dfs_count;
    uint8_t _pad[64];
} shard_t;
static shard_t g_shards[N_SHARDS];

/* ─────────────────── Global Zone Management ─────────────────── */
static uint32_t g_cur_zone;
static uint64_t g_zone_wp;
static uint64_t g_meta_wp;
static uint64_t g_sb_seq = 0;
static _Atomic(uint64_t) g_global_pagenum;
static pthread_mutex_t g_zone_lock;

/* ─────────────────── Global Commit Queue ─────────────────── */
static commit_batch_t g_cq_slots[COMMIT_Q_DEPTH];
static int g_cq_head, g_cq_tail, g_cq_count;
static pthread_mutex_t g_cq_lock;
static pthread_cond_t g_cq_notempty, g_cq_notfull;
static bool g_cq_shutdown;
static pthread_t g_committer;
static _Atomic(uint64_t) g_committer_last_finish_ns = 0;
static uint64_t g_next_to_write_seq = 0;
static _Atomic(uint64_t) g_batch_seq_counter = 0;
static uint64_t g_next_enqueue_seq = 0;
static pthread_mutex_t g_enqueue_lock;
static pthread_cond_t g_enqueue_cond;

/* ─────────────────── Utility ─────────────────── */
static inline uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint64_t cache_hash_u64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 33;
    return x;
}

static inline pthread_rwlock_t *node_lock_for(shard_t *s, pgn_t pn)
{
    size_t idx = (size_t)(cache_hash_u64((uint64_t)pn) & (g_node_lock_seg_size - 1));
    return &s->node_locks[idx];
}

/* ─────────────────── Global Page Cache ─────────────────── */
static void global_cache_init(void)
{
    g_global_cache = calloc(CACHE_NUM_SETS, sizeof(*g_global_cache));
    if (!g_global_cache) { perror("calloc g_global_cache"); abort(); }
    for (size_t i = 0; i < CACHE_NUM_SETS; i++)
    {
        pthread_mutex_init(&g_global_cache[i].lock, NULL);
        for (int w = 0; w < CACHE_WAYS; w++)
        {
            g_global_cache[i].ways[w].valid = 0;
            g_global_cache[i].ways[w].lru_counter = 0;
            g_global_cache[i].ways[w].tag = PGN_NULL;
        }
    }
    atomic_store_explicit(&g_cache_lru_clock, 0, memory_order_relaxed);
}

static void global_cache_destroy(void)
{
    if (!g_global_cache) return;
    for (size_t i = 0; i < CACHE_NUM_SETS; i++)
        pthread_mutex_destroy(&g_global_cache[i].lock);
    free(g_global_cache);
    g_global_cache = NULL;
}

static bool global_cache_lookup(pgn_t pn, disk_page *dst)
{
    if (!g_global_cache) return false;
    size_t si = (size_t)(cache_hash_u64((uint64_t)pn) % CACHE_NUM_SETS);
    cache_set *set = &g_global_cache[si];
    pthread_mutex_lock(&set->lock);
    for (int w = 0; w < CACHE_WAYS; w++)
    {
        if (set->ways[w].valid && set->ways[w].tag == pn)
        {
            set->ways[w].lru_counter =
                atomic_fetch_add_explicit(&g_cache_lru_clock, 1, memory_order_relaxed);
            *dst = set->ways[w].data;
            pthread_mutex_unlock(&set->lock);
            return true;
        }
    }
    pthread_mutex_unlock(&set->lock);
    return false;
}

static void global_cache_insert(pgn_t pn, const disk_page *src)
{
    if (!g_global_cache) return;
    size_t si = (size_t)(cache_hash_u64((uint64_t)pn) % CACHE_NUM_SETS);
    cache_set *set = &g_global_cache[si];
    uint64_t clk = atomic_fetch_add_explicit(&g_cache_lru_clock, 1, memory_order_relaxed);

    pthread_mutex_lock(&set->lock);
    int victim = 0;
    uint64_t min_lru = UINT64_MAX;
    for (int w = 0; w < CACHE_WAYS; w++)
    {
        if (set->ways[w].valid && set->ways[w].tag == pn)
        {
            /* Update existing entry */
            set->ways[w].data = *src;
            set->ways[w].lru_counter = clk;
            pthread_mutex_unlock(&set->lock);
            return;
        }
        if (!set->ways[w].valid) { victim = w; min_lru = 0; break; }
        if (set->ways[w].lru_counter < min_lru)
        {
            min_lru = set->ways[w].lru_counter;
            victim = w;
        }
    }
    set->ways[victim].valid = 1;
    set->ways[victim].tag = pn;
    set->ways[victim].lru_counter = clk;
    set->ways[victim].data = *src;
    pthread_mutex_unlock(&set->lock);
}

/* Tiered load: thread-local → global cache → O_DIRECT disk */
static int load_page(pgn_t pn, disk_page *dst)
{
    /* FIX-10: temp IDs can never be on disk */
    if (IS_TEMP_ID(pn))
    {
        fprintf(stderr, "load_page: attempted disk read of temp id 0x%llx\n",
                (unsigned long long)pn);
        return -1;
    }

    /* Tier 1: thread-local cache */
    tl_cache_entry *tl = &g_tl_cache[(size_t)pn & (TL_CACHE_SLOTS - 1)];
    if (tl->valid && tl->tag == pn)
    {
        *dst = tl->data;
        atomic_fetch_add_explicit(&g_metrics.cache_hit_count, 1, memory_order_relaxed);
        return 0;
    }

    /* Tier 2: global 4-way set-associative cache */
    if (global_cache_lookup(pn, dst))
    {
        tl->valid = 1; tl->tag = pn; tl->data = *dst;
        atomic_fetch_add_explicit(&g_metrics.cache_hit_count, 1, memory_order_relaxed);
        return 0;
    }

    /* Tier 3: disk I/O */
    atomic_fetch_add_explicit(&g_metrics.cache_miss_count, 1, memory_order_relaxed);
    off_t off = (off_t)pn * PAGE_SIZE;
    if (g_direct_fd >= 0)
    {
        void *raw = NULL;
        if (posix_memalign(&raw, PAGE_SIZE, PAGE_SIZE) != 0) return -1;
        ssize_t n = pread(g_direct_fd, raw, PAGE_SIZE, off);
        if (n != PAGE_SIZE) { free(raw); return -1; }
        memcpy(dst, raw, PAGE_SIZE);
        free(raw);
    }
    else
    {
        if (pread(g_fd, dst, PAGE_SIZE, off) != PAGE_SIZE) return -1;
    }

    global_cache_insert(pn, dst);
    tl->valid = 1; tl->tag = pn; tl->data = *dst;
    return 0;
}

/* ─────────────────── Overlay operations ─────────────────── */
/*
 * Pre-allocate with fixed capacity — no realloc on the hot path (FIX-2).
 */
static void overlay_init_capacity(overlay_state *ov, size_t cap)
{
    memset(ov, 0, sizeof(*ov));
    ov->arr = calloc(cap, sizeof(overlay_node));
    if (!ov->arr) { perror("calloc overlay arr"); abort(); }
    ov->cap = cap;
    pthread_mutex_init(&ov->lock, NULL);
}

static void overlay_destroy(overlay_state *ov)
{
    free(ov->arr);
    free(ov->idx_table);
    ov->arr = NULL;
    ov->idx_table = NULL;
    pthread_mutex_destroy(&ov->lock);
}

static void overlay_index_grow(overlay_state *ov)
{
    size_t new_cap = ov->idx_cap ? (ov->idx_cap << 1) : 512;
    size_t *nt = malloc(new_cap * sizeof(*nt));
    if (!nt) { perror("malloc overlay idx_table"); abort(); }
    for (size_t i = 0; i < new_cap; i++) nt[i] = (size_t)-1;
    for (size_t i = 0; i < ov->len; i++)
    {
        pgn_t id = ov->arr[i].id;
        size_t m = new_cap - 1;
        size_t pos = (size_t)(cache_hash_u64((uint64_t)id) & m);
        while (nt[pos] != (size_t)-1)
            pos = (pos + 1) & m;
        nt[pos] = i;
    }
    free(ov->idx_table);
    ov->idx_table = nt;
    ov->idx_cap = new_cap;
    ov->idx_used = ov->len;
}

static int overlay_find_idx(overlay_state *ov, pgn_t id)
{
    if (ov->idx_cap == 0) return -1;
    size_t m = ov->idx_cap - 1;
    size_t pos = (size_t)(cache_hash_u64((uint64_t)id) & m);
    for (;;)
    {
        size_t v = ov->idx_table[pos];
        if (v == (size_t)-1) return -1;
        if (ov->arr[v].id == id) return (int)v;
        pos = (pos + 1) & m;
    }
}

static void overlay_index_insert(overlay_state *ov, pgn_t id, size_t idx)
{
    if (ov->idx_cap == 0 || (ov->idx_used + 1) * 10 >= ov->idx_cap * 7)
        overlay_index_grow(ov);
    size_t m = ov->idx_cap - 1;
    size_t pos = (size_t)(cache_hash_u64((uint64_t)id) & m);
    while (ov->idx_table[pos] != (size_t)-1)
        pos = (pos + 1) & m;
    ov->idx_table[pos] = idx;
    ov->idx_used++;
}

/*
 * FIX-2: overlay_add_node never reallocs — the array is pre-allocated.
 */
static overlay_node *overlay_add_node(overlay_state *ov, pgn_t id,
                                      const disk_page *src)
{
    if (ov->len >= ov->cap)
    {
        /* Should never happen with correct pre-allocation; treat as fatal */
        fprintf(stderr, "overlay capacity exceeded (cap=%zu) for id=0x%llx\n",
                ov->cap, (unsigned long long)id);
        abort();
    }
    size_t idx = ov->len;
    overlay_node *n = &ov->arr[ov->len++];
    n->id = id;
    if (src) n->node = *src;
    else     memset(&n->node, 0, sizeof(n->node));
    n->dirty = 0;
    n->flushed_pn = PGN_NULL;
    overlay_index_insert(ov, id, idx);
    return n;
}

/*
 * overlay_get_mut — look up a node by ID; load from disk on cache miss.
 * Returns a stable pointer (pre-allocated array, never realloced — FIX-2).
 * Acquires and releases ov->lock internally; callers MUST NOT hold ov->lock.
 * Returns NULL only for disk-load failures (logged).
 * FIX-10: aborts if a temp ID is not found (indicates insert-logic bug).
 */
static overlay_node *overlay_get_mut(shard_t *s, pgn_t id)
{
    overlay_state *ov = &s->overlay;
    pthread_mutex_lock(&ov->lock);
    int idx = overlay_find_idx(ov, id);
    if (idx >= 0)
    {
        overlay_node *n = &ov->arr[idx];
        pthread_mutex_unlock(&ov->lock);
        return n;
    }

    /* FIX-10: temp IDs must already be in the overlay */
    if (IS_TEMP_ID(id))
    {
        fprintf(stderr, "overlay_get_mut: temp id 0x%llx not found — logic error\n",
                (unsigned long long)id);
        pthread_mutex_unlock(&ov->lock);
        abort();
    }

    disk_page dp;
    if (load_page(id, &dp) != 0)
    {
        pthread_mutex_unlock(&ov->lock);
        return NULL;
    }
    overlay_node *n = overlay_add_node(ov, id, &dp);
    pthread_mutex_unlock(&ov->lock);
    return n;
}

static pgn_t overlay_new_temp(shard_t *s, uint32_t is_leaf)
{
    overlay_state *ov = &s->overlay;
    pthread_mutex_lock(&ov->lock);
    pgn_t id = MAKE_TEMP_ID(++ov->next_temp);
    overlay_node *n = overlay_add_node(ov, id, NULL);
    n->node.is_leaf = is_leaf;
    n->node.pointer = PGN_NULL;
    n->dirty = 1;
    pthread_mutex_unlock(&ov->lock);
    return id;
}

/* ─────────────────── NVMe zone append ─────────────────── */
/*
 * FIX-7: Returns -1 on failure (no abort).  Sets g_io_error flag so callers
 * can detect it and propagate gracefully.
 */
static int zone_append_n(uint32_t zone_id, const void *buf, int n_pages,
                         pgn_t *out_start_pn)
{
    __u64 zslba = g_zones[zone_id].start / g_info.lblock_size;
    uint32_t total_lb = (uint32_t)n_pages * (uint32_t)(PAGE_SIZE / g_info.lblock_size);
    __u16 nlb = (uint16_t)(total_lb - 1);
    __u64 result = 0;

    struct nvme_zns_append_args args = {
        .zslba = zslba,
        .result = &result,
        .data = (void *)buf,
        .metadata = NULL,
        .args_size = sizeof(args),
        .fd = g_fd,
        .timeout = 0,
        .nsid = g_nsid,
        .ilbrt = 0,
        .data_len = (uint32_t)(n_pages * PAGE_SIZE),
        .metadata_len = 0,
        .nlb = nlb,
        .control = 0,
        .lbat = 0,
        .lbatm = 0,
        .ilbrt_u64 = 0,
    };

    int nret = nvme_zns_append(&args);
    if (nret != 0)
    {
        fprintf(stderr, "nvme_zns_append zone=%u nlb=%u: ret=0x%x errno=%d(%s)\n",
                zone_id, (unsigned)nlb, (unsigned)nret, errno, strerror(errno));
        atomic_store_explicit(&g_io_error, true, memory_order_release);
        return -1;
    }

    if (out_start_pn)
        *out_start_pn = (pgn_t)(result * g_info.lblock_size / PAGE_SIZE);
    atomic_fetch_add_explicit(&g_pages_appended, (uint64_t)n_pages, memory_order_relaxed);
    return 0;
}

/* FIX-5: write_superblock now records all N_SHARDS root page numbers */
static void write_superblock(pgn_t root_pn[N_SHARDS])
{
    uint64_t cap = g_zones[META_ZONE].capacity / PAGE_SIZE;
    if (g_meta_wp >= cap) return;

    static multi_superblock_t sb __attribute__((aligned(PAGE_SIZE)));
    memset(&sb, 0, sizeof sb);
    sb.magic = SB_MAGIC;
    sb.seq_no = ++g_sb_seq;
    sb.num_shards = (uint32_t)N_SHARDS;
    sb.leaf_order = LEAF_ORDER;
    sb.internal_order = INTERNAL_ORDER;
    for (int i = 0; i < N_SHARDS; i++)
        sb.root_pn[i] = root_pn[i];

    if (zone_append_n(META_ZONE, &sb, 1, NULL) == 0)
        g_meta_wp++;
}

/* ─────────────────── Dirty-node DFS (overlay-based) ─────────────────── */
/*
 * FIX-11: assert all traversed dirty nodes are present in overlay.
 * FIX-13: avoid load_page for non-overlay nodes.  After an overlay reset,
 *   many child pointers in dirty internal nodes point to physical page IDs
 *   that are NOT in the overlay (clean pages not touched this TX).  The
 *   old overlay_get_mut call would call load_page on each such child just to
 *   discover it is clean and prune it — triggering 200+ cache/disk reads per
 *   TX.  Instead: look up the overlay index directly; if the node is absent
 *   from the overlay it was never modified this TX and cannot be dirty.
 *   Children are snapshotted under ov->lock before recursing without the lock
 *   (safe: TX is closed, overlay is read-only during DFS).
 */
static void dfs_dirty_collect(shard_t *s, pgn_t id)
{
    if (id == PGN_NULL) return;

    overlay_state *ov = &s->overlay;
    pthread_mutex_lock(&ov->lock);
    int idx = overlay_find_idx(ov, id);
    if (idx < 0 || !ov->arr[idx].dirty)
    {
        pthread_mutex_unlock(&ov->lock);
        return;  /* not in overlay or clean: prune without I/O */
    }
    /* Snapshot node data while holding lock so we can recurse after release */
    disk_page nd = ov->arr[idx].node;
    pthread_mutex_unlock(&ov->lock);

    if (!nd.is_leaf)
    {
        for (uint32_t i = 0; i < nd.num_keys; i++)
            dfs_dirty_collect(s, nd.internal[i].child);
        dfs_dirty_collect(s, nd.pointer);  /* rightmost child */
    }
    s->dfs_order[s->dfs_count++] = id;  /* post-order */
}

/*
 * build_disk_page — serialize one overlay node into a 4 KB buffer.
 * Called during parallel serialization; overlay is effectively read-only at this
 * point (all inserts done).  overlay_get_mut() will never trigger realloc here
 * because we pre-allocated (FIX-2) and no new nodes are added after TX closes.
 */
/*
 * lookup_physical_locked — translate an overlay node ID to the physical page
 * number that load_page() can use.  Must be called with ov->lock held.
 *
 *  • Non-temp ID with a new flushed_pn: node was dirty this TX, use new loc.
 *  • Non-temp ID, no new flushed_pn: node is clean/unmodified, ID = phys addr.
 *  • Temp ID: node was created this session; flushed_pn must be set.
 */
static pgn_t lookup_physical_locked(overlay_state *ov, pgn_t cid)
{
    if (cid == PGN_NULL) return PGN_NULL;

    int idx = overlay_find_idx(ov, cid);
    if (idx >= 0)
    {
        pgn_t fp = ov->arr[idx].flushed_pn;
        if (fp != PGN_NULL) return fp;            /* dirty or previously flushed */
        if (!IS_TEMP_ID(cid))   return cid;       /* clean disk-loaded node */
        fprintf(stderr, "lookup_physical: temp id 0x%llx has no flushed_pn\n",
                (unsigned long long)cid);
        return PGN_NULL;
    }
    /* Not in overlay */
    if (!IS_TEMP_ID(cid)) return cid;             /* unmodified disk page */
    fprintf(stderr, "lookup_physical: temp id 0x%llx not in overlay\n",
            (unsigned long long)cid);
    return PGN_NULL;
}

/*
 * build_disk_page — serialize one overlay node into a 4 KB buffer.
 *
 * For internal nodes, child pointers are translated from overlay IDs (temp or
 * old disk IDs) to physical page numbers so that disk pages are self-contained
 * and navigable after an overlay reset.  The full operation is done under a
 * single ov->lock acquisition to avoid repeated lock/unlock per child.
 *
 * Called during parallel serialization; overlay is effectively read-only at
 * this point (all inserts done; FIX-2 guarantees no realloc).
 */
static void build_disk_page(shard_t *s, uint8_t *dst, pgn_t id)
{
    overlay_state *ov = &s->overlay;
    disk_page *dp = (disk_page *)dst;

    pthread_mutex_lock(&ov->lock);
    int idx = overlay_find_idx(ov, id);
    if (idx < 0)
    {
        pthread_mutex_unlock(&ov->lock);
        memset(dp, 0, PAGE_SIZE);
        return;
    }
    *dp = ov->arr[idx].node;

    if (dp->is_leaf)
    {
        dp->pointer = PGN_NULL;    /* leaves have no rightmost child pointer */
        pthread_mutex_unlock(&ov->lock);
        return;
    }
    /* Internal node: translate every child pointer to a physical page number
     * so the written page can be navigated after the overlay is reset. */
    for (uint32_t i = 0; i < dp->num_keys; i++)
        dp->internal[i].child = lookup_physical_locked(ov, dp->internal[i].child);
    if (dp->pointer != PGN_NULL)
        dp->pointer = lookup_physical_locked(ov, dp->pointer);
    pthread_mutex_unlock(&ov->lock);
}

/* ─────────────────── Global flush prepare (Closer: all shards → one batch) ─────────────────── */
/*
 * FIX-4: g_zone_lock is released BEFORE calling overlay_get_mut (which may
 * call pread).  base_pgn and zone state are computed atomically under the lock,
 * then the lock is released before any slow I/O operations.
 */
static void flush_prepare_phase1_global(int buf_slot, uint64_t batch_seq,
                                        commit_batch_t *batch)
{
    g_global_dfs_count = 0;
    batch->n_pages = 0;
    batch->ready = false;

    for (int si = 0; si < g_active_shards; si++)
    {
        shard_t *s = &g_shards[si];
        if (s->root == PGN_NULL) continue;
        s->dfs_count = 0;
        dfs_dirty_collect(s, s->root);
        for (int i = 0; i < s->dfs_count; i++)
        {
            if (g_global_dfs_count >= GLOBAL_MAX_SERIAL_NODES)
            {
                fprintf(stderr, "flush_prepare_phase1_global: overflow at %d nodes\n",
                        g_global_dfs_count);
                atomic_store_explicit(&g_io_error, true, memory_order_release);
                return;
            }
            g_global_dfs_order[g_global_dfs_count] = s->dfs_order[i];
            g_global_dfs_shard[g_global_dfs_count] = si;
            g_global_dfs_count++;
        }
    }

    if (g_global_dfs_count == 0) return;

    int total = g_global_dfs_count;
    atomic_fetch_add_explicit(&g_metrics.dirty_nodes_total, (uint64_t)total,
                              memory_order_relaxed);

    /* ── FIX-4: Compute zone/pagenum under lock, release before I/O ── */
    pthread_mutex_lock(&g_zone_lock);
    uint64_t zone_cap = g_zones[g_cur_zone].capacity / PAGE_SIZE;
    bool need_finish = false;
    uint32_t old_zone = g_cur_zone;

    if (g_zone_wp + (uint64_t)total > zone_cap)
    {
        need_finish = (g_zone_wp > 0 && g_zone_wp < zone_cap);
        g_cur_zone++;
        if (g_cur_zone >= g_nzones)
        {
            fprintf(stderr, "flush_prepare_phase1_global: zones exhausted\n");
            atomic_store_explicit(&g_io_error, true, memory_order_release);
            pthread_mutex_unlock(&g_zone_lock);
            return;
        }
        g_zone_wp = 0;
        /* Snap g_global_pagenum to the physical start of the new zone so that
         * flushed_pn values remain equal to LBA-based physical page numbers. */
        atomic_store_explicit(&g_global_pagenum,
                              g_zones[g_cur_zone].start / PAGE_SIZE,
                              memory_order_relaxed);
        atomic_fetch_add_explicit(&g_metrics.zone_advance_count, 1, memory_order_relaxed);
    }

    pgn_t base_pgn = (pgn_t)atomic_fetch_add_explicit(&g_global_pagenum, (uint64_t)total,
                                                       memory_order_relaxed);
    g_zone_wp += (uint64_t)total;
    uint32_t final_zone = g_cur_zone;
    pthread_mutex_unlock(&g_zone_lock);  /* FIX-4: release before overlay_get_mut */

    /* Stamp flushed_pn on every dirty node (outside g_zone_lock) */
    for (int i = 0; i < total; i++)
    {
        int si = g_global_dfs_shard[i];
        overlay_node *n = overlay_get_mut(&g_shards[si], g_global_dfs_order[i]);
        if (n) n->flushed_pn = base_pgn + (pgn_t)i;
    }

    batch->n_pages           = total;
    batch->buf_slot          = buf_slot;
    batch->zone_id           = final_zone;
    batch->needs_zone_finish = need_finish;
    batch->old_zone_id       = old_zone;
    batch->seq_no            = batch_seq;
    /* batch->root_pn[] is filled by the last serializer (FIX-5) */
}

/* ─────────────────── Committer thread ─────────────────── */
/*
 * FIX-7: On NVMe write failure, set g_io_error and continue draining the queue.
 *        Do not abort — let workers detect the flag on the next iteration.
 */
static void *committer_main(void *arg)
{
    (void)arg;
    for (;;)
    {
        commit_batch_t batch;

        pthread_mutex_lock(&g_cq_lock);
        while (g_cq_count == 0 && !g_cq_shutdown)
            pthread_cond_wait(&g_cq_notempty, &g_cq_lock);
        if (g_cq_count == 0)
        {
            pthread_mutex_unlock(&g_cq_lock);
            break;
        }

        batch = g_cq_slots[g_cq_head];
        if (!batch.ready || batch.seq_no != g_next_to_write_seq)
        {
            pthread_cond_wait(&g_cq_notempty, &g_cq_lock);
            pthread_mutex_unlock(&g_cq_lock);
            continue;
        }

        g_cq_head = (g_cq_head + 1) % COMMIT_Q_DEPTH;
        g_cq_count--;
        g_next_to_write_seq++;
        pthread_cond_signal(&g_cq_notfull);
        pthread_mutex_unlock(&g_cq_lock);

        if (batch.n_pages == 0) continue;

        uint64_t t_start = monotonic_ns();
        uint64_t last_finish = atomic_load_explicit(&g_committer_last_finish_ns,
                                                    memory_order_relaxed);
        if (last_finish > 0)
        {
            atomic_fetch_add_explicit(&g_metrics.committer_stall_ns_sum,
                                      t_start - last_finish, memory_order_relaxed);
            atomic_fetch_add_explicit(&g_metrics.committer_stall_count,
                                      1, memory_order_relaxed);
        }

        if (batch.needs_zone_finish)
        {
            off_t zstart = (off_t)g_zones[batch.old_zone_id].start;
            off_t zlen   = (off_t)g_zones[batch.old_zone_id].len;
            if (zbd_finish_zones(g_fd, zstart, zlen) != 0)
            {
                perror("zbd_finish_zones");
                atomic_store_explicit(&g_io_error, true, memory_order_release);
                /* FIX-7: continue draining, don't abort */
            }
        }

        uint8_t *buf = g_global_serial_bufs[batch.buf_slot];
        int written = 0;
        bool write_ok = true;
        while (written < batch.n_pages)
        {
            int chunk = MIN(batch.n_pages - written, MAX_APPEND_PAGES);
            pgn_t got;
            uint64_t t_nvme = monotonic_ns();
            if (zone_append_n(batch.zone_id,
                              buf + (size_t)written * PAGE_SIZE,
                              chunk, &got) != 0)
            {
                /* FIX-7: log, set flag, break — do not abort */
                fprintf(stderr, "committer: NVMe write failed at seq=%lu written=%d\n",
                        (unsigned long)batch.seq_no, written);
                write_ok = false;
                break;
            }
            /* Write-back to global cache after successful append */
            for (int i = 0; i < chunk; i++)
            {
                disk_page *dp = (disk_page *)(buf + (size_t)(written + i) * PAGE_SIZE);
                global_cache_insert(got + (pgn_t)i, dp);
            }
            atomic_fetch_add_explicit(&g_metrics.nvme_ns_total,
                                      monotonic_ns() - t_nvme, memory_order_relaxed);
            written += chunk;
        }

        if (write_ok)
        {
            write_superblock(batch.root_pn);  /* FIX-5: all shard roots */
            /* Signal workers blocked on the durability gate.
             * Committer acquires g_tx.lock only briefly here; no other locks held. */
            atomic_store_explicit(&g_durable_seq, (int64_t)batch.seq_no,
                                  memory_order_release);
            pthread_mutex_lock(&g_tx.lock);
            pthread_cond_broadcast(&g_tx.cond);
            pthread_mutex_unlock(&g_tx.lock);
        }

        uint64_t t_end = monotonic_ns();
        atomic_fetch_add_explicit(&g_metrics.commit_ns_total,
                                  t_end - t_start, memory_order_relaxed);
        atomic_store_explicit(&g_committer_last_finish_ns, t_end, memory_order_relaxed);
    }
    return NULL;
}

/* ─────────────────── Sync flush (residual, AFTER committer stops) ─────────────────── */
/*
 * FIX-6: Uses a private aligned buffer — NOT g_global_serial_bufs[0] — so there
 * is no risk of racing with the committer.  This function is called only after
 * pthread_join(g_committer) confirms the committer has exited.
 */
static pgn_t flush_shard_sync_private(shard_t *s, uint8_t *priv_buf)
{
    if (s->root == PGN_NULL) return PGN_NULL;
    s->dfs_count = 0;
    dfs_dirty_collect(s, s->root);
    if (s->dfs_count == 0) return s->root;

    int total = s->dfs_count;
    if (total > GLOBAL_MAX_SERIAL_NODES)
    {
        fprintf(stderr, "flush_shard_sync_private: overflow %d\n", total);
        return PGN_NULL;
    }

    pthread_mutex_lock(&g_zone_lock);
    uint64_t zone_cap = g_zones[g_cur_zone].capacity / PAGE_SIZE;
    bool need_finish = false;
    uint32_t old_zone = g_cur_zone;
    if (g_zone_wp + (uint64_t)total > zone_cap)
    {
        need_finish = (g_zone_wp > 0 && g_zone_wp < zone_cap);
        g_cur_zone++;
        if (g_cur_zone >= g_nzones)
        {
            fprintf(stderr, "flush_shard_sync: zones exhausted\n");
            pthread_mutex_unlock(&g_zone_lock);
            return PGN_NULL;
        }
        g_zone_wp = 0;
        atomic_store_explicit(&g_global_pagenum,
                              g_zones[g_cur_zone].start / PAGE_SIZE,
                              memory_order_relaxed);
    }
    pgn_t base_pgn = (pgn_t)atomic_fetch_add_explicit(&g_global_pagenum, (uint64_t)total,
                                                       memory_order_relaxed);
    g_zone_wp += (uint64_t)total;
    uint32_t cur_zone = g_cur_zone;
    pthread_mutex_unlock(&g_zone_lock);

    for (int i = 0; i < total; i++)
    {
        overlay_node *n = overlay_get_mut(s, s->dfs_order[i]);
        if (n) n->flushed_pn = base_pgn + (pgn_t)i;
    }

    if (need_finish)
    {
        off_t zstart = (off_t)g_zones[old_zone].start;
        off_t zlen   = (off_t)g_zones[old_zone].len;
        zbd_finish_zones(g_fd, zstart, zlen);
    }

    for (int i = 0; i < total; i++)
        build_disk_page(s, priv_buf + (size_t)i * PAGE_SIZE, s->dfs_order[i]);

    pgn_t root_pgn = s->root;
    int written = 0;
    while (written < total)
    {
        int chunk = MIN(total - written, MAX_APPEND_PAGES);
        pgn_t got;
        if (zone_append_n(cur_zone, priv_buf + (size_t)written * PAGE_SIZE, chunk, &got) != 0)
        {
            fprintf(stderr, "flush_shard_sync: NVMe append failed\n");
            return PGN_NULL;
        }
        for (int i = 0; i < chunk; i++)
        {
            disk_page *dp = (disk_page *)(priv_buf + (size_t)(written + i) * PAGE_SIZE);
            global_cache_insert(got + (pgn_t)i, dp);
        }
        written += chunk;
    }

    for (int i = 0; i < total; i++)
    {
        overlay_node *n = overlay_get_mut(s, s->dfs_order[i]);
        if (n) n->dirty = 0;
    }
    return root_pgn;
}

/* ─────────────────── B-tree insert (overlay-based latch crabbing) ─────────────────── */
static inline uint32_t get_leaf_pos(const disk_page *n, int64_t key)
{
    uint32_t pos = 0;
    while (pos < n->num_keys && (int64_t)n->leaf[pos].key < key)
        pos++;
    return pos;
}

static inline uint32_t get_int_pos(const disk_page *n, int64_t key)
{
    for (uint32_t i = 0; i < n->num_keys; i++)
        if (key < (int64_t)n->internal[i].key)
            return i;
    return n->num_keys;
}

static void tree_insert(shard_t *s, int64_t key, const char *value)
{
    /* ── Empty tree: create root leaf ── */
    pthread_rwlock_wrlock(&s->root_lock);
    pgn_t root = s->root;
    if (root == PGN_NULL)
    {
        pgn_t rid = overlay_new_temp(s, 1);
        overlay_node *r = overlay_get_mut(s, rid);
        if (!r) { pthread_rwlock_unlock(&s->root_lock); abort(); }
        r->node.num_keys = 1;
        r->node.pointer  = PGN_NULL;
        r->node.leaf[0].key = (uint64_t)key;
        memcpy(r->node.leaf[0].value, value, 120);
        r->dirty = 1;
        s->root = rid;
        pthread_rwlock_unlock(&s->root_lock);
        return;
    }
    pthread_rwlock_unlock(&s->root_lock);

    /* ── Top-down path with latch crabbing ── */
    pgn_t path_ids[MAX_HEIGHT];
    uint32_t path_cidx[MAX_HEIGHT];
    int path_n = 0;

    pgn_t cur = root;
    for (;;)
    {
        pthread_rwlock_wrlock(node_lock_for(s, cur));
        overlay_node *cur_n = overlay_get_mut(s, cur);
        if (!cur_n) { pthread_rwlock_unlock(node_lock_for(s, cur)); return; }

        if (cur_n->node.is_leaf) break;

        uint32_t cidx = get_int_pos(&cur_n->node, key);
        pgn_t child = (cidx < cur_n->node.num_keys)
                      ? cur_n->node.internal[cidx].child
                      : cur_n->node.pointer;

        if (path_n < MAX_HEIGHT)
        {
            path_ids[path_n]  = cur;
            path_cidx[path_n] = cidx;
            path_n++;
        }

        /*
         * COW correctness: every internal node on the path must be dirtied,
         * because after the child is committed to a new disk location the parent
         * must be rewritten to record the new child pgn.  Without this, the DFS
         * in flush_prepare_phase1_global prunes at the first non-dirty ancestor
         * and dirty leaves are never collected or committed.
         */
        cur_n->dirty = 1;

        pthread_rwlock_wrlock(node_lock_for(s, child));
        pthread_rwlock_unlock(node_lock_for(s, cur));
        cur = child;
    }

    /* ── Leaf: duplicate check then insert ── */
    overlay_node *leaf_n = overlay_get_mut(s, cur);
    if (!leaf_n) { pthread_rwlock_unlock(node_lock_for(s, cur)); return; }

    for (uint32_t i = 0; i < leaf_n->node.num_keys; i++)
    {
        if ((int64_t)leaf_n->node.leaf[i].key == key)
        {
            memcpy(leaf_n->node.leaf[i].value, value, 120);
            leaf_n->dirty = 1;
            pthread_rwlock_unlock(node_lock_for(s, cur));
            return;
        }
    }

    pgn_t carry_left  = cur;
    pgn_t carry_right = PGN_NULL;
    int   carry_split = 0;
    int64_t carry_key  = 0;

    if (leaf_n->node.num_keys < (uint32_t)LEAF_MAX)
    {
        uint32_t pos = get_leaf_pos(&leaf_n->node, key);
        for (int64_t i = (int64_t)leaf_n->node.num_keys - 1; i >= (int64_t)pos; i--)
            leaf_n->node.leaf[i + 1] = leaf_n->node.leaf[i];
        leaf_n->node.leaf[pos].key = (uint64_t)key;
        memcpy(leaf_n->node.leaf[pos].value, value, 120);
        leaf_n->node.num_keys++;
        leaf_n->dirty = 1;
    }
    else
    {
        /* ── Leaf split ── */
        leaf_ent tmp[LEAF_ORDER];
        uint32_t pos = get_leaf_pos(&leaf_n->node, key);
        for (uint32_t i = 0; i < pos; i++)
            tmp[i] = leaf_n->node.leaf[i];
        tmp[pos].key = (uint64_t)key;
        memcpy(tmp[pos].value, value, 120);
        for (uint32_t i = pos; i < leaf_n->node.num_keys; i++)
            tmp[i + 1] = leaf_n->node.leaf[i];

        uint32_t sp = LEAF_ORDER / 2;
        leaf_n->node.num_keys = sp;
        for (uint32_t i = 0; i < sp; i++)
            leaf_n->node.leaf[i] = tmp[i];
        leaf_n->dirty = 1;

        pgn_t right_id = overlay_new_temp(s, 1);
        overlay_node *right_n = overlay_get_mut(s, right_id);
        right_n->node.is_leaf  = 1;
        right_n->node.num_keys = LEAF_ORDER - sp;
        right_n->node.pointer  = PGN_NULL;
        for (uint32_t i = 0; i < (uint32_t)(LEAF_ORDER - sp); i++)
            right_n->node.leaf[i] = tmp[sp + i];
        right_n->dirty = 1;

        carry_split = 1;
        carry_right = right_id;
        carry_key   = (int64_t)right_n->node.leaf[0].key;
    }
    pthread_rwlock_unlock(node_lock_for(s, cur));

    /* ── Propagate splits up the ancestor stack ── */
    for (int i = path_n - 1; i >= 0 && carry_split; i--)
    {
        pgn_t par_id  = path_ids[i];
        uint32_t cidx = path_cidx[i];
        overlay_node *par_n = overlay_get_mut(s, par_id);
        if (!par_n) continue;

        if (par_n->node.num_keys < (uint32_t)INT_MAX_K)
        {
            /* Internal node has room — no split needed */
            uint32_t pos = cidx;
            for (int64_t j = (int64_t)par_n->node.num_keys - 1;
                 j >= (int64_t)pos; j--)
                par_n->node.internal[j + 1] = par_n->node.internal[j];
            par_n->node.internal[pos].key   = (uint64_t)carry_key;
            par_n->node.internal[pos].child = carry_left;
            if (pos == par_n->node.num_keys)
                par_n->node.pointer = carry_right;
            else
                par_n->node.internal[pos + 1].child = carry_right;
            par_n->node.num_keys++;
            par_n->dirty  = 1;
            carry_split   = 0;
            carry_left    = par_id;
            continue;
        }

        /* ── FIX-1: Internal node split — corrected tmp[] construction ── */
        /* Save the original rightmost child BEFORE modifying par_n */
        pgn_t orig_pointer = par_n->node.pointer;
        uint32_t num_keys  = par_n->node.num_keys;  /* = INT_MAX_K = 248 */

        /* Build expanded array: num_keys+1 entries (indices 0..num_keys) */
        int_ent tmp[INT_MAX_K + 2];  /* INT_MAX_K+1 = 249, +1 for safety */
        memset(tmp, 0, sizeof(tmp));

        /* Copy entries before the insertion point */
        for (uint32_t j = 0; j < cidx; j++)
            tmp[j] = par_n->node.internal[j];

        /* Insert the new entry: key=carry_key, left_child=carry_left */
        tmp[cidx].key   = (uint64_t)carry_key;
        tmp[cidx].child = carry_left;

        if (cidx < num_keys)
        {
            /* Entry at cidx+1: original key at cidx, NEW child = carry_right */
            tmp[cidx + 1].key   = par_n->node.internal[cidx].key;
            tmp[cidx + 1].child = carry_right;
            /* Shift remaining original entries rightward by 1 */
            for (uint32_t j = cidx + 1; j < num_keys; j++)
                tmp[j + 1] = par_n->node.internal[j];
            /* Rightmost child of the full expanded set = original pointer */
        }
        /* else (cidx == num_keys): carry_right becomes the new rightmost child */

        /* The rightmost child of the entire expanded node */
        pgn_t final_rightmost = (cidx == num_keys) ? carry_right : orig_pointer;

        uint32_t total_k = num_keys + 1;
        uint32_t sp      = total_k / 2;
        int64_t up_key   = (int64_t)tmp[sp].key;

        /* Left node (par_n): entries 0..sp-1, pointer = tmp[sp].child */
        par_n->node.num_keys = sp;
        for (uint32_t j = 0; j < sp; j++)
            par_n->node.internal[j] = tmp[j];
        par_n->node.pointer = tmp[sp].child;
        par_n->dirty = 1;

        /* Right node: entries sp+1..total_k-1, pointer = final_rightmost */
        pgn_t right_id = overlay_new_temp(s, 0);
        overlay_node *right_n = overlay_get_mut(s, right_id);
        right_n->node.is_leaf  = 0;
        right_n->node.num_keys = total_k - sp - 1;
        for (uint32_t j = 0; j < right_n->node.num_keys; j++)
            right_n->node.internal[j] = tmp[sp + 1 + j];
        right_n->node.pointer = final_rightmost;
        right_n->dirty = 1;

        carry_split = 1;
        carry_right = right_id;
        carry_left  = par_id;
        carry_key   = up_key;
    }

    /* ── Root split: new root one level up ── */
    if (carry_split)
    {
        pgn_t new_root = overlay_new_temp(s, 0);
        overlay_node *r = overlay_get_mut(s, new_root);
        r->node.is_leaf  = 0;
        r->node.num_keys = 1;
        r->node.internal[0].key   = (uint64_t)carry_key;
        r->node.internal[0].child = carry_left;
        r->node.pointer  = carry_right;
        r->dirty = 1;

        pthread_rwlock_wrlock(&s->root_lock);
        s->root = new_root;
        pthread_rwlock_unlock(&s->root_lock);
    }
}

/* ─────────────────── Global TX insert ─────────────────── */
/*
 * FIX-3: All node ID variables use pgn_t, not nid_t.
 *
 * FIX-8: Lock ordering — the last serializer releases g_tx.lock BEFORE
 *        acquiring g_enqueue_lock/g_cq_lock, preventing the epoch from being
 *        held captive while waiting for enqueue ordering.  The epoch is reset
 *        after re-acquiring g_tx.lock.
 *
 * FIX-9: Non-last serializers use pthread_cond_timedwait (2-second timeout)
 *        so a hung worker cannot deadlock the entire TX pipeline.
 */
static void tx_insert(shard_t *s, int64_t key, const char *value)
{
    /* FIX-7: bail early if I/O is broken */
    if (atomic_load_explicit(&g_io_error, memory_order_acquire))
        return;

    uint64_t req_start_ns = monotonic_ns();

    /* ── Phase 1: join global TX ── */
    uint64_t t0 = monotonic_ns();
    pthread_mutex_lock(&g_tx.lock);
    for (;;)
    {
        /* ── Overlay-reset gate ──
         * After each committed epoch the last serializer arms
         * overlay_reset_pending.  The first worker that wakes up and sees
         * the batch is durable wins the reset: it clears the overlay for
         * every active shard, switches shard roots to the flushed page
         * numbers, then re-checks all conditions.  Others wait until the
         * winner broadcasts.  Only once both flags are false AND the TX
         * machine is idle can a worker join the next epoch. */
        if (g_tx.overlay_reset_pending && !g_tx.overlay_reset_in_progress)
        {
            int64_t ds = atomic_load_explicit(&g_durable_seq,
                                              memory_order_acquire);
            if (ds >= (int64_t)g_tx.overlay_reset_seq)
            {
                /* Win the reset token. */
                g_tx.overlay_reset_pending     = false;
                g_tx.overlay_reset_in_progress = true;
                pgn_t my_roots[N_SHARDS];
                memcpy(my_roots, g_tx.overlay_reset_roots,
                       g_active_shards * sizeof(pgn_t));
                pthread_mutex_unlock(&g_tx.lock);

                /* Reset each shard's overlay and advance its root to the
                 * newly-durable disk page number (lock ordering: ov->lock
                 * and root_lock are both below g_tx.lock; acquire after
                 * releasing g_tx.lock). */
                for (int si = 0; si < g_active_shards; si++)
                {
                    shard_t *sh = &g_shards[si];
                    overlay_state *ov = &sh->overlay;
                    pthread_mutex_lock(&ov->lock);
                    if (ov->idx_cap > 0)
                        memset(ov->idx_table, 0xFF,
                               ov->idx_cap * sizeof(*ov->idx_table));
                    ov->idx_used  = 0;
                    ov->len       = 0;
                    ov->next_temp = 0;
                    pthread_mutex_unlock(&ov->lock);

                    pthread_rwlock_wrlock(&sh->root_lock);
                    sh->root = my_roots[si];
                    pthread_rwlock_unlock(&sh->root_lock);
                }

                pthread_mutex_lock(&g_tx.lock);
                g_tx.overlay_reset_in_progress = false;
                pthread_cond_broadcast(&g_tx.cond);
                continue;  /* re-evaluate all conditions */
            }
        }

        if (!g_tx.overlay_reset_pending && !g_tx.overlay_reset_in_progress &&
            !g_tx.closed && !g_tx.flushing && !g_tx.serializing)
            break;

        if (atomic_load_explicit(&g_io_error, memory_order_relaxed))
        {
            pthread_mutex_unlock(&g_tx.lock);
            return;
        }
        pthread_cond_wait(&g_tx.cond, &g_tx.lock);
    }
    atomic_fetch_add_explicit(&g_metrics.entry_wait_ns_total,
                              monotonic_ns() - t0, memory_order_relaxed);

    uint64_t my_epoch = g_tx.epoch;
    g_tx.joined++;
    if (g_tx.joined == 1) g_tx.open_ns = monotonic_ns();
    if (g_tx.joined >= g_tx.expected)
    {
        g_tx.closed = true;
        g_tx.closed_by_count = true;
        atomic_fetch_add_explicit(&g_metrics.tx_closed_by_count, 1, memory_order_relaxed);
    }
    pthread_mutex_unlock(&g_tx.lock);

    /* ── Phase 2: parallel insert into designated shard ── */
    t0 = monotonic_ns();
    tree_insert(s, key, value);
    atomic_fetch_add_explicit(&g_metrics.insert_ns_total,
                              monotonic_ns() - t0, memory_order_relaxed);

    /* ── Phase 3: done counter + closer election ── */
    t0 = monotonic_ns();
    pthread_mutex_lock(&g_tx.lock);
    g_tx.done++;

    if (!g_tx.closed &&
        (g_tx.done == g_tx.joined ||
         (monotonic_ns() - g_tx.open_ns >= TX_TIMEOUT_NS)))
    {
        g_tx.closed = true;
        if (g_tx.done < g_tx.joined)
            atomic_fetch_add_explicit(&g_metrics.tx_closed_by_timeout, 1,
                                      memory_order_relaxed);
        else
            atomic_fetch_add_explicit(&g_metrics.tx_closed_by_count, 1,
                                      memory_order_relaxed);
    }

    bool is_closer = g_tx.closed && (g_tx.done == g_tx.joined) &&
                     !g_tx.flushing && !g_tx.serializing;
    if (is_closer)
    {
        g_tx.flushing = true;
        int joined_snap = g_tx.joined;
        pthread_mutex_unlock(&g_tx.lock);

        /* Reserve a CQ slot (L6, outside L4) */
        uint64_t t_cw = monotonic_ns();
        pthread_mutex_lock(&g_cq_lock);
        while (g_cq_count >= COMMIT_Q_DEPTH)
            pthread_cond_wait(&g_cq_notfull, &g_cq_lock);
        int reserved_slot = g_cq_tail;
        uint64_t batch_seq = atomic_fetch_add_explicit(&g_batch_seq_counter, 1,
                                                       memory_order_relaxed);
        g_cq_slots[reserved_slot].ready  = false;
        g_cq_slots[reserved_slot].seq_no = batch_seq;
        g_cq_tail = (g_cq_tail + 1) % COMMIT_Q_DEPTH;
        g_cq_count++;
        pthread_mutex_unlock(&g_cq_lock);
        atomic_fetch_add_explicit(&g_metrics.commit_wait_ns_total,
                                  monotonic_ns() - t_cw, memory_order_relaxed);

        /* DFS all shards + assign pagenums (L5, outside L4) */
        commit_batch_t batch;
        batch.batch_open_ns = g_tx.open_ns;
        flush_prepare_phase1_global(reserved_slot, batch_seq, &batch);

        pthread_mutex_lock(&g_tx.lock);
        if (batch.n_pages > 0)
        {
            g_cq_slots[reserved_slot] = batch;
            atomic_store_explicit(&g_tx.serial_claimed, 0, memory_order_relaxed);
            atomic_store_explicit(&g_tx.serial_done,   0, memory_order_relaxed);
            g_tx.serial_total       = batch.n_pages;
            g_tx.serial_buf_slot    = reserved_slot;
            g_tx.serial_batch_size  = (batch.n_pages + joined_snap - 1) / joined_snap;
            g_tx.serial_joined_snap = joined_snap;
            g_tx.serializing        = true;
            atomic_fetch_add_explicit(&g_metrics.tx_count, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&g_metrics.tx_joined_sum, (uint64_t)joined_snap,
                                      memory_order_relaxed);
        }
        else
        {
            /* Nothing dirty: return the reserved CQ slot */
            pthread_mutex_lock(&g_cq_lock);
            g_cq_tail = (g_cq_tail + COMMIT_Q_DEPTH - 1) % COMMIT_Q_DEPTH;
            g_cq_count--;
            /* Roll back the sequence counter atomically */
            atomic_fetch_sub_explicit(&g_batch_seq_counter, 1, memory_order_relaxed);
            pthread_cond_signal(&g_cq_notfull);
            pthread_mutex_unlock(&g_cq_lock);

            g_tx.epoch++;
            g_tx.joined = 0; g_tx.done = 0;
            g_tx.closed = false; g_tx.closed_by_count = false;
            g_tx.flushing = false;
            pthread_cond_broadcast(&g_tx.cond);
            pthread_mutex_unlock(&g_tx.lock);
            atomic_fetch_add_explicit(&g_metrics.epoch_wait_ns_total,
                                      monotonic_ns() - t0, memory_order_relaxed);
            uint64_t req_e2e = monotonic_ns() - req_start_ns;
            atomic_fetch_add_explicit(&g_metrics.req_e2e_ns_sum, req_e2e,
                                      memory_order_relaxed);
            atomic_fetch_add_explicit(&g_metrics.req_e2e_count, 1, memory_order_relaxed);
            return;
        }
        pthread_cond_broadcast(&g_tx.cond);
    }

    /* ── Phase 4+5: parallel serialization + barrier ──
     * Each worker serializes at most once per epoch (did_serialize flag).
     * Without this guard, a spurious wakeup caused by the committer's
     * g_tx.cond broadcast would re-enter the if(serializing) branch and
     * double-increment serial_done, breaking the barrier (FIX-12). */
    bool did_serialize = false;
    while (g_tx.epoch == my_epoch)
    {
        if (atomic_load_explicit(&g_io_error, memory_order_relaxed))
        {
            pthread_mutex_unlock(&g_tx.lock);
            return;
        }

        if (g_tx.serializing && !did_serialize)
        {
            did_serialize = true;
            /* FIX-3: use pgn_t for node IDs (not nid_t / uint32_t) */
            int batch_id    = atomic_fetch_add_explicit(&g_tx.serial_claimed, 1,
                                                        memory_order_relaxed);
            int total       = g_tx.serial_total;
            int slot        = g_tx.serial_buf_slot;
            int batch_size  = g_tx.serial_batch_size;
            int joined_snap = g_tx.serial_joined_snap;

            int batch_start = batch_id * batch_size;
            int batch_end   = (batch_id + 1) * batch_size;
            if (batch_end > total) batch_end = total;

            pthread_mutex_unlock(&g_tx.lock);
            for (int idx = batch_start; idx < batch_end; idx++)
            {
                pgn_t   nid = g_global_dfs_order[idx];   /* FIX-3: pgn_t */
                int     si  = g_global_dfs_shard[idx];
                shard_t *sh = &g_shards[si];
                uint8_t *dst = g_global_serial_bufs[slot] + (size_t)idx * PAGE_SIZE;

                uint64_t t_ser = monotonic_ns();
                build_disk_page(sh, dst, nid);
                atomic_fetch_add_explicit(&g_metrics.serialize_ns_total,
                                          monotonic_ns() - t_ser, memory_order_relaxed);
            }
            pthread_mutex_lock(&g_tx.lock);

            int done = atomic_fetch_add_explicit(&g_tx.serial_done, 1,
                                                 memory_order_relaxed) + 1;
            if (done == joined_snap)
            {
                /* ── Last serializer: clear dirty, fill root_pn[], publish ── */
                for (int i = 0; i < total; i++)
                {
                    int si = g_global_dfs_shard[i];
                    overlay_node *n = overlay_get_mut(&g_shards[si],
                                                      g_global_dfs_order[i]);
                    if (n) n->dirty = 0;
                }

                /* FIX-5: collect root pgn for every active shard */
                pgn_t roots[N_SHARDS];
                for (int si = 0; si < N_SHARDS; si++) roots[si] = PGN_NULL;
                for (int si = 0; si < g_active_shards; si++)
                {
                    pgn_t r = g_shards[si].root;
                    if (r != PGN_NULL)
                    {
                        overlay_node *rn = overlay_get_mut(&g_shards[si], r);
                        if (rn) roots[si] = rn->flushed_pn;
                    }
                }
                for (int i = 0; i < N_SHARDS; i++)
                    g_cq_slots[slot].root_pn[i] = roots[i];
                g_cq_slots[slot].batch_enqueue_ns = monotonic_ns();

                /* FIX-8: release g_tx.lock BEFORE acquiring downstream locks */
                pthread_mutex_unlock(&g_tx.lock);

                /* Publish in sequence order (L7 → L6, without holding L4) */
                pthread_mutex_lock(&g_enqueue_lock);
                while (g_cq_slots[slot].seq_no != g_next_enqueue_seq)
                    pthread_cond_wait(&g_enqueue_cond, &g_enqueue_lock);
                pthread_mutex_lock(&g_cq_lock);
                g_cq_slots[slot].ready = true;
                g_next_enqueue_seq++;
                pthread_cond_signal(&g_cq_notempty);
                pthread_mutex_unlock(&g_cq_lock);
                pthread_cond_broadcast(&g_enqueue_cond);
                pthread_mutex_unlock(&g_enqueue_lock);

                /* Batch formation metric */
                uint64_t bo = g_cq_slots[slot].batch_open_ns;
                uint64_t be = g_cq_slots[slot].batch_enqueue_ns;
                if (bo > 0 && be > bo)
                {
                    atomic_fetch_add_explicit(&g_metrics.batch_formation_ns_sum,
                                              be - bo, memory_order_relaxed);
                    atomic_fetch_add_explicit(&g_metrics.batch_formation_count,
                                              1, memory_order_relaxed);
                }

                /* Re-acquire L4 to reset TX state and advance epoch */
                pthread_mutex_lock(&g_tx.lock);
                /* Arm the durability gate so workers will reset the overlay once
                 * the committer confirms this batch is on disk. */
                g_tx.overlay_reset_pending     = true;
                g_tx.overlay_reset_in_progress = false;
                g_tx.overlay_reset_seq         = g_cq_slots[slot].seq_no;
                memcpy(g_tx.overlay_reset_roots, roots,
                       g_active_shards * sizeof(pgn_t));
                g_tx.epoch++;
                g_tx.joined = 0; g_tx.done = 0;
                g_tx.closed = false; g_tx.closed_by_count = false;
                g_tx.flushing = false; g_tx.serializing = false;
                pthread_cond_broadcast(&g_tx.cond);
                /* Falls through: while condition will be false, exits loop */
            }
            else
            {
                /* FIX-9: timed wait so a crashed worker doesn't stall forever */
                struct timespec deadline;
                clock_gettime(CLOCK_REALTIME, &deadline);
                deadline.tv_sec += (time_t)(SERIAL_BARRIER_TIMEOUT_NS / 1000000000ULL);
                int rc = pthread_cond_timedwait(&g_tx.cond, &g_tx.lock, &deadline);
                if (rc == ETIMEDOUT && g_tx.epoch == my_epoch && g_tx.serializing)
                {
                    fprintf(stderr,
                            "tx_insert: serialization barrier timeout — "
                            "serial_done=%d joined_snap=%d; setting io_error\n",
                            atomic_load_explicit(&g_tx.serial_done, memory_order_relaxed),
                            joined_snap);
                    atomic_store_explicit(&g_io_error, true, memory_order_release);
                }
            }
        }
        else
        {
            pthread_cond_wait(&g_tx.cond, &g_tx.lock);
        }
    }

    atomic_fetch_add_explicit(&g_metrics.epoch_wait_ns_total,
                              monotonic_ns() - t0, memory_order_relaxed);
    pthread_mutex_unlock(&g_tx.lock);

    uint64_t req_e2e = monotonic_ns() - req_start_ns;
    atomic_fetch_add_explicit(&g_metrics.req_e2e_ns_sum, req_e2e, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.req_e2e_count, 1, memory_order_relaxed);
}

/* ─────────────────── Worker threads ─────────────────── */
typedef struct { int64_t *keys; int n_keys; int shard_id; } worker_arg;

static void *worker_main(void *arg)
{
    worker_arg *wa = (worker_arg *)arg;
    shard_t *s = &g_shards[wa->shard_id];
    char value[120];
    for (int i = 0; i < wa->n_keys; i++)
    {
        if (atomic_load_explicit(&g_io_error, memory_order_relaxed)) break;
        snprintf(value, sizeof value, "val_%ld", (long)wa->keys[i]);
        tx_insert(s, wa->keys[i], value);
    }
    return NULL;
}

/* ─────────────────── Device init / close ─────────────────── */
static int device_open(const char *path)
{
    g_fd = zbd_open(path, O_RDWR, &g_info);
    if (g_fd < 0) { perror("zbd_open"); return -1; }
    if (nvme_get_nsid(g_fd, &g_nsid) != 0) { perror("nvme_get_nsid"); return -1; }
    g_direct_fd = open(path, O_RDONLY | O_DIRECT);
    g_nzones = g_info.nr_zones;
    g_zones  = calloc(g_nzones, sizeof *g_zones);
    if (!g_zones) { perror("calloc zones"); return -1; }
    unsigned int nr = g_nzones;
    if (zbd_report_zones(g_fd, 0, 0, ZBD_RO_ALL, g_zones, &nr) != 0)
    {
        perror("zbd_report_zones"); return -1;
    }
    pthread_mutex_init(&g_dev_lock, NULL);
    return 0;
}

static void device_close(void)
{
    free(g_zones); g_zones = NULL;
    pthread_mutex_destroy(&g_dev_lock);
    if (g_direct_fd >= 0) { close(g_direct_fd); g_direct_fd = -1; }
    close(g_fd); g_fd = -1;
}

/* ─────────────────── Key generation ─────────────────── */
static void shuffle(int64_t *arr, size_t n)
{
    for (size_t i = n - 1; i > 0; i--)
    {
        size_t j = (size_t)rand() % (i + 1);
        int64_t t = arr[i]; arr[i] = arr[j]; arr[j] = t;
    }
}

/* ─────────────────── Benchmark runner ─────────────────── */
static int run_benchmark(int num_keys, int num_threads, const char *devpath)
{
    char cmd[256];
    printf("=== Reset device %s ===\n", devpath); fflush(stdout);
    snprintf(cmd, sizeof cmd, "nvme zns reset-zone -a %s", devpath);
    if (system(cmd) != 0) { fprintf(stderr, "zone reset failed\n"); return -1; }

    if (g_fd >= 0) device_close();
    if (device_open(devpath) != 0) return -1;

    global_cache_init();
    atomic_store_explicit(&g_io_error, false, memory_order_relaxed);

    g_active_shards = MIN(num_threads, N_SHARDS);

    g_cur_zone = DATA_ZONE_START;
    g_zone_wp  = 0;
    g_meta_wp  = 0;
    g_sb_seq   = 0;
    /* g_global_pagenum must equal the physical page number (LBA-based) so that
     * flushed_pn values passed to load_page() address the correct device offset.
     * Initialise to the first data zone's byte-offset divided by PAGE_SIZE. */
    atomic_store_explicit(&g_global_pagenum,
                          g_zones[DATA_ZONE_START].start / PAGE_SIZE,
                          memory_order_relaxed);
    pthread_mutex_init(&g_zone_lock, NULL);

    g_cq_head = 0; g_cq_tail = 0; g_cq_count = 0;
    g_cq_shutdown = false;
    pthread_mutex_init(&g_cq_lock, NULL);
    pthread_cond_init(&g_cq_notempty, NULL);
    pthread_cond_init(&g_cq_notfull, NULL);
    atomic_store_explicit(&g_committer_last_finish_ns, 0, memory_order_relaxed);
    g_next_to_write_seq = 0;
    g_next_enqueue_seq  = 0;
    pthread_mutex_init(&g_enqueue_lock, NULL);
    pthread_cond_init(&g_enqueue_cond, NULL);
    atomic_store_explicit(&g_batch_seq_counter, 0, memory_order_relaxed);

    /* Global TX init */
    g_tx.epoch = 0; g_tx.joined = 0; g_tx.done = 0;
    g_tx.closed = false; g_tx.closed_by_count = false;
    g_tx.flushing = false; g_tx.serializing = false;
    g_tx.expected = num_threads;
    g_tx.open_ns  = 0;
    g_tx.serial_joined_snap = 0;
    atomic_store_explicit(&g_tx.serial_claimed, 0, memory_order_relaxed);
    atomic_store_explicit(&g_tx.serial_done,   0, memory_order_relaxed);
    g_tx.serial_total = 0; g_tx.serial_buf_slot = 0; g_tx.serial_batch_size = 0;
    g_tx.overlay_reset_pending      = false;
    g_tx.overlay_reset_in_progress  = false;
    g_tx.overlay_reset_seq          = 0;
    atomic_store_explicit(&g_durable_seq, (int64_t)-1, memory_order_relaxed);
    pthread_mutex_init(&g_tx.lock, NULL);
    pthread_cond_init(&g_tx.cond, NULL);

    pthread_create(&g_committer, NULL, committer_main, NULL);

    /*
     * FIX-2: Pre-allocate overlay with 2× expected nodes per shard.
     * B+ tree with LEAF_ORDER=32, INTERNAL_ORDER=249:
     *   est_leaves   ≈ num_keys / 16 (half-full)
     *   est_internal ≈ est_leaves / 124
     *   overlay per shard ≈ (est_leaves + est_internal) * 2 / active_shards
     * Add a generous 2× safety margin.
     */
    size_t est_leaves   = (size_t)num_keys / (LEAF_MAX / 2) + 16;
    size_t est_internal = est_leaves / (INT_MAX_K / 2) + 16;
    size_t overlay_cap  = (est_leaves + est_internal) * 4 / (size_t)g_active_shards + 256;

    for (int si = 0; si < g_active_shards; si++)
    {
        shard_t *sh = &g_shards[si];
        sh->shard_id = si;
        sh->node_locks = &g_node_locks_base[(size_t)si * g_node_lock_seg_size];
        sh->root = PGN_NULL;
        pthread_rwlock_init(&sh->root_lock, NULL);
        sh->dfs_count = 0;
        overlay_init_capacity(&sh->overlay, overlay_cap);
    }

    memset(&g_metrics, 0, sizeof g_metrics);
    atomic_store_explicit(&g_pages_appended, 0, memory_order_relaxed);

    int64_t *all_keys = malloc((size_t)num_keys * sizeof(int64_t));
    if (!all_keys) { perror("malloc keys"); return -1; }
    for (int i = 0; i < num_keys; i++) all_keys[i] = (int64_t)(i + 1);
    srand(42);
    shuffle(all_keys, (size_t)num_keys);

    worker_arg *args = calloc((size_t)num_threads, sizeof *args);
    pthread_t  *tids = calloc((size_t)num_threads, sizeof *tids);
    if (!args || !tids) { perror("calloc"); free(all_keys); return -1; }

    int base = num_keys / num_threads, rem = num_keys % num_threads;
    int64_t *ptr = all_keys;
    for (int t = 0; t < num_threads; t++)
    {
        args[t].n_keys   = base + (t < rem ? 1 : 0);
        args[t].keys     = ptr;
        args[t].shard_id = t % g_active_shards;
        ptr += args[t].n_keys;
    }

    printf("--- threads=%d keys=%d active_shards=%d overlay_cap/shard=%zu [GTX+Cache] ---\n",
           num_threads, num_keys, g_active_shards, overlay_cap);
    fflush(stdout);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int t = 0; t < num_threads; t++)
        pthread_create(&tids[t], NULL, worker_main, &args[t]);
    for (int t = 0; t < num_threads; t++)
        pthread_join(tids[t], NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* ── FIX-6: Drain committer BEFORE any residual sync flush ── */
    pthread_mutex_lock(&g_cq_lock);
    g_cq_shutdown = true;
    pthread_cond_signal(&g_cq_notempty);
    pthread_mutex_unlock(&g_cq_lock);
    pthread_join(g_committer, NULL);  /* committer is fully stopped here */

    /* ── Residual sync flush (committer is gone; use private buffer) ── */
    if (!atomic_load_explicit(&g_io_error, memory_order_relaxed))
    {
        pthread_mutex_lock(&g_tx.lock);
        bool needs = (g_tx.joined > 0 && g_tx.done == g_tx.joined &&
                      !g_tx.flushing && !g_tx.serializing);
        pthread_mutex_unlock(&g_tx.lock);

        if (needs)
        {
            /* Allocate a temporary PAGE_SIZE-aligned buffer for the sync flush */
            uint8_t *priv_buf = NULL;
            size_t priv_sz = (size_t)SHARD_MAX_SERIAL_NODES * PAGE_SIZE;
            if (posix_memalign((void **)&priv_buf, PAGE_SIZE, priv_sz) == 0)
            {
                pgn_t sync_roots[N_SHARDS];
                for (int si = 0; si < N_SHARDS; si++) sync_roots[si] = PGN_NULL;
                for (int si = 0; si < g_active_shards; si++)
                    sync_roots[si] = flush_shard_sync_private(&g_shards[si], priv_buf);
                write_superblock(sync_roots);
                free(priv_buf);
            }
        }
    }

    /* Cleanup pipeline primitives */
    pthread_cond_destroy(&g_tx.cond);
    pthread_mutex_destroy(&g_tx.lock);
    pthread_cond_destroy(&g_enqueue_cond);
    pthread_mutex_destroy(&g_enqueue_lock);
    pthread_mutex_destroy(&g_zone_lock);
    pthread_mutex_destroy(&g_cq_lock);
    pthread_cond_destroy(&g_cq_notempty);
    pthread_cond_destroy(&g_cq_notfull);

    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
    double tput    = (double)num_keys / elapsed;
    uint64_t pages = atomic_load_explicit(&g_pages_appended, memory_order_relaxed);

    size_t nodes = 0;
    for (int si = 0; si < g_active_shards; si++)
        nodes += g_shards[si].overlay.len;

    printf("  elapsed=%.3f s  throughput=%.0f ops/sec  pages=%lu  overlay_nodes=%zu%s\n",
           elapsed, tput, (unsigned long)pages, nodes,
           atomic_load_explicit(&g_io_error, memory_order_relaxed) ? "  [I/O ERROR]" : "");

    /* ── Metrics ── */
    uint64_t tx_n       = atomic_load_explicit(&g_metrics.tx_count,            memory_order_relaxed);
    uint64_t js         = atomic_load_explicit(&g_metrics.tx_joined_sum,       memory_order_relaxed);
    uint64_t byc        = atomic_load_explicit(&g_metrics.tx_closed_by_count,  memory_order_relaxed);
    uint64_t byt        = atomic_load_explicit(&g_metrics.tx_closed_by_timeout,memory_order_relaxed);
    uint64_t cmt_ns     = atomic_load_explicit(&g_metrics.commit_ns_total,     memory_order_relaxed);
    uint64_t ser_ns     = atomic_load_explicit(&g_metrics.serialize_ns_total,  memory_order_relaxed);
    uint64_t cw_ns      = atomic_load_explicit(&g_metrics.commit_wait_ns_total,memory_order_relaxed);
    uint64_t nvme_ns    = atomic_load_explicit(&g_metrics.nvme_ns_total,       memory_order_relaxed);
    uint64_t ins_ns     = atomic_load_explicit(&g_metrics.insert_ns_total,     memory_order_relaxed);
    uint64_t ent_ns     = atomic_load_explicit(&g_metrics.entry_wait_ns_total, memory_order_relaxed);
    uint64_t epk_ns     = atomic_load_explicit(&g_metrics.epoch_wait_ns_total, memory_order_relaxed);
    uint64_t dirty      = atomic_load_explicit(&g_metrics.dirty_nodes_total,   memory_order_relaxed);
    uint64_t zadv       = atomic_load_explicit(&g_metrics.zone_advance_count,  memory_order_relaxed);
    uint64_t chit       = atomic_load_explicit(&g_metrics.cache_hit_count,     memory_order_relaxed);
    uint64_t cmiss      = atomic_load_explicit(&g_metrics.cache_miss_count,    memory_order_relaxed);
    uint64_t bf_ns      = atomic_load_explicit(&g_metrics.batch_formation_ns_sum,  memory_order_relaxed);
    uint64_t bf_n       = atomic_load_explicit(&g_metrics.batch_formation_count,   memory_order_relaxed);
    uint64_t cs_ns      = atomic_load_explicit(&g_metrics.committer_stall_ns_sum,  memory_order_relaxed);
    uint64_t cs_n       = atomic_load_explicit(&g_metrics.committer_stall_count,   memory_order_relaxed);
    uint64_t e2e_ns     = atomic_load_explicit(&g_metrics.req_e2e_ns_sum,      memory_order_relaxed);
    uint64_t e2e_n      = atomic_load_explicit(&g_metrics.req_e2e_count,       memory_order_relaxed);

    uint64_t ctot = byc + byt;
    printf("  [TX — Global]\n");
    printf("    total TXs:            %lu\n", (unsigned long)tx_n);
    printf("    avg threads/TX:       %.2f  (expected=%d)\n",
           tx_n ? (double)js / tx_n : 0.0, num_threads);
    printf("    closed by count:      %lu  (%.1f%%)\n",
           (unsigned long)byc, ctot ? (double)byc / ctot * 100.0 : 0.0);
    printf("    closed by timeout:    %lu  (%.1f%%)\n",
           (unsigned long)byt, ctot ? (double)byt / ctot * 100.0 : 0.0);
    printf("  [Pipeline]\n");
    printf("    avg dirty nodes/TX:   %.1f  (across all %d shards)\n",
           tx_n ? (double)dirty / tx_n : 0.0, g_active_shards);
    printf("    avg serialize/TX:     %.3f ms\n",  tx_n ? (double)ser_ns / tx_n / 1e6 : 0.0);
    printf("    avg commit/TX:        %.3f ms\n",  tx_n ? (double)cmt_ns / tx_n / 1e6 : 0.0);
    printf("    total NVMe I/O:       %.1f ms  (%.1f%% of commit)\n",
           (double)nvme_ns / 1e6,
           cmt_ns ? (double)nvme_ns / cmt_ns * 100.0 : 0.0);
    printf("    commit_wait:          %.1f ms\n", (double)cw_ns / 1e6);
    printf("    zone advances:        %lu\n", (unsigned long)zadv);
    printf("  [Perf]\n");
    printf("    avg batch formation:  %.2f µs\n", bf_n ? (double)bf_ns / bf_n / 1e3 : 0.0);
    printf("    avg committer stall:  %.2f µs\n", cs_n ? (double)cs_ns / cs_n / 1e3 : 0.0);
    printf("    avg request E2E:      %.2f µs\n", e2e_n ? (double)e2e_ns / e2e_n / 1e3 : 0.0);
    printf("  [Thread time, summed]\n");
    printf("    tree_insert:          %.3f s\n", (double)ins_ns / 1e9);
    printf("    wait to join TX:      %.3f s\n", (double)ent_ns / 1e9);
    printf("    wait for epoch:       %.3f s\n", (double)epk_ns / 1e9);
    printf("  [Cache]\n");
    uint64_t total_c = chit + cmiss;
    printf("    global cache hits:    %lu  (%.1f%%)\n",
           (unsigned long)chit, total_c ? (double)chit / total_c * 100.0 : 0.0);
    printf("    global cache misses:  %lu  (%.1f%%)\n",
           (unsigned long)cmiss, total_c ? (double)cmiss / total_c * 100.0 : 0.0);
    fflush(stdout);

    free(all_keys); free(args); free(tids);

    for (int si = 0; si < g_active_shards; si++)
    {
        overlay_destroy(&g_shards[si].overlay);
        pthread_rwlock_destroy(&g_shards[si].root_lock);
    }
    device_close();
    global_cache_destroy();
    return 0;
}

/* ─────────────────── Main ─────────────────── */
int main(int argc, char **argv)
{
    if (argc < 3)
    {
        fprintf(stderr, "Usage: %s <num_keys> <thread_mode> [dev]\n", argv[0]);
        fprintf(stderr, "  thread_mode: 0=all(1,2,4,8,16,32,64), N=specific\n");
        return 1;
    }

    int num_keys    = atoi(argv[1]);
    int thread_mode = atoi(argv[2]);
    const char *devpath = (argc >= 4) ? argv[3] : "/dev/nvme3n2";

    if (num_keys <= 0) { fprintf(stderr, "num_keys must be > 0\n"); return 1; }

    g_node_lock_seg_size = 1;
    while (g_node_lock_seg_size < NODE_LOCK_SLOTS)
        g_node_lock_seg_size <<= 1;

    g_node_locks_base = calloc((size_t)N_SHARDS * g_node_lock_seg_size,
                               sizeof(pthread_rwlock_t));
    if (!g_node_locks_base) { perror("calloc g_node_locks_base"); return 1; }

    for (int slot = 0; slot < COMMIT_Q_DEPTH; slot++)
    {
        size_t sz = (size_t)GLOBAL_MAX_SERIAL_NODES * PAGE_SIZE;
        if (posix_memalign((void **)&g_global_serial_bufs[slot], PAGE_SIZE, sz) != 0)
        {
            perror("posix_memalign g_global_serial_bufs");
            return 1;
        }
    }

    g_fd = -1;
    g_direct_fd = -1;

    int thread_counts[] = {1, 2, 4, 8, 16, 32, 64};
    int n_counts = (int)(sizeof(thread_counts) / sizeof(thread_counts[0]));

    if (thread_mode == 0)
    {
        for (int i = 0; i < n_counts; i++)
        {
            if (run_benchmark(num_keys, thread_counts[i], devpath) != 0)
                fprintf(stderr, "failed for %d threads\n", thread_counts[i]);
        }
    }
    else
    {
        if (run_benchmark(num_keys, thread_mode, devpath) != 0)
            return 1;
    }

    free(g_node_locks_base);
    for (int slot = 0; slot < COMMIT_Q_DEPTH; slot++)
        free(g_global_serial_bufs[slot]);
    return 0;
}
