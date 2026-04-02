/*
 * cow_zfs_gtx.c — COW B-tree for ZNS NVMe with Global TX, Multi-Shard Architecture
 *
 * Key difference from cow_zfs_shard.c (per-shard TX machines):
 * - Single global TX machine (g_tx) instead of N_SHARDS independent tx_t machines
 * - All threads synchronize on ONE global epoch regardless of which shard they insert into
 * - The Closer collects dirty nodes from ALL active shards into one combined commit batch
 * - Results in 1 NVMe write per global TX instead of N_SHARDS writes
 * - At 8 threads / 8 shards: 1 global TX with 8 shard updates → 1 NVMe write (was 8)
 *
 * Build:
 *   gcc -O2 -g -Wall -Wextra -std=c11 -pthread -Iinclude -Iinclude/variants \
 *       -I. src/variants/cow_zfs_gtx.c -o build/bin/cow-bench-zfs-gtx -lzbd -lnvme -lpthread
 * Run:
 *   sudo ./build/bin/cow-bench-zfs-gtx <num_keys> <thread_mode> [dev]
 *   thread_mode: 0=sweep 1/2/4/8/16/32/64=specific count
 */

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
/* Global TX combines all shards: worst case N_SHARDS * SHARD_MAX_SERIAL_NODES dirty nodes */
#define GLOBAL_MAX_SERIAL_NODES (N_SHARDS * SHARD_MAX_SERIAL_NODES)  /* 2048 */
#define PAGE_SIZE 4096
#define LEAF_ORDER 32      /* max 31 keys/leaf            */
#define INTERNAL_ORDER 249 /* max 248 keys, 249 children  */
#define MAX_APPEND_PAGES 64
#define TX_TIMEOUT_NS (2000000ULL) /* 2000 µs = 2 ms */
#define COMMIT_Q_DEPTH 16         /* pipeline depth: queued NVMe batches */
#define META_ZONE 0
#define DATA_ZONE_START 2
#define SB_MAGIC 0x434F574252414D31ULL
#define MAX_HEIGHT 32
#define HS_MAX (MAX_HEIGHT + 4)

#define LEAF_MAX (LEAF_ORDER - 1)      /* 31  */
#define INT_MAX_K (INTERNAL_ORDER - 1) /* 248 */
#define INT_CHILDREN INTERNAL_ORDER    /* 249 */
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

typedef uint32_t nid_t;
typedef uint64_t pgn_t;
#define NID_NULL ((nid_t)0xFFFFFFFFu)
#define PGN_NULL ((pgn_t)0xFFFFFFFFFFFFFFFFull)

/* ─────────────────── Disk page layout (4096 B) ─────────────────── */
typedef struct
{
    uint64_t key;
    char value[120];
} leaf_ent; /* 128 B */
typedef struct
{
    uint64_t key;
    pgn_t child;
} int_ent; /*  16 B */

typedef struct
{
    uint32_t is_leaf;
    uint32_t num_keys;
    pgn_t pointer;     /* rightmost child (internal) or PGN_NULL (leaf) */
    uint8_t hpad[112]; /* header = 128 B total */
    union
    {
        leaf_ent leaf[LEAF_MAX];     /* 31 × 128 = 3968 B */
        int_ent internal[INT_MAX_K]; /* 248 × 16 = 3968 B */
    };
} disk_page;
_Static_assert(sizeof(disk_page) == PAGE_SIZE, "disk_page != 4096");

typedef struct
{
    uint64_t magic;
    uint64_t seq_no;
    pgn_t root_pn;
    uint32_t leaf_order;
    uint32_t internal_order;
    uint8_t pad[PAGE_SIZE - 8 - 8 - 8 - 4 - 4];
} superblock_t;
_Static_assert(sizeof(superblock_t) == PAGE_SIZE, "superblock != 4096");

/* ─────────────────── RAM node ─────────────────── */
typedef struct
{
    uint32_t num_keys;
    uint8_t is_leaf;
    uint8_t dirty;
    uint8_t _pad[2];
    pgn_t pagenum;
    uint64_t keys[INT_MAX_K];
    union
    {
        char values[LEAF_MAX][120];
        nid_t children[INT_CHILDREN];
    };
} ram_node;

/* ─────────────────── ZNS Device (global) ─────────────────── */
static int g_fd;
static __u32 g_nsid;
static struct zbd_info g_info;
static struct zbd_zone *g_zones;
static uint32_t g_nzones;
static pthread_mutex_t g_dev_lock;
static _Atomic(uint64_t) g_pages_appended;

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
    _Atomic uint64_t restart_count;
    _Atomic uint64_t serialize_ns_total;
    _Atomic uint64_t commit_ns_total;
    _Atomic uint64_t commit_wait_ns_total;
    _Atomic uint64_t batch_formation_ns_sum;
    _Atomic uint64_t batch_formation_count;
    _Atomic uint64_t committer_stall_ns_sum;
    _Atomic uint64_t committer_stall_count;
    _Atomic uint64_t req_e2e_ns_sum;
    _Atomic uint64_t req_e2e_count;
} run_metrics_t;
static run_metrics_t g_metrics;

/* ─────────────────── Commit pipeline ─────────────────── */
/*
 * One commit_batch_t describes a single global TX's pages to write.
 * Pages from ALL active shards are pre-serialized into g_global_serial_bufs[buf_slot]
 * by workers in parallel.  The committer thread picks up batches and writes them to
 * NVMe asynchronously, so TX N+1 inserts can overlap with TX N's NVMe write.
 * Note: no shard_id field — the global serial buffer is used directly.
 */
typedef struct
{
    int      n_pages;
    int      buf_slot;
    uint32_t zone_id;
    pgn_t    root_pgn;
    bool     needs_zone_finish;
    uint32_t old_zone_id;
    uint64_t batch_open_ns;
    uint64_t batch_enqueue_ns;
    uint64_t seq_no;
    bool     ready;
} commit_batch_t;

/* ─────────────────── Global TX machine ─────────────────── */
/*
 * Single global TX that ALL threads join, regardless of which shard they insert into.
 * This eliminates the "1-TX-per-shard" bottleneck at low thread counts:
 * 8 threads → 1 global TX (not 8 per-shard TXs) → 1 NVMe write.
 */
typedef struct
{
    uint64_t epoch;
    int expected;       /* = num_threads for current run */
    int joined;
    int done;
    bool closed;
    bool closed_by_count;
    bool flushing;
    bool serializing;
    uint64_t open_ns;
    pthread_mutex_t lock;
    pthread_cond_t cond;

    /* Parallel serialization state (valid only while serializing=true) */
    _Atomic int serial_claimed;   /* next batch index for workers to claim */
    _Atomic int serial_done;      /* workers that finished serialization    */
    int serial_total;             /* total dirty nodes across all shards    */
    int serial_buf_slot;          /* which g_global_serial_bufs[] slot      */
    int serial_batch_size;        /* nodes per worker = ceil(total/joined)  */
} global_tx_t;

static global_tx_t g_tx;

/* ─────────────────── Global DFS state (closer writes, serializers read) ─────────────────── */
/*
 * The Closer runs flush_prepare_phase1_global() which fills these arrays.
 * Workers then read them during the parallel serialization phase.
 * Thread safety: the Closer sets serializing=true under g_tx.lock AFTER filling
 * these arrays; the mutex/cond broadcast provides the required memory barrier.
 */
static nid_t g_global_dfs_order[GLOBAL_MAX_SERIAL_NODES];
static int   g_global_dfs_shard[GLOBAL_MAX_SERIAL_NODES];
static int   g_global_dfs_count;

/* How many shards are active for the current run (set by run_benchmark) */
static int g_active_shards;

/* ─────────────────── Global serial buffers ─────────────────── */
/*
 * COMMIT_Q_DEPTH slots; each slot holds GLOBAL_MAX_SERIAL_NODES pages (8 MB per slot).
 * Previously per-shard (SHARD_MAX_SERIAL_NODES pages per slot × N_SHARDS shards = same total).
 * Now unified: one slot covers all shards' dirty nodes in one global TX.
 */
static uint8_t *g_global_serial_bufs[COMMIT_Q_DEPTH];

/* ─────────────────── Shard struct ─────────────────── */
/*
 * Each shard is an independent B-tree with its own node pool and locks.
 * The tx_t machine has been removed — all TX synchronization is in g_tx.
 * dfs_order is scratch used during the Closer's DFS collection phase.
 */
typedef struct shard_t
{
    int shard_id;
    ram_node *pool;
    size_t pool_cap;
    _Atomic(uint32_t) pool_next;
    pthread_rwlock_t *node_locks;
    nid_t root;
    pthread_rwlock_t root_lock;
    nid_t dfs_order[SHARD_MAX_SERIAL_NODES * 2];  /* scratch for DFS traversal */
    int dfs_count;
    uint8_t _pad[64];
} shard_t;

static shard_t g_shards[N_SHARDS];
static ram_node *g_pool_base;
static pthread_rwlock_t *g_node_locks_base;
static size_t g_pool_seg_size;

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

/* ─────────────────── NVMe zone append ─────────────────── */
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
        fprintf(stderr, "nvme_zns_append zone=%u zslba=%llu nlb=%u data_len=%u: "
                        "ret=0x%x errno=%d(%s)\n"
                        "  [state] zone_cap=%lu capacity=0x%llx start=0x%llx len=0x%llx "
                        "lblock=%u total_lb=%u\n",
                zone_id, (unsigned long long)zslba, (unsigned)nlb,
                (unsigned)(n_pages * PAGE_SIZE),
                (unsigned)nret, errno, strerror(errno),
                (unsigned long)(g_zones[zone_id].capacity / PAGE_SIZE),
                (unsigned long long)g_zones[zone_id].capacity,
                (unsigned long long)g_zones[zone_id].start,
                (unsigned long long)g_zones[zone_id].len,
                g_info.lblock_size, total_lb);
        return -1;
    }

    if (out_start_pn)
        *out_start_pn = (pgn_t)(result * g_info.lblock_size / PAGE_SIZE);

    atomic_fetch_add_explicit(&g_pages_appended, (uint64_t)n_pages, memory_order_relaxed);
    return 0;
}

static void write_superblock(pgn_t root_pn)
{
    uint64_t cap = g_zones[META_ZONE].capacity / PAGE_SIZE;
    if (g_meta_wp >= cap)
        return;

    static superblock_t sb __attribute__((aligned(PAGE_SIZE)));
    memset(&sb, 0, sizeof sb);
    sb.magic = SB_MAGIC;
    sb.seq_no = ++g_sb_seq;
    sb.root_pn = root_pn;
    sb.leaf_order = LEAF_ORDER;
    sb.internal_order = INTERNAL_ORDER;

    if (zone_append_n(META_ZONE, &sb, 1, NULL) == 0)
        g_meta_wp++;
}

/* ─────────────────── Dirty-node DFS (per-shard scratch) ─────────────────── */
static void dfs_dirty_collect(shard_t *s, nid_t nid)
{
    if (nid == NID_NULL)
        return;
    ram_node *n = &s->pool[nid];
    if (!n->dirty)
        return;
    if (!n->is_leaf)
    {
        for (uint32_t i = 0; i <= n->num_keys; i++)
            dfs_dirty_collect(s, n->children[i]);
    }
    s->dfs_order[s->dfs_count++] = nid; /* post-order */
}

static void build_disk_page(shard_t *s, uint8_t *dst, nid_t nid)
{
    ram_node *rn = &s->pool[nid];
    disk_page *dp = (disk_page *)dst;
    memset(dp, 0, PAGE_SIZE);
    dp->is_leaf = rn->is_leaf;
    dp->num_keys = rn->num_keys;
    if (rn->is_leaf)
    {
        dp->pointer = PGN_NULL;
        for (uint32_t k = 0; k < rn->num_keys; k++)
        {
            dp->leaf[k].key = rn->keys[k];
            memcpy(dp->leaf[k].value, rn->values[k], 120);
        }
    }
    else
    {
        for (uint32_t k = 0; k < rn->num_keys; k++)
        {
            dp->internal[k].key = rn->keys[k];
            dp->internal[k].child = s->pool[rn->children[k]].pagenum;
        }
        dp->pointer = s->pool[rn->children[rn->num_keys]].pagenum;
    }
}

/* ─────────────────── Global flush prepare (Closer: all shards → one batch) ─────────────────── */
/*
 * Called by the global TX Closer AFTER all threads have finished their inserts for this epoch.
 * Collects dirty nodes from ALL active shards into g_global_dfs_order/shard arrays,
 * assigns sequential pagenums across all shards, and fills *batch.
 *
 * This is the key change from cow_zfs_shard.c: previously each shard ran its own
 * flush_prepare_phase1, producing N_SHARDS independent batches and N_SHARDS NVMe writes.
 * Now the Closer sweeps all shards in one pass → one batch → one NVMe write per TX.
 */
static void flush_prepare_phase1_global(int buf_slot, uint64_t batch_seq,
                                        commit_batch_t *batch)
{
    g_global_dfs_count = 0;
    batch->n_pages = 0;
    batch->ready = false;

    /* Collect dirty nodes from all active shards in shard order (post-order per shard). */
    for (int si = 0; si < g_active_shards; si++)
    {
        shard_t *s = &g_shards[si];
        if (s->root == NID_NULL)
            continue;
        s->dfs_count = 0;
        dfs_dirty_collect(s, s->root);
        for (int i = 0; i < s->dfs_count; i++)
        {
            if (g_global_dfs_count >= GLOBAL_MAX_SERIAL_NODES)
            {
                fprintf(stderr, "flush_prepare_phase1_global: dirty node overflow %d\n",
                        g_global_dfs_count);
                abort();
            }
            g_global_dfs_order[g_global_dfs_count] = s->dfs_order[i];
            g_global_dfs_shard[g_global_dfs_count] = si;
            g_global_dfs_count++;
        }
    }

    if (g_global_dfs_count == 0)
        return;

    int total = g_global_dfs_count;
    atomic_fetch_add_explicit(&g_metrics.dirty_nodes_total, (uint64_t)total,
                              memory_order_relaxed);

    /* Zone advance check (single call for the entire combined batch). */
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
            fprintf(stderr, "Global zones exhausted!\n");
            abort();
        }
        g_zone_wp = 0;
        atomic_fetch_add_explicit(&g_metrics.zone_advance_count, 1, memory_order_relaxed);
    }

    /* Assign pagenums sequentially across all shards' dirty nodes in one atomic bump. */
    pgn_t base_pgn = (pgn_t)atomic_fetch_add_explicit(&g_global_pagenum, (uint64_t)total,
                                                       memory_order_relaxed);
    for (int i = 0; i < total; i++)
    {
        int si = g_global_dfs_shard[i];
        g_shards[si].pool[g_global_dfs_order[i]].pagenum = base_pgn + (pgn_t)i;
    }

    g_zone_wp += (uint64_t)total;
    pthread_mutex_unlock(&g_zone_lock);

    batch->n_pages           = total;
    batch->buf_slot          = buf_slot;
    batch->zone_id           = g_cur_zone;
    batch->needs_zone_finish = need_finish;
    batch->old_zone_id       = old_zone;
    batch->seq_no            = batch_seq;
    /* batch->root_pgn is set by the last serializer */
}

/* ─────────────────── Committer thread ─────────────────── */
/*
 * Reads from g_global_serial_bufs[batch.buf_slot] — no per-shard buffer lookup needed.
 * One NVMe write per global TX batch (previously one per shard TX batch).
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

        if (batch.n_pages == 0)
            continue;

        uint64_t seq_to_process = batch.seq_no;

        uint64_t t_batch_start = monotonic_ns();
        uint64_t last_finish = atomic_load_explicit(&g_committer_last_finish_ns,
                                                    memory_order_relaxed);
        if (last_finish > 0)
        {
            uint64_t stall_ns = t_batch_start - last_finish;
            atomic_fetch_add_explicit(&g_metrics.committer_stall_ns_sum,
                                      stall_ns, memory_order_relaxed);
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
                abort();
            }
        }

        /* Write all pages from the unified global serial buffer. */
        uint8_t *buf = g_global_serial_bufs[batch.buf_slot];
        int written = 0;
        while (written < batch.n_pages)
        {
            int chunk = batch.n_pages - written;
            if (chunk > MAX_APPEND_PAGES)
                chunk = MAX_APPEND_PAGES;

            pgn_t got __attribute__((unused));
            uint64_t t_nvme = monotonic_ns();
            if (zone_append_n(batch.zone_id,
                              buf + (size_t)written * PAGE_SIZE,
                              chunk, &got) != 0)
            {
                fprintf(stderr, "committer: append failed seq=%lu\n",
                        (unsigned long)seq_to_process);
                abort();
            }
            atomic_fetch_add_explicit(&g_metrics.nvme_ns_total,
                                      monotonic_ns() - t_nvme, memory_order_relaxed);
            written += chunk;
        }

        write_superblock(batch.root_pgn);

        uint64_t t_batch_end = monotonic_ns();
        atomic_fetch_add_explicit(&g_metrics.commit_ns_total,
                                  t_batch_end - t_batch_start, memory_order_relaxed);
        atomic_store_explicit(&g_committer_last_finish_ns, t_batch_end,
                              memory_order_relaxed);
    }
    return NULL;
}

/* ─────────────────── Sync flush (residual, after workers exit) ─────────────────── */
/*
 * Flush one shard's dirty nodes synchronously using g_global_serial_bufs[0] as scratch.
 * Called only on the drain path after all worker threads have joined — not hot path.
 */
static pgn_t flush_shard_sync(shard_t *s)
{
    if (s->root == NID_NULL)
        return PGN_NULL;

    s->dfs_count = 0;
    dfs_dirty_collect(s, s->root);
    if (s->dfs_count == 0)
        return s->pool[s->root].pagenum;

    int total = s->dfs_count;
    if (total > GLOBAL_MAX_SERIAL_NODES)
    {
        fprintf(stderr, "flush_shard_sync: dirty node overflow %d\n", total);
        abort();
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
            abort();
        }
        g_zone_wp = 0;
        atomic_fetch_add_explicit(&g_metrics.zone_advance_count, 1, memory_order_relaxed);
    }

    pgn_t base_pgn = (pgn_t)atomic_fetch_add_explicit(&g_global_pagenum, (uint64_t)total,
                                                       memory_order_relaxed);
    for (int i = 0; i < total; i++)
        s->pool[s->dfs_order[i]].pagenum = base_pgn + (pgn_t)i;
    g_zone_wp += (uint64_t)total;
    uint32_t cur_zone = g_cur_zone;
    pthread_mutex_unlock(&g_zone_lock);

    if (need_finish)
    {
        off_t zstart = (off_t)g_zones[old_zone].start;
        off_t zlen   = (off_t)g_zones[old_zone].len;
        if (zbd_finish_zones(g_fd, zstart, zlen) != 0)
            perror("zbd_finish_zones");
    }

    for (int i = 0; i < total; i++)
        build_disk_page(s, g_global_serial_bufs[0] + (size_t)i * PAGE_SIZE,
                        s->dfs_order[i]);

    pgn_t root_pgn = s->pool[s->root].pagenum;

    int written = 0;
    while (written < total)
    {
        int chunk = MIN(total - written, MAX_APPEND_PAGES);
        pgn_t got __attribute__((unused));
        if (zone_append_n(cur_zone,
                          g_global_serial_bufs[0] + (size_t)written * PAGE_SIZE,
                          chunk, &got) != 0)
        {
            fprintf(stderr, "flush_shard_sync: append failed\n");
            abort();
        }
        written += chunk;
    }
    write_superblock(root_pgn);

    for (int i = 0; i < total; i++)
        s->pool[s->dfs_order[i]].dirty = 0;

    return root_pgn;
}

/* ─────────────────── B-tree insert (latch crabbing) ─────────────────── */
#define NID_LOCK_SHARD(s, n) (&(s)->node_locks[n])

static inline nid_t shard_pool_alloc(shard_t *s)
{
    uint32_t idx = atomic_fetch_add_explicit(&s->pool_next, 1, memory_order_relaxed);
    if (idx >= (uint32_t)s->pool_cap)
    {
        fprintf(stderr, "pool exhausted\n");
        abort();
    }
    ram_node *n = &s->pool[idx];
    memset(n, 0, sizeof *n);
    n->pagenum = PGN_NULL;
    n->dirty = 1;
    return (nid_t)idx;
}

static inline uint32_t get_leaf_pos(const ram_node *n, int64_t key)
{
    uint32_t pos = 0;
    while (pos < n->num_keys && (int64_t)n->keys[pos] < key)
        pos++;
    return pos;
}

static inline uint32_t get_int_pos(const ram_node *n, int64_t key)
{
    for (uint32_t i = 0; i < n->num_keys; i++)
        if (key < (int64_t)n->keys[i])
            return i;
    return n->num_keys;
}

static void tree_insert(shard_t *s, int64_t key, const char *value)
{
    pthread_rwlock_t *hs[HS_MAX];
    int hs_n = 0;
    bool cur_rdlocked = false;
    nid_t cur = NID_NULL;
    bool wrlock_mode = true;

    typedef struct
    {
        nid_t id;
        uint32_t cidx;
    } anc_t;
    anc_t anc[MAX_HEIGHT];
    int anc_n = 0;

#define HS_WRLOCK(lk)              \
    do                             \
    {                              \
        pthread_rwlock_wrlock(lk); \
        hs[hs_n++] = (lk);         \
    } while (0)
#define HS_UNLOCK(lk)                      \
    do                                     \
    {                                      \
        for (int _i = 0; _i < hs_n; _i++)  \
            if (hs[_i] == (lk))            \
            {                              \
                hs[_i] = hs[--hs_n];       \
                pthread_rwlock_unlock(lk); \
                break;                     \
            }                              \
    } while (0)
#define HS_UNLOCK_ALL()                    \
    do                                     \
    {                                      \
        for (int _i = 0; _i < hs_n; _i++)  \
            pthread_rwlock_unlock(hs[_i]); \
        hs_n = 0;                          \
    } while (0)
#define HS_RELEASE_EXCEPT(keep)                \
    do                                         \
    {                                          \
        int _n = 0;                            \
        for (int _i = 0; _i < hs_n; _i++)      \
        {                                      \
            if (hs[_i] == (keep))              \
                hs[_n++] = hs[_i];             \
            else                               \
                pthread_rwlock_unlock(hs[_i]); \
        }                                      \
        hs_n = _n;                             \
    } while (0)

restart:
    hs_n = 0;
    cur_rdlocked = false;
    anc_n = 0;
    cur = NID_NULL;

    for (;;)
    {
        hs_n = 0;
        cur_rdlocked = false;

        pthread_rwlock_rdlock(&s->root_lock);
        nid_t cur_root = s->root;
        pthread_rwlock_unlock(&s->root_lock);

        if (cur_root == NID_NULL)
        {
            pthread_rwlock_wrlock(&s->root_lock);
            if (s->root == NID_NULL)
            {
                nid_t rid = shard_pool_alloc(s);
                ram_node *r = &s->pool[rid];
                r->is_leaf = 1;
                r->num_keys = 1;
                r->keys[0] = (uint64_t)key;
                memcpy(r->values[0], value, 120);
                s->root = rid;
                pthread_rwlock_unlock(&s->root_lock);
                return;
            }
            cur_root = s->root;
            pthread_rwlock_unlock(&s->root_lock);
        }

        HS_WRLOCK(NID_LOCK_SHARD(s, cur_root));

        pthread_rwlock_rdlock(&s->root_lock);
        bool valid = (s->root == cur_root);
        pthread_rwlock_unlock(&s->root_lock);

        if (valid)
        {
            cur = cur_root;
            break;
        }
        HS_UNLOCK_ALL();
    }

    for (;;)
    {
        ram_node *n = &s->pool[cur];
        if (n->is_leaf)
            break;

        bool cur_safe = (n->num_keys < (uint32_t)INT_MAX_K);
        uint32_t cidx = get_int_pos(n, key);
        nid_t child = n->children[cidx];

        bool child_is_leaf = s->pool[child].is_leaf;
        bool child_safe = !child_is_leaf &&
                          (s->pool[child].num_keys < (uint32_t)INT_MAX_K);

        if (cur_safe && child_safe && !wrlock_mode)
        {
            __atomic_store_n(&n->dirty, (uint8_t)1, __ATOMIC_RELAXED);
            pthread_rwlock_rdlock(NID_LOCK_SHARD(s, child));
            HS_UNLOCK_ALL();
            hs[hs_n++] = NID_LOCK_SHARD(s, child);
            anc_n = 0;
            cur_rdlocked = true;
        }
        else
        {
            if (cur_rdlocked)
            {
                HS_UNLOCK_ALL();
                wrlock_mode = true;
                atomic_fetch_add_explicit(&g_metrics.restart_count, 1,
                                          memory_order_relaxed);
                goto restart;
            }

            n->dirty = 1;

            if (cur_safe)
            {
                HS_RELEASE_EXCEPT(NID_LOCK_SHARD(s, cur));
                anc_n = 0;
            }

            if (anc_n >= MAX_HEIGHT)
            {
                HS_UNLOCK_ALL();
                return;
            }
            anc[anc_n].id = cur;
            anc[anc_n].cidx = cidx;
            anc_n++;

            HS_WRLOCK(NID_LOCK_SHARD(s, child));
            cur_rdlocked = false;
        }

        cur = child;
    }

    ram_node *leaf = &s->pool[cur];

    if (cur_rdlocked)
    {
        HS_UNLOCK_ALL();
        wrlock_mode = true;
        atomic_fetch_add_explicit(&g_metrics.restart_count, 1, memory_order_relaxed);
        goto restart;
    }

    leaf->dirty = 1;

    for (uint32_t i = 0; i < leaf->num_keys; i++)
    {
        if ((int64_t)leaf->keys[i] == key)
        {
            memcpy(leaf->values[i], value, 120);
            HS_UNLOCK_ALL();
            return;
        }
    }

    nid_t carry_left = cur;
    nid_t carry_right = NID_NULL;
    int carry_split = 0;
    int64_t carry_key = 0;

    if (leaf->num_keys < (uint32_t)LEAF_MAX)
    {
        uint32_t pos = get_leaf_pos(leaf, key);
        for (int64_t i = (int64_t)leaf->num_keys - 1; i >= (int64_t)pos; i--)
        {
            leaf->keys[i + 1] = leaf->keys[i];
            memcpy(leaf->values[i + 1], leaf->values[i], 120);
        }
        leaf->keys[pos] = (uint64_t)key;
        memcpy(leaf->values[pos], value, 120);
        leaf->num_keys++;
    }
    else
    {
        uint64_t tmp_keys[LEAF_ORDER];
        char tmp_vals[LEAF_ORDER][120];
        uint32_t pos = get_leaf_pos(leaf, key);

        for (uint32_t i = 0; i < pos; i++)
        {
            tmp_keys[i] = leaf->keys[i];
            memcpy(tmp_vals[i], leaf->values[i], 120);
        }
        tmp_keys[pos] = (uint64_t)key;
        memcpy(tmp_vals[pos], value, 120);
        for (uint32_t i = pos; i < leaf->num_keys; i++)
        {
            tmp_keys[i + 1] = leaf->keys[i];
            memcpy(tmp_vals[i + 1], leaf->values[i], 120);
        }

        uint32_t sp = LEAF_ORDER / 2;
        leaf->num_keys = sp;
        for (uint32_t i = 0; i < sp; i++)
        {
            leaf->keys[i] = tmp_keys[i];
            memcpy(leaf->values[i], tmp_vals[i], 120);
        }

        nid_t right_id = shard_pool_alloc(s);
        ram_node *right = &s->pool[right_id];
        right->is_leaf = 1;
        right->num_keys = LEAF_ORDER - sp;
        for (uint32_t i = 0; i < (uint32_t)(LEAF_ORDER - sp); i++)
        {
            right->keys[i] = tmp_keys[sp + i];
            memcpy(right->values[i], tmp_vals[sp + i], 120);
        }

        carry_split = 1;
        carry_right = right_id;
        carry_key = (int64_t)right->keys[0];
    }

    if (!carry_split)
    {
        HS_UNLOCK_ALL();
        return;
    }

    HS_UNLOCK(NID_LOCK_SHARD(s, cur));

    for (int i = anc_n - 1; i >= 0; i--)
    {
        nid_t par_id = anc[i].id;
        uint32_t cidx = anc[i].cidx;
        ram_node *par = &s->pool[par_id];

        par->children[cidx] = carry_left;

        if (par->num_keys < (uint32_t)INT_MAX_K)
        {
            uint32_t pos = cidx;
            for (int64_t j = (int64_t)par->num_keys - 1; j >= (int64_t)pos; j--)
            {
                par->keys[j + 1] = par->keys[j];
                par->children[j + 2] = par->children[j + 1];
            }
            par->keys[pos] = (uint64_t)carry_key;
            par->children[pos + 1] = carry_right;
            par->num_keys++;
            HS_UNLOCK_ALL();
            return;
        }

        uint64_t tkeys[INTERNAL_ORDER + 1];
        nid_t tchld[INTERNAL_ORDER + 2];

        for (uint32_t j = 0; j < cidx; j++)
        {
            tkeys[j] = par->keys[j];
            tchld[j] = par->children[j];
        }
        tchld[cidx] = carry_left;
        tkeys[cidx] = (uint64_t)carry_key;
        tchld[cidx + 1] = carry_right;
        for (uint32_t j = cidx; j < par->num_keys; j++)
        {
            tkeys[j + 1] = par->keys[j];
            tchld[j + 2] = par->children[j + 1];
        }

        uint32_t total_k = par->num_keys + 1;
        uint32_t sp = total_k / 2;
        int64_t up_key = (int64_t)tkeys[sp];

        par->num_keys = sp;
        for (uint32_t j = 0; j < sp; j++)
        {
            par->keys[j] = tkeys[j];
            par->children[j] = tchld[j];
        }
        par->children[sp] = tchld[sp];

        nid_t right_id = shard_pool_alloc(s);
        ram_node *right = &s->pool[right_id];
        right->is_leaf = 0;
        right->num_keys = total_k - sp - 1;
        for (uint32_t j = 0; j < right->num_keys; j++)
        {
            right->keys[j] = tkeys[sp + 1 + j];
            right->children[j] = tchld[sp + 1 + j];
        }
        right->children[right->num_keys] = tchld[total_k];

        carry_split = 1;
        carry_right = right_id;
        carry_left = par_id;
        carry_key = up_key;

        HS_UNLOCK(NID_LOCK_SHARD(s, par_id));
    }

    if (carry_split)
    {
        nid_t new_root = shard_pool_alloc(s);
        ram_node *r = &s->pool[new_root];
        r->is_leaf = 0;
        r->num_keys = 1;
        r->keys[0] = (uint64_t)carry_key;
        r->children[0] = carry_left;
        r->children[1] = carry_right;
        pthread_rwlock_wrlock(&s->root_lock);
        s->root = new_root;
        pthread_rwlock_unlock(&s->root_lock);
    }

    HS_UNLOCK_ALL();

#undef HS_WRLOCK
#undef HS_UNLOCK
#undef HS_UNLOCK_ALL
#undef HS_RELEASE_EXCEPT
}

/* ─────────────────── Global TX insert ─────────────────── */
/*
 * Hot path for every insert.  Uses the single global g_tx instead of per-shard tx machines.
 *
 * Phase 1 (Join): Thread waits for an open global TX and joins it.
 * Phase 2 (Insert): Thread inserts into its designated shard with latch-crabbing.
 * Phase 3 (Commit): Last thread to finish triggers the Closer.
 *   Closer: sweeps ALL shards for dirty nodes (flush_prepare_phase1_global),
 *           then broadcasts to wake serializers.
 * Phase 4 (Serialize): All joined threads serialize a batch of dirty nodes in parallel
 *                      into g_global_serial_bufs[slot].
 * Phase 5 (Publish): Last serializer pushes the batch to the committer and advances epoch.
 */
static void tx_insert(shard_t *s, int64_t key, const char *value)
{
    uint64_t req_start_ns = monotonic_ns();

    /* ── Phase 1: wait to join an open global TX ── */
    uint64_t t0 = monotonic_ns();
    pthread_mutex_lock(&g_tx.lock);
    for (;;)
    {
        if (!g_tx.closed && !g_tx.flushing && !g_tx.serializing)
            break;
        pthread_cond_wait(&g_tx.cond, &g_tx.lock);
    }
    atomic_fetch_add_explicit(&g_metrics.entry_wait_ns_total,
                              monotonic_ns() - t0, memory_order_relaxed);

    uint64_t my_epoch = g_tx.epoch;
    g_tx.joined++;
    if (g_tx.joined == 1)
        g_tx.open_ns = monotonic_ns();
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

    /* ── Phase 3: commit phase ── */
    t0 = monotonic_ns();
    pthread_mutex_lock(&g_tx.lock);
    g_tx.done++;

    /* Close check: all done or timeout. */
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

    /* Closer election: last thread done when TX is closed and not yet being processed. */
    bool is_closer = g_tx.closed && (g_tx.done == g_tx.joined) &&
                     !g_tx.flushing && !g_tx.serializing;
    if (is_closer)
    {
        g_tx.flushing = true;
        int joined_snap = g_tx.joined;
        pthread_mutex_unlock(&g_tx.lock);

        /* Reserve a commit queue slot (outside g_tx.lock). */
        uint64_t t_cw = monotonic_ns();
        pthread_mutex_lock(&g_cq_lock);
        while (g_cq_count >= COMMIT_Q_DEPTH)
            pthread_cond_wait(&g_cq_notfull, &g_cq_lock);
        int reserved_slot = g_cq_tail;
        uint64_t batch_seq = g_batch_seq_counter++;
        g_cq_slots[reserved_slot].ready = false;
        g_cq_slots[reserved_slot].seq_no = batch_seq;
        g_cq_tail = (g_cq_tail + 1) % COMMIT_Q_DEPTH;
        g_cq_count++;
        pthread_mutex_unlock(&g_cq_lock);
        atomic_fetch_add_explicit(&g_metrics.commit_wait_ns_total,
                                  monotonic_ns() - t_cw, memory_order_relaxed);

        /* Sweep ALL shards: DFS + zone advance + pagenum assignment (no NVMe). */
        commit_batch_t batch;
        batch.batch_open_ns = g_tx.open_ns;
        flush_prepare_phase1_global(reserved_slot, batch_seq, &batch);

        pthread_mutex_lock(&g_tx.lock);
        if (batch.n_pages > 0)
        {
            g_cq_slots[reserved_slot] = batch;
            atomic_store_explicit(&g_tx.serial_claimed, 0, memory_order_relaxed);
            atomic_store_explicit(&g_tx.serial_done,   0, memory_order_relaxed);
            g_tx.serial_total      = batch.n_pages;
            g_tx.serial_buf_slot   = reserved_slot;
            g_tx.serial_batch_size = (batch.n_pages + joined_snap - 1) / joined_snap;
            g_tx.serializing       = true;
            atomic_fetch_add_explicit(&g_metrics.tx_count, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&g_metrics.tx_joined_sum, (uint64_t)joined_snap,
                                      memory_order_relaxed);
        }
        else
        {
            /* No dirty nodes: release the reserved slot and advance epoch. */
            pthread_mutex_lock(&g_cq_lock);
            g_cq_tail = (g_cq_tail + COMMIT_Q_DEPTH - 1) % COMMIT_Q_DEPTH;
            g_cq_count--;
            g_batch_seq_counter--;
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
            uint64_t req_e2e_ns = monotonic_ns() - req_start_ns;
            atomic_fetch_add_explicit(&g_metrics.req_e2e_ns_sum, req_e2e_ns,
                                      memory_order_relaxed);
            atomic_fetch_add_explicit(&g_metrics.req_e2e_count, 1, memory_order_relaxed);
            return;
        }
        pthread_cond_broadcast(&g_tx.cond); /* wake other threads to serialize */
    }

    /* ── Phase 4 + 5: Parallel serialization + epoch-wait loop ── */
    while (g_tx.epoch == my_epoch)
    {
        if (g_tx.serializing)
        {
            /*
             * Each worker claims a range of dirty nodes from across all shards.
             * g_global_dfs_order[idx] + g_global_dfs_shard[idx] identify the node and its shard.
             */
            int batch_id   = atomic_fetch_add_explicit(&g_tx.serial_claimed, 1,
                                                       memory_order_relaxed);
            int total      = g_tx.serial_total;
            int slot       = g_tx.serial_buf_slot;
            int batch_size = g_tx.serial_batch_size;
            int joined_snap = g_tx.joined;

            int batch_start = batch_id * batch_size;
            int batch_end   = (batch_id + 1) * batch_size;
            if (batch_end > total)
                batch_end = total;

            pthread_mutex_unlock(&g_tx.lock);
            for (int idx = batch_start; idx < batch_end; idx++)
            {
                nid_t    nid = g_global_dfs_order[idx];
                int      si  = g_global_dfs_shard[idx];
                shard_t *sh  = &g_shards[si];
                uint8_t *dst = g_global_serial_bufs[slot] + (size_t)idx * PAGE_SIZE;

                uint64_t t_ser = monotonic_ns();
                build_disk_page(sh, dst, nid);
                atomic_fetch_add_explicit(&g_metrics.serialize_ns_total,
                                          monotonic_ns() - t_ser, memory_order_relaxed);
            }
            pthread_mutex_lock(&g_tx.lock);

            /* Barrier: wait for all workers to finish their serialization slice. */
            int done = atomic_fetch_add_explicit(&g_tx.serial_done, 1,
                                                 memory_order_relaxed) + 1;
            if (done == joined_snap)
            {
                /* Last serializer: clear dirty flags across all shards. */
                for (int i = 0; i < total; i++)
                {
                    int si = g_global_dfs_shard[i];
                    g_shards[si].pool[g_global_dfs_order[i]].dirty = 0;
                }

                /* Determine root_pgn: use first active shard's root for the superblock. */
                pgn_t root_pgn = PGN_NULL;
                for (int si = 0; si < g_active_shards; si++)
                {
                    if (g_shards[si].root != NID_NULL)
                    {
                        root_pgn = g_shards[si].pool[g_shards[si].root].pagenum;
                        break;
                    }
                }
                g_cq_slots[slot].root_pgn = root_pgn;
                g_cq_slots[slot].batch_enqueue_ns = monotonic_ns();

                /* Publish batch in sequence order. */
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

                /* Batch formation metric. */
                uint64_t batch_open    = g_cq_slots[slot].batch_open_ns;
                uint64_t batch_enqueue = g_cq_slots[slot].batch_enqueue_ns;
                if (batch_open > 0 && batch_enqueue > batch_open)
                {
                    atomic_fetch_add_explicit(&g_metrics.batch_formation_ns_sum,
                                              batch_enqueue - batch_open,
                                              memory_order_relaxed);
                    atomic_fetch_add_explicit(&g_metrics.batch_formation_count,
                                              1, memory_order_relaxed);
                }

                /* Advance epoch → all waiting threads wake and exit. */
                g_tx.epoch++;
                g_tx.joined = 0; g_tx.done = 0;
                g_tx.closed = false; g_tx.closed_by_count = false;
                g_tx.flushing = false; g_tx.serializing = false;
                pthread_cond_broadcast(&g_tx.cond);
            }
            else
            {
                pthread_cond_wait(&g_tx.cond, &g_tx.lock);
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

    uint64_t req_e2e_ns = monotonic_ns() - req_start_ns;
    atomic_fetch_add_explicit(&g_metrics.req_e2e_ns_sum, req_e2e_ns, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_metrics.req_e2e_count, 1, memory_order_relaxed);
}

/* ─────────────────── Worker threads ─────────────────── */
typedef struct
{
    int64_t *keys;
    int n_keys;
    int shard_id;
} worker_arg;

static void *worker_main(void *arg)
{
    worker_arg *wa = (worker_arg *)arg;
    shard_t *s = &g_shards[wa->shard_id];
    char value[120];

    for (int i = 0; i < wa->n_keys; i++)
    {
        snprintf(value, sizeof value, "val_%ld", (long)wa->keys[i]);
        tx_insert(s, wa->keys[i], value);
    }
    return NULL;
}

/* ─────────────────── Device init ─────────────────── */
static int device_open(const char *path)
{
    g_fd = zbd_open(path, O_RDWR, &g_info);
    if (g_fd < 0) { perror("zbd_open"); return -1; }

    if (nvme_get_nsid(g_fd, &g_nsid) != 0) { perror("nvme_get_nsid"); return -1; }

    g_nzones = g_info.nr_zones;
    g_zones = calloc(g_nzones, sizeof *g_zones);
    if (!g_zones) { perror("calloc zones"); return -1; }

    unsigned int nr = g_nzones;
    if (zbd_report_zones(g_fd, 0, 0, ZBD_RO_ALL, g_zones, &nr) != 0)
    {
        perror("zbd_report_zones");
        return -1;
    }

    pthread_mutex_init(&g_dev_lock, NULL);
    return 0;
}

static void device_close(void)
{
    free(g_zones);
    g_zones = NULL;
    pthread_mutex_destroy(&g_dev_lock);
    close(g_fd);
    g_fd = -1;
}

/* ─────────────────── Key generation ─────────────────── */
static void shuffle(int64_t *arr, size_t n)
{
    for (size_t i = n - 1; i > 0; i--)
    {
        size_t j = (size_t)rand() % (i + 1);
        int64_t t = arr[i];
        arr[i] = arr[j];
        arr[j] = t;
    }
}

/* ─────────────────── Benchmark runner ─────────────────── */
static int run_benchmark(int num_keys, int num_threads, const char *devpath)
{
    char cmd[256];

    printf("=== Reset device %s ===\n", devpath);
    fflush(stdout);
    snprintf(cmd, sizeof cmd, "nvme zns reset-zone -a %s", devpath);
    if (system(cmd) != 0)
    {
        fprintf(stderr, "zone reset failed\n");
        return -1;
    }

    if (g_fd >= 0)
        device_close();
    if (device_open(devpath) != 0)
        return -1;

    /* Sharding parameters */
    g_active_shards = MIN(num_threads, N_SHARDS);
    int threads_per_shard = num_threads / g_active_shards;
    int extra = num_threads % g_active_shards;
    (void)threads_per_shard; (void)extra; /* shard assignment is by thread_id % active_shards */

    /* Global zone + pagenum management */
    g_cur_zone = DATA_ZONE_START;
    g_zone_wp = 0;
    g_meta_wp = 0;
    g_sb_seq = 0;
    atomic_store_explicit(&g_global_pagenum, 0, memory_order_relaxed);
    pthread_mutex_init(&g_zone_lock, NULL);

    /* Global commit queue */
    g_cq_head = 0;
    g_cq_tail = 0;
    g_cq_count = 0;
    g_cq_shutdown = false;
    pthread_mutex_init(&g_cq_lock, NULL);
    pthread_cond_init(&g_cq_notempty, NULL);
    pthread_cond_init(&g_cq_notfull, NULL);
    atomic_store_explicit(&g_committer_last_finish_ns, 0, memory_order_relaxed);
    g_next_to_write_seq = 0;
    g_next_enqueue_seq = 0;
    pthread_mutex_init(&g_enqueue_lock, NULL);
    pthread_cond_init(&g_enqueue_cond, NULL);
    atomic_store_explicit(&g_batch_seq_counter, 0, memory_order_relaxed);

    /* ── Initialize single global TX machine ── */
    g_tx.epoch = 0;
    g_tx.joined = 0;
    g_tx.done = 0;
    g_tx.closed = false;
    g_tx.closed_by_count = false;
    g_tx.flushing = false;
    g_tx.serializing = false;
    g_tx.expected = num_threads;  /* ALL threads synchronize on this one TX */
    g_tx.open_ns = 0;
    atomic_store_explicit(&g_tx.serial_claimed, 0, memory_order_relaxed);
    atomic_store_explicit(&g_tx.serial_done,   0, memory_order_relaxed);
    g_tx.serial_total = 0;
    g_tx.serial_buf_slot = 0;
    g_tx.serial_batch_size = 0;
    pthread_mutex_init(&g_tx.lock, NULL);
    pthread_cond_init(&g_tx.cond, NULL);

    /* Spawn single global committer thread */
    pthread_create(&g_committer, NULL, committer_main, NULL);

    /* Per-shard B-tree initialization (no per-shard TX machine) */
    for (int si = 0; si < g_active_shards; si++)
    {
        shard_t *sh = &g_shards[si];
        sh->shard_id = si;
        sh->pool = &g_pool_base[si * g_pool_seg_size];
        sh->pool_cap = g_pool_seg_size;
        atomic_store_explicit(&sh->pool_next, 0, memory_order_relaxed);
        sh->node_locks = &g_node_locks_base[si * g_pool_seg_size];
        sh->root = NID_NULL;
        pthread_rwlock_init(&sh->root_lock, NULL);
        sh->dfs_count = 0;
    }

    /* Reset per-run metrics */
    memset(&g_metrics, 0, sizeof g_metrics);
    atomic_store_explicit(&g_pages_appended, 0, memory_order_relaxed);

    /* Shuffled keys */
    int64_t *all_keys = malloc((size_t)num_keys * sizeof(int64_t));
    if (!all_keys) { perror("malloc keys"); return -1; }
    for (int i = 0; i < num_keys; i++)
        all_keys[i] = (int64_t)(i + 1);
    srand(42);
    shuffle(all_keys, (size_t)num_keys);

    /* Partition keys among threads; route each thread to a shard */
    worker_arg *args = calloc((size_t)num_threads, sizeof *args);
    pthread_t *tids  = calloc((size_t)num_threads, sizeof *tids);
    if (!args || !tids) { perror("calloc"); free(all_keys); return -1; }

    int base = num_keys / num_threads;
    int rem  = num_keys % num_threads;
    int64_t *ptr = all_keys;
    for (int t = 0; t < num_threads; t++)
    {
        args[t].n_keys  = base + (t < rem ? 1 : 0);
        args[t].keys    = ptr;
        args[t].shard_id = t % g_active_shards;
        ptr += args[t].n_keys;
    }

    printf("--- threads=%d keys=%d active_shards=%d [Global TX] ---\n",
           num_threads, num_keys, g_active_shards);
    fflush(stdout);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int t = 0; t < num_threads; t++)
        pthread_create(&tids[t], NULL, worker_main, &args[t]);
    for (int t = 0; t < num_threads; t++)
        pthread_join(tids[t], NULL);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Residual flush (safety net for any uncommitted dirty nodes). */
    pthread_mutex_lock(&g_tx.lock);
    bool needs_residual = (g_tx.joined > 0 && g_tx.done == g_tx.joined &&
                           !g_tx.flushing && !g_tx.serializing);
    pthread_mutex_unlock(&g_tx.lock);
    if (needs_residual)
    {
        for (int si = 0; si < g_active_shards; si++)
            flush_shard_sync(&g_shards[si]);
    }

    /* Drain commit queue and stop committer. */
    pthread_mutex_lock(&g_cq_lock);
    g_cq_shutdown = true;
    pthread_cond_signal(&g_cq_notempty);
    pthread_mutex_unlock(&g_cq_lock);
    pthread_join(g_committer, NULL);

    pthread_cond_destroy(&g_tx.cond);
    pthread_mutex_destroy(&g_tx.lock);
    pthread_cond_destroy(&g_enqueue_cond);
    pthread_mutex_destroy(&g_enqueue_lock);
    pthread_mutex_destroy(&g_zone_lock);

    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
    double tput = (double)num_keys / elapsed;
    uint64_t pages = atomic_load_explicit(&g_pages_appended, memory_order_relaxed);

    uint32_t nodes = 0;
    for (int si = 0; si < g_active_shards; si++)
        nodes += atomic_load_explicit(&g_shards[si].pool_next, memory_order_relaxed);

    printf("  elapsed=%.3f s  throughput=%.0f ops/sec  pages_appended=%lu  nodes_used=%u\n",
           elapsed, tput, (unsigned long)pages, (unsigned)nodes);

    /* ── Detailed metrics ── */
    uint64_t tx_n         = atomic_load_explicit(&g_metrics.tx_count,            memory_order_relaxed);
    uint64_t joined_sum   = atomic_load_explicit(&g_metrics.tx_joined_sum,       memory_order_relaxed);
    uint64_t by_count     = atomic_load_explicit(&g_metrics.tx_closed_by_count,  memory_order_relaxed);
    uint64_t by_timeout   = atomic_load_explicit(&g_metrics.tx_closed_by_timeout,memory_order_relaxed);
    uint64_t commit_ns    = atomic_load_explicit(&g_metrics.commit_ns_total,     memory_order_relaxed);
    uint64_t ser_ns       = atomic_load_explicit(&g_metrics.serialize_ns_total,  memory_order_relaxed);
    uint64_t cw_ns        = atomic_load_explicit(&g_metrics.commit_wait_ns_total,memory_order_relaxed);
    uint64_t nvme_ns      = atomic_load_explicit(&g_metrics.nvme_ns_total,       memory_order_relaxed);
    uint64_t ins_ns       = atomic_load_explicit(&g_metrics.insert_ns_total,     memory_order_relaxed);
    uint64_t entry_ns     = atomic_load_explicit(&g_metrics.entry_wait_ns_total, memory_order_relaxed);
    uint64_t epoch_ns     = atomic_load_explicit(&g_metrics.epoch_wait_ns_total, memory_order_relaxed);
    uint64_t dirty_tot    = atomic_load_explicit(&g_metrics.dirty_nodes_total,   memory_order_relaxed);
    uint64_t zone_adv     = atomic_load_explicit(&g_metrics.zone_advance_count,  memory_order_relaxed);
    uint64_t restarts     = atomic_load_explicit(&g_metrics.restart_count,       memory_order_relaxed);
    uint64_t bf_ns_sum    = atomic_load_explicit(&g_metrics.batch_formation_ns_sum,  memory_order_relaxed);
    uint64_t bf_count     = atomic_load_explicit(&g_metrics.batch_formation_count,   memory_order_relaxed);
    uint64_t cs_ns_sum    = atomic_load_explicit(&g_metrics.committer_stall_ns_sum,  memory_order_relaxed);
    uint64_t cs_count     = atomic_load_explicit(&g_metrics.committer_stall_count,   memory_order_relaxed);
    uint64_t e2e_ns_sum   = atomic_load_explicit(&g_metrics.req_e2e_ns_sum,      memory_order_relaxed);
    uint64_t e2e_count    = atomic_load_explicit(&g_metrics.req_e2e_count,        memory_order_relaxed);

    double avg_joined        = tx_n      ? (double)joined_sum / tx_n          : 0.0;
    double avg_dirty         = tx_n      ? (double)dirty_tot  / tx_n          : 0.0;
    double avg_commit_ms     = tx_n      ? (double)commit_ns  / tx_n / 1e6    : 0.0;
    double avg_ser_ms        = tx_n      ? (double)ser_ns     / tx_n / 1e6    : 0.0;
    double total_commit_ms   = (double)commit_ns  / 1e6;
    double total_ser_ms      = (double)ser_ns     / 1e6;
    double total_cw_ms       = (double)cw_ns      / 1e6;
    double total_nvme_ms     = (double)nvme_ns    / 1e6;
    double nvme_pct          = commit_ns ? (double)nvme_ns / commit_ns * 100.0 : 0.0;
    double ins_s             = (double)ins_ns   / 1e9;
    double entry_s           = (double)entry_ns / 1e9;
    double epoch_s           = (double)epoch_ns / 1e9;
    uint64_t closed_total    = by_count + by_timeout;
    double by_count_pct      = closed_total ? (double)by_count   / closed_total * 100.0 : 0.0;
    double by_timeout_pct    = closed_total ? (double)by_timeout / closed_total * 100.0 : 0.0;
    double restarts_per      = num_keys ? (double)restarts / num_keys : 0.0;
    double avg_bf_us         = bf_count ? (double)bf_ns_sum / bf_count / 1e3 : 0.0;
    double avg_cs_us         = cs_count ? (double)cs_ns_sum / cs_count / 1e3 : 0.0;
    double avg_e2e_us        = e2e_count ? (double)e2e_ns_sum / e2e_count / 1e3 : 0.0;

    printf("  [TX — Global]\n");
    printf("    total TXs:            %lu\n", (unsigned long)tx_n);
    printf("    avg threads/TX:       %.2f  (expected=%d, all threads in one global TX)\n",
           avg_joined, num_threads);
    printf("    closed by count:      %lu  (%.1f%%)\n", (unsigned long)by_count, by_count_pct);
    printf("    closed by timeout:    %lu  (%.1f%%)\n", (unsigned long)by_timeout, by_timeout_pct);
    printf("  [Pipeline]\n");
    printf("    avg dirty nodes/TX:   %.1f  (across all %d shards)\n", avg_dirty, g_active_shards);
    printf("    avg serialize time:   %.3f ms  (all workers, summed)\n", avg_ser_ms);
    printf("    avg commit time:      %.3f ms  (committer NVMe+SB, wall)\n", avg_commit_ms);
    printf("    total serialize:      %.1f ms\n", total_ser_ms);
    printf("    total commit:         %.1f ms\n", total_commit_ms);
    printf("    total NVMe I/O:       %.1f ms  (%.1f%% of commit)\n",
           total_nvme_ms, nvme_pct);
    printf("    commit_wait (closer): %.1f ms\n", total_cw_ms);
    printf("    zone advances:        %lu\n", (unsigned long)zone_adv);
    printf("  [Pipeline Performance]\n");
    printf("    avg batch formation:  %.2f µs  (TX open → batch enqueue)\n", avg_bf_us);
    printf("    avg committer stall:  %.2f µs  (idle between batches)\n", avg_cs_us);
    printf("    avg request E2E:      %.2f µs  (total time per insert call)\n", avg_e2e_us);
    printf("  [Thread time, summed]\n");
    printf("    tree_insert:          %.3f s\n", ins_s);
    printf("    waiting to join TX:   %.3f s\n", entry_s);
    printf("    waiting for epoch:    %.3f s\n", epoch_s);
    printf("  [Contention]\n");
    printf("    wrlock restarts:      %lu  (avg %.4f per insert)\n",
           (unsigned long)restarts, restarts_per);
    fflush(stdout);

    free(all_keys);
    free(args);
    free(tids);
    device_close();
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

    if (num_keys <= 0)
    {
        fprintf(stderr, "num_keys must be > 0\n");
        return 1;
    }

    /* Pre-allocate node pool (shared across all shards) */
    size_t est_leaves   = (size_t)num_keys / (LEAF_MAX / 2) + 16;
    size_t est_internal = est_leaves / (INT_MAX_K / 2) + 16;
    size_t total_pool   = (est_leaves + est_internal) * 16;
    if (total_pool < 65536)
        total_pool = 65536;

    g_pool_seg_size  = MAX(total_pool / N_SHARDS, 8192);
    g_pool_base      = calloc(g_pool_seg_size * N_SHARDS, sizeof(ram_node));
    if (!g_pool_base) { perror("calloc g_pool_base"); return 1; }

    g_node_locks_base = calloc(g_pool_seg_size * N_SHARDS, sizeof(pthread_rwlock_t));
    if (!g_node_locks_base) { perror("calloc g_node_locks_base"); return 1; }

    /*
     * Allocate global serial buffers: COMMIT_Q_DEPTH slots, each covering
     * GLOBAL_MAX_SERIAL_NODES pages (8 MB/slot; 128 MB total — same aggregate
     * as cow_zfs_shard.c which allocates 16 MB × 8 shards = 128 MB).
     */
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

    free(g_pool_base);
    free(g_node_locks_base);
    for (int slot = 0; slot < COMMIT_Q_DEPTH; slot++)
        free(g_global_serial_bufs[slot]);
    return 0;
}
