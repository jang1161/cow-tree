/*
 * cow_zfs.c — Standalone COW B-tree for ZNS NVMe
 * Parallel latch-crabbing insert + barrier TX + batched NVMe append
 * Dirty-node tracking: only modified path nodes are written per TX flush.
 *
 * Build:
 *   gcc -O2 -g -Wall -Wextra -std=c11 -pthread -Iinclude -Iinclude/variants \
 *       -I. src/variants/cow_zfs.c -o build/bin/cow-bench-zfs -lzbd -lnvme -lpthread
 * Run:
 *   sudo ./build/bin/cow-bench-zfs <num_keys> <thread_mode> [dev]
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
#define PAGE_SIZE 4096
#define LEAF_ORDER 32      /* max 31 keys/leaf            */
#define INTERNAL_ORDER 249 /* max 248 keys, 249 children  */
#define MAX_APPEND_PAGES 64
#define TX_TIMEOUT_NS (500000ULL) /* 500 µs */
#define COMMIT_Q_DEPTH 4          /* pipeline depth: queued NVMe batches */
#define MAX_SERIAL_NODES 2048     /* upper bound on dirty nodes per TX */
#define META_ZONE 0
#define DATA_ZONE_START 1
#define SB_MAGIC 0x434F574252414D31ULL
#define MAX_HEIGHT 32
#define HS_MAX (MAX_HEIGHT + 4)

#define LEAF_MAX (LEAF_ORDER - 1)      /* 31  */
#define INT_MAX_K (INTERNAL_ORDER - 1) /* 248 */
#define INT_CHILDREN INTERNAL_ORDER    /* 249 */

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
/*
 * dirty=1: this node was modified since last flush; path-copy invariant
 *          guarantees that dirty child ⇒ dirty parent.
 * pagenum: disk page number assigned at last flush (PGN_NULL if never flushed).
 */
typedef struct
{
    uint32_t num_keys;
    uint8_t is_leaf;
    uint8_t dirty; /* 1 if modified since last flush */
    uint8_t _pad[2];
    pgn_t pagenum;            /* last written disk page number */
    uint64_t keys[INT_MAX_K]; /* 248 keys */
    union
    {
        char values[LEAF_MAX][120];   /* leaf: 31 × 120 */
        nid_t children[INT_CHILDREN]; /* internal: 249 */
    };
} ram_node;

/* ─────────────────── Node pool ─────────────────── */
static ram_node *pool;
static size_t pool_cap;
static _Atomic(uint32_t) pool_next;

static inline nid_t pool_alloc(void)
{
    uint32_t idx = atomic_fetch_add_explicit(&pool_next, 1, memory_order_relaxed);
    if (idx >= (uint32_t)pool_cap)
    {
        fprintf(stderr, "pool exhausted\n");
        abort();
    }
    ram_node *n = &pool[idx];
    memset(n, 0, sizeof *n);
    n->pagenum = PGN_NULL;
    n->dirty = 1; /* new nodes are always dirty */
    return (nid_t)idx;
}

/* ─────────────────── Per-node locks + root_lock ─────────────────── */
/*
 * Per-node RW locks guarantee deadlock freedom:
 * top-down acquisition (parent before child) means no lock-order cycle.
 * RW locks allow future concurrent reads; inserts use write locks.
 * On Linux, calloc-zeroed pthread_rwlock_t == PTHREAD_RWLOCK_INITIALIZER.
 */
static pthread_rwlock_t *node_locks; /* one per pool slot, allocated in main */
static pthread_rwlock_t root_lock;
static nid_t g_root = NID_NULL;

#define NID_LOCK(n) (&node_locks[n])

/* ─────────────────── ZNS Device ─────────────────── */
static int g_fd;
static __u32 g_nsid;
static struct zbd_info g_info;
static struct zbd_zone *g_zones;
static uint32_t g_nzones;

static uint32_t g_cur_zone; /* current data zone index */
static uint64_t g_zone_wp;  /* pages written to current data zone */
static uint64_t g_meta_wp;  /* pages written to META_ZONE */
static uint64_t g_sb_seq = 0;
static pthread_mutex_t g_dev_lock;

static _Atomic(uint64_t) g_pages_appended;

/* ─────────────────── Per-run metrics ─────────────────── */
typedef struct
{
    _Atomic uint64_t tx_count;             /* TXs actually flushed              */
    _Atomic uint64_t tx_joined_sum;        /* sum of joined counts per TX        */
    _Atomic uint64_t tx_closed_by_count;   /* TXs closed because joined==expected*/
    _Atomic uint64_t tx_closed_by_timeout; /* TXs closed by timeout              */
    _Atomic uint64_t flush_ns_total;       /* reserved / unused after pipeline refactor     */
    _Atomic uint64_t nvme_ns_total;        /* wall time inside zone_append_n     */
    _Atomic uint64_t insert_ns_total;      /* sum of tree_insert() times         */
    _Atomic uint64_t entry_wait_ns_total;  /* time waiting to join TX (phase 1)  */
    _Atomic uint64_t epoch_wait_ns_total;  /* time waiting for TX completion      */
    _Atomic uint64_t dirty_nodes_total;    /* total dirty nodes flushed           */
    _Atomic uint64_t zone_advance_count;   /* number of zone transitions          */
    _Atomic uint64_t restart_count;        /* wrlock-mode restarts in tree_insert */
    _Atomic uint64_t serialize_ns_total;   /* parallel serialization time (summed)*/
    _Atomic uint64_t commit_ns_total;      /* committer NVMe write time           */
    _Atomic uint64_t commit_wait_ns_total; /* time closer waits for a free slot   */
} run_metrics_t;

static run_metrics_t g_metrics;

/* ─────────────────── Commit pipeline ─────────────────── */
/*
 * One commit_batch_t describes a single TX's pages to write.
 * Pages are pre-serialized into g_serial_bufs[buf_slot] by workers in parallel.
 * The committer thread picks up batches and writes them to NVMe asynchronously,
 * so TX N+1 inserts can overlap with TX N's NVMe write.
 */
typedef struct
{
    int      n_pages;           /* number of 4KB pages in this batch      */
    int      buf_slot;          /* which g_serial_bufs[buf_slot] to read  */
    uint32_t zone_id;           /* zone to append to                      */
    pgn_t    root_pgn;          /* root page number for the superblock    */
    bool     needs_zone_finish; /* call zbd_finish_zones before writing   */
    uint32_t old_zone_id;       /* zone to finish (if needs_zone_finish)  */
} commit_batch_t;

/* Per-slot write buffers: PAGE_SIZE-aligned for NVMe DMA.
 * Each slot is owned by one TX batch at a time. */
static uint8_t g_serial_bufs[COMMIT_Q_DEPTH][MAX_SERIAL_NODES * PAGE_SIZE]
    __attribute__((aligned(PAGE_SIZE)));

/* Circular commit queue */
static commit_batch_t g_cq_slots[COMMIT_Q_DEPTH];
static int g_cq_head;           /* committer reads here    */
static int g_cq_tail;           /* next slot to fill       */
static int g_cq_count;          /* items currently queued  */
static pthread_mutex_t g_cq_lock;
static pthread_cond_t  g_cq_notempty; /* committer waits when empty */
static pthread_cond_t  g_cq_notfull;  /* closer waits when full     */
static bool            g_cq_shutdown;
static pthread_t       g_committer;

/* ─────────────────── TX machine ─────────────────── */
typedef struct
{
    uint64_t epoch;
    int expected; /* = num_threads for current run */
    int joined;
    int done;
    bool closed;
    bool closed_by_count; /* true if closed because joined==expected */
    bool flushing;
    bool serializing;     /* true while parallel serialization is in progress */
    uint64_t open_ns;
    pthread_mutex_t lock;
    pthread_cond_t cond;

    /* Parallel serialization state (valid only while serializing=true) */
    _Atomic int serial_claimed; /* next dfs_order[] index to serialize (atomic) */
    _Atomic int serial_done;    /* workers finished their serialization duty     */
    int serial_total;           /* total dirty nodes (= dfs_count at phase1)     */
    int serial_buf_slot;        /* which g_serial_bufs[] slot workers write into */
} tx_t;

static tx_t g_tx;

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
                        "  [state at error] g_zone_wp=%lu  zone_cap=%lu  "
                        "capacity_raw=0x%llx  start_raw=0x%llx  len_raw=0x%llx  "
                        "lblock=%u  total_lb=%u\n",
                zone_id, (unsigned long long)zslba, (unsigned)nlb,
                (unsigned)(n_pages * PAGE_SIZE),
                (unsigned)nret, errno, strerror(errno),
                (unsigned long)g_zone_wp,
                (unsigned long)(g_zones[zone_id].capacity / PAGE_SIZE),
                (unsigned long long)g_zones[zone_id].capacity,
                (unsigned long long)g_zones[zone_id].start,
                (unsigned long long)g_zones[zone_id].len,
                g_info.lblock_size,
                total_lb);
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
        return; /* meta zone full - skip */

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

/* ─────────────────── Flush: dirty-node DFS ─────────────────── */

/*
 * Invariant: dirty child ⇒ dirty parent.
 * Flushing only traverses dirty nodes (prune at non-dirty nodes).
 * Post-order: children written before parents, so parent can use
 * children's new pagenum values.
 */
static nid_t *dfs_order; /* pool_cap entries */
static int dfs_count;

/* (write buffers are now g_serial_bufs[] — see commit pipeline above) */

static void dfs_dirty_collect(nid_t nid)
{
    if (nid == NID_NULL)
        return;
    ram_node *n = &pool[nid];
    if (!n->dirty)
        return; /* prune: no dirty descendants below non-dirty */
    if (!n->is_leaf)
    {
        for (uint32_t i = 0; i <= n->num_keys; i++)
            dfs_dirty_collect(n->children[i]);
    }
    dfs_order[dfs_count++] = nid; /* post-order */
}

/*
 * build_disk_page — serialize one RAM node into a 4KB disk page.
 * Called by each worker thread during the parallel serialization phase.
 * No locks required: the destination slot is exclusively owned by this call
 * (workers claim different indices via atomic serial_claimed), and the
 * source ram_node is read-only once all inserts for this TX have completed.
 */
static void build_disk_page(uint8_t *dst, nid_t nid)
{
    ram_node *rn = &pool[nid];
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
            dp->internal[k].child = pool[rn->children[k]].pagenum;
        }
        dp->pointer = pool[rn->children[rn->num_keys]].pagenum;
    }
}

/*
 * flush_prepare_phase1 — single-threaded fast phase, called by the TX closer
 * OUTSIDE g_tx.lock.  Does DFS, zone-advance, pagenum assignment.
 * After this returns, dfs_order[0..dfs_count-1] are stable and pagenums are
 * assigned.  Workers can immediately build_disk_page() in parallel.
 * Fills *batch with metadata the committer needs; root_pgn is filled later
 * by the last serializer.
 */
static void flush_prepare_phase1(int buf_slot, commit_batch_t *batch)
{
    batch->n_pages = 0; /* will be overwritten if there are dirty nodes */

    if (g_root == NID_NULL)
        return;

    dfs_count = 0;
    dfs_dirty_collect(g_root);
    if (dfs_count == 0)
        return;

    int total = dfs_count;
    if (total > MAX_SERIAL_NODES)
    {
        fprintf(stderr, "flush_prepare_phase1: too many dirty nodes %d > %d\n",
                total, MAX_SERIAL_NODES);
        abort();
    }

    atomic_fetch_add_explicit(&g_metrics.dirty_nodes_total, (uint64_t)total,
                              memory_order_relaxed);

    /* Zone advance check (g_cur_zone / g_zone_wp not protected by a lock here
     * because only one TX closer runs at a time — enforced by flushing=true). */
    uint64_t zone_cap = g_zones[g_cur_zone].capacity / PAGE_SIZE;
    bool need_finish = false;
    uint32_t old_zone = g_cur_zone;

    if (g_zone_wp + (uint64_t)total > zone_cap)
    {
        need_finish = (g_zone_wp > 0 && g_zone_wp < zone_cap);
        g_cur_zone++;
        if (g_cur_zone >= g_nzones)
        {
            fprintf(stderr, "All zones exhausted!\n");
            abort();
        }
        g_zone_wp = 0;
        atomic_fetch_add_explicit(&g_metrics.zone_advance_count, 1, memory_order_relaxed);
    }

    /* Pre-assign page numbers: post-order guarantees children < parents. */
    pgn_t base_pgn = (pgn_t)(g_zones[g_cur_zone].start / PAGE_SIZE + g_zone_wp);
    for (int i = 0; i < total; i++)
        pool[dfs_order[i]].pagenum = base_pgn + (pgn_t)i;

    /* Advance the software write pointer now so the next TX's phase1 sees
     * the correct starting position even before NVMe writes complete. */
    g_zone_wp += (uint64_t)total;

    batch->n_pages          = total;
    batch->buf_slot         = buf_slot;
    batch->zone_id          = g_cur_zone;
    batch->needs_zone_finish = need_finish;
    batch->old_zone_id      = old_zone;
    /* batch->root_pgn is set by the last serializer */
}

/*
 * committer_main — dedicated background thread that writes serialized pages
 * to NVMe.  Receives commit_batch_t entries from g_cq_slots[] and performs
 * zone_append_n() + write_superblock() while the next TX's workers are
 * already inserting into the B-tree.
 */
static void *committer_main(void *arg)
{
    (void)arg;
    for (;;)
    {
        pthread_mutex_lock(&g_cq_lock);
        while (g_cq_count == 0 && !g_cq_shutdown)
            pthread_cond_wait(&g_cq_notempty, &g_cq_lock);
        if (g_cq_count == 0) /* shutdown with empty queue */
        {
            pthread_mutex_unlock(&g_cq_lock);
            break;
        }
        commit_batch_t batch = g_cq_slots[g_cq_head];
        g_cq_head = (g_cq_head + 1) % COMMIT_Q_DEPTH;
        g_cq_count--;
        pthread_cond_signal(&g_cq_notfull);
        pthread_mutex_unlock(&g_cq_lock);

        if (batch.n_pages == 0)
            continue;

        uint64_t t_commit = monotonic_ns();

        /* If this TX crossed a zone boundary, finish the old zone first. */
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

        /* Write all pages in MAX_APPEND_PAGES-page chunks. */
        uint8_t *buf = g_serial_bufs[batch.buf_slot];
        int written = 0;
        while (written < batch.n_pages)
        {
            int chunk = batch.n_pages - written;
            if (chunk > MAX_APPEND_PAGES)
                chunk = MAX_APPEND_PAGES;

            pgn_t got __attribute__((unused));
            uint64_t t_nvme = monotonic_ns();
            if (zone_append_n(batch.zone_id, buf + (size_t)written * PAGE_SIZE,
                              chunk, &got) != 0)
            {
                fprintf(stderr, "committer: append failed\n");
                abort();
            }
            atomic_fetch_add_explicit(&g_metrics.nvme_ns_total,
                                      monotonic_ns() - t_nvme, memory_order_relaxed);
            written += chunk;
        }

        write_superblock(batch.root_pgn);

        atomic_fetch_add_explicit(&g_metrics.commit_ns_total,
                                  monotonic_ns() - t_commit, memory_order_relaxed);
    }
    return NULL;
}

/*
 * flush_tree_sync — synchronous fallback used for the residual partial TX
 * after all worker threads have exited.  Not on the hot path.
 */
static pgn_t flush_tree_sync(void)
{
    if (g_root == NID_NULL)
        return PGN_NULL;

    commit_batch_t batch;
    flush_prepare_phase1(0, &batch); /* always use slot 0 */
    if (batch.n_pages == 0)
        return pool[g_root].pagenum;

    /* Serialize all nodes into slot 0 */
    for (int i = 0; i < batch.n_pages; i++)
        build_disk_page(g_serial_bufs[0] + (size_t)i * PAGE_SIZE, dfs_order[i]);

    pgn_t root_pgn = pool[g_root].pagenum;
    batch.root_pgn = root_pgn;

    /* Inline NVMe write (synchronous) */
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

    int written = 0;
    while (written < batch.n_pages)
    {
        int chunk = batch.n_pages - written;
        if (chunk > MAX_APPEND_PAGES)
            chunk = MAX_APPEND_PAGES;
        pgn_t got __attribute__((unused));
        if (zone_append_n(batch.zone_id,
                          g_serial_bufs[0] + (size_t)written * PAGE_SIZE,
                          chunk, &got) != 0)
        {
            fprintf(stderr, "flush_tree_sync: append failed\n");
            abort();
        }
        written += chunk;
    }
    write_superblock(root_pgn);

    for (int i = 0; i < batch.n_pages; i++)
        pool[dfs_order[i]].dirty = 0;

    return root_pgn;
}

/* ─────────────────── B-tree insert (latch crabbing) ─────────────────── */

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
    return n->num_keys; /* rightmost */
}

static void tree_insert(int64_t key, const char *value)
{
    /*
     * root_lock usage — kept as brief as possible to avoid serialization:
     *   • Start: rdlock briefly to read g_root; wrlock only for empty-tree create.
     *     Release BEFORE locking the root node (avoids lock-order inversion).
     *     Validate g_root after locking root node; retry if a concurrent root
     *     split changed g_root during the lock gap (extremely rare).
     *   • Phase 4 (root split): wrlock briefly to write g_root, then release.
     * All other phases hold NO root_lock — parallel inserts run concurrently
     * once they hold their respective node locks.
     *
     * Read/write lock separation on node locks:
     *   - Safe internal nodes passed through in the "pure safe zone": rdlock
     *     (multiple threads can traverse concurrently)
     *   - First unsafe node (and the safe boundary node just above it): wrlock
     *   - All nodes below the first unsafe node, plus the leaf: wrlock
     * No in-place rdlock→wrlock upgrade: if rdlocked traversal reaches an
     * unsafe boundary, release locks and restart from root in wrlock_mode.
     * This avoids Linux rwlock writer starvation and stale-path inserts.
     */
    pthread_rwlock_t *hs[HS_MAX];
    int hs_n = 0;
    bool cur_rdlocked = false;
    nid_t cur = NID_NULL;
    bool wrlock_mode = true; /* always wrlock; rdlock saves nothing for insert-only workload */

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

    /*
     * restart: full re-traversal from the root in wrlock mode.
     * Used instead of rdlock→wrlock upgrade to avoid write starvation
     * (Linux PTHREAD_RWLOCK_PREFER_READER_NP) and stale-path inserts.
     * wrlock_mode is NOT reset here — once set, stays set for this call.
     */
restart:
    hs_n = 0;
    cur_rdlocked = false;
    anc_n = 0;
    cur = NID_NULL;

    /* ── Root acquisition loop (retry if root changed during lock gap) ── */
    for (;;)
    {
        hs_n = 0;
        cur_rdlocked = false;

        /* Read g_root under rdlock (wrlock only if tree is empty) */
        pthread_rwlock_rdlock(&root_lock);
        nid_t cur_root = g_root;
        pthread_rwlock_unlock(&root_lock);

        if (cur_root == NID_NULL)
        {
            /* Empty tree: need wrlock to create root */
            pthread_rwlock_wrlock(&root_lock);
            if (g_root == NID_NULL)
            {
                nid_t rid = pool_alloc();
                ram_node *r = &pool[rid];
                r->is_leaf = 1;
                r->num_keys = 1;
                r->keys[0] = (uint64_t)key;
                memcpy(r->values[0], value, 120);
                g_root = rid;
                pthread_rwlock_unlock(&root_lock);
                return;
            }
            cur_root = g_root; /* another thread just created the root */
            pthread_rwlock_unlock(&root_lock);
        }

        /* Lock root node WITHOUT holding root_lock (avoids lock-order inversion
         * with Phase-4 threads that hold root-node wrlock and want root_lock). */
        HS_WRLOCK(NID_LOCK(cur_root));

        /* Validate: root might have been replaced by a concurrent root split
         * in the window between reading g_root and locking the root node. */
        pthread_rwlock_rdlock(&root_lock);
        bool valid = (g_root == cur_root);
        pthread_rwlock_unlock(&root_lock);

        if (valid)
        {
            cur = cur_root;
            break; /* root node locked and validated */
        }
        /* Stale root — release and retry (fires at most O(log N) times total) */
        HS_UNLOCK_ALL();
    }

    /* Phase 1: top-down descent with latch crabbing + read/write lock separation */
    for (;;)
    {
        ram_node *n = &pool[cur];
        if (n->is_leaf)
            break;

        bool cur_safe = (n->num_keys < (uint32_t)INT_MAX_K);

        uint32_t cidx = get_int_pos(n, key);
        nid_t child = n->children[cidx];

        /* Peek at child safety without locking (stale read is OK as a heuristic;
         * we re-read after acquiring the child's lock when needed). */
        bool child_is_leaf = pool[child].is_leaf;
        bool child_safe = !child_is_leaf &&
                          (pool[child].num_keys < (uint32_t)INT_MAX_K);

        if (cur_safe && child_safe && !wrlock_mode)
        {
            /*
             * Both cur and child are safe internal nodes.
             * Rdlock child: multiple threads can traverse these hot upper-level
             * nodes concurrently without blocking each other.
             *
             * dirty is set atomically because other threads may simultaneously
             * hold rdlock on this node.
             */
            __atomic_store_n(&n->dirty, (uint8_t)1, __ATOMIC_RELAXED);
            pthread_rwlock_rdlock(NID_LOCK(child)); /* acquire before releasing cur */
            HS_UNLOCK_ALL();                        /* release cur (and any others) */
            hs[hs_n++] = NID_LOCK(child);           /* track child (rdlocked) */
            anc_n = 0;
            cur_rdlocked = true;
        }
        else
        {
            /*
             * cur is unsafe, OR child is unsafe/leaf: switch to wrlock mode.
             * If cur was rdlocked, restart from root in wrlock_mode.
             */
            if (cur_rdlocked)
            {
                /* Restart in full wrlock mode instead of upgrading.
                 * Upgrade (unlock→wrlock same node) causes write starvation
                 * under Linux's PREFER_READER rwlock policy: new rdlockers
                 * keep arriving, the upgrader never gets wrlock.  With many
                 * TX-parallel threads this deadlocks the TX barrier.
                 * Restarting is also correct: avoids inserting into a stale
                 * subtree when a concurrent split happened in the lock gap. */
                HS_UNLOCK_ALL();
                wrlock_mode = true;
                atomic_fetch_add_explicit(&g_metrics.restart_count, 1,
                                          memory_order_relaxed);
                goto restart;
            }

            /* cur has wrlock */
            n->dirty = 1;

            if (cur_safe)
            {
                HS_RELEASE_EXCEPT(NID_LOCK(cur));
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

            HS_WRLOCK(NID_LOCK(child));
            cur_rdlocked = false;
        }

        cur = child;
    }

    /* Phase 2: leaf insert / split */
    ram_node *leaf = &pool[cur];

    /* Defensive restart if cur is rdlocked at the leaf (rare: concurrent split
     * changed a peeked-safe internal into a leaf between peek and rdlock). */
    if (cur_rdlocked)
    {
        HS_UNLOCK_ALL();
        wrlock_mode = true;
        atomic_fetch_add_explicit(&g_metrics.restart_count, 1, memory_order_relaxed);
        goto restart;
    }

    leaf->dirty = 1;

    /* Duplicate key update */
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
        /* Leaf split */
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

        nid_t right_id = pool_alloc();
        ram_node *right = &pool[right_id];
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

    HS_UNLOCK(NID_LOCK(cur)); /* release leaf; ancestors remain locked */

    /* Phase 3: propagate splits up ancestor stack */
    for (int i = anc_n - 1; i >= 0; i--)
    {
        nid_t par_id = anc[i].id;
        uint32_t cidx = anc[i].cidx;
        ram_node *par = &pool[par_id];

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

        /* Internal split */
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

        nid_t right_id = pool_alloc();
        ram_node *right = &pool[right_id];
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

        HS_UNLOCK(NID_LOCK(par_id));
    }

    /* Phase 4: root split — brief wrlock on root_lock to update g_root */
    if (carry_split)
    {
        nid_t new_root = pool_alloc();
        ram_node *r = &pool[new_root];
        r->is_leaf = 0;
        r->num_keys = 1;
        r->keys[0] = (uint64_t)carry_key;
        r->children[0] = carry_left;
        r->children[1] = carry_right;
        pthread_rwlock_wrlock(&root_lock);
        g_root = new_root;
        pthread_rwlock_unlock(&root_lock);
    }

    HS_UNLOCK_ALL();

#undef HS_WRLOCK
#undef HS_UNLOCK
#undef HS_UNLOCK_ALL
#undef HS_RELEASE_EXCEPT
}

/* ─────────────────── TX worker ─────────────────── */

/*
 * tx_insert — hot path for every worker insert.
 *
 * Phase 1: join an open TX (wait if closed/flushing/serializing).
 * Phase 2: parallel latch-crabbing insert.
 * Phase 3: commit pipeline —
 *   Closer (last done): runs flush_prepare_phase1 outside the lock (fast,
 *     ~20µs), then broadcasts to switch the TX into SERIALIZING state.
 *   All workers: atomically claim dirty nodes and call build_disk_page into
 *     g_serial_bufs[slot] in parallel.
 *   Last serializer: clears dirty flags, pushes batch to the async committer
 *     thread, increments epoch → workers immediately start the next TX.
 */
static void tx_insert(int64_t key, const char *value)
{
    /* ── Phase 1: wait to join an open TX ── */
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

    /* ── Phase 2: parallel latch-crabbing insert ── */
    t0 = monotonic_ns();
    tree_insert(key, value);
    atomic_fetch_add_explicit(&g_metrics.insert_ns_total,
                              monotonic_ns() - t0, memory_order_relaxed);

    /* ── Phase 3: commit phase ── */
    t0 = monotonic_ns();
    pthread_mutex_lock(&g_tx.lock);
    g_tx.done++;

    /* Close by timeout if we're the last done and TX still open */
    if (!g_tx.closed && g_tx.done == g_tx.joined)
    {
        if (monotonic_ns() - g_tx.open_ns >= TX_TIMEOUT_NS)
        {
            g_tx.closed = true;
            g_tx.closed_by_count = false;
            atomic_fetch_add_explicit(&g_metrics.tx_closed_by_timeout, 1,
                                      memory_order_relaxed);
        }
    }

    /* Helper lambda (as goto target) for the closer logic.
     * Entered when: closed && done==joined && !flushing && !serializing. */
    bool is_closer = g_tx.closed && (g_tx.done == g_tx.joined) &&
                     !g_tx.flushing && !g_tx.serializing;
    if (is_closer)
    {
        g_tx.flushing = true;
        int joined_snap = g_tx.joined;
        pthread_mutex_unlock(&g_tx.lock);

        /* Wait for a free commit slot (outside g_tx.lock). */
        uint64_t t_cw = monotonic_ns();
        pthread_mutex_lock(&g_cq_lock);
        while (g_cq_count >= COMMIT_Q_DEPTH)
            pthread_cond_wait(&g_cq_notfull, &g_cq_lock);
        int reserved_slot = g_cq_tail;
        pthread_mutex_unlock(&g_cq_lock);
        atomic_fetch_add_explicit(&g_metrics.commit_wait_ns_total,
                                  monotonic_ns() - t_cw, memory_order_relaxed);

        /* DFS + zone advance + pagenum assign (fast, no NVMe). */
        commit_batch_t batch;
        flush_prepare_phase1(reserved_slot, &batch);

        pthread_mutex_lock(&g_tx.lock);
        if (batch.n_pages > 0)
        {
            g_cq_slots[reserved_slot] = batch;
            atomic_store_explicit(&g_tx.serial_claimed, 0, memory_order_relaxed);
            atomic_store_explicit(&g_tx.serial_done,   0, memory_order_relaxed);
            g_tx.serial_total    = batch.n_pages;
            g_tx.serial_buf_slot = reserved_slot;
            g_tx.serializing     = true;
            atomic_fetch_add_explicit(&g_metrics.tx_count, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&g_metrics.tx_joined_sum, (uint64_t)joined_snap,
                                      memory_order_relaxed);
        }
        else
        {
            /* Nothing dirty: skip serialization, advance epoch directly. */
            g_tx.epoch++;
            g_tx.joined = 0; g_tx.done = 0;
            g_tx.closed = false; g_tx.closed_by_count = false;
            g_tx.flushing = false;
            pthread_cond_broadcast(&g_tx.cond);
            pthread_mutex_unlock(&g_tx.lock);
            atomic_fetch_add_explicit(&g_metrics.epoch_wait_ns_total,
                                      monotonic_ns() - t0, memory_order_relaxed);
            return;
        }
        pthread_cond_broadcast(&g_tx.cond); /* wake workers to serialize */
    }

    /* Serialization + epoch-wait loop */
    while (g_tx.epoch == my_epoch)
    {
        if (g_tx.serializing)
        {
            int idx        = atomic_fetch_add_explicit(&g_tx.serial_claimed, 1,
                                                       memory_order_relaxed);
            int total      = g_tx.serial_total;
            int slot       = g_tx.serial_buf_slot;
            int joined_snap = g_tx.joined; /* stable: TX closed, no new joiners */

            if (idx < total)
            {
                nid_t   nid = dfs_order[idx];
                uint8_t *dst = g_serial_bufs[slot] + (size_t)idx * PAGE_SIZE;
                pthread_mutex_unlock(&g_tx.lock);

                uint64_t t_ser = monotonic_ns();
                build_disk_page(dst, nid);
                atomic_fetch_add_explicit(&g_metrics.serialize_ns_total,
                                          monotonic_ns() - t_ser, memory_order_relaxed);

                pthread_mutex_lock(&g_tx.lock);
            }
            /* Workers with idx >= total skip building but still count as done. */

            int done = atomic_fetch_add_explicit(&g_tx.serial_done, 1,
                                                 memory_order_relaxed) + 1;
            if (done == joined_snap)
            {
                /* Last worker: clear dirty, push batch, advance epoch. */
                for (int i = 0; i < total; i++)
                    pool[dfs_order[i]].dirty = 0;

                g_cq_slots[slot].root_pgn =
                    (g_root != NID_NULL) ? pool[g_root].pagenum : PGN_NULL;

                pthread_mutex_lock(&g_cq_lock);
                g_cq_tail  = (g_cq_tail + 1) % COMMIT_Q_DEPTH;
                g_cq_count++;
                pthread_cond_signal(&g_cq_notempty);
                pthread_mutex_unlock(&g_cq_lock);

                g_tx.epoch++;
                g_tx.joined = 0; g_tx.done = 0;
                g_tx.closed = false; g_tx.closed_by_count = false;
                g_tx.flushing = false; g_tx.serializing = false;
                pthread_cond_broadcast(&g_tx.cond);
                /* epoch changed; while condition exits */
            }
            else
            {
                /* Wait for last serializer to broadcast epoch++ */
                pthread_cond_wait(&g_tx.cond, &g_tx.lock);
            }
        }
        else
        {
            /* TX not yet closed or not all done: timedwait + timeout-closer. */
            uint64_t deadline_ns = g_tx.open_ns + TX_TIMEOUT_NS;
            uint64_t now_ns      = monotonic_ns();
            if (now_ns < deadline_ns)
            {
                uint64_t wait_ns = deadline_ns - now_ns;
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec  += (time_t)(wait_ns / 1000000000ULL);
                ts.tv_nsec += (long)(wait_ns % 1000000000ULL);
                if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
                pthread_cond_timedwait(&g_tx.cond, &g_tx.lock, &ts);
            }
            else
            {
                if (!g_tx.closed && !g_tx.flushing && !g_tx.serializing &&
                    g_tx.done == g_tx.joined)
                {
                    /* Self-rescue: become closer (end-of-benchmark edge case) */
                    g_tx.closed          = true;
                    g_tx.closed_by_count = false;
                    g_tx.flushing        = true;
                    atomic_fetch_add_explicit(&g_metrics.tx_closed_by_timeout, 1,
                                              memory_order_relaxed);
                    int joined_snap = g_tx.joined;
                    pthread_mutex_unlock(&g_tx.lock);

                    uint64_t t_cw = monotonic_ns();
                    pthread_mutex_lock(&g_cq_lock);
                    while (g_cq_count >= COMMIT_Q_DEPTH)
                        pthread_cond_wait(&g_cq_notfull, &g_cq_lock);
                    int reserved_slot = g_cq_tail;
                    pthread_mutex_unlock(&g_cq_lock);
                    atomic_fetch_add_explicit(&g_metrics.commit_wait_ns_total,
                                              monotonic_ns() - t_cw, memory_order_relaxed);

                    commit_batch_t batch;
                    flush_prepare_phase1(reserved_slot, &batch);

                    pthread_mutex_lock(&g_tx.lock);
                    if (batch.n_pages > 0)
                    {
                        g_cq_slots[reserved_slot] = batch;
                        atomic_store_explicit(&g_tx.serial_claimed, 0, memory_order_relaxed);
                        atomic_store_explicit(&g_tx.serial_done,   0, memory_order_relaxed);
                        g_tx.serial_total    = batch.n_pages;
                        g_tx.serial_buf_slot = reserved_slot;
                        g_tx.serializing     = true;
                        atomic_fetch_add_explicit(&g_metrics.tx_count, 1, memory_order_relaxed);
                        atomic_fetch_add_explicit(&g_metrics.tx_joined_sum,
                                                  (uint64_t)joined_snap, memory_order_relaxed);
                    }
                    else
                    {
                        g_tx.epoch++;
                        g_tx.joined = 0; g_tx.done = 0;
                        g_tx.closed = false; g_tx.closed_by_count = false;
                        g_tx.flushing = false;
                        pthread_cond_broadcast(&g_tx.cond);
                        pthread_mutex_unlock(&g_tx.lock);
                        atomic_fetch_add_explicit(&g_metrics.epoch_wait_ns_total,
                                                  monotonic_ns() - t0, memory_order_relaxed);
                        return;
                    }
                    pthread_cond_broadcast(&g_tx.cond);
                }
                else
                {
                    pthread_cond_wait(&g_tx.cond, &g_tx.lock);
                }
            }
        }
    }

    atomic_fetch_add_explicit(&g_metrics.epoch_wait_ns_total,
                              monotonic_ns() - t0, memory_order_relaxed);
    pthread_mutex_unlock(&g_tx.lock);
}

/* ─────────────────── Worker threads ─────────────────── */

typedef struct
{
    int64_t *keys;
    int n_keys;
} worker_arg;

static void *worker_main(void *arg)
{
    worker_arg *wa = (worker_arg *)arg;
    char value[120];

    for (int i = 0; i < wa->n_keys; i++)
    {
        snprintf(value, sizeof value, "val_%ld", (long)wa->keys[i]);
        tx_insert(wa->keys[i], value);
    }
    return NULL;
}

/* ─────────────────── Device init ─────────────────── */

static int device_open(const char *path)
{
    g_fd = zbd_open(path, O_RDWR, &g_info);
    if (g_fd < 0)
    {
        perror("zbd_open");
        return -1;
    }

    if (nvme_get_nsid(g_fd, &g_nsid) != 0)
    {
        perror("nvme_get_nsid");
        return -1;
    }

    g_nzones = g_info.nr_zones;
    g_zones = calloc(g_nzones, sizeof *g_zones);
    if (!g_zones)
    {
        perror("calloc zones");
        return -1;
    }

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

    /* Reset tree */
    atomic_store_explicit(&pool_next, 0, memory_order_relaxed);
    g_root = NID_NULL;
    g_sb_seq = 0;
    g_meta_wp = 0;
    g_cur_zone = DATA_ZONE_START;
    g_zone_wp = 0;
    atomic_store_explicit(&g_pages_appended, 0, memory_order_relaxed);

    /* Reset TX */
    g_tx.epoch = 0;
    g_tx.joined = 0;
    g_tx.done = 0;
    g_tx.closed = false;
    g_tx.closed_by_count = false;
    g_tx.flushing = false;
    g_tx.serializing = false;
    g_tx.expected = num_threads;
    atomic_store_explicit(&g_tx.serial_claimed, 0, memory_order_relaxed);
    atomic_store_explicit(&g_tx.serial_done,   0, memory_order_relaxed);
    g_tx.serial_total    = 0;
    g_tx.serial_buf_slot = 0;

    /* Reset commit queue */
    g_cq_head  = 0;
    g_cq_tail  = 0;
    g_cq_count = 0;
    g_cq_shutdown = false;

    /* Start committer thread */
    pthread_create(&g_committer, NULL, committer_main, NULL);

    /* Reset per-run metrics */
    memset(&g_metrics, 0, sizeof g_metrics);

    /* Shuffled keys */
    int64_t *all_keys = malloc((size_t)num_keys * sizeof(int64_t));
    if (!all_keys)
    {
        perror("malloc keys");
        return -1;
    }
    for (int i = 0; i < num_keys; i++)
        all_keys[i] = (int64_t)(i + 1);
    srand(42);
    shuffle(all_keys, (size_t)num_keys);

    /* Partition among threads */
    worker_arg *args = calloc((size_t)num_threads, sizeof *args);
    pthread_t *tids = calloc((size_t)num_threads, sizeof *tids);
    if (!args || !tids)
    {
        perror("calloc");
        free(all_keys);
        return -1;
    }

    int base = num_keys / num_threads;
    int rem = num_keys % num_threads;
    int64_t *ptr = all_keys;
    for (int t = 0; t < num_threads; t++)
    {
        args[t].n_keys = base + (t < rem ? 1 : 0);
        args[t].keys = ptr;
        ptr += args[t].n_keys;
    }

    printf("--- threads=%d keys=%d ---\n", num_threads, num_keys);
    fflush(stdout);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (int t = 0; t < num_threads; t++)
        pthread_create(&tids[t], NULL, worker_main, &args[t]);
    for (int t = 0; t < num_threads; t++)
        pthread_join(tids[t], NULL);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Flush any remaining partial TX synchronously (no committer thread needed) */
    pthread_mutex_lock(&g_tx.lock);
    if (g_tx.joined > 0 && g_tx.done == g_tx.joined &&
        !g_tx.flushing && !g_tx.serializing)
    {
        g_tx.flushing = true;
        pthread_mutex_unlock(&g_tx.lock);
        flush_tree_sync();
        pthread_mutex_lock(&g_tx.lock);
        g_tx.flushing = false;
        g_tx.joined   = 0;
        g_tx.done     = 0;
    }
    pthread_mutex_unlock(&g_tx.lock);

    /* Drain the commit queue and stop the committer thread */
    pthread_mutex_lock(&g_cq_lock);
    g_cq_shutdown = true;
    pthread_cond_signal(&g_cq_notempty);
    pthread_mutex_unlock(&g_cq_lock);
    pthread_join(g_committer, NULL);

    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
    double tput = (double)num_keys / elapsed;
    uint64_t pages = atomic_load_explicit(&g_pages_appended, memory_order_relaxed);
    uint32_t nodes = atomic_load_explicit(&pool_next, memory_order_relaxed);

    printf("  elapsed=%.3f s  throughput=%.0f ops/sec  pages_appended=%lu  nodes_used=%u\n",
           elapsed, tput, (unsigned long)pages, (unsigned)nodes);

    /* ── Detailed metrics ── */
    uint64_t tx_n = atomic_load_explicit(&g_metrics.tx_count, memory_order_relaxed);
    uint64_t joined_sum = atomic_load_explicit(&g_metrics.tx_joined_sum, memory_order_relaxed);
    uint64_t by_count = atomic_load_explicit(&g_metrics.tx_closed_by_count, memory_order_relaxed);
    uint64_t by_timeout = atomic_load_explicit(&g_metrics.tx_closed_by_timeout, memory_order_relaxed);
    uint64_t flush_ns    = atomic_load_explicit(&g_metrics.flush_ns_total,       memory_order_relaxed);
    uint64_t commit_ns   = atomic_load_explicit(&g_metrics.commit_ns_total,      memory_order_relaxed);
    uint64_t ser_ns      = atomic_load_explicit(&g_metrics.serialize_ns_total,   memory_order_relaxed);
    uint64_t cw_ns       = atomic_load_explicit(&g_metrics.commit_wait_ns_total, memory_order_relaxed);
    uint64_t nvme_ns     = atomic_load_explicit(&g_metrics.nvme_ns_total,        memory_order_relaxed);
    uint64_t ins_ns = atomic_load_explicit(&g_metrics.insert_ns_total, memory_order_relaxed);
    uint64_t entry_ns = atomic_load_explicit(&g_metrics.entry_wait_ns_total, memory_order_relaxed);
    uint64_t epoch_ns = atomic_load_explicit(&g_metrics.epoch_wait_ns_total, memory_order_relaxed);
    uint64_t dirty_tot = atomic_load_explicit(&g_metrics.dirty_nodes_total, memory_order_relaxed);
    uint64_t zone_adv = atomic_load_explicit(&g_metrics.zone_advance_count, memory_order_relaxed);
    uint64_t restarts = atomic_load_explicit(&g_metrics.restart_count, memory_order_relaxed);

    double avg_joined = tx_n ? (double)joined_sum / tx_n : 0.0;
    double avg_dirty = tx_n ? (double)dirty_tot / tx_n : 0.0;
    double avg_flush_ms   = tx_n ? (double)flush_ns  / tx_n / 1e6 : 0.0;
    double avg_commit_ms  = tx_n ? (double)commit_ns  / tx_n / 1e6 : 0.0;
    double avg_ser_ms     = tx_n ? (double)ser_ns     / tx_n / 1e6 : 0.0;
    double total_flush_ms = (double)flush_ns  / 1e6;
    double total_commit_ms = (double)commit_ns / 1e6;
    double total_ser_ms   = (double)ser_ns    / 1e6;
    double total_cw_ms    = (double)cw_ns     / 1e6;
    double total_nvme_ms  = (double)nvme_ns   / 1e6;
    double nvme_pct = commit_ns ? (double)nvme_ns / commit_ns * 100.0 : 0.0;
    (void)avg_flush_ms;
    double ins_s = (double)ins_ns / 1e9;
    double entry_s = (double)entry_ns / 1e9;
    double epoch_s = (double)epoch_ns / 1e9;
    uint64_t closed_total = by_count + by_timeout;
    double by_count_pct = closed_total ? (double)by_count / closed_total * 100.0 : 0.0;
    double by_timeout_pct = closed_total ? (double)by_timeout / closed_total * 100.0 : 0.0;
    double restarts_per = num_keys ? (double)restarts / num_keys : 0.0;

    printf("  [TX]\n");
    printf("    total TXs:            %lu\n", (unsigned long)tx_n);
    printf("    avg threads/TX:       %.2f  (expected=%d)\n", avg_joined, num_threads);
    printf("    closed by count:      %lu  (%.1f%%)\n", (unsigned long)by_count, by_count_pct);
    printf("    closed by timeout:    %lu  (%.1f%%)\n", (unsigned long)by_timeout, by_timeout_pct);
    printf("  [Pipeline]\n");
    printf("    avg dirty nodes/TX:   %.1f\n", avg_dirty);
    printf("    avg serialize time:   %.3f ms  (all workers, summed)\n", avg_ser_ms);
    printf("    avg commit time:      %.3f ms  (committer NVMe+SB, wall)\n", avg_commit_ms);
    printf("    total serialize:      %.1f ms\n", total_ser_ms);
    printf("    total commit:         %.1f ms\n", total_commit_ms);
    printf("    total NVMe I/O:       %.1f ms  (%.1f%% of commit)\n",
           total_nvme_ms, nvme_pct);
    printf("    commit_wait (closer): %.1f ms  (time waiting for free slot)\n", total_cw_ms);
    printf("    zone advances:        %lu\n", (unsigned long)zone_adv);
    (void)total_flush_ms;
    printf("  [Thread time, summed across all threads]\n");
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

    int num_keys = atoi(argv[1]);
    int thread_mode = atoi(argv[2]);
    const char *devpath = (argc >= 4) ? argv[3] : "/dev/nvme3n2";

    if (num_keys <= 0)
    {
        fprintf(stderr, "num_keys must be > 0\n");
        return 1;
    }

    /* Pre-allocate node pool */
    size_t est_leaves = (size_t)num_keys / (LEAF_MAX / 2) + 16;
    size_t est_internal = est_leaves / (INT_MAX_K / 2) + 16;
    pool_cap = (est_leaves + est_internal) * 4; /* 4× safety margin */
    if (pool_cap < 65536)
        pool_cap = 65536;

    pool = calloc(pool_cap, sizeof(ram_node));
    if (!pool)
    {
        perror("calloc pool");
        return 1;
    }

    /* DFS scratch */
    dfs_order = malloc(pool_cap * sizeof(nid_t));
    if (!dfs_order)
    {
        perror("malloc dfs_order");
        return 1;
    }

    /* Per-node locks: calloc zero == PTHREAD_RWLOCK_INITIALIZER on Linux */
    node_locks = calloc(pool_cap, sizeof(pthread_rwlock_t));
    if (!node_locks)
    {
        perror("calloc node_locks");
        return 1;
    }
    pthread_rwlock_init(&root_lock, NULL);

    /* Init TX */
    pthread_mutex_init(&g_tx.lock, NULL);
    pthread_cond_init(&g_tx.cond, NULL);

    /* Init commit queue */
    pthread_mutex_init(&g_cq_lock, NULL);
    pthread_cond_init(&g_cq_notempty, NULL);
    pthread_cond_init(&g_cq_notfull, NULL);

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

    free(pool);
    free(dfs_order);
    return 0;
}
