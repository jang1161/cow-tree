/*
 * impl.c — Standalone COW B-tree for ZNS NVMe
 * Parallel latch-crabbing insert + barrier TX + batched NVMe append
 * Dirty-node tracking: only modified path nodes are written per TX flush.
 *
 * Build:
 *   gcc -O2 -g -Wall -Wextra -std=c11 -pthread -Iinclude -Iinclude/variants \
 *       -I. impl.c -o impl -lzbd -lnvme -lpthread
 * Run:
 *   sudo ./impl <num_keys> <thread_mode> [dev]
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
#define PAGE_SIZE        4096
#define LEAF_ORDER       32        /* max 31 keys/leaf            */
#define INTERNAL_ORDER   249       /* max 248 keys, 249 children  */
#define MAX_APPEND_PAGES 128
#define TX_TIMEOUT_NS    (500000ULL)  /* 500 µs */
#define META_ZONE        0
#define DATA_ZONE_START  1
#define SB_MAGIC         0x434F574252414D31ULL
#define MAX_HEIGHT       32
#define HS_MAX           (MAX_HEIGHT + 4)

#define LEAF_MAX    (LEAF_ORDER - 1)      /* 31  */
#define INT_MAX_K   (INTERNAL_ORDER - 1)  /* 248 */
#define INT_CHILDREN INTERNAL_ORDER       /* 249 */

typedef uint32_t nid_t;
typedef uint64_t pgn_t;
#define NID_NULL ((nid_t)0xFFFFFFFFu)
#define PGN_NULL ((pgn_t)0xFFFFFFFFFFFFFFFFull)

/* ─────────────────── Disk page layout (4096 B) ─────────────────── */
typedef struct { uint64_t key; char value[120]; } leaf_ent;  /* 128 B */
typedef struct { uint64_t key; pgn_t child;     } int_ent;   /*  16 B */

typedef struct {
    uint32_t is_leaf;
    uint32_t num_keys;
    pgn_t    pointer;    /* rightmost child (internal) or PGN_NULL (leaf) */
    uint8_t  hpad[112];  /* header = 128 B total */
    union {
        leaf_ent  leaf[LEAF_MAX];      /* 31 × 128 = 3968 B */
        int_ent   internal[INT_MAX_K]; /* 248 × 16 = 3968 B */
    };
} disk_page;
_Static_assert(sizeof(disk_page) == PAGE_SIZE, "disk_page != 4096");

typedef struct {
    uint64_t magic;
    uint64_t seq_no;
    pgn_t    root_pn;
    uint32_t leaf_order;
    uint32_t internal_order;
    uint8_t  pad[PAGE_SIZE - 8 - 8 - 8 - 4 - 4];
} superblock_t;
_Static_assert(sizeof(superblock_t) == PAGE_SIZE, "superblock != 4096");

/* ─────────────────── RAM node ─────────────────── */
/*
 * dirty=1: this node was modified since last flush; path-copy invariant
 *          guarantees that dirty child ⇒ dirty parent.
 * pagenum: disk page number assigned at last flush (PGN_NULL if never flushed).
 */
typedef struct {
    uint32_t num_keys;
    uint8_t  is_leaf;
    uint8_t  dirty;    /* 1 if modified since last flush */
    uint8_t  _pad[2];
    pgn_t    pagenum;  /* last written disk page number */
    uint64_t keys[INT_MAX_K];         /* 248 keys */
    union {
        char   values[LEAF_MAX][120]; /* leaf: 31 × 120 */
        nid_t  children[INT_CHILDREN]; /* internal: 249 */
    };
} ram_node;

/* ─────────────────── Node pool ─────────────────── */
static ram_node         *pool;
static size_t            pool_cap;
static _Atomic(uint32_t) pool_next;

static inline nid_t pool_alloc(void) {
    uint32_t idx = atomic_fetch_add_explicit(&pool_next, 1, memory_order_relaxed);
    if (idx >= (uint32_t)pool_cap) { fprintf(stderr, "pool exhausted\n"); abort(); }
    ram_node *n = &pool[idx];
    memset(n, 0, sizeof *n);
    n->pagenum = PGN_NULL;
    n->dirty   = 1; /* new nodes are always dirty */
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
static pthread_rwlock_t  root_lock;
static nid_t             g_root = NID_NULL;

#define NID_LOCK(n)  (&node_locks[n])

/* ─────────────────── ZNS Device ─────────────────── */
static int              g_fd;
static __u32            g_nsid;
static struct zbd_info  g_info;
static struct zbd_zone *g_zones;
static uint32_t         g_nzones;

static uint32_t  g_cur_zone;   /* current data zone index */
static uint64_t  g_zone_wp;    /* pages written to current data zone */
static uint64_t  g_meta_wp;    /* pages written to META_ZONE */
static uint64_t  g_sb_seq = 0;
static pthread_mutex_t g_dev_lock;

static _Atomic(uint64_t) g_pages_appended;

/* ─────────────────── TX machine ─────────────────── */
typedef struct {
    uint64_t epoch;
    int      expected;  /* = num_threads for current run */
    int      joined;
    int      done;
    bool     closed;
    bool     flushing;
    uint64_t open_ns;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
} tx_t;

static tx_t g_tx;

/* ─────────────────── Utility ─────────────────── */
static inline uint64_t monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ─────────────────── NVMe zone append ─────────────────── */
static int zone_append_n(uint32_t zone_id, const void *buf, int n_pages,
                          pgn_t *out_start_pn) {
    __u64    zslba    = g_zones[zone_id].start / g_info.lblock_size;
    uint32_t total_lb = (uint32_t)n_pages * (uint32_t)(PAGE_SIZE / g_info.lblock_size);
    __u16    nlb      = (uint16_t)(total_lb - 1);
    __u64    result   = 0;

    struct nvme_zns_append_args args = {
        .zslba        = zslba,
        .result       = &result,
        .data         = (void *)buf,
        .metadata     = NULL,
        .args_size    = sizeof(args),
        .fd           = g_fd,
        .timeout      = 0,
        .nsid         = g_nsid,
        .ilbrt        = 0,
        .data_len     = (uint32_t)(n_pages * PAGE_SIZE),
        .metadata_len = 0,
        .nlb          = nlb,
        .control      = 0,
        .lbat         = 0,
        .lbatm        = 0,
        .ilbrt_u64    = 0,
    };

    if (nvme_zns_append(&args) != 0) {
        fprintf(stderr, "nvme_zns_append zone=%u zslba=%llu nlb=%u data_len=%u: %s\n",
                zone_id, (unsigned long long)zslba, (unsigned)nlb,
                (unsigned)(n_pages * PAGE_SIZE), strerror(errno));
        return -1;
    }

    if (out_start_pn)
        *out_start_pn = (pgn_t)(result * g_info.lblock_size / PAGE_SIZE);

    atomic_fetch_add_explicit(&g_pages_appended, (uint64_t)n_pages, memory_order_relaxed);
    return 0;
}

static void write_superblock(pgn_t root_pn) {
    uint64_t cap = g_zones[META_ZONE].capacity / PAGE_SIZE;
    if (g_meta_wp >= cap) return; /* meta zone full - skip */

    static superblock_t sb __attribute__((aligned(PAGE_SIZE)));
    memset(&sb, 0, sizeof sb);
    sb.magic          = SB_MAGIC;
    sb.seq_no         = ++g_sb_seq;
    sb.root_pn        = root_pn;
    sb.leaf_order     = LEAF_ORDER;
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
static nid_t   *dfs_order;   /* pool_cap entries */
static int      dfs_count;

/* Write buffer: PAGE_SIZE-aligned for NVMe DMA */
static uint8_t g_wbuf[MAX_APPEND_PAGES * PAGE_SIZE] __attribute__((aligned(PAGE_SIZE)));

static void dfs_dirty_collect(nid_t nid) {
    if (nid == NID_NULL) return;
    ram_node *n = &pool[nid];
    if (!n->dirty) return; /* prune: no dirty descendants below non-dirty */
    if (!n->is_leaf) {
        for (uint32_t i = 0; i <= n->num_keys; i++)
            dfs_dirty_collect(n->children[i]);
    }
    dfs_order[dfs_count++] = nid; /* post-order */
}

static pgn_t flush_tree(void) {
    if (g_root == NID_NULL) return PGN_NULL;

    dfs_count = 0;
    dfs_dirty_collect(g_root);
    if (dfs_count == 0) return pool[g_root].pagenum;

    int total = dfs_count;

    pthread_mutex_lock(&g_dev_lock);

    /* Advance zone if needed */
    uint64_t zone_cap = g_zones[g_cur_zone].capacity / PAGE_SIZE;
    if (g_zone_wp + (uint64_t)total > zone_cap) {
        g_cur_zone++;
        if (g_cur_zone >= g_nzones) {
            fprintf(stderr, "All zones exhausted!\n");
            abort();
        }
        g_zone_wp = 0;
    }

    /* Pre-assign page numbers (post-order = children before parents) */
    pgn_t base_pgn = (pgn_t)(g_zones[g_cur_zone].start / PAGE_SIZE + g_zone_wp);
    for (int i = 0; i < total; i++)
        pool[dfs_order[i]].pagenum = base_pgn + (pgn_t)i;

    /* Build disk pages and batch-write */
    int written = 0;
    while (written < total) {
        int chunk = total - written;
        if (chunk > MAX_APPEND_PAGES) chunk = MAX_APPEND_PAGES;

        for (int i = 0; i < chunk; i++) {
            nid_t nid = dfs_order[written + i];
            ram_node *rn = &pool[nid];
            disk_page *dp = (disk_page *)(g_wbuf + (size_t)i * PAGE_SIZE);
            memset(dp, 0, PAGE_SIZE);
            dp->is_leaf  = rn->is_leaf;
            dp->num_keys = rn->num_keys;
            if (rn->is_leaf) {
                dp->pointer = PGN_NULL;
                for (uint32_t k = 0; k < rn->num_keys; k++) {
                    dp->leaf[k].key = rn->keys[k];
                    memcpy(dp->leaf[k].value, rn->values[k], 120);
                }
            } else {
                for (uint32_t k = 0; k < rn->num_keys; k++) {
                    dp->internal[k].key   = rn->keys[k];
                    /* child pagenum is either new (dirty child, already assigned
                     * above) or old (non-dirty child, valid from prev flush) */
                    dp->internal[k].child = pool[rn->children[k]].pagenum;
                }
                dp->pointer = pool[rn->children[rn->num_keys]].pagenum;
            }
        }

        pgn_t got __attribute__((unused));
        if (zone_append_n(g_cur_zone, g_wbuf, chunk, &got) != 0) {
            fprintf(stderr, "flush: append failed\n");
            abort();
        }
        written += chunk;
    }
    g_zone_wp += (uint64_t)total;

    pgn_t root_pgn = pool[g_root].pagenum;
    write_superblock(root_pgn);

    /* Clear dirty flags */
    for (int i = 0; i < total; i++)
        pool[dfs_order[i]].dirty = 0;

    pthread_mutex_unlock(&g_dev_lock);
    return root_pgn;
}

/* ─────────────────── B-tree insert (latch crabbing) ─────────────────── */

static inline uint32_t get_leaf_pos(const ram_node *n, int64_t key) {
    uint32_t pos = 0;
    while (pos < n->num_keys && (int64_t)n->keys[pos] < key) pos++;
    return pos;
}

static inline uint32_t get_int_pos(const ram_node *n, int64_t key) {
    for (uint32_t i = 0; i < n->num_keys; i++)
        if (key < (int64_t)n->keys[i]) return i;
    return n->num_keys; /* rightmost */
}

static void tree_insert(int64_t key, const char *value) {
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
     * Upgrade: if cur was rdlocked and child turns out unsafe, release rdlock,
     * re-acquire as wrlock, re-read cur's state, then continue in write mode.
     */
    pthread_rwlock_t *hs[HS_MAX];
    int  hs_n = 0;
    bool cur_rdlocked = false;
    nid_t cur = NID_NULL;

#define HS_WRLOCK(lk) do { pthread_rwlock_wrlock(lk); hs[hs_n++]=(lk); } while(0)
#define HS_UNLOCK(lk) do {                                                  \
        for(int _i=0;_i<hs_n;_i++)                                         \
            if(hs[_i]==(lk)){hs[_i]=hs[--hs_n];pthread_rwlock_unlock(lk);break;} \
    } while(0)
#define HS_UNLOCK_ALL() do {                                                \
        for(int _i=0;_i<hs_n;_i++) pthread_rwlock_unlock(hs[_i]);          \
        hs_n=0; } while(0)
#define HS_RELEASE_EXCEPT(keep) do {                                        \
        int _n=0;                                                           \
        for(int _i=0;_i<hs_n;_i++){                                        \
            if(hs[_i]==(keep)) hs[_n++]=hs[_i];                            \
            else pthread_rwlock_unlock(hs[_i]);                             \
        } hs_n=_n; } while(0)

    /* ── Root acquisition loop (retry if root changed during lock gap) ── */
    for (;;) {
        hs_n = 0;
        cur_rdlocked = false;

        /* Read g_root under rdlock (wrlock only if tree is empty) */
        pthread_rwlock_rdlock(&root_lock);
        nid_t cur_root = g_root;
        pthread_rwlock_unlock(&root_lock);

        if (cur_root == NID_NULL) {
            /* Empty tree: need wrlock to create root */
            pthread_rwlock_wrlock(&root_lock);
            if (g_root == NID_NULL) {
                nid_t rid = pool_alloc();
                ram_node *r = &pool[rid];
                r->is_leaf  = 1;
                r->num_keys = 1;
                r->keys[0]  = (uint64_t)key;
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

        if (valid) {
            cur = cur_root;
            break; /* root node locked and validated */
        }
        /* Stale root — release and retry (fires at most O(log N) times total) */
        HS_UNLOCK_ALL();
    }

    typedef struct { nid_t id; uint32_t cidx; } anc_t;
    anc_t anc[MAX_HEIGHT];
    int   anc_n = 0;

    /* Phase 1: top-down descent with latch crabbing + read/write lock separation */
    for (;;) {
        ram_node *n = &pool[cur];
        if (n->is_leaf) break;

        bool cur_safe = (n->num_keys < (uint32_t)INT_MAX_K);

        uint32_t cidx  = get_int_pos(n, key);
        nid_t    child = n->children[cidx];

        /* Peek at child safety without locking (stale read is OK as a heuristic;
         * we re-read after acquiring the child's lock when needed). */
        bool child_is_leaf = pool[child].is_leaf;
        bool child_safe    = !child_is_leaf &&
                             (pool[child].num_keys < (uint32_t)INT_MAX_K);

        if (cur_safe && child_safe) {
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
        } else {
            /*
             * cur is unsafe, OR child is unsafe/leaf: switch to wrlock mode.
             * If cur was rdlocked, upgrade it first.
             */
            if (cur_rdlocked) {
                pthread_rwlock_unlock(NID_LOCK(cur));
                hs_n = 0; /* cur was the sole entry */
                pthread_rwlock_wrlock(NID_LOCK(cur));
                hs[hs_n++] = NID_LOCK(cur);
                /* Re-read after lock gap */
                cur_safe = (n->num_keys < (uint32_t)INT_MAX_K);
                cidx     = get_int_pos(n, key);
                child    = n->children[cidx];
                cur_rdlocked = false;
            }

            /* cur has wrlock */
            n->dirty = 1;

            if (cur_safe) {
                HS_RELEASE_EXCEPT(NID_LOCK(cur));
                anc_n = 0;
            }

            if (anc_n >= MAX_HEIGHT) { HS_UNLOCK_ALL(); return; }
            anc[anc_n].id   = cur;
            anc[anc_n].cidx = cidx;
            anc_n++;

            HS_WRLOCK(NID_LOCK(child));
            cur_rdlocked = false;
        }

        cur = child;
    }

    /* Phase 2: leaf insert / split */
    ram_node *leaf = &pool[cur];

    /* Defensive upgrade in case cur somehow ended up rdlocked at the leaf */
    if (cur_rdlocked) {
        pthread_rwlock_unlock(NID_LOCK(cur));
        hs_n = 0;
        pthread_rwlock_wrlock(NID_LOCK(cur));
        hs[hs_n++] = NID_LOCK(cur);
        cur_rdlocked = false;
    }

    leaf->dirty = 1;

    /* Duplicate key update */
    for (uint32_t i = 0; i < leaf->num_keys; i++) {
        if ((int64_t)leaf->keys[i] == key) {
            memcpy(leaf->values[i], value, 120);
            HS_UNLOCK_ALL();
            return;
        }
    }

    nid_t   carry_left  = cur;
    nid_t   carry_right = NID_NULL;
    int     carry_split = 0;
    int64_t carry_key   = 0;

    if (leaf->num_keys < (uint32_t)LEAF_MAX) {
        uint32_t pos = get_leaf_pos(leaf, key);
        for (int64_t i = (int64_t)leaf->num_keys - 1; i >= (int64_t)pos; i--) {
            leaf->keys[i + 1] = leaf->keys[i];
            memcpy(leaf->values[i + 1], leaf->values[i], 120);
        }
        leaf->keys[pos] = (uint64_t)key;
        memcpy(leaf->values[pos], value, 120);
        leaf->num_keys++;
    } else {
        /* Leaf split */
        uint64_t tmp_keys[LEAF_ORDER];
        char     tmp_vals[LEAF_ORDER][120];
        uint32_t pos = get_leaf_pos(leaf, key);

        for (uint32_t i = 0; i < pos; i++) {
            tmp_keys[i] = leaf->keys[i];
            memcpy(tmp_vals[i], leaf->values[i], 120);
        }
        tmp_keys[pos] = (uint64_t)key;
        memcpy(tmp_vals[pos], value, 120);
        for (uint32_t i = pos; i < leaf->num_keys; i++) {
            tmp_keys[i + 1] = leaf->keys[i];
            memcpy(tmp_vals[i + 1], leaf->values[i], 120);
        }

        uint32_t sp = LEAF_ORDER / 2;
        leaf->num_keys = sp;
        for (uint32_t i = 0; i < sp; i++) {
            leaf->keys[i] = tmp_keys[i];
            memcpy(leaf->values[i], tmp_vals[i], 120);
        }

        nid_t right_id = pool_alloc();
        ram_node *right = &pool[right_id];
        right->is_leaf  = 1;
        right->num_keys = LEAF_ORDER - sp;
        for (uint32_t i = 0; i < (uint32_t)(LEAF_ORDER - sp); i++) {
            right->keys[i] = tmp_keys[sp + i];
            memcpy(right->values[i], tmp_vals[sp + i], 120);
        }

        carry_split = 1;
        carry_right = right_id;
        carry_key   = (int64_t)right->keys[0];
    }

    if (!carry_split) { HS_UNLOCK_ALL(); return; }

    HS_UNLOCK(NID_LOCK(cur)); /* release leaf; ancestors remain locked */

    /* Phase 3: propagate splits up ancestor stack */
    for (int i = anc_n - 1; i >= 0; i--) {
        nid_t    par_id = anc[i].id;
        uint32_t cidx   = anc[i].cidx;
        ram_node *par   = &pool[par_id];

        par->children[cidx] = carry_left;

        if (par->num_keys < (uint32_t)INT_MAX_K) {
            uint32_t pos = cidx;
            for (int64_t j = (int64_t)par->num_keys - 1; j >= (int64_t)pos; j--) {
                par->keys[j + 1]     = par->keys[j];
                par->children[j + 2] = par->children[j + 1];
            }
            par->keys[pos]         = (uint64_t)carry_key;
            par->children[pos + 1] = carry_right;
            par->num_keys++;
            HS_UNLOCK_ALL();
            return;
        }

        /* Internal split */
        uint64_t tkeys[INTERNAL_ORDER + 1];
        nid_t    tchld[INTERNAL_ORDER + 2];

        for (uint32_t j = 0; j < cidx; j++) {
            tkeys[j] = par->keys[j];
            tchld[j] = par->children[j];
        }
        tchld[cidx]     = carry_left;
        tkeys[cidx]     = (uint64_t)carry_key;
        tchld[cidx + 1] = carry_right;
        for (uint32_t j = cidx; j < par->num_keys; j++) {
            tkeys[j + 1] = par->keys[j];
            tchld[j + 2] = par->children[j + 1];
        }

        uint32_t total_k = par->num_keys + 1;
        uint32_t sp      = total_k / 2;
        int64_t  up_key  = (int64_t)tkeys[sp];

        par->num_keys = sp;
        for (uint32_t j = 0; j < sp; j++) {
            par->keys[j]     = tkeys[j];
            par->children[j] = tchld[j];
        }
        par->children[sp] = tchld[sp];

        nid_t right_id  = pool_alloc();
        ram_node *right = &pool[right_id];
        right->is_leaf  = 0;
        right->num_keys = total_k - sp - 1;
        for (uint32_t j = 0; j < right->num_keys; j++) {
            right->keys[j]     = tkeys[sp + 1 + j];
            right->children[j] = tchld[sp + 1 + j];
        }
        right->children[right->num_keys] = tchld[total_k];

        carry_split = 1;
        carry_right = right_id;
        carry_left  = par_id;
        carry_key   = up_key;

        HS_UNLOCK(NID_LOCK(par_id));
    }

    /* Phase 4: root split — brief wrlock on root_lock to update g_root */
    if (carry_split) {
        nid_t new_root = pool_alloc();
        ram_node *r    = &pool[new_root];
        r->is_leaf     = 0;
        r->num_keys    = 1;
        r->keys[0]     = (uint64_t)carry_key;
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
 * TX machine: `expected` workers join, each applies one insert,
 * last one to complete flushes. Epoch changes signal TX completion.
 */
static void tx_insert(int64_t key, const char *value) {
    pthread_mutex_lock(&g_tx.lock);

    /* Wait until there's room in an open (non-closed, non-flushing) TX */
    for (;;) {
        if (!g_tx.closed && !g_tx.flushing) break;
        pthread_cond_wait(&g_tx.cond, &g_tx.lock);
    }

    uint64_t my_epoch = g_tx.epoch;
    g_tx.joined++;
    if (g_tx.joined == 1) g_tx.open_ns = monotonic_ns();
    if (g_tx.joined >= g_tx.expected) g_tx.closed = true;

    pthread_mutex_unlock(&g_tx.lock);

    /* Parallel latch-crabbing insert */
    tree_insert(key, value);

    pthread_mutex_lock(&g_tx.lock);
    g_tx.done++;

    /* Check close-by-timeout */
    if (!g_tx.closed && g_tx.done == g_tx.joined) {
        if (monotonic_ns() - g_tx.open_ns >= TX_TIMEOUT_NS)
            g_tx.closed = true;
    }

    bool do_flush = g_tx.closed && (g_tx.done == g_tx.joined) && !g_tx.flushing;

    if (do_flush) {
        g_tx.flushing = true;
        pthread_mutex_unlock(&g_tx.lock);

        flush_tree();

        pthread_mutex_lock(&g_tx.lock);
        g_tx.epoch++;
        g_tx.joined   = 0;
        g_tx.done     = 0;
        g_tx.closed   = false;
        g_tx.flushing = false;
        pthread_cond_broadcast(&g_tx.cond);
        pthread_mutex_unlock(&g_tx.lock);
    } else {
        while (g_tx.epoch == my_epoch)
            pthread_cond_wait(&g_tx.cond, &g_tx.lock);
        pthread_mutex_unlock(&g_tx.lock);
    }
}

/* ─────────────────── Worker threads ─────────────────── */

typedef struct {
    int64_t *keys;
    int      n_keys;
} worker_arg;

static void *worker_main(void *arg) {
    worker_arg *wa = (worker_arg *)arg;
    char value[120];

    for (int i = 0; i < wa->n_keys; i++) {
        snprintf(value, sizeof value, "val_%ld", (long)wa->keys[i]);
        tx_insert(wa->keys[i], value);
    }
    return NULL;
}

/* ─────────────────── Device init ─────────────────── */

static int device_open(const char *path) {
    g_fd = zbd_open(path, O_RDWR, &g_info);
    if (g_fd < 0) { perror("zbd_open"); return -1; }

    if (nvme_get_nsid(g_fd, &g_nsid) != 0) { perror("nvme_get_nsid"); return -1; }

    g_nzones = g_info.nr_zones;
    g_zones  = calloc(g_nzones, sizeof *g_zones);
    if (!g_zones) { perror("calloc zones"); return -1; }

    unsigned int nr = g_nzones;
    if (zbd_report_zones(g_fd, 0, 0, ZBD_RO_ALL, g_zones, &nr) != 0) {
        perror("zbd_report_zones"); return -1;
    }

    pthread_mutex_init(&g_dev_lock, NULL);
    return 0;
}

static void device_close(void) {
    free(g_zones);
    g_zones = NULL;
    pthread_mutex_destroy(&g_dev_lock);
    close(g_fd);
    g_fd = -1;
}

/* ─────────────────── Key generation ─────────────────── */
static void shuffle(int64_t *arr, size_t n) {
    for (size_t i = n - 1; i > 0; i--) {
        size_t j = (size_t)rand() % (i + 1);
        int64_t t = arr[i]; arr[i] = arr[j]; arr[j] = t;
    }
}

/* ─────────────────── Benchmark runner ─────────────────── */

static int run_benchmark(int num_keys, int num_threads, const char *devpath) {
    char cmd[256];

    printf("=== Reset device %s ===\n", devpath);
    fflush(stdout);
    snprintf(cmd, sizeof cmd, "nvme zns reset-zone -a %s", devpath);
    if (system(cmd) != 0) { fprintf(stderr, "zone reset failed\n"); return -1; }

    if (g_fd >= 0) device_close();
    if (device_open(devpath) != 0) return -1;

    /* Reset tree */
    atomic_store_explicit(&pool_next, 0, memory_order_relaxed);
    g_root    = NID_NULL;
    g_sb_seq  = 0;
    g_meta_wp = 0;
    g_cur_zone = DATA_ZONE_START;
    g_zone_wp  = 0;
    atomic_store_explicit(&g_pages_appended, 0, memory_order_relaxed);

    /* Reset TX */
    g_tx.epoch    = 0;
    g_tx.joined   = 0;
    g_tx.done     = 0;
    g_tx.closed   = false;
    g_tx.flushing = false;
    g_tx.expected = num_threads;

    /* Shuffled keys */
    int64_t *all_keys = malloc((size_t)num_keys * sizeof(int64_t));
    if (!all_keys) { perror("malloc keys"); return -1; }
    for (int i = 0; i < num_keys; i++) all_keys[i] = (int64_t)(i + 1);
    srand(42);
    shuffle(all_keys, (size_t)num_keys);

    /* Partition among threads */
    worker_arg *args = calloc((size_t)num_threads, sizeof *args);
    pthread_t  *tids = calloc((size_t)num_threads, sizeof *tids);
    if (!args || !tids) { perror("calloc"); free(all_keys); return -1; }

    int base = num_keys / num_threads;
    int rem  = num_keys % num_threads;
    int64_t *ptr = all_keys;
    for (int t = 0; t < num_threads; t++) {
        args[t].n_keys = base + (t < rem ? 1 : 0);
        args[t].keys   = ptr;
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

    /* Flush any remaining partial TX */
    pthread_mutex_lock(&g_tx.lock);
    if (g_tx.joined > 0 && g_tx.done == g_tx.joined && !g_tx.flushing) {
        g_tx.flushing = true;
        pthread_mutex_unlock(&g_tx.lock);
        flush_tree();
        pthread_mutex_lock(&g_tx.lock);
        g_tx.flushing = false;
        g_tx.joined   = 0;
        g_tx.done     = 0;
    }
    pthread_mutex_unlock(&g_tx.lock);

    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) * 1e-9;
    double tput    = (double)num_keys / elapsed;
    uint64_t pages = atomic_load_explicit(&g_pages_appended, memory_order_relaxed);
    uint32_t nodes = atomic_load_explicit(&pool_next, memory_order_relaxed);

    printf("  elapsed=%.3f s  throughput=%.0f ops/sec  pages_appended=%lu  nodes_used=%u\n",
           elapsed, tput, (unsigned long)pages, (unsigned)nodes);
    fflush(stdout);

    free(all_keys);
    free(args);
    free(tids);

    device_close();
    return 0;
}

/* ─────────────────── Main ─────────────────── */

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <num_keys> <thread_mode> [dev]\n", argv[0]);
        fprintf(stderr, "  thread_mode: 0=all(1,2,4,8,16,32,64), N=specific\n");
        return 1;
    }

    int num_keys    = atoi(argv[1]);
    int thread_mode = atoi(argv[2]);
    const char *devpath = (argc >= 4) ? argv[3] : "/dev/nvme3n2";

    if (num_keys <= 0) { fprintf(stderr, "num_keys must be > 0\n"); return 1; }

    /* Pre-allocate node pool */
    size_t est_leaves   = (size_t)num_keys / (LEAF_MAX / 2) + 16;
    size_t est_internal = est_leaves / (INT_MAX_K / 2) + 16;
    pool_cap = (est_leaves + est_internal) * 4; /* 4× safety margin */
    if (pool_cap < 65536) pool_cap = 65536;

    pool = calloc(pool_cap, sizeof(ram_node));
    if (!pool) { perror("calloc pool"); return 1; }

    /* DFS scratch */
    dfs_order = malloc(pool_cap * sizeof(nid_t));
    if (!dfs_order) { perror("malloc dfs_order"); return 1; }

    /* Per-node locks: calloc zero == PTHREAD_RWLOCK_INITIALIZER on Linux */
    node_locks = calloc(pool_cap, sizeof(pthread_rwlock_t));
    if (!node_locks) { perror("calloc node_locks"); return 1; }
    pthread_rwlock_init(&root_lock, NULL);

    /* Init TX */
    pthread_mutex_init(&g_tx.lock, NULL);
    pthread_cond_init(&g_tx.cond, NULL);

    g_fd = -1;

    int thread_counts[] = {1, 2, 4, 8, 16, 32, 64};
    int n_counts = (int)(sizeof(thread_counts) / sizeof(thread_counts[0]));

    if (thread_mode == 0) {
        for (int i = 0; i < n_counts; i++) {
            if (run_benchmark(num_keys, thread_counts[i], devpath) != 0)
                fprintf(stderr, "failed for %d threads\n", thread_counts[i]);
        }
    } else {
        if (run_benchmark(num_keys, thread_mode, devpath) != 0)
            return 1;
    }

    free(pool);
    free(dfs_order);
    return 0;
}
