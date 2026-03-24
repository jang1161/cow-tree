#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cow_tree_v1_3.h"

#define MAX_HEIGHT    16
#define RIGHTMOST_IDX UINT32_MAX
#define RETRY_YIELD_INTERVAL 64

typedef struct { 
    pagenum_t pn; 
    uint32_t cidx; 
} path_entry;

typedef struct { 
    path_entry e[MAX_HEIGHT]; 
    int depth; 
} tpath;

static int is_empty_snapshot(pagenum_t root_pn) {
    return root_pn == INVALID_PGN;
}

static void read_tree_snapshot(cow_tree *t, pagenum_t *root_pn, uint64_t *seq_no) {
    for (;;) {
        uint64_t s1 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 & 1ULL) {
            continue;
        }
        pagenum_t r = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
        uint64_t s2 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 == s2 && ((s2 & 1ULL) == 0)) {
            *root_pn = r;
            *seq_no = s2;
            return;
        }
    }
}

static inline uint32_t zone_next(cow_tree *t, uint32_t zone_id) {
    return (zone_id + 1 < t->info.nr_zones) ? (zone_id + 1) : t->info.nr_zones;
}

static uint32_t reserve_writable_zone(cow_tree *t) {
    for (;;) {
        uint32_t cur = atomic_load_explicit(&t->current_zone, memory_order_acquire);
        if (cur < DATA_ZONE_START) {
            uint32_t expected = cur;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, DATA_ZONE_START,
                memory_order_acq_rel, memory_order_acquire);
            cur = DATA_ZONE_START;
        }

        if (cur >= t->info.nr_zones) {
            fprintf(stderr, "zones exhausted\n");
            exit(EXIT_FAILURE);
        }

        if (atomic_load_explicit(&t->zone_full[cur], memory_order_acquire)) {
            uint32_t next = zone_next(t, cur);
            uint32_t expected = cur;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }

        return cur;
    }
}

static void load_page(cow_tree *t, pagenum_t pn, page *dst) {
    off_t off = (off_t)pn * PAGE_SIZE;

    if (t->direct_fd >= 0) {
        void *raw = NULL;
        if (posix_memalign(&raw, PAGE_SIZE, PAGE_SIZE) != 0) {
            perror("posix_memalign");
            exit(EXIT_FAILURE);
        }

        ssize_t n = pread(t->direct_fd, raw, PAGE_SIZE, off);
        if (n != PAGE_SIZE) {
            perror("load_page(O_DIRECT)");
            free(raw);
            exit(EXIT_FAILURE);
        }

        memcpy(dst, raw, PAGE_SIZE);
        free(raw);
        return;
    }

    if (pread(t->fd, dst, PAGE_SIZE, off) != PAGE_SIZE) {
        perror("load_page");
        exit(EXIT_FAILURE);
    }
}

static int zone_append_raw_nolock(cow_tree *t, uint32_t zone_id, const void *buf,
                                  pagenum_t *out_pn, uint64_t *out_wp_bytes) {
    __u64 zslba = t->zones[zone_id].start / t->info.lblock_size;
    __u16 nlb = (PAGE_SIZE / t->info.lblock_size) - 1;
    __u64 result = 0;

    struct nvme_zns_append_args args = {
        .zslba = zslba,
        .result = &result,
        .data = (void *)buf,
        .metadata = NULL,
        .args_size = sizeof(args),
        .fd = t->fd,
        .timeout = 0,
        .nsid = t->nsid,
        .ilbrt = 0,
        .data_len = PAGE_SIZE,
        .metadata_len = 0,
        .nlb = nlb,
        .control = 0,
        .lbat = 0,
        .lbatm = 0,
        .ilbrt_u64 = 0
    };

    if (nvme_zns_append(&args) != 0) {
        return -1;
    }

    if (out_wp_bytes) *out_wp_bytes = (result + nlb + 1) * t->info.lblock_size;
    if (out_pn) *out_pn = (pagenum_t)(result * t->info.lblock_size / PAGE_SIZE);

    return 0;
}

static pagenum_t cow_append_page(cow_tree *t, page *p) {
    const uint64_t page_bytes = PAGE_SIZE;
    uint64_t retry = 0;

    for (;;) {
        uint32_t zone_id = reserve_writable_zone(t);

        uint64_t old_wp = atomic_fetch_add_explicit(
            &t->zone_wp_bytes[zone_id], page_bytes, memory_order_acq_rel);
        uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;

        if (old_wp + page_bytes > zone_end) {
            atomic_fetch_sub_explicit(&t->zone_wp_bytes[zone_id], page_bytes, memory_order_acq_rel);
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);

            uint32_t expected = zone_id;
            uint32_t next = zone_next(t, zone_id);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }

        pagenum_t pn;
        uint64_t wp_bytes;
        if (zone_append_raw_nolock(t, zone_id, p, &pn, &wp_bytes) == 0) {
            uint64_t cur_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
            while (wp_bytes > cur_wp &&
                   !atomic_compare_exchange_weak_explicit(
                       &t->zone_wp_bytes[zone_id], &cur_wp, wp_bytes,
                       memory_order_acq_rel, memory_order_acquire)) {
            }

            if (wp_bytes >= zone_end) {
                atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
                uint32_t expected = zone_id;
                uint32_t next = zone_next(t, zone_id);
                atomic_compare_exchange_weak_explicit(
                    &t->current_zone, &expected, next,
                    memory_order_acq_rel, memory_order_acquire);
            }

            p->pn = pn;
            return pn;
        }

        atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
        uint32_t expected = zone_id;
        uint32_t next = zone_next(t, zone_id);
        atomic_compare_exchange_weak_explicit(
            &t->current_zone, &expected, next,
            memory_order_acq_rel, memory_order_acquire);

        if (++retry > t->info.nr_zones) {
            perror("nvme_zns_append");
            fprintf(stderr, "append retry exceeded number of zones\n");
            exit(EXIT_FAILURE);
        }
    }
}

static uint64_t scan_meta_zone(int fd, uint32_t zone_id, uint64_t zone_size, superblock_entry *out) {
    uint64_t zone_pages = zone_size / PAGE_SIZE;
    uint64_t last_wp = 0;

    for (uint64_t i = 1; i < zone_pages; i++) {
        superblock_entry tmp;
        off_t off = (off_t)zone_id * zone_size + (off_t)i * PAGE_SIZE;
        if (pread(fd, &tmp, PAGE_SIZE, off) != PAGE_SIZE) break;
        if (tmp.magic != SB_MAGIC) break;
        *out = tmp;
        last_wp = i;
    }
    return last_wp;
}

static void activate_meta_zone(cow_tree *t, uint32_t zone_id, uint64_t version) {
    off_t zstart = (off_t)zone_id * t->info.zone_size;
    if (zbd_reset_zones(t->fd, zstart, (off_t)t->info.zone_size) != 0) {
        perror("zbd_reset_zones");
        exit(EXIT_FAILURE);
    }

    atomic_store_explicit(&t->zone_wp_bytes[zone_id], t->zones[zone_id].start, memory_order_release);
    atomic_store_explicit(&t->zone_full[zone_id], 0, memory_order_release);

    zone_header zh;
    memset(&zh, 0, sizeof zh);
    zh.magic = ZH_MAGIC;
    zh.state = ZH_ACTIVE;
    zh.version = version;

    pagenum_t ignored_pn;
    uint64_t wp_bytes;
    if (zone_append_raw_nolock(t, zone_id, &zh, &ignored_pn, &wp_bytes) != 0) {
        perror("nvme_zns_append(meta_zone_header)");
        exit(EXIT_FAILURE);
    }
    atomic_store_explicit(&t->zone_wp_bytes[zone_id], wp_bytes, memory_order_release);
    atomic_store_explicit(
        &t->zone_full[zone_id],
        (wp_bytes >= t->zones[zone_id].start + t->zones[zone_id].capacity) ? 1 : 0,
        memory_order_release);

    t->active_zone = zone_id;
    t->meta_wp = 1;
    t->version = version;
}

static void load_superblock(cow_tree *t) {
    zone_header zh0, zh1;
    int v0 = (pread(t->fd, &zh0, PAGE_SIZE, 0) == PAGE_SIZE) && (zh0.magic == ZH_MAGIC);
    int v1 = (pread(t->fd, &zh1, PAGE_SIZE, (off_t)META_ZONE_1 * t->info.zone_size) == PAGE_SIZE)
             && (zh1.magic == ZH_MAGIC);

    superblock_entry sb0, sb1;
    uint64_t wp0 = 0, wp1 = 0;

    if (v0) wp0 = scan_meta_zone(t->fd, META_ZONE_0, t->info.zone_size, &sb0);
    if (v1) wp1 = scan_meta_zone(t->fd, META_ZONE_1, t->info.zone_size, &sb1);

    if (wp0 == 0 && wp1 == 0) {
        memset(&t->durable_sb, 0, sizeof t->durable_sb);
        t->durable_sb.root_pn = INVALID_PGN;
        t->durable_sb.leaf_order = LEAF_ORDER;
        t->durable_sb.internal_order = INTERNAL_ORDER;
        t->durable_sb.seq_no = 0;
        activate_meta_zone(t, META_ZONE_0, 0);
    } else if (wp0 > 0 && wp1 > 0) {
        if (sb0.seq_no >= sb1.seq_no) {
            t->durable_sb = sb0;
            t->active_zone = META_ZONE_0;
            t->meta_wp = wp0 + 1;
            t->version = zh0.version;
        } else {
            t->durable_sb = sb1;
            t->active_zone = META_ZONE_1;
            t->meta_wp = wp1 + 1;
            t->version = zh1.version;
        }
    } else if (wp0 > 0) {
        t->durable_sb = sb0;
        t->active_zone = META_ZONE_0;
        t->meta_wp = wp0 + 1;
        t->version = zh0.version;
    } else {
        t->durable_sb = sb1;
        t->active_zone = META_ZONE_1;
        t->meta_wp = wp1 + 1;
        t->version = zh1.version;
    }

    // Initialize volatile state from durable
    atomic_store_explicit(&t->volatile_sb.root_pn, t->durable_sb.root_pn, memory_order_release);
    // seq_no uses seqlock-style even values in memory; on-disk stores logical seq.
    atomic_store_explicit(&t->volatile_sb.seq_no, t->durable_sb.seq_no * 2, memory_order_release);
}

static void write_superblock_sync(cow_tree *t) {
    pthread_mutex_lock(&t->flush_lock);

    t->durable_sb.magic = SB_MAGIC;

    if (t->meta_wp >= t->info.zone_size / PAGE_SIZE) {
        uint32_t new_zone = 1 - t->active_zone;
        activate_meta_zone(t, new_zone, t->version + 1);
    }

    pagenum_t ignored_pn;
    uint64_t wp_bytes;
    if (zone_append_raw_nolock(t, t->active_zone, &t->durable_sb, &ignored_pn, &wp_bytes) != 0) {
        pthread_mutex_unlock(&t->flush_lock);
        perror("nvme_zns_append(superblock)");
        exit(EXIT_FAILURE);
    }
    atomic_store_explicit(&t->zone_wp_bytes[t->active_zone], wp_bytes, memory_order_release);
    atomic_store_explicit(
        &t->zone_full[t->active_zone],
        (wp_bytes >= t->zones[t->active_zone].start + t->zones[t->active_zone].capacity) ? 1 : 0,
        memory_order_release);

    t->meta_wp++;

    pthread_mutex_unlock(&t->flush_lock);
}

/* V1.3: Background flusher thread */
static void *sb_flusher_thread(void *arg) {
    cow_tree *t = (cow_tree *)arg;

    while (!atomic_load_explicit(&t->flusher_stop, memory_order_acquire)) {
        usleep(FLUSH_INTERVAL_MS * 1000);

        if (!atomic_exchange_explicit(&t->dirty, false, memory_order_acq_rel)) {
            continue;
        }

        pagenum_t root;
        uint64_t seq;
        read_tree_snapshot(t, &root, &seq);

        pthread_mutex_lock(&t->flush_lock);
        t->durable_sb.root_pn = root;
        t->durable_sb.seq_no = seq / 2;
        pthread_mutex_unlock(&t->flush_lock);

        write_superblock_sync(t);
    }

    return NULL;
}

static uint32_t get_position(page *p, int64_t key) {
    if (p->is_leaf) {
        for (uint32_t i = 0; i < p->num_keys; i++)
            if (key < (int64_t)p->leaf[i].key) return i;
    } else {
        for (uint32_t i = 0; i < p->num_keys; i++)
            if (key < (int64_t)p->internal[i].key) return i;
    }
    return p->num_keys;
}

static int leaf_is_full(page *p)     { return p->num_keys == LEAF_ORDER - 1; }
static int internal_is_full(page *p) { return p->num_keys == INTERNAL_ORDER - 1; }

static pagenum_t find_leaf_from_root(cow_tree *t, pagenum_t root_pn, int64_t key, tpath *path) {
    path->depth = 0;
    pagenum_t cur_pn = root_pn;
    page p;
    load_page(t, cur_pn, &p);

    while (!p.is_leaf) {
        uint32_t idx = RIGHTMOST_IDX;
        for (uint32_t i = 0; i < p.num_keys; i++) {
            if (key < (int64_t)p.internal[i].key) {
                idx = i;
                break;
            }
        }

        path->e[path->depth++] = (path_entry){ cur_pn, idx };
        cur_pn = (idx == RIGHTMOST_IDX) ? p.pointer : p.internal[idx].child;
        load_page(t, cur_pn, &p);
    }

    path->e[path->depth++] = (path_entry){ cur_pn, RIGHTMOST_IDX };
    return cur_pn;
}

static pagenum_t propagate_candidate_root(cow_tree *t, tpath *path, int from_level,
                                          pagenum_t new_child,
                                          int has_split, int64_t split_key, pagenum_t split_right,
                                          int has_sep,   int64_t old_sep,   int64_t new_sep) {
    pagenum_t cur = new_child;
    int split = has_split;
    int64_t pkey = split_key;
    pagenum_t pright = split_right;

    for (int i = from_level - 1; i >= 0; i--) {
        page anc;
        load_page(t, path->e[i].pn, &anc);
        uint32_t cidx = path->e[i].cidx;
        uint32_t pos = (cidx == RIGHTMOST_IDX) ? anc.num_keys : cidx;

        if (has_sep) {
            for (uint32_t k = 0; k < anc.num_keys; k++) {
                if ((int64_t)anc.internal[k].key == old_sep) {
                    anc.internal[k].key = (uint64_t)new_sep;
                    has_sep = 0;
                    break;
                }
            }
        }

        if (cidx == RIGHTMOST_IDX) anc.pointer = cur;
        else anc.internal[cidx].child = cur;

        if (!split) {
            cur = cow_append_page(t, &anc);
        } 
        else if (!internal_is_full(&anc)) {
            for (int64_t j = (int64_t)anc.num_keys - 1; j >= (int64_t)pos; j--)
                anc.internal[j + 1] = anc.internal[j];

            anc.internal[pos].key = (uint64_t)pkey;
            anc.internal[pos].child = cur;

            if (pos == anc.num_keys) anc.pointer = pright;
            else anc.internal[pos + 1].child = pright;

            anc.num_keys++;
            cur = cow_append_page(t, &anc);
            split = 0;
        } 
        else {
            const uint32_t order = INTERNAL_ORDER;
            int64_t *tkeys = malloc(order * sizeof(*tkeys));
            pagenum_t *tchld = malloc((order + 1) * sizeof(*tchld));

            for (uint32_t j = 0; j < pos; j++)
                tkeys[j] = (int64_t)anc.internal[j].key;
            tkeys[pos] = pkey;
            for (uint32_t j = pos; j < order - 1; j++)
                tkeys[j + 1] = (int64_t)anc.internal[j].key;

            for (uint32_t j = 0; j < pos; j++)
                tchld[j] = anc.internal[j].child;
            tchld[pos] = cur;
            tchld[pos + 1] = pright;
            for (uint32_t j = pos + 1; j < order; j++)
                tchld[j + 1] = (j < order - 1) ? anc.internal[j].child : anc.pointer;

            uint32_t sp = (order + 1) / 2;
            int64_t newpk = tkeys[sp - 1];

            for (uint32_t j = 0; j < sp - 1; j++) {
                anc.internal[j].key = (uint64_t)tkeys[j];
                anc.internal[j].child = tchld[j];
            }
            anc.pointer = tchld[sp - 1];
            anc.num_keys = sp - 1;

            page r;
            memset(&r, 0, sizeof r);
            r.is_leaf = 0;
            for (uint32_t j = sp; j < order; j++) {
                r.internal[j - sp].key = (uint64_t)tkeys[j];
                r.internal[j - sp].child = tchld[j];
            }
            r.pointer = tchld[order];
            r.num_keys = order - sp;

            free(tkeys);
            free(tchld);

            pagenum_t r_pn = cow_append_page(t, &r);
            cur = cow_append_page(t, &anc);
            pkey = newpk;
            pright = r_pn;
            split = 1;
        }
    }

    if (!split) return cur;

    page root;
    memset(&root, 0, sizeof root);
    root.is_leaf = 0;
    root.num_keys = 1;
    root.internal[0].key = (uint64_t)pkey;
    root.internal[0].child = cur;
    root.pointer = pright;
    return cow_append_page(t, &root);
}

/* V1.3 OPTIMIZATION: Lock-free atomic CAS */
static int try_publish_root(cow_tree *t, pagenum_t expected_root, uint64_t expected_seq, pagenum_t new_root) {
    // Seqlock-style publish:
    // 1) CAS seq even->odd (writer lock)
    // 2) store new root
    // 3) store seq odd->next even (commit)
    uint64_t lock_seq = expected_seq;
    if (!atomic_compare_exchange_strong_explicit(
            &t->volatile_sb.seq_no, &lock_seq, expected_seq + 1,
            memory_order_acq_rel, memory_order_acquire)) {
        return 0;
    }

    pagenum_t cur_root = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
    if (cur_root != expected_root) {
        atomic_store_explicit(&t->volatile_sb.seq_no, expected_seq + 2, memory_order_release);
        return 0;
    }

    atomic_store_explicit(&t->volatile_sb.root_pn, new_root, memory_order_release);
    atomic_store_explicit(&t->volatile_sb.seq_no, expected_seq + 2, memory_order_release);
    atomic_store_explicit(&t->dirty, true, memory_order_release);
    return 1;
}

cow_tree *cow_open(const char *path) {
    cow_tree *t = malloc(sizeof *t);
    if (!t) {
        perror("malloc");
        return NULL;
    }
    memset(t, 0, sizeof *t);

    if (pthread_mutex_init(&t->flush_lock, NULL) != 0) {
        perror("pthread_mutex_init flush_lock");
        free(t);
        return NULL;
    }

    t->fd = zbd_open(path, O_RDWR, &t->info);
    if (t->fd < 0) {
        perror("zbd_open");
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    if (nvme_get_nsid(t->fd, &t->nsid) != 0) {
        perror("nvme_get_nsid");
        zbd_close(t->fd);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    t->direct_fd = open(path, O_RDONLY | O_DIRECT);

    t->zones = calloc(t->info.nr_zones, sizeof *t->zones);
    if (!t->zones) {
        perror("calloc");
        if (t->direct_fd >= 0) close(t->direct_fd);
        zbd_close(t->fd);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    unsigned int nr = t->info.nr_zones;
    if (zbd_report_zones(t->fd, 0, 0, ZBD_RO_ALL, t->zones, &nr) != 0) {
        perror("zbd_report_zones");
        free(t->zones);
        if (t->direct_fd >= 0) close(t->direct_fd);
        zbd_close(t->fd);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    t->zone_wp_bytes = calloc(t->info.nr_zones, sizeof(*t->zone_wp_bytes));
    t->zone_full = calloc(t->info.nr_zones, sizeof(*t->zone_full));
    if (!t->zone_wp_bytes || !t->zone_full) {
        perror("calloc zone atomic caches");
        free(t->zone_wp_bytes);
        free(t->zone_full);
        free(t->zones);
        if (t->direct_fd >= 0) close(t->direct_fd);
        zbd_close(t->fd);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    for (uint32_t z = 0; z < t->info.nr_zones; z++) {
        atomic_store_explicit(&t->zone_wp_bytes[z], t->zones[z].wp, memory_order_relaxed);
        atomic_store_explicit(
            &t->zone_full[z],
            (t->zones[z].cond == ZBD_ZONE_COND_FULL) ? 1 : 0,
            memory_order_relaxed);
    }

    load_superblock(t);

    uint32_t initial_zone = DATA_ZONE_START;
    for (uint32_t z = DATA_ZONE_START; z < t->info.nr_zones; z++) {
        if (!atomic_load_explicit(&t->zone_full[z], memory_order_acquire)) {
            initial_zone = z;
            break;
        }
    }
    atomic_store_explicit(&t->current_zone, initial_zone, memory_order_release);

    // Start background flusher thread
    atomic_store_explicit(&t->flusher_stop, false, memory_order_release);
    atomic_store_explicit(&t->dirty, false, memory_order_release);
    
    if (pthread_create(&t->flusher_tid, NULL, sb_flusher_thread, t) != 0) {
        perror("pthread_create flusher");
        free(t->zone_wp_bytes);
        free(t->zone_full);
        free(t->zones);
        if (t->direct_fd >= 0) close(t->direct_fd);
        zbd_close(t->fd);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    return t;
}

void cow_close(cow_tree *t) {
    if (!t) return;

    // Stop flusher thread
    atomic_store_explicit(&t->flusher_stop, true, memory_order_release);
    pthread_join(t->flusher_tid, NULL);

    // Final flush if dirty
    if (atomic_exchange_explicit(&t->dirty, false, memory_order_acq_rel)) {
        pagenum_t root;
        uint64_t seq;
        read_tree_snapshot(t, &root, &seq);

        pthread_mutex_lock(&t->flush_lock);
        t->durable_sb.root_pn = root;
        t->durable_sb.seq_no = seq / 2;
        pthread_mutex_unlock(&t->flush_lock);

        write_superblock_sync(t);
    }

    if (t->direct_fd >= 0) close(t->direct_fd);
    free(t->zone_wp_bytes);
    free(t->zone_full);
    free(t->zones);
    zbd_close(t->fd);
    pthread_mutex_destroy(&t->flush_lock);
    free(t);
}

record *cow_find(cow_tree *t, int64_t key) {
    pagenum_t root;
    uint64_t seq;
    read_tree_snapshot(t, &root, &seq);
    (void)seq;
    if (is_empty_snapshot(root)) return NULL;

    tpath path;
    pagenum_t leaf_pn = find_leaf_from_root(t, root, key, &path);
    page leaf;
    load_page(t, leaf_pn, &leaf);

    for (uint32_t i = 0; i < leaf.num_keys; i++) {
        if ((int64_t)leaf.leaf[i].key == key) {
            record *r = malloc(sizeof *r);
            if (!r) return NULL;
            *r = leaf.leaf[i].record;
            return r;
        }
    }
    return NULL;
}

void cow_insert(cow_tree *t, int64_t key, const char *value) {
    for (uint64_t retry = 0;; retry++) {
        pagenum_t snap_root;
        uint64_t snap_seq;
        read_tree_snapshot(t, &snap_root, &snap_seq);
        pagenum_t candidate_root = INVALID_PGN;

        if (is_empty_snapshot(snap_root)) {
            page root;
            memset(&root, 0, sizeof root);
            root.is_leaf = 1;
            root.num_keys = 1;
            root.pointer = INVALID_PGN;
            root.leaf[0].key = (uint64_t)key;
            memcpy(root.leaf[0].record.value, value, 120);
            candidate_root = cow_append_page(t, &root);
        } else {
            tpath path;
            pagenum_t leaf_pn;
            page leaf;
            leaf_pn = find_leaf_from_root(t, snap_root, key, &path);
            load_page(t, leaf_pn, &leaf);

            if (!leaf_is_full(&leaf)) {
                uint32_t pos = get_position(&leaf, key);

                for (int64_t i = (int64_t)leaf.num_keys - 1; i >= (int64_t)pos; i--)
                    leaf.leaf[i + 1] = leaf.leaf[i];

                leaf.leaf[pos].key = (uint64_t)key;
                memcpy(leaf.leaf[pos].record.value, value, 120);
                leaf.num_keys++;

                pagenum_t new_leaf = cow_append_page(t, &leaf);
                candidate_root = propagate_candidate_root(t, &path, path.depth - 1,
                                                         new_leaf, 0, 0, INVALID_PGN, 0, 0, 0);
            } 
            else {
                const uint32_t order = LEAF_ORDER;
                leaf_entity *tmp = malloc(order * sizeof *tmp);
                if (!tmp) {
                    perror("malloc");
                    exit(EXIT_FAILURE);
                }

                uint32_t pos = 0;
                while (pos < leaf.num_keys && (int64_t)leaf.leaf[pos].key < key)
                    pos++;

                for (uint32_t i = 0; i < pos; i++)
                    tmp[i] = leaf.leaf[i];
                for (uint32_t i = pos; i < leaf.num_keys; i++)
                    tmp[i + 1] = leaf.leaf[i];

                tmp[pos].key = (uint64_t)key;
                memcpy(tmp[pos].record.value, value, 120);

                uint32_t sp = order / 2;

                for (uint32_t i = 0; i < sp; i++)
                    leaf.leaf[i] = tmp[i];
                leaf.num_keys = sp;

                page right;
                memset(&right, 0, sizeof right);
                right.is_leaf = 1;
                for (uint32_t i = 0; i < order - sp; i++)
                    right.leaf[i] = tmp[sp + i];
                right.num_keys = order - sp;
                right.pointer = leaf.pointer;

                free(tmp);

                pagenum_t right_pn = cow_append_page(t, &right);
                leaf.pointer = right_pn;
                pagenum_t left_pn = cow_append_page(t, &leaf);

                int64_t promote = (int64_t)right.leaf[0].key;
                candidate_root = propagate_candidate_root(t, &path, path.depth - 1,
                                                         left_pn, 1, promote, right_pn, 0, 0, 0);
            }
        }

        if (try_publish_root(t, snap_root, snap_seq, candidate_root)) return;

        if ((retry % RETRY_YIELD_INTERVAL) == 0) {
            sched_yield();
        }
    }
}
