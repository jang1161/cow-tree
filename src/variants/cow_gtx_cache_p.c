/*
 * Architecture: Global Page Cache + Sorting Batch Processing
 * - 샤드 대신 4-Way 세트 연관 글로벌 페이지 캐시를 도입하여 메모리 효율 극대화
 * - 워커 스레드는 큐에 요청만 삽입하고, 전담 스레드가 키 순서 정렬 후 트리 반영
 * - 개별 스레드의 직접 수정을 배제하고 배치(Batch) 단위로 I/O 병합 성능을 높인 최종형

gcc -O2 -g -Wall -Wextra -std=c11  \
-Iinclude -Iinclude/variants -I. \
bench/bench_main.c src/variants/cow_gtx_cache_p.c \
-o build/bin/cow-bench-gtx_cache_p \
-pthread -lzbd -lnvme
*/

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "cow_gtx_cache.h"

#define MAX_HEIGHT 32
#define RIGHTMOST_IDX UINT32_MAX

#define TXG_BATCH_MAX 256
#define TXG_BATCH_WAIT_US 150
#define TXG_BATCH_MIN_WAIT_US 30
#define TXG_COMMIT_MERGE_JOBS 4
#define TXG_COMMIT_MERGE_ITEMS 512
#define FLUSH_INTERVAL_MS 10

#define MAX_BATCH_PAGES 2048
#define MAX_NVME_PAGES  64

typedef uint64_t node_id_t;

typedef struct
{
    node_id_t id;
    page node;
    uint8_t dirty;
    uint8_t flushed;
    pagenum_t flushed_pn;
} overlay_node;

typedef struct
{
    cow_tree *t;
    overlay_node *arr;
    size_t len;
    size_t cap;
    uint64_t next_temp;
    size_t *idx_table;
    size_t idx_cap;
    size_t idx_used;
} overlay_state;

typedef struct
{
    node_id_t id;
    uint32_t cidx;
} overlay_path_entry;

typedef struct
{
    overlay_path_entry e[MAX_HEIGHT];
    int depth;
} overlay_path;

typedef struct
{
    overlay_node *n;
} batch_entry;

typedef struct
{
    insert_req *req;
    int ord;
} batch_item;

typedef struct txg_batch_job
{
    uint64_t start_ns;
    int n;
    batch_item items[TXG_BATCH_MAX];
    struct txg_batch_job *next;
} txg_batch_job;

static inline uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline void stat_update_max_u64(_Atomic(uint64_t) *dst, uint64_t v)
{
    uint64_t cur = atomic_load_explicit(dst, memory_order_relaxed);
    while (v > cur &&
           !atomic_compare_exchange_weak_explicit(dst, &cur, v,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed))
    {
    }
}

static inline int is_temp_id(node_id_t id)
{
    return (id & (1ULL << 63)) != 0;
}

static inline node_id_t make_temp_id(uint64_t n)
{
    return (1ULL << 63) | n;
}

static inline uint32_t zone_next(cow_tree *t, uint32_t zone_id)
{
    return (zone_id + 1 < t->info.nr_zones) ? (zone_id + 1) : t->info.nr_zones;
}

static inline uint32_t other_meta_zone(uint32_t zone_id)
{
    return (zone_id == META_ZONE_0) ? META_ZONE_1 : META_ZONE_0;
}

static void finish_zone_if_full(cow_tree *t, uint32_t zone_id)
{
    if (zone_id >= t->info.nr_zones)
        return;

    /* If zone is full, explicitly finish it to free the active zone slot */
    if (atomic_load_explicit(&t->zone_full[zone_id], memory_order_acquire))
    {
        off_t zstart = (off_t)zone_id * t->info.zone_size;
        if (zbd_finish_zones(t->fd, zstart, (off_t)t->info.zone_size) != 0)
        {
            perror("zbd_finish_zones");
            /* Don't exit, just log and continue */
        }
    }
}

static inline uint64_t overlay_hash_u64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 33;
    return x;
}

/*
 * Global Page Cache (per-set fine-grained locking)
 */
static void global_cache_init(cow_tree *t)
{
    t->global_cache = calloc(CACHE_NUM_SETS, sizeof(cache_set));
    if (!t->global_cache)
    {
        perror("calloc global_cache sets");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < CACHE_NUM_SETS; i++)
    {
        if (pthread_mutex_init(&t->global_cache[i].lock, NULL) != 0)
        {
            perror("pthread_mutex_init cache_set");
            exit(EXIT_FAILURE);
        }
        for (int j = 0; j < CACHE_WAYS; j++)
        {
            t->global_cache[i].ways[j].valid = 0;
            t->global_cache[i].ways[j].lru_counter = 0;
            t->global_cache[i].ways[j].tag = INVALID_PGN;
        }
    }
    atomic_store_explicit(&t->cache_lru_clock, 0, memory_order_relaxed);
}

static void global_cache_destroy(cow_tree *t)
{
    if (!t->global_cache)
        return;
    for (size_t i = 0; i < CACHE_NUM_SETS; i++)
        pthread_mutex_destroy(&t->global_cache[i].lock);
    free(t->global_cache);
    t->global_cache = NULL;
}

static int global_cache_lookup(cow_tree *t, pagenum_t pn, page *dst)
{
    size_t set_idx = (size_t)(overlay_hash_u64((uint64_t)pn) % CACHE_NUM_SETS);
    cache_set *set = &t->global_cache[set_idx];

    pthread_mutex_lock(&set->lock);

    /* Check all 4 ways for a tag match */
    for (int i = 0; i < CACHE_WAYS; i++)
    {
        if (set->ways[i].valid && set->ways[i].tag == pn)
        {
            /* Hit: update LRU counter and return data */
            set->ways[i].lru_counter = atomic_fetch_add_explicit(&t->cache_lru_clock, 1, memory_order_relaxed);
            *dst = set->ways[i].data;
            pthread_mutex_unlock(&set->lock);
            return 1;
        }
    }

    pthread_mutex_unlock(&set->lock);
    return 0;
}

static void global_cache_insert(cow_tree *t, pagenum_t pn, const page *src)
{
    size_t set_idx = (size_t)(overlay_hash_u64((uint64_t)pn) % CACHE_NUM_SETS);
    cache_set *set = &t->global_cache[set_idx];
    uint64_t current_clock = atomic_fetch_add_explicit(&t->cache_lru_clock, 1, memory_order_relaxed);

    pthread_mutex_lock(&set->lock);

    /* Find an empty way or the LRU way */
    int victim_way = 0;
    uint64_t min_lru = set->ways[0].lru_counter;

    for (int i = 0; i < CACHE_WAYS; i++)
    {
        /* Prefer empty slots */
        if (!set->ways[i].valid)
        {
            victim_way = i;
            break;
        }
        /* Otherwise track LRU */
        if (set->ways[i].lru_counter < min_lru)
        {
            min_lru = set->ways[i].lru_counter;
            victim_way = i;
        }
    }

    /* Install page into victim way */
    set->ways[victim_way].valid = 1;
    set->ways[victim_way].tag = pn;
    set->ways[victim_way].lru_counter = current_clock;
    set->ways[victim_way].data = *src;

    pthread_mutex_unlock(&set->lock);
}

/*
 * Load page: Global Cache → Disk I/O
 */
static void load_page(cow_tree *t, pagenum_t pn, page *dst)
{
    off_t off = (off_t)pn * PAGE_SIZE;

    /* Try global page cache */
    if (global_cache_lookup(t, pn, dst))
    {
        atomic_fetch_add_explicit(&t->stat_cache_global_hit, 1, memory_order_relaxed);
        return;
    }

    /* Cache miss: read from disk */
    atomic_fetch_add_explicit(&t->stat_cache_miss, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_odirect_reads, 1, memory_order_relaxed);

    if (t->direct_fd >= 0)
    {
        void *raw;
        if (posix_memalign(&raw, PAGE_SIZE, PAGE_SIZE) != 0)
        {
            perror("posix_memalign load_page");
            exit(EXIT_FAILURE);
        }
        ssize_t n = pread(t->direct_fd, raw, PAGE_SIZE, off);
        if (n != PAGE_SIZE)
        {
            printf("pread ret=%ld errno=%d offset=%lu \n", n, errno, off);
            perror("load_page pread(O_DIRECT)");
            free(raw);
            exit(EXIT_FAILURE);
        }
        memcpy(dst, raw, PAGE_SIZE);
        free(raw);
    }
    else
    {
        if (pread(t->fd, dst, PAGE_SIZE, off) != PAGE_SIZE)
        {
            perror("load_page pread");
            exit(EXIT_FAILURE);
        }
    }

    /* Insert into global cache */
    global_cache_insert(t, pn, dst);
}

static int zone_pwrite_raw_nolock(cow_tree *t, uint32_t zone_id, const void *buf,
                                  pagenum_t *out_pn, uint64_t *out_wp_bytes)
{
    uint64_t cur_wp = atomic_fetch_add_explicit(&t->zone_wp_bytes[zone_id], PAGE_SIZE, memory_order_acq_rel);

    if (pwrite(t->fd, buf, PAGE_SIZE, (off_t)cur_wp) != PAGE_SIZE)
        return -1;

    if (out_wp_bytes)
        *out_wp_bytes = cur_wp + PAGE_SIZE;
    if (out_pn)
        *out_pn = (pagenum_t)(cur_wp / PAGE_SIZE);
    return 0;
}

static uint32_t reserve_writable_zone(cow_tree *t)
{
    for (;;)
    {
        uint32_t cur = atomic_load_explicit(&t->current_zone, memory_order_acquire);
        if (cur < DATA_ZONE_START)
        {
            uint32_t expected = cur;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, DATA_ZONE_START,
                memory_order_acq_rel, memory_order_acquire);
            cur = DATA_ZONE_START;
        }

        if (cur >= t->info.nr_zones)
        {
            fprintf(stderr, "zones exhausted\n");
            exit(EXIT_FAILURE);
        }

        if (atomic_load_explicit(&t->zone_full[cur], memory_order_acquire))
        {
            finish_zone_if_full(t, cur);
            uint32_t expected = cur;
            uint32_t next = zone_next(t, cur);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }

        return cur;
    }
}

static uint64_t scan_meta_zone(int fd, uint32_t zone_id, uint64_t zone_size, superblock_entry *out)
{
    uint64_t zone_pages = zone_size / PAGE_SIZE;
    uint64_t last_wp = 0;

    for (uint64_t i = 1; i < zone_pages; i++)
    {
        superblock_entry tmp;
        off_t off = (off_t)zone_id * zone_size + (off_t)i * PAGE_SIZE;
        if (pread(fd, &tmp, PAGE_SIZE, off) != PAGE_SIZE)
            break;
        if (tmp.magic != SB_MAGIC)
            break;
        *out = tmp;
        last_wp = i;
    }
    return last_wp;
}

static void activate_meta_zone(cow_tree *t, uint32_t zone_id, uint64_t version)
{
    off_t zstart = (off_t)zone_id * t->info.zone_size;
    /* Flush buffered writes so the driver sees no in-flight dirty pages,
     * then finish the zone (no-op if already empty) before resetting. */
    fdatasync(t->fd);
    zbd_finish_zones(t->fd, zstart, (off_t)t->info.zone_size); /* ignore error: zone may be empty */
    if (zbd_reset_zones(t->fd, zstart, (off_t)t->info.zone_size) != 0)
    {
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
    if (zone_pwrite_raw_nolock(t, zone_id, &zh, &ignored_pn, &wp_bytes) != 0)
    {
        perror("pwrite(meta_zone_header)");
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

static void rotate_meta_zone(cow_tree *t)
{
    uint32_t new_zone = other_meta_zone(t->active_zone);
    /* activate_meta_zone flushes, finishes, and resets new_zone before writing
     * the zone header, so we don't need to touch the old zone here. */
    activate_meta_zone(t, new_zone, t->version + 1);
}

static void load_superblock(cow_tree *t)
{
    zone_header zh0, zh1;

    /* Cross-check zone WP: if WP == zone start, the zone was just reset.
     * ZNS reset moves the WP back to zone start but does NOT erase data,
     * so pread() would return stale ZH_MAGIC from a prior run.
     * Only trust pread if the zone actually has data written (WP > start). */
    uint64_t zone0_wp = atomic_load_explicit(&t->zone_wp_bytes[META_ZONE_0], memory_order_acquire);
    uint64_t zone1_wp = atomic_load_explicit(&t->zone_wp_bytes[META_ZONE_1], memory_order_acquire);

    int v0 = (zone0_wp > t->zones[META_ZONE_0].start) &&
             (pread(t->fd, &zh0, PAGE_SIZE, 0) == PAGE_SIZE) &&
             (zh0.magic == ZH_MAGIC);
    int v1 = (zone1_wp > t->zones[META_ZONE_1].start) &&
             (pread(t->fd, &zh1, PAGE_SIZE, (off_t)META_ZONE_1 * t->info.zone_size) == PAGE_SIZE) &&
             (zh1.magic == ZH_MAGIC);

    superblock_entry sb0, sb1;
    uint64_t wp0 = 0, wp1 = 0;

    if (v0)
        wp0 = scan_meta_zone(t->fd, META_ZONE_0, t->info.zone_size, &sb0);
    if (v1)
        wp1 = scan_meta_zone(t->fd, META_ZONE_1, t->info.zone_size, &sb1);

    if (wp0 == 0 && wp1 == 0)
    {
        memset(&t->durable_sb, 0, sizeof t->durable_sb);
        t->durable_sb.root_pn = INVALID_PGN;
        t->durable_sb.leaf_order = LEAF_ORDER;
        t->durable_sb.internal_order = INTERNAL_ORDER;
        t->durable_sb.seq_no = 0;
        activate_meta_zone(t, META_ZONE_0, 0);
    }
    else if (wp0 > 0 && wp1 > 0)
    {
        if (sb0.seq_no >= sb1.seq_no)
        {
            t->durable_sb = sb0;
            t->active_zone = META_ZONE_0;
            t->meta_wp = wp0 + 1;
            t->version = zh0.version;
        }
        else
        {
            t->durable_sb = sb1;
            t->active_zone = META_ZONE_1;
            t->meta_wp = wp1 + 1;
            t->version = zh1.version;
        }
    }
    else if (wp0 > 0)
    {
        t->durable_sb = sb0;
        t->active_zone = META_ZONE_0;
        t->meta_wp = wp0 + 1;
        t->version = zh0.version;
    }
    else
    {
        t->durable_sb = sb1;
        t->active_zone = META_ZONE_1;
        t->meta_wp = wp1 + 1;
        t->version = zh1.version;
    }

    atomic_store_explicit(&t->volatile_sb.root_pn, t->durable_sb.root_pn, memory_order_release);
    atomic_store_explicit(&t->volatile_sb.seq_no, t->durable_sb.seq_no * 2, memory_order_release);
}

static void write_superblock_sync(cow_tree *t)
{
    pthread_mutex_lock(&t->flush_lock);

    t->durable_sb.magic = SB_MAGIC;

    if (t->meta_wp >= t->zones[t->active_zone].capacity / PAGE_SIZE)
    {
        printf("[DEBUG] Meta zone full! Rotating to other zone...\n");
        rotate_meta_zone(t);
    }

    pagenum_t ignored_pn;
    uint64_t wp_bytes;
    if (zone_pwrite_raw_nolock(t, t->active_zone, &t->durable_sb, &ignored_pn, &wp_bytes) != 0)
    {
        pthread_mutex_unlock(&t->flush_lock);
        perror("pwrite(superblock)");
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

static void *sb_flusher_thread(void *arg)
{
    cow_tree *t = (cow_tree *)arg;

    while (!atomic_load_explicit(&t->stop_flusher, memory_order_acquire))
    {
        usleep(FLUSH_INTERVAL_MS * 1000);

        if (!atomic_exchange_explicit(&t->dirty_sb, false, memory_order_acq_rel))
            continue;

        pagenum_t root;
        uint64_t seq;
        for (;;)
        {
            uint64_t s1 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
            if (s1 & 1ULL)
                continue;
            root = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
            uint64_t s2 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
            if (s1 == s2 && ((s2 & 1ULL) == 0))
            {
                seq = s2;
                break;
            }
        }

        pthread_mutex_lock(&t->flush_lock);
        t->durable_sb.root_pn = root;
        t->durable_sb.seq_no = seq / 2;
        pthread_mutex_unlock(&t->flush_lock);

        write_superblock_sync(t);
    }

    return NULL;
}

static void publish_root_txg(cow_tree *t, pagenum_t new_root, uint64_t txg)
{
    (void)txg;
    uint64_t s = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
    if (s & 1ULL)
        s++;

    atomic_store_explicit(&t->volatile_sb.seq_no, s + 1, memory_order_release);
    atomic_store_explicit(&t->volatile_sb.root_pn, new_root, memory_order_release);
    atomic_store_explicit(&t->volatile_sb.seq_no, s + 2, memory_order_release);
    atomic_store_explicit(&t->dirty_sb, true, memory_order_release);
}

static void overlay_index_grow(overlay_state *ov)
{
    size_t new_cap = ov->idx_cap ? (ov->idx_cap << 1) : 512;
    size_t *new_tab = malloc(new_cap * sizeof(*new_tab));
    if (!new_tab)
    {
        perror("malloc overlay idx_table");
        exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < new_cap; i++)
        new_tab[i] = (size_t)-1;

    for (size_t i = 0; i < ov->len; i++)
    {
        node_id_t id = ov->arr[i].id;
        size_t m = new_cap - 1;
        size_t pos = (size_t)(overlay_hash_u64((uint64_t)id) & m);
        while (new_tab[pos] != (size_t)-1)
            pos = (pos + 1) & m;
        new_tab[pos] = i;
    }

    free(ov->idx_table);
    ov->idx_table = new_tab;
    ov->idx_cap = new_cap;
    ov->idx_used = ov->len;
}

static int overlay_find_idx(overlay_state *ov, node_id_t id)
{
    if (ov->idx_cap == 0)
        return -1;

    size_t m = ov->idx_cap - 1;
    size_t pos = (size_t)(overlay_hash_u64((uint64_t)id) & m);
    for (;;)
    {
        size_t v = ov->idx_table[pos];
        if (v == (size_t)-1)
            return -1;
        if (ov->arr[v].id == id)
            return (int)v;
        pos = (pos + 1) & m;
    }
}

static void overlay_index_insert(overlay_state *ov, node_id_t id, size_t idx)
{
    if (ov->idx_cap == 0 || (ov->idx_used + 1) * 10 >= ov->idx_cap * 7)
        overlay_index_grow(ov);

    size_t m = ov->idx_cap - 1;
    size_t pos = (size_t)(overlay_hash_u64((uint64_t)id) & m);
    while (ov->idx_table[pos] != (size_t)-1)
        pos = (pos + 1) & m;
    ov->idx_table[pos] = idx;
    ov->idx_used++;
}

static overlay_node *overlay_add_node(overlay_state *ov, node_id_t id, const page *src)
{
    if (ov->len == ov->cap)
    {
        size_t new_cap = ov->cap ? ov->cap * 2 : 256;
        overlay_node *next = realloc(ov->arr, new_cap * sizeof(*next));
        if (!next)
        {
            perror("realloc overlay");
            exit(EXIT_FAILURE);
        }
        ov->arr = next;
        ov->cap = new_cap;
    }

    size_t idx = ov->len;
    overlay_node *n = &ov->arr[ov->len++];
    n->id = id;
    if (src)
        n->node = *src;
    else
        memset(&n->node, 0, sizeof(n->node));
    n->dirty = 0;
    n->flushed = 0;
    n->flushed_pn = INVALID_PGN;

    overlay_index_insert(ov, id, idx);
    return n;
}

static overlay_node *overlay_get_mut(overlay_state *ov, node_id_t id)
{
    int idx = overlay_find_idx(ov, id);
    if (idx >= 0)
        return &ov->arr[idx];

    if (is_temp_id(id))
        return NULL;

    page p;
    load_page(ov->t, (pagenum_t)id, &p);
    return overlay_add_node(ov, id, &p);
}

static node_id_t overlay_new_temp(overlay_state *ov, uint32_t is_leaf)
{
    node_id_t id = make_temp_id(++ov->next_temp);
    overlay_node *n = overlay_add_node(ov, id, NULL);
    n->node.is_leaf = is_leaf;
    n->dirty = 1;
    return id;
}

static uint32_t get_position(page *p, int64_t key)
{
    if (p->is_leaf)
    {
        for (uint32_t i = 0; i < p->num_keys; i++)
            if (key < (int64_t)p->leaf[i].key)
                return i;
    }
    else
    {
        for (uint32_t i = 0; i < p->num_keys; i++)
            if (key < (int64_t)p->internal[i].key)
                return i;
    }
    return p->num_keys;
}

/* [Core tree insertion and overlay operations - adapted from cow_zfs.c] */
/* For brevity, I'll include the essential parts but marked where full cow_zfs.c code should be */

static void apply_insert_overlay(overlay_state *ov, node_id_t *root_id, int64_t key, const char *value)
{
    if (*root_id == INVALID_PGN)
    {
        node_id_t rid = overlay_new_temp(ov, 1);
        overlay_node *r = overlay_get_mut(ov, rid);
        r->node.num_keys = 1;
        r->node.pointer = INVALID_PGN;
        r->node.leaf[0].key = (uint64_t)key;
        memcpy(r->node.leaf[0].record.value, value, 120);
        r->dirty = 1;
        *root_id = rid;
        return;
    }

    overlay_path path;
    path.depth = 0;

    node_id_t cur = *root_id;
    while (1)
    {
        overlay_node *n = overlay_get_mut(ov, cur);
        if (!n)
        {
            fprintf(stderr, "overlay: missing node %lu\n", (unsigned long)cur);
            exit(EXIT_FAILURE);
        }

        if (n->node.is_leaf)
            break;

        if (path.depth >= MAX_HEIGHT)
        {
            fprintf(stderr, "tree depth exceeded MAX_HEIGHT\n");
            exit(EXIT_FAILURE);
        }

        uint32_t idx = RIGHTMOST_IDX;
        for (uint32_t i = 0; i < n->node.num_keys; i++)
        {
            if (key < (int64_t)n->node.internal[i].key)
            {
                idx = i;
                break;
            }
        }

        path.e[path.depth].id = cur;
        path.e[path.depth].cidx = idx;
        path.depth++;

        cur = (idx == RIGHTMOST_IDX) ? n->node.pointer : n->node.internal[idx].child;
    }

    overlay_node *leaf_n = overlay_get_mut(ov, cur);
    page *leaf = &leaf_n->node;

    for (uint32_t i = 0; i < leaf->num_keys; i++)
    {
        if ((int64_t)leaf->leaf[i].key == key)
        {
            memcpy(leaf->leaf[i].record.value, value, 120);
            leaf_n->dirty = 1;
            return;
        }
    }

    node_id_t carry_left = cur;
    node_id_t carry_right = INVALID_PGN;
    int carry_split = 0;
    int64_t carry_key = 0;

    if (leaf->num_keys < LEAF_ORDER - 1)
    {
        uint32_t pos = get_position(leaf, key);
        for (int64_t i = (int64_t)leaf->num_keys - 1; i >= (int64_t)pos; i--)
            leaf->leaf[i + 1] = leaf->leaf[i];
        leaf->leaf[pos].key = (uint64_t)key;
        memcpy(leaf->leaf[pos].record.value, value, 120);
        leaf->num_keys++;
        leaf_n->dirty = 1;
        carry_split = 0;
    }
    else
    {
        leaf_entity tmp[LEAF_ORDER];
        uint32_t pos = 0;
        while (pos < leaf->num_keys && (int64_t)leaf->leaf[pos].key < key)
            pos++;

        for (uint32_t i = 0; i < pos; i++)
            tmp[i] = leaf->leaf[i];
        for (uint32_t i = pos; i < leaf->num_keys; i++)
            tmp[i + 1] = leaf->leaf[i];

        tmp[pos].key = (uint64_t)key;
        memcpy(tmp[pos].record.value, value, 120);

        uint32_t sp = LEAF_ORDER / 2;
        for (uint32_t i = 0; i < sp; i++)
            leaf->leaf[i] = tmp[i];
        leaf->num_keys = sp;

        node_id_t right_id = overlay_new_temp(ov, 1);
        overlay_node *right_n = overlay_get_mut(ov, right_id);
        right_n->node.num_keys = LEAF_ORDER - sp;
        for (uint32_t i = 0; i < LEAF_ORDER - sp; i++)
            right_n->node.leaf[i] = tmp[sp + i];
        right_n->node.pointer = leaf->pointer;

        leaf->pointer = right_id;
        leaf_n->dirty = 1;
        right_n->dirty = 1;

        carry_split = 1;
        carry_right = right_id;
        carry_key = (int64_t)right_n->node.leaf[0].key;
    }

    while (path.depth > 0)
    {
        overlay_path_entry pe = path.e[--path.depth];
        overlay_node *par_n = overlay_get_mut(ov, pe.id);
        page *par = &par_n->node;

        uint32_t cidx = pe.cidx;
        uint32_t pos = (cidx == RIGHTMOST_IDX) ? par->num_keys : cidx;

        if (cidx == RIGHTMOST_IDX)
            par->pointer = carry_left;
        else
            par->internal[cidx].child = carry_left;

        if (!carry_split)
        {
            par_n->dirty = 1;
            carry_left = pe.id;
            continue;
        }

        if (par->num_keys < INTERNAL_ORDER - 1)
        {
            for (int64_t j = (int64_t)par->num_keys - 1; j >= (int64_t)pos; j--)
                par->internal[j + 1] = par->internal[j];

            par->internal[pos].key = (uint64_t)carry_key;
            par->internal[pos].child = carry_left;

            if (pos == par->num_keys)
                par->pointer = carry_right;
            else
                par->internal[pos + 1].child = carry_right;

            par->num_keys++;
            par_n->dirty = 1;

            carry_split = 0;
            carry_left = pe.id;
            continue;
        }

        int64_t tkeys[INTERNAL_ORDER];
        node_id_t tchld[INTERNAL_ORDER + 1];

        for (uint32_t j = 0; j < pos; j++)
            tkeys[j] = (int64_t)par->internal[j].key;
        tkeys[pos] = carry_key;
        for (uint32_t j = pos; j < INTERNAL_ORDER - 1; j++)
            tkeys[j + 1] = (int64_t)par->internal[j].key;

        for (uint32_t j = 0; j < pos; j++)
            tchld[j] = par->internal[j].child;
        tchld[pos] = carry_left;
        tchld[pos + 1] = carry_right;
        for (uint32_t j = pos + 1; j < INTERNAL_ORDER; j++)
            tchld[j + 1] = (j < INTERNAL_ORDER - 1) ? par->internal[j].child : par->pointer;

        uint32_t sp = (INTERNAL_ORDER + 1) / 2;
        int64_t up_key = tkeys[sp - 1];

        for (uint32_t j = 0; j < sp - 1; j++)
        {
            par->internal[j].key = (uint64_t)tkeys[j];
            par->internal[j].child = tchld[j];
        }
        par->pointer = tchld[sp - 1];
        par->num_keys = sp - 1;
        par_n->dirty = 1;

        node_id_t right_id = overlay_new_temp(ov, 0);
        overlay_node *right_n = overlay_get_mut(ov, right_id);
        for (uint32_t j = sp; j < INTERNAL_ORDER; j++)
        {
            right_n->node.internal[j - sp].key = (uint64_t)tkeys[j];
            right_n->node.internal[j - sp].child = tchld[j];
        }
        right_n->node.pointer = tchld[INTERNAL_ORDER];
        right_n->node.num_keys = INTERNAL_ORDER - sp;
        right_n->dirty = 1;

        carry_split = 1;
        carry_left = pe.id;
        carry_right = right_id;
        carry_key = up_key;
    }

    if (carry_split)
    {
        node_id_t new_root_id = overlay_new_temp(ov, 0);
        overlay_node *r = overlay_get_mut(ov, new_root_id);
        r->node.num_keys = 1;
        r->node.internal[0].key = (uint64_t)carry_key;
        r->node.internal[0].child = carry_left;
        r->node.pointer = carry_right;
        r->dirty = 1;
        *root_id = new_root_id;
    }
}

/* [BATCHED FLUSH IMPLEMENTATION - same as in cow_zfs.c] */

static pagenum_t resolve_node_pn(overlay_state *ov, node_id_t child_id)
{
    if (child_id == INVALID_PGN)
        return INVALID_PGN;
    int idx = overlay_find_idx(ov, child_id);
    if (idx >= 0)
        return ov->arr[idx].flushed_pn;
    return (pagenum_t)child_id;
}

static int collect_dirty_nodes(overlay_state *ov, node_id_t id,
                                batch_entry *entries, int *count, int max)
{
    if (id == INVALID_PGN)
        return 0;

    int idx = overlay_find_idx(ov, id);
    if (idx < 0)
        return 0;

    overlay_node *n = &ov->arr[idx];
    if (n->flushed)
        return (n->flushed_pn != (pagenum_t)n->id || is_temp_id(n->id));

    int child_changed = 0;

    if (!n->node.is_leaf)
    {
        for (uint32_t i = 0; i < n->node.num_keys; i++)
            child_changed |= collect_dirty_nodes(ov, n->node.internal[i].child,
                                                  entries, count, max);
        child_changed |= collect_dirty_nodes(ov, n->node.pointer, entries, count, max);
    }

    if (n->dirty || child_changed || is_temp_id(n->id))
    {
        if (*count >= max)
        {
            fprintf(stderr, "collect_dirty_nodes: exceeded MAX_BATCH_PAGES (%d)\n", max);
            exit(EXIT_FAILURE);
        }
        n->flushed = 1;
        n->flushed_pn = (pagenum_t)(*count);
        entries[*count].n = n;
        (*count)++;
        return 1;
    }
    else
    {
        n->flushed = 1;
        n->flushed_pn = (pagenum_t)n->id;
        return 0;
    }
}

static int zone_pwrite_n_pages(cow_tree *t, uint32_t zone_id, const void *buf,
                               int n_pages, uint64_t write_off_bytes,
                               pagenum_t *out_pn, uint64_t *out_wp_bytes)
{
    (void)zone_id;
    uint32_t total_bytes = (uint32_t)n_pages * PAGE_SIZE;

    int wfd = (t->direct_fd >= 0) ? t->direct_fd : t->fd;
    if (pwrite(wfd, buf, total_bytes, (off_t)write_off_bytes) != (ssize_t)total_bytes)
        return -1;

    if (out_wp_bytes)
        *out_wp_bytes = write_off_bytes + total_bytes;
    if (out_pn)
        *out_pn = (pagenum_t)(write_off_bytes / PAGE_SIZE);

    return 0;
}

static pagenum_t flush_overlay_batched(overlay_state *ov, node_id_t root_id)
{
    if (root_id == INVALID_PGN)
        return INVALID_PGN;

    cow_tree *t = ov->t;
    batch_entry entries[MAX_BATCH_PAGES];
    int count = 0;

    collect_dirty_nodes(ov, root_id, entries, &count, MAX_BATCH_PAGES);

    int root_idx = overlay_find_idx(ov, root_id);

    if (count == 0)
        return (root_idx >= 0) ? ov->arr[root_idx].flushed_pn : (pagenum_t)root_id;

    uint32_t zone_id;
    uint64_t base_wp;

    for (;;)
    {
        zone_id = reserve_writable_zone(t);

        base_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;
        uint64_t needed   = (uint64_t)count * PAGE_SIZE;

        if (base_wp + needed > zone_end)
        {
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
            finish_zone_if_full(t, zone_id);
            uint32_t expected = zone_id;
            uint32_t next = zone_next(t, zone_id);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }
        break;
    }

    pagenum_t base_pn = (pagenum_t)(base_wp / PAGE_SIZE);

    uint64_t total_bytes = (uint64_t)count * PAGE_SIZE;
    uint64_t old_wp = atomic_fetch_add_explicit(
        &t->zone_wp_bytes[zone_id], total_bytes, memory_order_acq_rel);

    uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;
    if (old_wp + total_bytes > zone_end)
    {
        fprintf(stderr, "batch flush: zone space check failed\n");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < count; i++)
        entries[i].n->flushed_pn = base_pn + (pagenum_t)i;

    void *buf;
    if (posix_memalign(&buf, PAGE_SIZE, (size_t)count * PAGE_SIZE) != 0)
    {
        perror("posix_memalign batch flush");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < count; i++)
    {
        overlay_node *n = entries[i].n;
        page out = n->node;
        out.pn = base_pn + (pagenum_t)i;

        if (!out.is_leaf)
        {
            for (uint32_t j = 0; j < out.num_keys; j++)
                out.internal[j].child = resolve_node_pn(ov, out.internal[j].child);
            out.pointer = resolve_node_pn(ov, out.pointer);
        }

        memcpy((char *)buf + (size_t)i * PAGE_SIZE, &out, sizeof(page));
    }

    uint64_t t0 = monotonic_ns();
    int done = 0;
    pagenum_t actual_base = 0;

    while (done < count)
    {
        int chunk = count - done;
        if (chunk > MAX_NVME_PAGES)
            chunk = MAX_NVME_PAGES;

        char    *chunk_buf = (char *)buf + (size_t)done * PAGE_SIZE;
        uint64_t wp_bytes  = 0;
        pagenum_t chunk_pn = 0;

        uint64_t chunk_off = old_wp + (uint64_t)done * PAGE_SIZE;
        if (zone_pwrite_n_pages(t, zone_id, chunk_buf, chunk, chunk_off, &chunk_pn, &wp_bytes) != 0)
        {
            perror("zone_pwrite_n_pages");
            exit(EXIT_FAILURE);
        }

        if (done == 0)
            actual_base = chunk_pn;

        uint64_t cur_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        while (wp_bytes > cur_wp &&
               !atomic_compare_exchange_weak_explicit(
                   &t->zone_wp_bytes[zone_id], &cur_wp, wp_bytes,
                   memory_order_acq_rel, memory_order_acquire))
        {
        }

        done += chunk;
    }
    uint64_t append_dt = monotonic_ns() - t0;

    if (actual_base != base_pn)
    {
        pagenum_t delta = actual_base - base_pn;
        for (int i = 0; i < count; i++)
        {
            entries[i].n->flushed_pn += delta;
            page *pg = (page *)((char *)buf + (size_t)i * PAGE_SIZE);
            pg->pn += delta;
        }
    }

    for (int i = 0; i < count; i++)
    {
        overlay_node *n  = entries[i].n;
        page         *pg = (page *)((char *)buf + (size_t)i * PAGE_SIZE);
        global_cache_insert(t, n->flushed_pn, pg);
    }

    atomic_fetch_add_explicit(&t->stat_append_ns_sum, append_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_append_ns_samples, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_page_appends, (uint64_t)count, memory_order_relaxed);

    {
        uint64_t cur_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        uint64_t zone_end_chk = t->zones[zone_id].start + t->zones[zone_id].capacity;
        if (cur_wp >= zone_end_chk)
        {
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
            finish_zone_if_full(t, zone_id);
            uint32_t expected = zone_id;
            uint32_t next = zone_next(t, zone_id);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
        }
    }

    free(buf);

    return (root_idx >= 0) ? ov->arr[root_idx].flushed_pn : (pagenum_t)root_id;
}

/* [Queue and TXG processing] */

static int pop_batch(cow_tree *t, insert_req **batch, int max_batch)
{
    int n = 0;
    uint64_t t_lock0 = monotonic_ns();
    pthread_mutex_lock(&t->q_lock);
    uint64_t t_lock1 = monotonic_ns();
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_ns_sync, t_lock1 - t_lock0, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_samples_sync, 1, memory_order_relaxed);

    while (!atomic_load_explicit(&t->stop_sync, memory_order_acquire) && t->q_head == NULL)
        pthread_cond_wait(&t->q_cv, &t->q_lock);

    if (atomic_load_explicit(&t->stop_sync, memory_order_acquire) && t->q_head == NULL)
    {
        pthread_mutex_unlock(&t->q_lock);
        return 0;
    }

    while (n < max_batch && t->q_head != NULL)
    {
        insert_req *req = t->q_head;
        t->q_head = req->next;
        if (t->q_head == NULL)
            t->q_tail = NULL;
        req->next = NULL;
        batch[n++] = req;
    }

    if (n < max_batch && !atomic_load_explicit(&t->stop_sync, memory_order_acquire))
    {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        uint64_t deadline_ns = (uint64_t)now.tv_nsec + 150000ULL;
        struct timespec deadline = {
            .tv_sec = now.tv_sec + (time_t)(deadline_ns / 1000000000ULL),
            .tv_nsec = (long)(deadline_ns % 1000000000ULL)};

        for (;;)
        {
            if (n >= max_batch || atomic_load_explicit(&t->stop_sync, memory_order_acquire))
                break;
            int rc = pthread_cond_timedwait(&t->q_cv, &t->q_lock, &deadline);
            if (rc == ETIMEDOUT)
                break;
            if (rc != 0)
                break;
            while (n < max_batch && t->q_head != NULL)
            {
                insert_req *req = t->q_head;
                t->q_head = req->next;
                if (t->q_head == NULL)
                    t->q_tail = NULL;
                req->next = NULL;
                batch[n++] = req;
            }
        }
    }

    pthread_mutex_unlock(&t->q_lock);
    return n;
}

static void complete_req(insert_req *req)
{
    pthread_mutex_lock(&req->done_lock);
    req->done = 1;
    pthread_cond_signal(&req->done_cv);
    pthread_mutex_unlock(&req->done_lock);
}

static int cmp_batch_item(const void *a, const void *b)
{
    const batch_item *x = (const batch_item *)a;
    const batch_item *y = (const batch_item *)b;

    if (x->req->key < y->req->key)
        return -1;
    if (x->req->key > y->req->key)
        return 1;

    if (x->ord < y->ord)
        return -1;
    if (x->ord > y->ord)
        return 1;
    return 0;
}

static void process_txg_jobs(cow_tree *t, txg_batch_job **jobs, int njobs, int free_jobs)
{
    batch_item merged_items[TXG_COMMIT_MERGE_ITEMS];
    int merged_n = 0;
    uint64_t start_ns = jobs[0]->start_ns;

    for (int j = 0; j < njobs; j++)
    {
        txg_batch_job *cur = jobs[j];
        if (cur->start_ns < start_ns)
            start_ns = cur->start_ns;

        for (int i = 0; i < cur->n; i++)
        {
            if (merged_n >= TXG_COMMIT_MERGE_ITEMS)
                break;
            merged_items[merged_n] = cur->items[i];
            merged_n++;
        }
    }

    if (merged_n <= 0)
    {
        if (free_jobs)
            for (int j = 0; j < njobs; j++)
                free(jobs[j]);
        return;
    }

    if (merged_n > 1)
        qsort(merged_items, (size_t)merged_n, sizeof(merged_items[0]), cmp_batch_item);

    pthread_mutex_lock(&t->flush_lock);

    pagenum_t root;
    for (;;)
    {
        uint64_t s1 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 & 1ULL)
            continue;
        root = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
        uint64_t s2 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 == s2 && ((s2 & 1ULL) == 0))
            break;
    }

    overlay_state ov;
    memset(&ov, 0, sizeof(ov));
    ov.t = t;

    node_id_t root_id = root;
    uint64_t apply_t0 = monotonic_ns();
    for (int i = 0; i < merged_n; i++)
        apply_insert_overlay(&ov, &root_id, merged_items[i].req->key, merged_items[i].req->value);
    uint64_t apply_dt = monotonic_ns() - apply_t0;
    atomic_fetch_add_explicit(&t->stat_apply_ns_sum, apply_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_apply_ns_samples, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_overlay_nodes_sum, (uint64_t)ov.len, memory_order_relaxed);
    stat_update_max_u64(&t->stat_overlay_nodes_max, (uint64_t)ov.len);
    atomic_fetch_add_explicit(&t->stat_batch_sz_sum, (uint64_t)merged_n, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_batch_sz_samples, 1, memory_order_relaxed);

    uint64_t flush_t0 = monotonic_ns();
    pagenum_t new_root = flush_overlay_batched(&ov, root_id);
    uint64_t flush_dt = monotonic_ns() - flush_t0;
    atomic_fetch_add_explicit(&t->stat_flush_ns_sum, flush_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_flush_ns_samples, 1, memory_order_relaxed);

    free(ov.arr);
    free(ov.idx_table);

    uint64_t txg = atomic_fetch_add_explicit(&t->txg_next, 1, memory_order_acq_rel) + 1;

    uint64_t pub_t0 = monotonic_ns();
    publish_root_txg(t, new_root, txg);
    atomic_store_explicit(&t->txg_synced, txg, memory_order_release);
    atomic_store_explicit(&t->dirty_sb, true, memory_order_release);
    uint64_t pub_dt = monotonic_ns() - pub_t0;
    atomic_fetch_add_explicit(&t->stat_publish_ns_sum, pub_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_publish_ns_samples, 1, memory_order_relaxed);

    uint64_t txg_dt = monotonic_ns() - start_ns;
    atomic_fetch_add_explicit(&t->stat_txg_total_ns_sum, txg_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_txg_total_ns_samples, 1, memory_order_relaxed);

    pthread_mutex_unlock(&t->flush_lock);

    for (int i = 0; i < merged_n; i++)
        complete_req(merged_items[i].req);

    if (free_jobs)
        for (int j = 0; j < njobs; j++)
            free(jobs[j]);
}

static void *txg_batch_main(void *arg)
{
    cow_tree *t = (cow_tree *)arg;
    insert_req *batch[TXG_BATCH_MAX];

    for (;;)
    {
        uint64_t txg_t0 = monotonic_ns();
        int n = pop_batch(t, batch, TXG_BATCH_MAX);
        if (n == 0)
            break;

        txg_batch_job inline_job;
        txg_batch_job *job = &inline_job;

        job->start_ns = txg_t0;
        job->n = n;
        job->next = NULL;

        atomic_fetch_add_explicit(&t->stat_batches, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_batch_items, (uint64_t)n, memory_order_relaxed);

        for (int i = 0; i < n; i++)
        {
            job->items[i].req = batch[i];
            job->items[i].ord = i;
        }

        uint64_t sort_t0 = monotonic_ns();
        if (n > 1)
            qsort(job->items, (size_t)n, sizeof(job->items[0]), cmp_batch_item);
        uint64_t sort_dt = monotonic_ns() - sort_t0;
        atomic_fetch_add_explicit(&t->stat_sort_ns_sum, sort_dt, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_sort_ns_samples, 1, memory_order_relaxed);

        txg_batch_job *single[1] = {job};
        process_txg_jobs(t, single, 1, 0);
    }

    atomic_store_explicit(&t->stop_commit, true, memory_order_release);
    pthread_mutex_lock(&t->stage2_lock);
    pthread_cond_broadcast(&t->stage2_cv);
    pthread_mutex_unlock(&t->stage2_lock);

    return NULL;
}

static insert_req *get_tls_insert_req(void)
{
    insert_req *req = calloc(1, sizeof(*req));
    if (!req)
    {
        perror("calloc tls insert_req");
        exit(EXIT_FAILURE);
    }

    if (pthread_mutex_init(&req->done_lock, NULL) != 0)
    {
        perror("pthread_mutex_init tls req.done_lock");
        free(req);
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_init(&req->done_cv, NULL) != 0)
    {
        perror("pthread_cond_init tls req.done_cv");
        pthread_mutex_destroy(&req->done_lock);
        free(req);
        exit(EXIT_FAILURE);
    }

    return req;
}

cow_tree *cow_open(const char *path)
{
    printf("CACHE_NUM_SETS: %d, CACHE_WAYS: %d \n", CACHE_NUM_SETS, CACHE_WAYS);

    cow_tree *t = calloc(1, sizeof(*t));
    if (!t)
    {
        perror("calloc");
        return NULL;
    }

    if (pthread_mutex_init(&t->flush_lock, NULL) != 0 ||
        pthread_mutex_init(&t->q_lock, NULL) != 0 ||
        pthread_cond_init(&t->q_cv, NULL) != 0 ||
        pthread_mutex_init(&t->stage2_lock, NULL) != 0 ||
        pthread_cond_init(&t->stage2_cv, NULL) != 0)
    {
        perror("mutex/cond init");
        free(t);
        return NULL;
    }

    t->fd = zbd_open(path, O_RDWR, &t->info);
    if (t->fd < 0)
    {
        perror("zbd_open");
        free(t);
        return NULL;
    }

    if (nvme_get_nsid(t->fd, &t->nsid) != 0)
    {
        perror("nvme_get_nsid");
        zbd_close(t->fd);
        free(t);
        return NULL;
    }

    t->direct_fd = open(path, O_RDWR | O_DIRECT);

    t->zones = calloc(t->info.nr_zones, sizeof *t->zones);
    t->zone_wp_bytes = calloc(t->info.nr_zones, sizeof(*t->zone_wp_bytes));
    t->zone_full = calloc(t->info.nr_zones, sizeof(*t->zone_full));
    if (!t->zones || !t->zone_wp_bytes || !t->zone_full)
    {
        perror("calloc zones");
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        zbd_close(t->fd);
        free(t);
        return NULL;
    }

    unsigned int nr = t->info.nr_zones;
    if (zbd_report_zones(t->fd, 0, 0, ZBD_RO_ALL, t->zones, &nr) != 0)
    {
        perror("zbd_report_zones");
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        zbd_close(t->fd);
        free(t);
        return NULL;
    }

    for (uint32_t z = 0; z < t->info.nr_zones; z++)
    {
        atomic_store_explicit(&t->zone_wp_bytes[z], t->zones[z].wp, memory_order_relaxed);
        atomic_store_explicit(
            &t->zone_full[z],
            (t->zones[z].cond == ZBD_ZONE_COND_FULL) ? 1 : 0,
            memory_order_relaxed);
    }

    global_cache_init(t);
    load_superblock(t);

    uint32_t initial_zone = DATA_ZONE_START;
    for (uint32_t z = DATA_ZONE_START; z < t->info.nr_zones; z++)
    {
        if (!atomic_load_explicit(&t->zone_full[z], memory_order_acquire))
        {
            initial_zone = z;
            break;
        }
    }

    atomic_store_explicit(&t->current_zone, initial_zone, memory_order_release);
    atomic_store_explicit(&t->dirty_sb, false, memory_order_release);
    atomic_store_explicit(&t->stop_sync, false, memory_order_release);
    atomic_store_explicit(&t->stop_commit, false, memory_order_release);
    atomic_store_explicit(&t->stop_flusher, false, memory_order_release);

    if (pthread_create(&t->sync_tid, NULL, txg_batch_main, t) != 0)
    {
        perror("pthread_create txg_batch_main");
        global_cache_destroy(t);
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        zbd_close(t->fd);
        free(t);
        return NULL;
    }

    if (pthread_create(&t->flusher_tid, NULL, sb_flusher_thread, t) != 0)
    {
        perror("pthread_create flusher");
        atomic_store_explicit(&t->stop_sync, true, memory_order_release);
        pthread_join(t->sync_tid, NULL);
        global_cache_destroy(t);
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        zbd_close(t->fd);
        free(t);
        return NULL;
    }

    return t;
}

void cow_close(cow_tree *t)
{
    if (!t)
        return;

    atomic_store_explicit(&t->stop_sync, true, memory_order_release);
    pthread_mutex_lock(&t->q_lock);
    pthread_cond_broadcast(&t->q_cv);
    pthread_mutex_unlock(&t->q_lock);
    pthread_join(t->sync_tid, NULL);

    atomic_store_explicit(&t->stop_flusher, true, memory_order_release);
    pthread_join(t->flusher_tid, NULL);

    uint64_t cache_hit = atomic_load_explicit(&t->stat_cache_global_hit, memory_order_relaxed);
    uint64_t cache_miss = atomic_load_explicit(&t->stat_cache_miss, memory_order_relaxed);
    uint64_t appends = atomic_load_explicit(&t->stat_page_appends, memory_order_relaxed);

    uint64_t batch_sz_sum = atomic_load_explicit(&t->stat_batch_sz_sum, memory_order_relaxed);
    uint64_t batch_sz_samples = atomic_load_explicit(&t->stat_batch_sz_samples, memory_order_relaxed);
    uint64_t overlay_nodes_sum = atomic_load_explicit(&t->stat_overlay_nodes_sum, memory_order_relaxed);
    uint64_t overlay_nodes_max = atomic_load_explicit(&t->stat_overlay_nodes_max, memory_order_relaxed);
    uint64_t flush_ns_sum = atomic_load_explicit(&t->stat_flush_ns_sum, memory_order_relaxed);
    uint64_t flush_ns_samples = atomic_load_explicit(&t->stat_flush_ns_samples, memory_order_relaxed);

    fprintf(stderr,
            "\n[cow_gtx_cache_p profile]\n"
            " cache_global_hit=%llu cache_miss=%llu hit_rate=%.1f%%\n"
            " page_appends=%llu\n"
            " avg_batch_sz=%.1f\n"
            " avg_dirty_nodes=%.1f max_dirty_nodes=%llu\n"
            " avg_flush_time_us=%.1f\n",
            (unsigned long long)cache_hit,
            (unsigned long long)cache_miss,
            (cache_hit + cache_miss > 0) ? 100.0 * cache_hit / (cache_hit + cache_miss) : 0.0,
            (unsigned long long)appends,
            batch_sz_samples > 0 ? (double)batch_sz_sum / batch_sz_samples : 0.0,
            flush_ns_samples > 0 ? (double)overlay_nodes_sum / flush_ns_samples : 0.0,
            (unsigned long long)overlay_nodes_max,
            flush_ns_samples > 0 ? (double)flush_ns_sum / flush_ns_samples / 1000.0 : 0.0);

    global_cache_destroy(t);
    free(t->zones);
    free(t->zone_wp_bytes);
    free(t->zone_full);
    zbd_close(t->fd);
    if (t->direct_fd >= 0)
        close(t->direct_fd);

    pthread_cond_destroy(&t->q_cv);
    pthread_mutex_destroy(&t->stage2_lock);
    pthread_cond_destroy(&t->stage2_cv);
    pthread_mutex_destroy(&t->q_lock);
    pthread_mutex_destroy(&t->flush_lock);

    free(t);
}

record *cow_find(cow_tree *t, int64_t key)
{
    pagenum_t root;
    for (;;)
    {
        uint64_t s1 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 & 1ULL)
            continue;
        root = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
        uint64_t s2 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 == s2 && ((s2 & 1ULL) == 0))
            break;
    }

    if (root == INVALID_PGN)
        return NULL;

    page p;
    load_page(t, root, &p);

    while (!p.is_leaf)
    {
        pagenum_t child = p.pointer;
        for (uint32_t i = 0; i < p.num_keys; i++)
        {
            if (key < (int64_t)p.internal[i].key)
            {
                child = p.internal[i].child;
                break;
            }
        }
        load_page(t, child, &p);
    }

    for (uint32_t i = 0; i < p.num_keys; i++)
    {
        if ((int64_t)p.leaf[i].key == key)
        {
            record *r = malloc(sizeof *r);
            if (!r)
                return NULL;
            *r = p.leaf[i].record;
            return r;
        }
    }

    return NULL;
}

void cow_insert(cow_tree *t, int64_t key, const char *value)
{
    insert_req *req = get_tls_insert_req();
    req->key = key;
    memcpy(req->value, value, 120);
    req->done = 0;
    req->next = NULL;

    uint64_t qwait_start = monotonic_ns();
    pthread_mutex_lock(&t->q_lock);
    uint64_t qwait_end = monotonic_ns();
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_ns_insert, qwait_end - qwait_start, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_samples_insert, 1, memory_order_relaxed);

    if (t->q_tail)
        t->q_tail->next = req;
    else
        t->q_head = req;
    t->q_tail = req;

    pthread_cond_signal(&t->q_cv);
    pthread_mutex_unlock(&t->q_lock);

    uint64_t client_wait_start = monotonic_ns();
    pthread_mutex_lock(&req->done_lock);
    while (!req->done)
        pthread_cond_wait(&req->done_cv, &req->done_lock);
    pthread_mutex_unlock(&req->done_lock);
    uint64_t client_wait_end = monotonic_ns();

    atomic_fetch_add_explicit(&t->stat_client_wait_ns, client_wait_end - client_wait_start, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_client_wait_samples, 1, memory_order_relaxed);

    atomic_fetch_add_explicit(&t->stat_inserts, 1, memory_order_relaxed);

    pthread_mutex_destroy(&req->done_lock);
    pthread_cond_destroy(&req->done_cv);
    free(req);
}
